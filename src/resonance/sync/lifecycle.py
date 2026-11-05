"""Type-agnostic task lifecycle helpers."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import enum
import uuid
from typing import TYPE_CHECKING

import arq.jobs as arq_jobs
import sqlalchemy as sa
import structlog

import resonance.models.task as task_models
import resonance.types as types_module

if TYPE_CHECKING:
    import arq
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


# ---------------------------------------------------------------------------
# Delivery: handing a task row to arq
# ---------------------------------------------------------------------------


def new_job_id(job_name: str, task_id: uuid.UUID) -> str:
    """A fresh arq job id for one delivery of a task.

    arq silently drops ``enqueue_job`` while a job with the same id (or its
    result key) exists, so every delivery gets its own id.
    """
    return f"{job_name}:{task_id}:{uuid.uuid4().hex}"


@dataclasses.dataclass(frozen=True)
class Delivery:
    """One arq job for a task, stamped on the row and enqueued after commit."""

    task_id: uuid.UUID
    job_name: str
    args: tuple[object, ...]
    job_id: str
    defer_by: datetime.timedelta | None = None


def stamp_delivery(
    task: task_models.Task,
    job_name: str,
    *args: object,
    defer_by: datetime.timedelta | None = None,
    job_id: str | None = None,
) -> Delivery:
    """Record a delivery on the row; the caller commits, then calls ``deliver``.

    Stamping in the same transaction that inserts or advances the row means a
    crash between commit and enqueue leaves a row whose job arq doesn't know,
    which the reaper re-delivers.
    """
    if task.id is None:
        # The column default only fires at flush; the job id needs it now.
        task.id = uuid.uuid4()
    delivery = Delivery(
        task_id=task.id,
        job_name=job_name,
        args=args,
        job_id=job_id or new_job_id(job_name, task.id),
        defer_by=defer_by,
    )
    task.enqueued_at = datetime.datetime.now(datetime.UTC)
    task.arq_job_id = delivery.job_id
    return delivery


async def deliver(arq_redis: arq.ArqRedis | None, delivery: Delivery) -> bool:
    """Enqueue a stamped delivery. Never raises: the reaper covers a failure."""
    log = logger.bind(
        task_id=str(delivery.task_id),
        job=delivery.job_name,
        job_id=delivery.job_id,
    )
    if arq_redis is None:
        log.warning("task_enqueue_skipped_no_redis")
        return False
    try:
        job = await arq_redis.enqueue_job(
            delivery.job_name,
            *delivery.args,
            _job_id=delivery.job_id,
            _defer_by=delivery.defer_by,
        )
    except Exception:
        log.exception("task_enqueue_failed")
        return False
    if job is None:
        log.warning("task_enqueue_dropped")
        return False
    return True


async def enqueue_task(
    session: AsyncSession,
    arq_redis: arq.ArqRedis | None,
    task: task_models.Task,
    job_name: str,
    *args: object,
    defer_by: datetime.timedelta | None = None,
) -> Delivery:
    """Stamp a delivery on *task*, commit the session, then enqueue the job."""
    delivery = stamp_delivery(task, job_name, *args, defer_by=defer_by)
    await session.commit()
    await deliver(arq_redis, delivery)
    return delivery


async def complete_task(
    session: AsyncSession,
    task: task_models.Task,
    result: dict[str, object],
) -> None:
    """Mark a task completed.

    Sets status to COMPLETED, records the result dict, and timestamps
    the completion.  Parent-completion cascading is handled by the
    caller (e.g. ``worker._check_parent_completion``) so that
    worker-specific concerns (sequential sibling enqueue, post-sync
    dedup, step-mode) stay in one place.
    """
    task.status = types_module.SyncStatus.COMPLETED
    task.result = result
    task.completed_at = datetime.datetime.now(datetime.UTC)


async def fail_task(
    session: AsyncSession,
    task: task_models.Task,
    error_message: str,
) -> None:
    """Mark a task as failed."""
    task.status = types_module.SyncStatus.FAILED
    task.error_message = error_message
    task.completed_at = datetime.datetime.now(datetime.UTC)


async def cancel_task(
    session: AsyncSession,
    task: task_models.Task,
    reason: str,
) -> None:
    """Mark a task cancelled, recording why in ``error_message``."""
    task.status = types_module.SyncStatus.CANCELLED
    task.error_message = reason
    task.completed_at = datetime.datetime.now(datetime.UTC)


def is_terminal(task: task_models.Task) -> bool:
    """Whether the task's row is in a terminal status (will never run again)."""
    return task.status in types_module.TERMINAL_STATUSES


def is_duplicate_delivery(task: task_models.Task, job: str) -> bool:
    """Entry guard for arq jobs: True (and logged) when the row is already terminal.

    arq can deliver a job more than once (SIGTERM retry, re-enqueue). A delivery
    whose task already finished must not run the work again.
    """
    if not is_terminal(task):
        return False
    logger.info(
        "duplicate_delivery",
        job=job,
        task_id=str(task.id),
        status=task.status.value,
    )
    return True


async def is_cancelled(
    session: AsyncSession,
    task: task_models.Task,
) -> bool:
    """Check whether this task or any ancestor has been cancelled.

    Walks up the parent chain so nested children (grandchildren, etc.) also
    detect cancellation.

    Args:
        session: Active database session.
        task: The task to check.

    Returns:
        True if this task or any ancestor was cancelled.
    """
    if task.status == types_module.SyncStatus.CANCELLED:
        return True

    current_parent_id = task.parent_id
    visited: set[object] = set()

    while current_parent_id is not None and current_parent_id not in visited:
        visited.add(current_parent_id)
        parent_result = await session.execute(
            sa.select(task_models.Task).where(task_models.Task.id == current_parent_id)
        )
        parent = parent_result.scalar_one_or_none()
        if parent is None:
            break

        if parent.status == types_module.SyncStatus.CANCELLED:
            logger.info(
                "cancellation_detected",
                task_id=str(task.id),
                cancelled_ancestor_id=str(current_parent_id),
            )
            return True

        current_parent_id = parent.parent_id

    return False


async def cancel_active_children(
    session: AsyncSession,
    parent_id: object,
) -> list[tuple[task_models.Task, types_module.SyncStatus]]:
    """Cancel every non-terminal child of a cancelled parent.

    PENDING and DEFERRED children are never picked up afterwards. A RUNNING
    child is marked too; ``request_cancel`` aborts its arq job.

    Args:
        session: Active database session.
        parent_id: The parent task ID whose children should be cancelled.

    Returns:
        Each cancelled child with the status it had before.
    """
    active_result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.parent_id == parent_id,
            task_models.Task.status.in_(types_module.ACTIVE_STATUSES),
        )
    )
    cancelled = [(child, child.status) for child in active_result.scalars().all()]
    for child, _ in cancelled:
        await cancel_task(session, child, "Parent task was cancelled")
    if cancelled:
        logger.info(
            "active_children_cancelled",
            parent_id=str(parent_id),
            count=len(cancelled),
        )
    return cancelled


class CancelOutcome(enum.StrEnum):
    """Result of a cancel request."""

    CANCELLED = "cancelled"
    ALREADY_TERMINAL = "already_terminal"
    NOT_FOUND = "not_found"


@dataclasses.dataclass(frozen=True)
class CancelResult:
    """Outcome of ``request_cancel`` plus the task (when found)."""

    outcome: CancelOutcome
    task: task_models.Task | None = None
    children_cancelled: int = 0


async def request_cancel(
    session: AsyncSession,
    task_id: uuid.UUID,
    user_id: uuid.UUID,
    reason: str = "Cancelled by user",
    arq_redis: arq.ArqRedis | None = None,
) -> CancelResult:
    """Cancel a user's task and its non-terminal children, then commit.

    Shared by the API and UI cancel routes. Only the owning user's tasks are
    visible; another user's task reads as not found. After the commit, the
    arq jobs of anything that was RUNNING are aborted (bounded wait).

    Args:
        session: Active database session.
        task_id: The task to cancel.
        user_id: The requesting user (ownership scope).
        reason: Stored in ``error_message`` on the cancelled task.
        arq_redis: arq pool used to abort running jobs; ``None`` skips aborts.

    Returns:
        A ``CancelResult``: CANCELLED, ALREADY_TERMINAL (task untouched), or
        NOT_FOUND.
    """
    result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.id == task_id,
            task_models.Task.user_id == user_id,
        )
    )
    task = result.scalar_one_or_none()
    if task is None:
        return CancelResult(CancelOutcome.NOT_FOUND)
    if is_terminal(task):
        return CancelResult(CancelOutcome.ALREADY_TERMINAL, task=task)

    was_running = task.status == types_module.SyncStatus.RUNNING
    await cancel_task(session, task, reason)
    children = await cancel_active_children(session, task.id)
    await session.commit()
    logger.info(
        "task_cancelled",
        task_id=str(task.id),
        children_cancelled=len(children),
    )

    running = [task] if was_running else []
    running += [c for c, prior in children if prior == types_module.SyncStatus.RUNNING]
    await asyncio.gather(
        *(
            abort_job(arq_redis, row.id, row.arq_job_id)
            for row in running
            if row.arq_job_id is not None
        )
    )
    return CancelResult(
        CancelOutcome.CANCELLED, task=task, children_cancelled=len(children)
    )


# arq's abort waits for the job's result unless bounded; a cancel request must
# return promptly. The row is already CANCELLED, so a slow abort only delays
# when the job notices.
ABORT_TIMEOUT_SECONDS = 2.0
_ABORT_POLL_SECONDS = 0.25


async def abort_job(
    arq_redis: arq.ArqRedis | None, task_id: uuid.UUID, job_id: str
) -> bool:
    """Ask arq to abort a running job; never raises.

    Returns:
        True if arq confirmed the abort within the timeout.
    """
    log = logger.bind(task_id=str(task_id), job_id=job_id)
    if arq_redis is None:
        log.warning("task_abort_skipped_no_redis")
        return False
    job = arq_jobs.Job(job_id, arq_redis, _queue_name=arq_redis.default_queue_name)
    try:
        aborted = await asyncio.wait_for(
            job.abort(timeout=ABORT_TIMEOUT_SECONDS, poll_delay=_ABORT_POLL_SECONDS),
            timeout=ABORT_TIMEOUT_SECONDS + 1,
        )
    except TimeoutError:
        log.info("task_abort_pending")
        return False
    except Exception:
        log.exception("task_abort_failed")
        return False
    log.info("task_abort_requested", confirmed=aborted)
    return aborted
