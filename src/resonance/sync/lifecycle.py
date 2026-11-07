"""Type-agnostic task lifecycle helpers."""

from __future__ import annotations

import asyncio
import dataclasses
import datetime
import enum
import functools
import uuid
from typing import TYPE_CHECKING, Any

import arq.jobs as arq_jobs
import sqlalchemy as sa
import sqlalchemy.orm as orm
import structlog

import resonance.models.task as task_models
import resonance.sync.dispatch as dispatch_module
import resonance.types as types_module

if TYPE_CHECKING:
    import collections.abc

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


def _check_job_args(
    task: task_models.Task, job_name: str, args: tuple[object, ...]
) -> None:
    """Fail at enqueue, not in the worker, when the job can't find its task."""
    if task.id is None or any(arg is None or arg == "None" for arg in args):
        msg = f"{job_name}: None job arg for task {task.id}: {args!r}"
        raise ValueError(msg)
    if str(task.id) not in args:
        msg = f"{job_name}: job args {args!r} don't include task id {task.id}"
        raise ValueError(msg)


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
    _check_job_args(task, job_name, args)
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
    job_id: str | None = None,
) -> Delivery:
    """Stamp a delivery on *task*, commit the session, then enqueue the job."""
    delivery = stamp_delivery(task, job_name, *args, defer_by=defer_by, job_id=job_id)
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
    """True (and logged) when the row is already terminal.

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


# ---------------------------------------------------------------------------
# Claims: one delivery at a time owns a task row
# ---------------------------------------------------------------------------

_CLAIMABLE = (types_module.SyncStatus.PENDING, types_module.SyncStatus.DEFERRED)
# A lost race re-reads the row and tries again; more than a couple of rounds
# means it keeps changing under us, and the reaper will redeliver if needed.
_CLAIM_ROUNDS = 3


@dataclasses.dataclass(frozen=True)
class Claim:
    """A won claim: the attempt this delivery owns and the status it took over."""

    attempt: int
    previous_status: types_module.SyncStatus


def _claimable(task: task_models.Task, job_id: str | None) -> bool:
    if task.status in _CLAIMABLE:
        return True
    # A RUNNING row is only taken over by a delivery with the same job id: arq's
    # retry of the job that claimed it. arq's in-progress key normally keeps
    # that id from running twice at once, but heartbeat.py rewrites it with a
    # 60s TTL, so a loop stalled longer lets another worker start the same id.
    # The claim then fences the stale run's DB writes; its external side
    # effects can still happen twice. Legacy RUNNING rows with no arq_job_id
    # never match and wait for startup recovery (worker._recover_running).
    return (
        task.status == types_module.SyncStatus.RUNNING
        and job_id is not None
        and task.arq_job_id == job_id
    )


def _remember_claim(task_id: uuid.UUID, attempt: int) -> None:
    claims = task_models.claimed_attempts.get()
    if claims is None:
        claims = {}
        task_models.claimed_attempts.set(claims)
    claims[task_id] = attempt


async def claim(
    session: AsyncSession,
    task: task_models.Task,
    job_id: str | None,
) -> Claim | None:
    """Move *task* to RUNNING for this delivery, or return None if it can't.

    One conditional UPDATE on the (status, attempt) this delivery read, so of
    two concurrent claimers exactly one matches. The winner bumps ``attempt``,
    which fences every earlier claimer's writes, and records its job id so a
    stray delivery can't take over a live RUNNING row. Commits on success.
    """
    for _ in range(_CLAIM_ROUNDS):
        if not _claimable(task, job_id):
            return None
        seen_status, seen_attempt = task.status, task.attempt
        now = datetime.datetime.now(datetime.UTC)
        row = (
            await session.execute(
                sa.update(task_models.Task)
                .where(
                    task_models.Task.id == task.id,
                    task_models.Task.status == seen_status,
                    task_models.Task.attempt == seen_attempt,
                )
                .values(
                    status=types_module.SyncStatus.RUNNING,
                    attempt=task_models.Task.attempt + 1,
                    started_at=now,
                    arq_job_id=sa.func.coalesce(job_id, task_models.Task.arq_job_id),
                )
                .returning(task_models.Task.attempt, task_models.Task.arq_job_id)
                .execution_options(synchronize_session=False)
            )
        ).one_or_none()
        if row is None:
            await session.rollback()
            await session.refresh(task)
            continue
        await session.commit()
        for key, value in (
            ("status", types_module.SyncStatus.RUNNING),
            ("attempt", row.attempt),
            ("started_at", now),
            ("arq_job_id", row.arq_job_id),
        ):
            orm.attributes.set_committed_value(task, key, value)
        _remember_claim(task.id, row.attempt)
        return Claim(attempt=row.attempt, previous_status=seen_status)
    return None


async def claim_delivery(
    session: AsyncSession,
    task: task_models.Task,
    job: str,
    job_id: str | None,
) -> Claim | None:
    """Entry guard for every task job: claim the row or say why not (logged).

    A terminal row is a duplicate delivery and touches nothing. An active row
    another delivery owns, or one that changed while claiming, loses.
    """
    if is_duplicate_delivery(task, job):
        return None
    won = await claim(session, task, job_id)
    if won is None:
        logger.info(
            "task_claim_lost",
            job=job,
            task_id=str(task.id),
            status=task.status.value,
            attempt=task.attempt,
            job_id=job_id,
            owner_job_id=task.arq_job_id,
        )
    return won


async def release_for_redelivery(
    session: AsyncSession,
    task: task_models.Task,
) -> bool:
    """Reset a recovered row to PENDING and fence whoever held it.

    Recovery (reaper, startup) calls this before stamping a new delivery. The
    bump means a stalled-but-alive holder can't write past this point, even
    before the new delivery claims. Conditional on the attempt read, so a claim
    that lands in between wins; the caller commits.

    Returns:
        False when the row changed since it was read; skip redelivery.
    """
    row = (
        await session.execute(
            sa.update(task_models.Task)
            .where(
                task_models.Task.id == task.id,
                task_models.Task.status == task.status,
                task_models.Task.attempt == task.attempt,
            )
            .values(
                status=types_module.SyncStatus.PENDING,
                attempt=task_models.Task.attempt + 1,
            )
            .returning(task_models.Task.attempt)
            .execution_options(synchronize_session=False)
        )
    ).scalar_one_or_none()
    if row is None:
        return False
    orm.attributes.set_committed_value(task, "status", types_module.SyncStatus.PENDING)
    orm.attributes.set_committed_value(task, "attempt", row)
    return True


def task_job[**P](
    fn: collections.abc.Callable[P, collections.abc.Awaitable[None]],
) -> collections.abc.Callable[P, collections.abc.Coroutine[Any, Any, None]]:
    """Wrap an arq task job: scope its claims, and stop it once fenced.

    A fenced write means a newer delivery owns the row; this one exits without
    touching it again (the session rolls back as the exception unwinds).
    """

    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> None:
        token = task_models.claimed_attempts.set({})
        try:
            await fn(*args, **kwargs)
        except task_models.TaskFenced as fenced:
            logger.warning(
                "task_write_fenced",
                job=fn.__name__,
                task_id=str(fenced.task_id),
                attempt=fenced.mine,
                current_attempt=fenced.current,
            )
        finally:
            task_models.claimed_attempts.reset(token)

    return wrapper


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
    await _fence_out_holders(session, [child for child, _ in cancelled])
    if cancelled:
        logger.info(
            "active_children_cancelled",
            parent_id=str(parent_id),
            count=len(cancelled),
        )
    return cancelled


async def _fence_out_holders(
    session: AsyncSession, tasks: list[task_models.Task]
) -> None:
    """Bump ``attempt`` on rows cancelled from outside their job.

    A run whose abort missed would otherwise still pass the fence, and its
    DEFERRED or PENDING write (both active, so ``_keep_cancelled`` lets them
    through) would revive the task. Not part of ``cancel_task``, which a
    claimer also calls on its own row.
    """
    if not tasks:
        return
    bumped = (
        await session.execute(
            sa.update(task_models.Task)
            .where(task_models.Task.id.in_([t.id for t in tasks]))
            .values(attempt=task_models.Task.attempt + 1)
            .returning(task_models.Task.id, task_models.Task.attempt)
            .execution_options(synchronize_session=False)
        )
    ).tuples()
    by_id = {t.id: t for t in tasks}
    for task_id, attempt in bumped:
        orm.attributes.set_committed_value(by_id[task_id], "attempt", attempt)


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
    await _fence_out_holders(session, [task])
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
    if was_running and task.parent_id is not None:
        await _redeliver_to_advance_parent(session, arq_redis, task)
    return CancelResult(
        CancelOutcome.CANCELLED, task=task, children_cancelled=len(children)
    )


async def _redeliver_to_advance_parent(
    session: AsyncSession,
    arq_redis: arq.ArqRedis | None,
    task: task_models.Task,
) -> None:
    """Hand an aborted child back to arq so its pipeline doesn't stall.

    An aborted job exits with CancelledError and never reaches its sibling
    advance. A fresh delivery hits the entry guard, which sees CANCELLED and
    advances the next sibling or settles the parent.
    """
    job = dispatch_module.job_for(task)
    if job is None:
        return
    await enqueue_task(session, arq_redis, task, job[0], *job[1])
    logger.info("cancelled_child_redelivered_to_advance", task_id=str(task.id))


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
