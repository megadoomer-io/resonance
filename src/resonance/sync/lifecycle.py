"""Type-agnostic task lifecycle helpers."""

from __future__ import annotations

import dataclasses
import datetime
import enum
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.models.task as task_models
import resonance.types as types_module

if TYPE_CHECKING:
    import uuid

    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


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
) -> int:
    """Cancel every non-terminal child of a cancelled parent.

    PENDING and DEFERRED children are never picked up afterwards. A RUNNING
    child is marked too, but its in-flight run still overwrites the status
    when it finishes (no hard abort yet).

    Args:
        session: Active database session.
        parent_id: The parent task ID whose children should be cancelled.

    Returns:
        Number of children cancelled.
    """
    active_result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.parent_id == parent_id,
            task_models.Task.status.in_(types_module.ACTIVE_STATUSES),
        )
    )
    active_children = active_result.scalars().all()
    for child in active_children:
        await cancel_task(session, child, "Parent task was cancelled")
    count = len(active_children)
    if count > 0:
        logger.info(
            "active_children_cancelled",
            parent_id=str(parent_id),
            count=count,
        )
    return count


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
) -> CancelResult:
    """Cancel a user's task and its non-terminal children, then commit.

    Shared by the API and UI cancel routes. Only the owning user's tasks are
    visible; another user's task reads as not found.

    Args:
        session: Active database session.
        task_id: The task to cancel.
        user_id: The requesting user (ownership scope).
        reason: Stored in ``error_message`` on the cancelled task.

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

    await cancel_task(session, task, reason)
    children_cancelled = await cancel_active_children(session, task.id)
    await session.commit()
    logger.info(
        "task_cancelled",
        task_id=str(task.id),
        children_cancelled=children_cancelled,
    )
    return CancelResult(
        CancelOutcome.CANCELLED, task=task, children_cancelled=children_cancelled
    )


async def _check_parent_completion(
    session: AsyncSession,
    parent_id: object,
) -> None:
    """Check if all children of a parent are done; if so, complete the parent."""
    parent_result = await session.execute(
        sa.select(task_models.Task).where(task_models.Task.id == parent_id)
    )
    parent = parent_result.scalar_one_or_none()
    if parent is None:
        return

    children_result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.parent_id == parent_id,
        )
    )
    children = children_result.scalars().all()

    completed = sum(
        1
        for c in children
        if c.status
        in (types_module.SyncStatus.COMPLETED, types_module.SyncStatus.FAILED)
    )

    if completed == len(children):
        failed = sum(1 for c in children if c.status == types_module.SyncStatus.FAILED)
        children_completed = completed - failed

        parent.status = types_module.SyncStatus.COMPLETED
        parent.completed_at = datetime.datetime.now(datetime.UTC)
        parent.result = {
            "children_completed": children_completed,
            "children_failed": failed,
        }
