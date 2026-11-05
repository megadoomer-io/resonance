"""Type-agnostic task lifecycle helpers."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.models.task as task_models
import resonance.types as types_module

if TYPE_CHECKING:
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
