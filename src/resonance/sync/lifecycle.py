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


async def is_cancelled(
    session: AsyncSession,
    task: task_models.Task,
) -> bool:
    """Check whether this task or any ancestor has been cancelled.

    Cancellation is represented as FAILED with an error_message containing
    "Cancelled".  Walks up the parent chain so nested children (grandchildren,
    etc.) also detect cancellation.

    Args:
        session: Active database session.
        task: The task to check.

    Returns:
        True if this task or any ancestor was cancelled.
    """
    # Check the task itself
    if _is_cancel_status(task.status, task.error_message):
        return True

    # Walk up the parent chain
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

        if _is_cancel_status(parent.status, parent.error_message):
            logger.info(
                "cancellation_detected",
                task_id=str(task.id),
                cancelled_ancestor_id=str(current_parent_id),
            )
            return True

        current_parent_id = parent.parent_id

    return False


def _is_cancel_status(
    status: types_module.SyncStatus, error_message: str | None
) -> bool:
    """Check whether a status + error_message represents cancellation."""
    return (
        status == types_module.SyncStatus.FAILED
        and error_message is not None
        and "Cancelled" in error_message
    )


async def cancel_pending_children(
    session: AsyncSession,
    parent_id: object,
) -> int:
    """Fail all PENDING children of a cancelled parent.

    When a parent task is cancelled, any children still in PENDING status
    should be failed immediately so they are never picked up by the worker.

    Args:
        session: Active database session.
        parent_id: The parent task ID whose children should be cancelled.

    Returns:
        Number of children cancelled.
    """
    now = datetime.datetime.now(datetime.UTC)
    # Find PENDING children first, then update them
    pending_result = await session.execute(
        sa.select(task_models.Task).where(
            task_models.Task.parent_id == parent_id,
            task_models.Task.status == types_module.SyncStatus.PENDING,
        )
    )
    pending_children = pending_result.scalars().all()
    for child in pending_children:
        child.status = types_module.SyncStatus.FAILED
        child.error_message = "Parent task was cancelled"
        child.completed_at = now
    count = len(pending_children)
    if count > 0:
        logger.info(
            "pending_children_cancelled",
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
