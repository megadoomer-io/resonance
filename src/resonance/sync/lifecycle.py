"""Type-agnostic task lifecycle helpers."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import sqlalchemy as sa

import resonance.models.task as task_models
import resonance.types as types_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


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
