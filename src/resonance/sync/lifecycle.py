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
    """Mark a task completed and propagate to parent if applicable."""
    task.status = types_module.SyncStatus.COMPLETED
    task.result = result
    task.completed_at = datetime.datetime.now(datetime.UTC)

    if task.parent_id is not None:
        await _check_parent_completion(session, task.parent_id)


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
