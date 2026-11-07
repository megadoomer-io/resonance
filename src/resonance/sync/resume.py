"""Resume an interrupted TIME_RANGE task from its connection's sync watermark."""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.models.user as user_models
import resonance.types as types_module

if TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

    import resonance.models.task as task_models

logger = structlog.get_logger()


def apply_watermark_resume(
    task: task_models.Task,
    connection: user_models.ServiceConnection,
) -> None:
    """Inject the watermark position into task params.

    For ListenBrainz TIME_RANGE tasks, sets ``max_ts`` from the connection's
    ``oldest_synced_at`` so the next run resumes where the previous one left
    off instead of re-processing every page.

    Args:
        task: The interrupted task.
        connection: The task's ServiceConnection with its current watermark.
    """
    if connection.service_type != types_module.ServiceType.LISTENBRAINZ:
        return

    listens_watermark = connection.sync_watermark.get("listens", {})
    oldest_synced_at = listens_watermark.get("oldest_synced_at")
    if oldest_synced_at is not None:
        items_so_far = int(task.progress_current or 0)
        task.params = {
            **task.params,
            "max_ts": int(str(oldest_synced_at)),
            "items_so_far": items_so_far,
        }
        logger.info(
            "watermark_resume_applied",
            task_id=str(task.id),
            max_ts=oldest_synced_at,
            items_so_far=items_so_far,
        )


async def resume_recovered(
    session: sa_async.AsyncSession, task: task_models.Task
) -> None:
    """Apply the watermark resume to a recovered RUNNING row before redelivery.

    Shared by startup recovery and the reaper's expired-lease recovery.
    """
    if task.task_type != types_module.TaskType.TIME_RANGE:
        return
    connection = (
        await session.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id == task.service_connection_id
            )
        )
    ).scalar_one_or_none()
    if connection is not None:
        apply_watermark_resume(task, connection)
