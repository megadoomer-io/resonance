"""In-flight playlist export status helpers.

A playlist export runs as a background ``PLAYLIST_EXPORT`` task per Spotify
connection. Until it finishes, nothing on the playlist page reflected that work,
so a user who browsed away and came back saw a plain "Export" button, assumed
the export had failed, clicked again, and ended up with duplicate Spotify
playlists. These helpers let both task-creating paths (the UI form and the JSON
API) refuse a second export while one is already in flight, and let the playlist
page show an "export in progress" indicator.
"""

import uuid

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.models.task as task_models
import resonance.types as types_module

# A PENDING or RUNNING export is "in flight"; anything else is terminal.
_ACTIVE_STATUSES = (
    types_module.SyncStatus.PENDING,
    types_module.SyncStatus.RUNNING,
)


async def in_progress_export_tasks(
    db: sa_async.AsyncSession,
    user_id: uuid.UUID,
    playlist_id: uuid.UUID,
) -> list[task_models.Task]:
    """Return the pending/running ``PLAYLIST_EXPORT`` tasks for one playlist.

    ``params`` is a JSON column, so the per-playlist filter happens in Python
    (export tasks are few). Used both to dedupe re-clicks and to drive the
    "export in progress" UI.

    Args:
        db: Active database session.
        user_id: The owning user's ID.
        playlist_id: The playlist whose exports to look up.

    Returns:
        The in-flight export tasks for this playlist, newest first.
    """
    result = await db.execute(
        sa.select(task_models.Task)
        .where(
            task_models.Task.user_id == user_id,
            task_models.Task.task_type == types_module.TaskType.PLAYLIST_EXPORT,
            task_models.Task.status.in_(_ACTIVE_STATUSES),
        )
        .order_by(task_models.Task.created_at.desc())
    )
    target = str(playlist_id)
    return [
        task
        for task in result.scalars().all()
        if str((task.params or {}).get("playlist_id")) == target
    ]


def export_connection_ids(tasks: list[task_models.Task]) -> set[uuid.UUID]:
    """Connection IDs that already have an in-flight export among ``tasks``."""
    busy: set[uuid.UUID] = set()
    for task in tasks:
        raw = (task.params or {}).get("connection_id")
        if raw:
            busy.add(uuid.UUID(str(raw)))
    return busy
