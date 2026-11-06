"""Which arq job runs each task type, and with which arguments.

Shared by sibling advance, startup recovery and the reaper, which deliver a
task knowing only its row.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import resonance.types as types_module

if TYPE_CHECKING:
    import collections.abc

    import resonance.models.task as task_models


def _task_id(task: task_models.Task) -> tuple[str, ...]:
    return (str(task.id),)


def _connection_and_task_id(task: task_models.Task) -> tuple[str, ...]:
    return (str(task.service_connection_id), str(task.id))


TASK_DISPATCH: dict[
    types_module.TaskType,
    tuple[str, collections.abc.Callable[[task_models.Task], tuple[str, ...]]],
] = {
    types_module.TaskType.SYNC_JOB: ("plan_sync", _task_id),
    types_module.TaskType.TIME_RANGE: ("sync_range", _task_id),
    types_module.TaskType.CALENDAR_SYNC: (
        "sync_calendar_feed",
        _connection_and_task_id,
    ),
    types_module.TaskType.BULK_JOB: ("run_bulk_job", _task_id),
    types_module.TaskType.PLAYLIST_GENERATION: ("generate_playlist", _task_id),
    types_module.TaskType.TRACK_DISCOVERY: ("discover_tracks_for_artist", _task_id),
    types_module.TaskType.TRACK_SCORING: ("score_and_build_playlist", _task_id),
    types_module.TaskType.CONCERT_ARCHIVES_IMPORT: ("sync_concert_archives", _task_id),
    types_module.TaskType.CONCERT_ARCHIVES_CHUNK: (
        "sync_concert_archives_chunk",
        _task_id,
    ),
    types_module.TaskType.PLAYLIST_EXPORT: ("export_playlist", _task_id),
    types_module.TaskType.MBID_BACKFILL: ("backfill_mbids", _task_id),
    types_module.TaskType.POPULARITY_BACKFILL: ("backfill_popularity", _task_id),
    types_module.TaskType.RELATED_ARTIST_ENRICHMENT: (
        "enrich_related_artists",
        _task_id,
    ),
    types_module.TaskType.GENRE_BACKFILL: ("backfill_genres", _task_id),
}


def job_for(task: task_models.Task) -> tuple[str, tuple[str, ...]] | None:
    """The arq job name and positional args that run *task*, if it has one."""
    dispatch = TASK_DISPATCH.get(task.task_type)
    if dispatch is None:
        return None
    job_name, args_builder = dispatch
    return job_name, args_builder(task)
