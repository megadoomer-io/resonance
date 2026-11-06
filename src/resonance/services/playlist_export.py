"""In-flight playlist export status helpers.

A playlist export runs as a background ``PLAYLIST_EXPORT`` task per Spotify
connection. Until it finishes, nothing on the playlist page reflected that work,
so a user who browsed away and came back saw a plain "Export" button, assumed
the export had failed, clicked again, and ended up with duplicate Spotify
playlists. These helpers let both task-creating paths (the UI form and the JSON
API) refuse a second export while one is already in flight, and let the playlist
page show an "export in progress" indicator.

It also owns export progress: the worker records phase, counts and a progress
timestamp through ``ExportProgressRecorder``, and the API and UI read them back
through ``export_progress``.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa

import resonance.models.task as task_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import Callable

    import arq
    import sqlalchemy.ext.asyncio as sa_async


async def in_progress_export_tasks(
    db: sa_async.AsyncSession,
    user_id: uuid.UUID,
    playlist_id: uuid.UUID,
) -> list[task_models.Task]:
    """Return the active ``PLAYLIST_EXPORT`` tasks for one playlist.

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
            task_models.Task.status.in_(types_module.ACTIVE_STATUSES),
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


# A healthy export can go a few minutes between commits: one Spotify
# Retry-After wait is capped at 120s, and search is paced to 10 requests per
# 30s window. Five minutes clears that with margin; the exact value is a guess.
STALL_THRESHOLD = datetime.timedelta(minutes=5)

# A progress commit fires on whichever comes first, so DB writes stay bounded
# whether matching is fast (cached ids) or slow (rate-limited search).
PROGRESS_BATCH_SIZE = 25
PROGRESS_MIN_INTERVAL = datetime.timedelta(seconds=5)


class ExportPhase(enum.StrEnum):
    """Where a running export is; stored in ``task.result["phase"]``."""

    MATCHING_MBID = "matching_mbid"
    MATCHING_SEARCH = "matching_search"
    CREATING_PLAYLIST = "creating_playlist"
    ADDING_TRACKS = "adding_tracks"


PHASE_LABELS: dict[str, str] = {
    ExportPhase.MATCHING_MBID: "Matching tracks (MusicBrainz)",
    ExportPhase.MATCHING_SEARCH: "Matching tracks (Spotify search)",
    ExportPhase.CREATING_PLAYLIST: "Creating Spotify playlist",
    ExportPhase.ADDING_TRACKS: "Adding tracks",
}


@dataclasses.dataclass(frozen=True)
class ExportProgress:
    """Read model of one export task's progress, shared by the API and UI."""

    phase: str | None
    current: int
    total: int | None
    matched: int
    skipped: int
    last_progress_at: datetime.datetime | None
    stalled: bool
    retryable: bool
    stalled_minutes: int

    @property
    def phase_label(self) -> str | None:
        """Human label for the phase; an unknown phase falls back to its key."""
        if self.phase is None:
            return None
        return PHASE_LABELS.get(self.phase, self.phase)

    def as_dict(self) -> dict[str, object]:
        """JSON-ready fields for API responses."""
        return {
            "phase": self.phase,
            "phase_label": self.phase_label,
            "progress_current": self.current,
            "progress_total": self.total,
            "matched": self.matched,
            "skipped": self.skipped,
            "last_progress_at": (
                self.last_progress_at.isoformat() if self.last_progress_at else None
            ),
            "stalled": self.stalled,
            "retryable": self.retryable,
        }


def _as_int(value: object) -> int:
    return value if isinstance(value, int) else 0


def _aware(value: datetime.datetime | None) -> datetime.datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=datetime.UTC)


def _parse_timestamp(value: object) -> datetime.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return _aware(datetime.datetime.fromisoformat(value))
    except ValueError:
        return None


def export_progress(
    task: task_models.Task, now: datetime.datetime | None = None
) -> ExportProgress:
    """Derive phase, counts, and stall state from an export task row.

    Args:
        task: A ``PLAYLIST_EXPORT`` task.
        now: Reference time for the stall check (defaults to the current time).

    Returns:
        The task's progress. A RUNNING task whose last progress (or start, if
        it never reported any) is older than ``STALL_THRESHOLD`` is stalled.
    """
    result = task.result or {}
    phase = result.get("phase")
    last = _parse_timestamp(result.get("progress_updated_at")) or _aware(
        task.started_at
    )
    now = now or datetime.datetime.now(datetime.UTC)
    idle = now - last if last is not None else None
    stalled = (
        task.status == types_module.SyncStatus.RUNNING
        and idle is not None
        and idle > STALL_THRESHOLD
    )
    return ExportProgress(
        phase=str(phase) if phase else None,
        current=task.progress_current or 0,
        total=task.progress_total,
        matched=_as_int(result.get("matched")),
        skipped=_as_int(result.get("skipped")),
        last_progress_at=last,
        stalled=stalled,
        retryable=stalled or task.status == types_module.SyncStatus.FAILED,
        stalled_minutes=int(idle.total_seconds() // 60) if stalled and idle else 0,
    )


class ExportProgressRecorder:
    """Writes a running export's progress onto its task row in batches."""

    def __init__(
        self,
        session: sa_async.AsyncSession,
        task: task_models.Task,
        *,
        batch_size: int = PROGRESS_BATCH_SIZE,
        min_interval: datetime.timedelta = PROGRESS_MIN_INTERVAL,
        clock: Callable[[], datetime.datetime] | None = None,
    ) -> None:
        self._session = session
        self._task = task
        self._batch_size = batch_size
        self._min_interval = min_interval
        self._clock = clock or (lambda: datetime.datetime.now(datetime.UTC))
        self._pending = 0
        self._last_commit = self._clock()

    def _write(self, fields: dict[str, object]) -> None:
        # result is a plain JSON column: reassign so SQLAlchemy sees the change.
        self._task.result = {
            **(self._task.result or {}),
            **fields,
            "progress_updated_at": self._clock().isoformat(),
        }

    async def _commit(self) -> None:
        await self._session.commit()
        self._pending = 0
        self._last_commit = self._clock()

    @staticmethod
    def _counts(matched: int | None, skipped: int | None) -> dict[str, object]:
        counts: dict[str, object] = {}
        if matched is not None:
            counts["matched"] = matched
        if skipped is not None:
            counts["skipped"] = skipped
        return counts

    async def phase(
        self,
        phase: ExportPhase,
        *,
        current: int = 0,
        total: int | None = None,
        matched: int | None = None,
        skipped: int | None = None,
    ) -> None:
        """Enter a phase and commit right away so the UI sees the transition."""
        self._task.progress_current = current
        self._task.progress_total = total
        self._write({"phase": phase.value, **self._counts(matched, skipped)})
        await self._commit()

    async def advance(
        self,
        *,
        current: int,
        matched: int | None = None,
        skipped: int | None = None,
        commit: bool = False,
    ) -> None:
        """Record a step; commits once a batch fills, the interval passes, or
        ``commit`` is set (for steps that are each a slow API call)."""
        self._task.progress_current = current
        self._write(self._counts(matched, skipped))
        self._pending += 1
        if (
            commit
            or self._pending >= self._batch_size
            or self._clock() - self._last_commit >= self._min_interval
        ):
            await self._commit()

    async def flush(self) -> None:
        """Commit any recorded steps that haven't been committed yet."""
        if self._pending:
            await self._commit()


class RetryStatus(enum.StrEnum):
    """Outcome of ``retry_export``."""

    STARTED = "started"
    ALREADY_RUNNING = "already_running"
    NOT_RETRYABLE = "not_retryable"
    NOT_FOUND = "not_found"


@dataclasses.dataclass(frozen=True)
class RetryOutcome:
    """``retry_export`` result: the new task, or the one that blocked a retry."""

    status: RetryStatus
    task: task_models.Task | None = None


def is_playlist_export(task: task_models.Task, playlist_id: uuid.UUID) -> bool:
    """Whether ``task`` is an export of this playlist."""
    return task.task_type == types_module.TaskType.PLAYLIST_EXPORT and str(
        (task.params or {}).get("playlist_id")
    ) == str(playlist_id)


async def retry_export(
    db: sa_async.AsyncSession,
    arq_redis: arq.ArqRedis | None,
    user_id: uuid.UUID,
    playlist_id: uuid.UUID,
    task_id: uuid.UUID,
) -> RetryOutcome:
    """Start a fresh export for the connection of a failed or stalled export.

    A stalled export is cancelled first (aborting its job), so the old and new
    tasks never run together. The new task reuses the old one's params; the
    worker replaces tracks on the Spotify playlist a prior attempt recorded,
    so a retry never creates a second playlist.

    Args:
        db: Active database session.
        arq_redis: arq pool used to abort and deliver jobs.
        user_id: The requesting user (ownership scope).
        playlist_id: The playlist the export belongs to.
        task_id: The failed or stalled export task.

    Returns:
        STARTED with the new task; ALREADY_RUNNING with the in-flight export
        for that connection; NOT_RETRYABLE with the task when it is neither
        failed nor stalled; or NOT_FOUND.
    """
    result = await db.execute(
        sa.select(task_models.Task).where(
            task_models.Task.id == task_id,
            task_models.Task.user_id == user_id,
        )
    )
    old = result.scalar_one_or_none()
    if old is None or not is_playlist_export(old, playlist_id):
        return RetryOutcome(RetryStatus.NOT_FOUND)
    if not export_progress(old).retryable:
        return RetryOutcome(RetryStatus.NOT_RETRYABLE, old)

    if old.status == types_module.SyncStatus.RUNNING:
        await lifecycle_module.request_cancel(
            db,
            old.id,
            user_id,
            reason="Stalled; retried by user",
            arq_redis=arq_redis,
        )

    connection_id = str((old.params or {}).get("connection_id"))
    for active in await in_progress_export_tasks(db, user_id, playlist_id):
        if str((active.params or {}).get("connection_id")) == connection_id:
            return RetryOutcome(RetryStatus.ALREADY_RUNNING, active)

    new = task_models.Task(
        id=uuid.uuid4(),
        user_id=user_id,
        service_connection_id=old.service_connection_id,
        task_type=types_module.TaskType.PLAYLIST_EXPORT,
        status=types_module.SyncStatus.PENDING,
        params=dict(old.params or {}),
        description=old.description,
    )
    db.add(new)
    delivery = lifecycle_module.stamp_delivery(new, "export_playlist", str(new.id))
    await db.commit()
    await lifecycle_module.deliver(arq_redis, delivery)
    return RetryOutcome(RetryStatus.STARTED, new)
