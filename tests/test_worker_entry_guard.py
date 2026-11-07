"""Terminal-row entry guard: a re-delivered job whose task already finished exits."""

from __future__ import annotations

import collections
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.concerts.worker as concert_worker
import resonance.config as config_module
import resonance.models.task as task_module
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module
import resonance.worker as worker_module

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_TASK_ID = str(uuid.uuid4())

# (module that owns _load_task, job, positional args after ctx)
_ENTRY_POINTS: list[tuple[Any, Callable[..., Awaitable[None]], tuple[str, ...]]] = [
    (worker_module, worker_module.plan_sync, (_TASK_ID,)),
    (worker_module, worker_module.sync_range, (_TASK_ID,)),
    (worker_module, worker_module.run_bulk_job, (_TASK_ID,)),
    (worker_module, worker_module.backfill_mbids, (_TASK_ID,)),
    (worker_module, worker_module.backfill_popularity, (_TASK_ID,)),
    (worker_module, worker_module.backfill_genres, (_TASK_ID,)),
    (worker_module, worker_module.generate_playlist, (_TASK_ID,)),
    (worker_module, worker_module.discover_tracks_for_artist, (_TASK_ID,)),
    (worker_module, worker_module.enrich_related_artists, (_TASK_ID,)),
    (worker_module, worker_module.score_and_build_playlist, (_TASK_ID,)),
    (worker_module, worker_module.export_playlist, (_TASK_ID,)),
    (concert_worker, concert_worker.sync_calendar_feed, (str(uuid.uuid4()), _TASK_ID)),
    (concert_worker, concert_worker.sync_concert_archives, (_TASK_ID,)),
    (concert_worker, concert_worker.sync_concert_archives_chunk, (_TASK_ID,)),
]


_PIPELINE_CHILD_JOBS = frozenset(
    {
        "sync_range",
        "discover_tracks_for_artist",
        "score_and_build_playlist",
        "sync_concert_archives_chunk",
    }
)


def _ctx(session: AsyncMock) -> collections.defaultdict[str, Any]:
    ctx_manager = AsyncMock()
    ctx_manager.__aenter__.return_value = session
    ctx_manager.__aexit__.return_value = False
    ctx: collections.defaultdict[str, Any] = collections.defaultdict(MagicMock)
    ctx["session_factory"] = MagicMock(return_value=ctx_manager)
    ctx["redis"] = AsyncMock()
    # Some jobs build HTTP clients from settings before opening the session.
    ctx["settings"] = config_module.Settings()
    return ctx


def test_covers_every_registered_arq_job() -> None:
    """Every arq function the worker registers is in the guard test list."""
    registered = {
        getattr(f.coroutine, "__wrapped__", f.coroutine).__name__
        for f in worker_module.WorkerSettings.functions
    }
    guarded = {job.__name__ for _module, job, _args in _ENTRY_POINTS}
    assert guarded == registered


@pytest.mark.parametrize(
    "status", sorted(types_module.TERMINAL_STATUSES, key=lambda s: s.value)
)
@pytest.mark.parametrize(
    ("owner", "job", "args"),
    _ENTRY_POINTS,
    ids=[job.__name__ for _o, job, _a in _ENTRY_POINTS],
)
async def test_terminal_row_is_a_duplicate_delivery(
    owner: Any,
    job: Callable[..., Awaitable[None]],
    args: tuple[str, ...],
    status: types_module.SyncStatus,
) -> None:
    task = task_module.Task(
        id=uuid.UUID(_TASK_ID),
        user_id=uuid.uuid4(),
        task_type=types_module.TaskType.SYNC_JOB,
        status=status,
        params={},
    )
    session = AsyncMock()
    ctx = _ctx(session)

    with (
        patch.object(owner, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            lifecycle_module, "is_cancelled", AsyncMock(return_value=False)
        ) as is_cancelled,
        patch.object(lifecycle_module, "logger") as log,
        patch.object(
            worker_module, "_check_parent_completion", AsyncMock()
        ) as check_parent,
    ):
        await job(ctx, *args)

    assert task.status == status
    session.commit.assert_not_called()
    session.execute.assert_not_called()
    is_cancelled.assert_not_called()
    ctx["redis"].enqueue_job.assert_not_called()
    # Pipeline-driving children still advance siblings past a cancelled row.
    if job.__name__ in _PIPELINE_CHILD_JOBS and (
        status == types_module.SyncStatus.CANCELLED
    ):
        check_parent.assert_awaited_once()
    else:
        check_parent.assert_not_called()
    log.info.assert_called_once()
    assert log.info.call_args.args[0] == "duplicate_delivery"
    assert log.info.call_args.kwargs["job"] == job.__name__


class TestIsDuplicateDelivery:
    def test_active_row_is_not_duplicate(self) -> None:
        for status in types_module.ACTIVE_STATUSES:
            task = task_module.Task(id=uuid.uuid4(), status=status)
            assert not lifecycle_module.is_duplicate_delivery(task, "job")

    def test_terminal_row_is_duplicate(self) -> None:
        for status in types_module.TERMINAL_STATUSES:
            task = task_module.Task(id=uuid.uuid4(), status=status)
            assert lifecycle_module.is_duplicate_delivery(task, "job")


@pytest.mark.parametrize(
    ("owner", "job", "args"),
    _ENTRY_POINTS,
    ids=[job.__name__ for _o, job, _a in _ENTRY_POINTS],
)
async def test_running_row_owned_by_another_job_is_not_claimed(
    owner: Any,
    job: Callable[..., Awaitable[None]],
    args: tuple[str, ...],
) -> None:
    task = task_module.Task(
        id=uuid.UUID(_TASK_ID),
        user_id=uuid.uuid4(),
        task_type=types_module.TaskType.SYNC_JOB,
        status=types_module.SyncStatus.RUNNING,
        attempt=1,
        arq_job_id="owner-job",
        params={},
    )
    session = AsyncMock()
    ctx = _ctx(session)
    ctx["job_id"] = "duplicate-job"

    with (
        patch.object(owner, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            lifecycle_module, "is_cancelled", AsyncMock(return_value=False)
        ) as is_cancelled,
        patch.object(lifecycle_module, "logger") as log,
    ):
        await job(ctx, *args)

    assert (task.status, task.attempt) == (types_module.SyncStatus.RUNNING, 1)
    session.commit.assert_not_called()
    is_cancelled.assert_not_called()
    assert log.info.call_args.args[0] == "task_claim_lost"


class TestTaskJob:
    async def test_fenced_write_is_logged_not_raised(self) -> None:
        async def job() -> None:
            raise task_module.TaskFenced(uuid.uuid4(), 1, 2)

        with patch.object(lifecycle_module, "logger") as log:
            await lifecycle_module.task_job(job)()

        assert log.warning.call_args.args[0] == "task_write_fenced"

    async def test_claims_are_scoped_to_one_job(self) -> None:
        seen: list[object] = []

        async def job() -> None:
            seen.append(task_module.claimed_attempts.get())

        await lifecycle_module.task_job(job)()

        assert seen == [{}]
        assert task_module.claimed_attempts.get() is None
