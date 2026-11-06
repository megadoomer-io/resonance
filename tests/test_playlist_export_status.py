"""Tests for in-flight playlist export status helpers (#dedup + UI indicator)."""

from __future__ import annotations

import datetime
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.models.task as task_models
import resonance.services.playlist_export as playlist_export_module
import resonance.types as types_module


def _export_task(
    *,
    playlist_id: uuid.UUID,
    connection_id: uuid.UUID | None,
    status: types_module.SyncStatus = types_module.SyncStatus.RUNNING,
) -> task_models.Task:
    params: dict[str, str] = {"playlist_id": str(playlist_id)}
    if connection_id is not None:
        params["connection_id"] = str(connection_id)
    return task_models.Task(
        id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        task_type=types_module.TaskType.PLAYLIST_EXPORT,
        status=status,
        params=params,
    )


def _session_returning(tasks: list[task_models.Task]) -> AsyncMock:
    """Mock an AsyncSession whose execute().scalars().all() yields ``tasks``."""
    scalars = MagicMock()
    scalars.all.return_value = tasks
    result_obj = MagicMock()
    result_obj.scalars.return_value = scalars
    session = AsyncMock()
    session.execute = AsyncMock(return_value=result_obj)
    return session


class TestInProgressExportTasks:
    async def test_keeps_only_matching_playlist(self) -> None:
        pid = uuid.uuid4()
        other = uuid.uuid4()
        mine = _export_task(playlist_id=pid, connection_id=uuid.uuid4())
        theirs = _export_task(playlist_id=other, connection_id=uuid.uuid4())
        session = _session_returning([mine, theirs])

        result = await playlist_export_module.in_progress_export_tasks(
            session, uuid.uuid4(), pid
        )

        assert result == [mine]

    async def test_empty_when_no_active_export(self) -> None:
        session = _session_returning([])
        result = await playlist_export_module.in_progress_export_tasks(
            session, uuid.uuid4(), uuid.uuid4()
        )
        assert result == []

    async def test_handles_missing_params(self) -> None:
        pid = uuid.uuid4()
        broken = _export_task(playlist_id=pid, connection_id=None)
        broken.params = {}  # defensive: a task with no playlist_id must not match
        session = _session_returning([broken])
        result = await playlist_export_module.in_progress_export_tasks(
            session, uuid.uuid4(), pid
        )
        assert result == []


class TestExportConnectionIds:
    def test_extracts_and_dedupes(self) -> None:
        pid = uuid.uuid4()
        c1 = uuid.uuid4()
        c2 = uuid.uuid4()
        tasks = [
            _export_task(playlist_id=pid, connection_id=c1),
            _export_task(playlist_id=pid, connection_id=c2),
            _export_task(playlist_id=pid, connection_id=c1),  # duplicate connection
        ]
        assert playlist_export_module.export_connection_ids(tasks) == {c1, c2}

    def test_ignores_tasks_without_connection(self) -> None:
        pid = uuid.uuid4()
        tasks = [_export_task(playlist_id=pid, connection_id=None)]
        assert playlist_export_module.export_connection_ids(tasks) == set()

    def test_empty_list(self) -> None:
        assert playlist_export_module.export_connection_ids([]) == set()


_NOW = datetime.datetime(2026, 11, 5, 12, 0, tzinfo=datetime.UTC)


def _task_with(
    result: dict[str, object] | None = None,
    *,
    started_at: datetime.datetime | None = None,
    status: types_module.SyncStatus = types_module.SyncStatus.RUNNING,
) -> task_models.Task:
    task = _export_task(
        playlist_id=uuid.uuid4(), connection_id=uuid.uuid4(), status=status
    )
    task.result = result or {}
    task.started_at = started_at
    return task


class TestExportProgress:
    def test_reads_phase_counts_and_progress(self) -> None:
        task = _task_with(
            {
                "phase": "matching_search",
                "matched": 7,
                "skipped": 2,
                "progress_updated_at": _NOW.isoformat(),
            }
        )
        task.progress_current = 9
        task.progress_total = 20
        progress = playlist_export_module.export_progress(task, now=_NOW)
        assert progress.phase == "matching_search"
        assert progress.phase_label == "Matching tracks (Spotify search)"
        assert (progress.current, progress.total) == (9, 20)
        assert (progress.matched, progress.skipped) == (7, 2)
        assert progress.last_progress_at == _NOW
        assert not progress.stalled
        assert not progress.retryable

    def test_running_past_threshold_is_stalled_and_retryable(self) -> None:
        last = _NOW - playlist_export_module.STALL_THRESHOLD
        last -= datetime.timedelta(seconds=1)
        task = _task_with(
            {"phase": "adding_tracks", "progress_updated_at": last.isoformat()}
        )
        progress = playlist_export_module.export_progress(task, now=_NOW)
        assert progress.stalled
        assert progress.retryable
        assert progress.stalled_minutes == 5

    def test_within_threshold_is_not_stalled(self) -> None:
        last = _NOW - playlist_export_module.STALL_THRESHOLD
        last += datetime.timedelta(seconds=1)
        task = _task_with({"progress_updated_at": last.isoformat()})
        assert not playlist_export_module.export_progress(task, now=_NOW).stalled

    def test_falls_back_to_started_at(self) -> None:
        task = _task_with({}, started_at=_NOW - datetime.timedelta(hours=1))
        progress = playlist_export_module.export_progress(task, now=_NOW)
        assert progress.last_progress_at == task.started_at
        assert progress.stalled

    def test_no_timestamp_at_all_is_not_stalled(self) -> None:
        progress = playlist_export_module.export_progress(_task_with(), now=_NOW)
        assert progress.last_progress_at is None
        assert not progress.stalled

    def test_unparseable_timestamp_falls_back_to_started_at(self) -> None:
        task = _task_with({"progress_updated_at": "nope"}, started_at=_NOW)
        progress = playlist_export_module.export_progress(task, now=_NOW)
        assert progress.last_progress_at == _NOW

    @pytest.mark.parametrize(
        "status",
        [
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.DEFERRED,
            types_module.SyncStatus.COMPLETED,
            types_module.SyncStatus.CANCELLED,
        ],
    )
    def test_only_running_can_stall(self, status: types_module.SyncStatus) -> None:
        old = (_NOW - datetime.timedelta(hours=1)).isoformat()
        task = _task_with({"progress_updated_at": old}, status=status)
        progress = playlist_export_module.export_progress(task, now=_NOW)
        assert not progress.stalled
        assert not progress.retryable

    def test_failed_is_retryable(self) -> None:
        task = _task_with(
            {"phase": "adding_tracks"}, status=types_module.SyncStatus.FAILED
        )
        assert playlist_export_module.export_progress(task, now=_NOW).retryable

    def test_missing_and_unknown_phase_labels(self) -> None:
        missing = playlist_export_module.export_progress(_task_with(), now=_NOW)
        assert missing.phase is None
        assert missing.phase_label is None
        weird = _task_with({"phase": "mystery"})
        progress = playlist_export_module.export_progress(weird, now=_NOW)
        assert progress.phase_label == "mystery"

    def test_as_dict_is_json_ready(self) -> None:
        task = _task_with(
            {
                "phase": "adding_tracks",
                "matched": 3,
                "skipped": 1,
                "progress_updated_at": _NOW.isoformat(),
            }
        )
        task.progress_current = 3
        task.progress_total = 3
        data = playlist_export_module.export_progress(task, now=_NOW).as_dict()
        assert data == {
            "phase": "adding_tracks",
            "phase_label": "Adding tracks",
            "progress_current": 3,
            "progress_total": 3,
            "matched": 3,
            "skipped": 1,
            "last_progress_at": _NOW.isoformat(),
            "stalled": False,
            "retryable": False,
        }


class TestExportProgressRecorder:
    def _recorder(self, batch: int = 3) -> tuple[task_models.Task, AsyncMock, Any]:
        task = _task_with({"keep": "me"})
        session = AsyncMock()
        recorder = playlist_export_module.ExportProgressRecorder(
            session, task, batch_size=batch, clock=lambda: _NOW
        )
        return task, session, recorder

    async def test_phase_commits_immediately(self) -> None:
        task, session, recorder = self._recorder()
        await recorder.phase(
            playlist_export_module.ExportPhase.MATCHING_MBID, current=2, total=10
        )
        session.commit.assert_awaited_once()
        assert (task.progress_current, task.progress_total) == (2, 10)
        assert task.result["phase"] == "matching_mbid"
        assert task.result["progress_updated_at"] == _NOW.isoformat()
        assert task.result["keep"] == "me"

    async def test_advance_commits_in_batches(self) -> None:
        task, session, recorder = self._recorder(batch=3)
        await recorder.phase(
            playlist_export_module.ExportPhase.MATCHING_SEARCH, current=0, total=10
        )
        session.commit.reset_mock()
        for i in range(1, 8):
            await recorder.advance(current=i, matched=i, skipped=0)
        # 7 steps in batches of 3: commits at 3 and 6 only.
        assert session.commit.await_count == 2
        assert task.progress_current == 7
        assert task.result["matched"] == 7
        await recorder.flush()
        assert session.commit.await_count == 3

    async def test_flush_without_pending_change_is_a_noop(self) -> None:
        _task, session, recorder = self._recorder()
        await recorder.phase(
            playlist_export_module.ExportPhase.ADDING_TRACKS, current=0, total=5
        )
        session.commit.reset_mock()
        await recorder.flush()
        session.commit.assert_not_awaited()

    async def test_forced_commit_on_advance(self) -> None:
        _task, session, recorder = self._recorder(batch=100)
        await recorder.advance(current=1, commit=True)
        session.commit.assert_awaited_once()

    async def test_counts_carry_across_phases(self) -> None:
        task, _session, recorder = self._recorder()
        await recorder.advance(current=4, matched=3, skipped=1)
        await recorder.phase(
            playlist_export_module.ExportPhase.ADDING_TRACKS, current=0, total=3
        )
        assert task.result["matched"] == 3
        assert task.result["skipped"] == 1


class _RetrySession:
    """Serves a fixed task to the retry helper's lookups and records writes."""

    def __init__(self, task: task_models.Task | None, active: list[Any]) -> None:
        self.task = task
        self.active = active
        self.added: list[Any] = []
        self.commits = 0
        self.statements: list[str] = []
        self.locked_before_insert: bool | None = None

    async def execute(self, stmt: Any, *args: Any, **kwargs: Any) -> Any:
        self.statements.append(str(stmt.compile()))
        result = MagicMock()
        result.scalar_one_or_none.return_value = self.task
        scalars = MagicMock()
        scalars.all.return_value = self.active
        result.scalars.return_value = scalars
        return result

    def add(self, obj: Any) -> None:
        self.locked_before_insert = any("FOR UPDATE" in s for s in self.statements)
        self.added.append(obj)

    async def commit(self) -> None:
        self.commits += 1


def _hours_ago(hours: int) -> str:
    then = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=hours)
    return then.isoformat()


async def _retry(session: _RetrySession, task: task_models.Task, **kw: Any) -> Any:
    assert task.user_id is not None
    return await playlist_export_module.retry_export(
        session,  # type: ignore[arg-type]
        None,
        kw.get("user_id", task.user_id),
        kw.get("playlist_id", uuid.UUID(str(task.params["playlist_id"]))),
        kw.get("task_id", task.id),
    )


class TestRetryExport:
    @pytest.fixture
    def delivered(self, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
        sent: list[Any] = []

        async def fake_deliver(arq_redis: Any, delivery: Any) -> bool:
            sent.append(delivery)
            return True

        monkeypatch.setattr(
            playlist_export_module.lifecycle_module, "deliver", fake_deliver
        )
        return sent

    @staticmethod
    def _failed() -> task_models.Task:
        task = _task_with(
            {"phase": "adding_tracks"}, status=types_module.SyncStatus.FAILED
        )
        task.description = "Export to Spotify (me)"
        task.service_connection_id = uuid.uuid4()
        return task

    async def test_failed_export_gets_a_fresh_task(self, delivered: list[Any]) -> None:
        old = self._failed()
        session = _RetrySession(old, active=[])
        outcome = await _retry(session, old)
        assert outcome.status == playlist_export_module.RetryStatus.STARTED
        new = outcome.task
        assert new is not None
        assert new is not old
        assert new.params == old.params
        assert new.description == old.description
        assert new.service_connection_id == old.service_connection_id
        assert new.status == types_module.SyncStatus.PENDING
        assert session.added == [new]
        assert session.commits == 1
        assert [d.task_id for d in delivered] == [new.id]

    async def test_playlist_row_is_locked_before_the_in_flight_check(
        self, delivered: list[Any]
    ) -> None:
        """A double-click can't start two exports: the second retry waits on
        the playlist row lock, then sees the first one's task."""
        old = self._failed()
        session = _RetrySession(old, active=[])
        await _retry(session, old)
        assert session.locked_before_insert is True
        lock = next(s for s in session.statements if "FOR UPDATE" in s)
        assert "FROM playlists" in lock

    async def test_stalled_running_export_is_cancelled_then_retried(
        self, monkeypatch: pytest.MonkeyPatch, delivered: list[Any]
    ) -> None:
        old = _task_with({"progress_updated_at": _hours_ago(1)})
        cancelled: list[uuid.UUID] = []

        async def fake_cancel(
            session: Any, task_id: uuid.UUID, user_id: Any, **kw: Any
        ) -> Any:
            cancelled.append(task_id)
            old.status = types_module.SyncStatus.CANCELLED
            return MagicMock()

        monkeypatch.setattr(
            playlist_export_module.lifecycle_module, "request_cancel", fake_cancel
        )
        outcome = await _retry(_RetrySession(old, active=[]), old)
        assert cancelled == [old.id]
        assert outcome.status == playlist_export_module.RetryStatus.STARTED
        assert len(delivered) == 1

    async def test_healthy_running_export_is_not_retryable(
        self, delivered: list[Any]
    ) -> None:
        old = _task_with({"progress_updated_at": _hours_ago(0)})
        session = _RetrySession(old, active=[old])
        outcome = await _retry(session, old)
        assert outcome.status == playlist_export_module.RetryStatus.NOT_RETRYABLE
        assert outcome.task is old
        assert session.added == []
        assert delivered == []

    async def test_other_playlist_or_missing_task_is_not_found(self) -> None:
        old = self._failed()
        outcome = await _retry(
            _RetrySession(old, active=[]), old, playlist_id=uuid.uuid4()
        )
        assert outcome.status == playlist_export_module.RetryStatus.NOT_FOUND
        outcome = await _retry(_RetrySession(None, active=[]), old)
        assert outcome.status == playlist_export_module.RetryStatus.NOT_FOUND

    async def test_connection_already_exporting_returns_that_task(
        self, delivered: list[Any]
    ) -> None:
        old = self._failed()
        other = _export_task(
            playlist_id=uuid.UUID(str(old.params["playlist_id"])),
            connection_id=uuid.UUID(str(old.params["connection_id"])),
        )
        session = _RetrySession(old, active=[other])
        outcome = await _retry(session, old)
        assert outcome.status == playlist_export_module.RetryStatus.ALREADY_RUNNING
        assert outcome.task is other
        assert session.added == []
        assert delivered == []
