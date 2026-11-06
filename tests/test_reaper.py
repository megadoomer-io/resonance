"""Reaper: re-deliver PENDING/DEFERRED rows whose arq job is gone."""

from __future__ import annotations

import datetime
import json
import uuid
from typing import Any

import arq.jobs as arq_jobs
import pytest

import resonance.models.task as task_models
import resonance.sync.reaper as reaper_module
import resonance.types as types_module
from tests.conftest import TaskDB, minutes_ago


class _FakeArq:
    def __init__(self) -> None:
        self.statuses: dict[str, str] = {}
        self.enqueued: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        self.kv: dict[str, Any] = {}
        self.lists: dict[str, list[str]] = {}
        self.default_queue_name = "arq:queue"
        self.fail_enqueue = False

    async def enqueue_job(self, *args: Any, **kwargs: Any) -> object:
        if self.fail_enqueue:
            raise ConnectionError("redis down")
        self.enqueued.append((args, kwargs))
        return object()

    def pipeline(self, transaction: bool = True) -> _Pipeline:
        return _Pipeline(self)


class _Pipeline:
    def __init__(self, arq: _FakeArq) -> None:
        self._arq = arq
        self._ops: list[Any] = []

    async def __aenter__(self) -> _Pipeline:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def set(self, key: str, value: str) -> None:
        self._ops.append(lambda: self._arq.kv.__setitem__(key, value))

    def get(self, key: str) -> None:
        self._ops.append(lambda: self._arq.kv.get(key))

    def lpush(self, key: str, *values: str) -> None:
        def op() -> None:
            current = self._arq.lists.setdefault(key, [])
            for value in values:
                current.insert(0, value)

        self._ops.append(op)

    def ltrim(self, key: str, start: int, end: int) -> None:
        def op() -> None:
            self._arq.lists[key] = self._arq.lists.get(key, [])[start : end + 1]

        self._ops.append(op)

    def lrange(self, key: str, start: int, end: int) -> None:
        self._ops.append(lambda: self._arq.lists.get(key, [])[start : end + 1])

    async def execute(self) -> list[Any]:
        return [op() for op in self._ops]


@pytest.fixture
def fake_arq(monkeypatch: pytest.MonkeyPatch) -> _FakeArq:
    fake = _FakeArq()

    async def _status(_redis: Any, job_id: str) -> arq_jobs.JobStatus:
        status = fake.statuses.get(job_id, "not_found")
        if status == "error":
            raise ConnectionError("redis down")
        return arq_jobs.JobStatus(status)

    monkeypatch.setattr(reaper_module, "job_status", _status)
    return fake


def _actions(decisions: list[reaper_module.Decision]) -> list[str]:
    return [d.action.value for d in decisions]


class TestReap:
    async def test_redelivers_pending_row_whose_job_is_gone(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        task = await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="old")

        decisions = await reaper_module.reap(task_db.factory, fake_arq)

        assert _actions(decisions) == ["redelivered"]
        assert [args for args, _ in fake_arq.enqueued] == [("sync_range", str(task.id))]
        row = await task_db.get(task.id)
        assert row.arq_job_id == fake_arq.enqueued[0][1]["_job_id"]
        assert row.arq_job_id != "old"

    async def test_redelivers_when_job_complete(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="done")
        fake_arq.statuses["done"] = "complete"

        decisions = await reaper_module.reap(task_db.factory, fake_arq)

        assert _actions(decisions) == ["redelivered"]
        assert decisions[0].arq_status == "complete"

    @pytest.mark.parametrize("status", ["queued", "deferred", "in_progress"])
    async def test_leaves_row_whose_job_arq_holds(
        self, task_db: TaskDB, fake_arq: _FakeArq, status: str
    ) -> None:
        task = await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="live")
        fake_arq.statuses["live"] = status

        for _ in range(2):
            decisions = await reaper_module.reap(task_db.factory, fake_arq)
            assert _actions(decisions) == ["arq_holds_job"]

        assert fake_arq.enqueued == []
        assert (await task_db.get(task.id)).arq_job_id == "live"

    async def test_never_delivers_unstamped_rows(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        parent = await task_db.add(
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.RUNNING,
        )
        await task_db.add(parent_id=parent.id)
        await task_db.add(parent_id=parent.id, params={"step_mode": True})

        assert await reaper_module.reap(task_db.factory, fake_arq) == []
        assert fake_arq.enqueued == []

    async def test_respects_grace_window_after_stamp(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        await task_db.add(
            enqueued_at=datetime.datetime.now(datetime.UTC), arq_job_id="fresh"
        )

        assert await reaper_module.reap(task_db.factory, fake_arq) == []

    async def test_skips_deferred_row_not_yet_due(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        await task_db.add(
            status=types_module.SyncStatus.DEFERRED,
            deferred_until=datetime.datetime.now(datetime.UTC)
            + datetime.timedelta(hours=1),
            enqueued_at=minutes_ago(5),
            arq_job_id="later",
        )

        assert await reaper_module.reap(task_db.factory, fake_arq) == []

    async def test_redelivers_due_deferred_row_as_pending(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        task = await task_db.add(
            status=types_module.SyncStatus.DEFERRED,
            deferred_until=minutes_ago(1),
            enqueued_at=minutes_ago(5),
            arq_job_id="due",
        )

        decisions = await reaper_module.reap(task_db.factory, fake_arq)

        assert _actions(decisions) == ["redelivered"]
        assert (await task_db.get(task.id)).status == types_module.SyncStatus.PENDING

    @pytest.mark.parametrize("status", sorted(types_module.TERMINAL_STATUSES))
    async def test_skips_children_of_terminal_parent(
        self, task_db: TaskDB, fake_arq: _FakeArq, status: types_module.SyncStatus
    ) -> None:
        parent = await task_db.add(
            task_type=types_module.TaskType.SYNC_JOB, status=status
        )
        await task_db.add(
            parent_id=parent.id,
            status=types_module.SyncStatus.DEFERRED,
            deferred_until=minutes_ago(1),
            enqueued_at=minutes_ago(5),
            arq_job_id="orphan",
        )

        assert await reaper_module.reap(task_db.factory, fake_arq) == []

    async def test_skips_rows_that_have_children(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        """A parent reset with children must not be re-planned every tick."""
        parent = await task_db.add(
            task_type=types_module.TaskType.SYNC_JOB,
            enqueued_at=minutes_ago(5),
            arq_job_id="planned",
        )
        await task_db.add(parent_id=parent.id, status=types_module.SyncStatus.COMPLETED)

        assert await reaper_module.reap(task_db.factory, fake_arq) == []

    async def test_skips_row_that_turned_terminal_after_scan(
        self,
        task_db: TaskDB,
        fake_arq: _FakeArq,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        task = await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="racing")

        async def _status_then_complete(_redis: Any, _job_id: str) -> Any:
            await task_db.update(task.id, status=types_module.SyncStatus.COMPLETED)
            return arq_jobs.JobStatus.complete

        monkeypatch.setattr(reaper_module, "job_status", _status_then_complete)

        decisions = await reaper_module.reap(task_db.factory, fake_arq)

        assert _actions(decisions) == ["row_changed"]
        assert fake_arq.enqueued == []
        assert (await task_db.get(task.id)).status == types_module.SyncStatus.COMPLETED

    async def test_skips_row_redelivered_by_someone_else_after_scan(
        self,
        task_db: TaskDB,
        fake_arq: _FakeArq,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        task = await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="first")

        async def _status_then_resumed(_redis: Any, _job_id: str) -> Any:
            await task_db.update(task.id, arq_job_id="second")
            return arq_jobs.JobStatus.not_found

        monkeypatch.setattr(reaper_module, "job_status", _status_then_resumed)

        decisions = await reaper_module.reap(task_db.factory, fake_arq)

        assert _actions(decisions) == ["row_changed"]
        assert fake_arq.enqueued == []

    async def test_status_error_skips_row(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="unknown")
        fake_arq.statuses["unknown"] = "error"

        decisions = await reaper_module.reap(task_db.factory, fake_arq)

        assert _actions(decisions) == ["status_error"]
        assert fake_arq.enqueued == []

    async def test_enqueue_failure_is_retried_next_tick(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        task = await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="old")
        fake_arq.fail_enqueue = True

        first = await reaper_module.reap(task_db.factory, fake_arq)
        assert _actions(first) == ["enqueue_failed"]

        fake_arq.fail_enqueue = False
        later = datetime.datetime.now(datetime.UTC) + datetime.timedelta(minutes=2)
        second = await reaper_module.reap(task_db.factory, fake_arq, now=later)

        assert _actions(second) == ["redelivered"]
        assert [args for args, _ in fake_arq.enqueued] == [("sync_range", str(task.id))]


class TestReaperState:
    async def test_records_last_tick_and_bounded_decisions(
        self, task_db: TaskDB, fake_arq: _FakeArq, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(reaper_module, "MAX_RECORDED_DECISIONS", 3)
        for _ in range(5):
            await task_db.add(enqueued_at=minutes_ago(5), arq_job_id=None)

        await reaper_module.reap(task_db.factory, fake_arq)

        tick = json.loads(fake_arq.kv[reaper_module.LAST_TICK_KEY])
        assert tick["candidates"] == 5
        assert tick["redelivered"] == 5
        assert len(fake_arq.lists[reaper_module.DECISIONS_KEY]) == 3

        state = await reaper_module.read_state(fake_arq)  # type: ignore[arg-type]
        assert state.last_tick == tick
        assert [d["action"] for d in state.decisions] == ["redelivered"] * 3

    async def test_held_jobs_are_not_recorded(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        await task_db.add(enqueued_at=minutes_ago(5), arq_job_id="live")
        fake_arq.statuses["live"] = "queued"

        await reaper_module.reap(task_db.factory, fake_arq)

        assert reaper_module.DECISIONS_KEY not in fake_arq.lists
        assert reaper_module.LAST_TICK_KEY in fake_arq.kv

    async def test_empty_state(self, fake_arq: _FakeArq) -> None:
        state = await reaper_module.read_state(fake_arq)  # type: ignore[arg-type]
        assert state.last_tick is None
        assert state.decisions == []

    def test_decision_json_round_trips(self) -> None:
        decision = reaper_module.Decision(
            at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            task_id=str(uuid.uuid4()),
            task_type="time_range",
            action=reaper_module.Action.REDELIVERED,
            arq_status="not_found",
            job_id="j",
        )
        assert json.loads(decision.to_json())["at"] == "2026-01-01T00:00:00+00:00"


class TestSiblingAdvance:
    """Advancing a sequential pipeline never re-delivers a stamped sibling."""

    async def test_stamped_pending_sibling_is_not_advanced_again(
        self, task_db: TaskDB, fake_arq: _FakeArq
    ) -> None:
        import structlog

        import resonance.worker as worker_module

        parent = await task_db.add(
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.RUNNING,
        )
        done = await task_db.add(
            parent_id=parent.id, status=types_module.SyncStatus.COMPLETED
        )
        await task_db.add(
            parent_id=parent.id, enqueued_at=minutes_ago(1), arq_job_id="queued"
        )
        unstarted = await task_db.add(parent_id=parent.id)

        async with task_db.factory() as session:
            row = await session.get(task_models.Task, done.id)
            assert row is not None
            await worker_module._check_parent_completion(
                session, row, fake_arq, structlog.get_logger()
            )

        assert [args for args, _ in fake_arq.enqueued] == [
            ("sync_range", str(unstarted.id))
        ]
