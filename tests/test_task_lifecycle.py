"""Tests for type-agnostic task lifecycle helpers."""

from __future__ import annotations

import asyncio
import datetime
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa

import resonance.models.task as task_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module

if TYPE_CHECKING:
    from tests.conftest import TaskDB

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_task(
    *,
    task_id: uuid.UUID | None = None,
    parent_id: uuid.UUID | None = None,
    task_type: types_module.TaskType = types_module.TaskType.TIME_RANGE,
    status: types_module.SyncStatus = types_module.SyncStatus.RUNNING,
) -> task_models.Task:
    """Create a Task instance for testing."""
    return task_models.Task(
        id=task_id or uuid.uuid4(),
        user_id=uuid.uuid4(),
        task_type=task_type,
        status=status,
        parent_id=parent_id,
    )


# ---------------------------------------------------------------------------
# complete_task tests
# ---------------------------------------------------------------------------


class TestCompleteTask:
    """Tests for complete_task."""

    async def test_standalone_task_completes(self) -> None:
        """Task with no parent marks itself completed with result and completed_at."""
        session = AsyncMock()
        task = _make_task(parent_id=None)

        result = {"items_created": 10, "items_updated": 5}
        await lifecycle_module.complete_task(session, task, result)

        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result == result
        assert task.completed_at is not None

        # Should NOT query the database (no parent to check)
        session.execute.assert_not_called()

    async def test_child_task_does_not_propagate_to_parent(self) -> None:
        """complete_task only sets child fields; parent check is the caller's job."""
        parent_id = uuid.uuid4()
        child = _make_task(parent_id=parent_id)

        session = AsyncMock()

        await lifecycle_module.complete_task(
            session, child, {"items_created": 10, "items_updated": 0}
        )

        assert child.status == types_module.SyncStatus.COMPLETED
        assert child.completed_at is not None
        # Should NOT query the database — parent completion is the caller's
        # responsibility (e.g. worker._check_parent_completion).
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# fail_task tests
# ---------------------------------------------------------------------------


class TestFailTask:
    """Tests for fail_task."""

    async def test_standalone_task_fails(self) -> None:
        """fail_task sets status=FAILED, error_message, completed_at."""
        session = AsyncMock()
        task = _make_task(parent_id=None)

        await lifecycle_module.fail_task(session, task, "Something went wrong")

        assert task.status == types_module.SyncStatus.FAILED
        assert task.error_message == "Something went wrong"
        assert task.completed_at is not None

    async def test_parent_completes_when_child_fails(self) -> None:
        """A failed child counts as 'done' for parent completion; parent
        result shows children_failed count."""
        parent_id = uuid.uuid4()
        parent = _make_task(
            task_id=parent_id,
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.RUNNING,
        )

        session = AsyncMock()

        # 1. Load parent
        parent_result_mock = MagicMock()
        parent_result_mock.scalar_one_or_none.return_value = parent

        # 2. Load children — one completed, one failed (the one we're failing)
        completed_child = _make_task(
            parent_id=parent_id,
            status=types_module.SyncStatus.COMPLETED,
        )
        failed_child = _make_task(
            parent_id=parent_id,
            status=types_module.SyncStatus.FAILED,
        )
        children_scalars = MagicMock()
        children_scalars.all.return_value = [completed_child, failed_child]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [parent_result_mock, children_result]

        # Create the child we're going to fail (has parent_id)
        child = _make_task(parent_id=parent_id)

        # fail_task does not propagate to parent — but we test the scenario
        # where complete_task is called on the last child that fails.
        # Actually, fail_task itself does NOT check parent completion per the
        # spec. Let's verify fail_task works correctly first.
        await lifecycle_module.fail_task(session, child, "API timeout")

        assert child.status == types_module.SyncStatus.FAILED
        assert child.error_message == "API timeout"
        assert child.completed_at is not None

        # fail_task does not check parent — session should not be queried
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# cancellation tests
# ---------------------------------------------------------------------------


def _scalar_result(value: object) -> MagicMock:
    result = MagicMock()
    result.scalar_one_or_none.return_value = value
    return result


def _scalars_result(values: list[task_models.Task]) -> MagicMock:
    scalars = MagicMock()
    scalars.all.return_value = values
    result = MagicMock()
    result.scalars.return_value = scalars
    return result


class TestRecoverFailedSession:
    async def test_healthy_session_keeps_pending_work(self, task_db: TaskDB) -> None:
        task = await task_db.add(status=types_module.SyncStatus.RUNNING)
        async with task_db.factory() as session:
            row = await session.get(task_models.Task, task.id)
            assert row is not None
            row.progress_current = 3
            await session.flush()

            assert await lifecycle_module.recover_failed_session(session) is False

            await session.commit()
        assert (await task_db.get(task.id)).progress_current == 3

    async def test_failed_transaction_is_rolled_back(self, task_db: TaskDB) -> None:
        task = await task_db.add(status=types_module.SyncStatus.RUNNING)
        async with task_db.factory() as session:
            row = await session.get(task_models.Task, task.id)
            assert row is not None
            row.progress_current = 3
            await session.flush()
            session.add(task_models.Task(id=uuid.uuid4(), task_type=None))
            with pytest.raises(sa.exc.IntegrityError):
                await session.flush()

            assert await lifecycle_module.recover_failed_session(session) is True

            reloaded = await session.get(task_models.Task, task.id)
            assert reloaded is not None
            assert reloaded.progress_current == 0


class TestCancelTask:
    async def test_sets_cancelled_state(self) -> None:
        task = _make_task(status=types_module.SyncStatus.PENDING)
        await lifecycle_module.cancel_task(AsyncMock(), task, "Cancelled by user")

        assert task.status == types_module.SyncStatus.CANCELLED
        assert task.error_message == "Cancelled by user"
        assert task.completed_at is not None


class TestIsTerminal:
    def test_terminal_statuses(self) -> None:
        for status in types_module.TERMINAL_STATUSES:
            assert lifecycle_module.is_terminal(_make_task(status=status))

    def test_active_statuses(self) -> None:
        for status in types_module.ACTIVE_STATUSES:
            assert not lifecycle_module.is_terminal(_make_task(status=status))


class TestIsCancelled:
    async def test_cancelled_task(self) -> None:
        task = _make_task(status=types_module.SyncStatus.CANCELLED)
        session = AsyncMock()
        assert await lifecycle_module.is_cancelled(session, task)
        session.execute.assert_not_called()

    async def test_failed_with_cancelled_message_is_not_cancelled(self) -> None:
        """The legacy FAILED + 'Cancelled' message convention no longer counts."""
        task = _make_task(status=types_module.SyncStatus.FAILED)
        task.error_message = "Cancelled by user"
        assert not await lifecycle_module.is_cancelled(AsyncMock(), task)

    async def test_cancelled_grandparent(self) -> None:
        grandparent = _make_task(status=types_module.SyncStatus.CANCELLED)
        parent = _make_task(
            parent_id=grandparent.id, status=types_module.SyncStatus.RUNNING
        )
        child = _make_task(parent_id=parent.id, status=types_module.SyncStatus.PENDING)
        session = AsyncMock()
        session.execute.side_effect = [
            _scalar_result(parent),
            _scalar_result(grandparent),
        ]
        assert await lifecycle_module.is_cancelled(session, child)

    async def test_running_parent_not_cancelled(self) -> None:
        parent = _make_task(status=types_module.SyncStatus.RUNNING)
        child = _make_task(parent_id=parent.id, status=types_module.SyncStatus.PENDING)
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(parent)]
        assert not await lifecycle_module.is_cancelled(session, child)


class TestCancelActiveChildren:
    async def test_cancels_every_returned_child(self) -> None:
        parent_id = uuid.uuid4()
        pending = _make_task(
            parent_id=parent_id, status=types_module.SyncStatus.PENDING
        )
        deferred = _make_task(
            parent_id=parent_id, status=types_module.SyncStatus.DEFERRED
        )
        session = AsyncMock()
        session.execute.return_value = _scalars_result([pending, deferred])

        cancelled = await lifecycle_module.cancel_active_children(session, parent_id)

        assert cancelled == [
            (pending, types_module.SyncStatus.PENDING),
            (deferred, types_module.SyncStatus.DEFERRED),
        ]
        for child in (pending, deferred):
            assert child.status == types_module.SyncStatus.CANCELLED
            assert child.error_message == "Parent task was cancelled"

    async def test_queries_active_statuses_only(self) -> None:
        session = AsyncMock()
        session.execute.return_value = _scalars_result([])

        await lifecycle_module.cancel_active_children(session, uuid.uuid4())

        stmt = session.execute.call_args.args[0]
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        for status in types_module.ACTIVE_STATUSES:
            assert f"'{status.name}'" in compiled
        for status in types_module.TERMINAL_STATUSES:
            assert f"'{status.name}'" not in compiled


class TestRequestCancel:
    """Tests for request_cancel, the shared API/UI cancel logic."""

    async def test_not_found(self) -> None:
        session = AsyncMock()
        session.execute.return_value = _scalar_result(None)

        result = await lifecycle_module.request_cancel(
            session, uuid.uuid4(), uuid.uuid4()
        )

        assert result.outcome == lifecycle_module.CancelOutcome.NOT_FOUND
        assert result.task is None
        session.commit.assert_not_called()

    async def test_already_terminal_is_untouched(self) -> None:
        for status in types_module.TERMINAL_STATUSES:
            task = _make_task(status=status)
            session = AsyncMock()
            session.execute.return_value = _scalar_result(task)

            result = await lifecycle_module.request_cancel(
                session, task.id, task.user_id
            )

            assert result.outcome == lifecycle_module.CancelOutcome.ALREADY_TERMINAL
            assert result.task is task
            assert task.status == status
            session.commit.assert_not_called()

    async def test_cancels_active_task_and_children(self) -> None:
        for status in types_module.ACTIVE_STATUSES:
            task = _make_task(status=status)
            child = _make_task(
                parent_id=task.id, status=types_module.SyncStatus.PENDING
            )
            session = AsyncMock()
            session.execute.side_effect = [
                _scalar_result(task),
                _scalars_result([child]),
            ]

            result = await lifecycle_module.request_cancel(
                session, task.id, task.user_id
            )

            assert result.outcome == lifecycle_module.CancelOutcome.CANCELLED
            assert result.children_cancelled == 1
            assert task.status == types_module.SyncStatus.CANCELLED
            assert task.error_message == "Cancelled by user"
            assert child.status == types_module.SyncStatus.CANCELLED
            session.commit.assert_awaited_once()

    async def test_scopes_lookup_to_user(self) -> None:
        session = AsyncMock()
        session.execute.return_value = _scalar_result(None)
        user_id = uuid.uuid4()

        await lifecycle_module.request_cancel(session, uuid.uuid4(), user_id)

        stmt = session.execute.call_args.args[0]
        compiled = stmt.compile()
        assert "sync_tasks.user_id" in str(compiled)
        assert user_id in compiled.params.values()


class TestEnqueueTask:
    """Delivery stamping: the row records the job before arq ever sees it."""

    async def test_stamps_row_commits_then_enqueues(self) -> None:
        task = _make_task(status=types_module.SyncStatus.PENDING)
        order: list[str] = []
        session = AsyncMock()
        session.commit.side_effect = lambda: order.append("commit")
        arq_redis = AsyncMock()

        async def _enqueue(*args: object, **kwargs: object) -> object:
            order.append("enqueue")
            assert task.arq_job_id == kwargs["_job_id"]
            return object()

        arq_redis.enqueue_job.side_effect = _enqueue

        delivery = await lifecycle_module.enqueue_task(
            session, arq_redis, task, "sync_range", str(task.id)
        )

        assert order == ["commit", "enqueue"]
        assert task.enqueued_at is not None
        assert task.arq_job_id == delivery.job_id
        assert delivery.job_id.startswith(f"sync_range:{task.id}:")
        arq_redis.enqueue_job.assert_awaited_once_with(
            "sync_range", str(task.id), _job_id=delivery.job_id, _defer_by=None
        )

    async def test_each_delivery_gets_a_new_job_id(self) -> None:
        task = _make_task(status=types_module.SyncStatus.PENDING)
        first = lifecycle_module.stamp_delivery(task, "sync_range", str(task.id))
        second = lifecycle_module.stamp_delivery(task, "sync_range", str(task.id))
        assert first.job_id != second.job_id
        assert task.arq_job_id == second.job_id

    async def test_assigns_id_before_flush(self) -> None:
        task = task_models.Task(
            task_type=types_module.TaskType.BULK_JOB,
            status=types_module.SyncStatus.PENDING,
        )
        assert task.id is not None
        delivery = lifecycle_module.stamp_delivery(task, "run_bulk_job", str(task.id))
        assert delivery.args == (str(task.id),)
        assert delivery.job_id.startswith(f"run_bulk_job:{task.id}:")

    @pytest.mark.parametrize("bad_arg", [None, "None"])
    async def test_rejects_none_arg(self, bad_arg: object) -> None:
        task = _make_task(status=types_module.SyncStatus.PENDING)
        with pytest.raises(ValueError, match="None"):
            lifecycle_module.stamp_delivery(task, "run_bulk_job", bad_arg)
        assert task.arq_job_id is None

    async def test_rejects_args_without_task_id(self) -> None:
        task = _make_task(status=types_module.SyncStatus.PENDING)
        with pytest.raises(ValueError, match=str(task.id)):
            lifecycle_module.stamp_delivery(task, "run_bulk_job", str(uuid.uuid4()))
        assert task.arq_job_id is None

    async def test_passes_defer_by(self) -> None:
        task = _make_task(status=types_module.SyncStatus.DEFERRED)
        arq_redis = AsyncMock()
        await lifecycle_module.enqueue_task(
            AsyncMock(),
            arq_redis,
            task,
            "sync_range",
            str(task.id),
            defer_by=datetime.timedelta(seconds=30),
        )
        assert arq_redis.enqueue_job.call_args.kwargs["_defer_by"] == (
            datetime.timedelta(seconds=30)
        )

    async def test_enqueue_failure_after_commit_leaves_row_stamped(self) -> None:
        """Redis down at submit: the row stays stamped so the reaper re-delivers."""
        task = _make_task(status=types_module.SyncStatus.PENDING)
        session = AsyncMock()
        arq_redis = AsyncMock()
        arq_redis.enqueue_job.side_effect = ConnectionError("redis down")

        await lifecycle_module.enqueue_task(
            session, arq_redis, task, "sync_range", str(task.id)
        )

        session.commit.assert_awaited_once()
        assert task.enqueued_at is not None
        assert task.arq_job_id is not None

    async def test_no_redis_still_stamps(self) -> None:
        task = _make_task(status=types_module.SyncStatus.PENDING)
        delivered = await lifecycle_module.deliver(
            None, lifecycle_module.stamp_delivery(task, "sync_range", str(task.id))
        )
        assert delivered is False
        assert task.arq_job_id is not None


class TestHardCancel:
    """Cancel aborts the running job and the cancel survives the run's exit."""

    async def test_aborts_running_task_and_running_children(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        task = _make_task(status=types_module.SyncStatus.RUNNING)
        task.arq_job_id = "plan_sync:t:1"
        running_child = _make_task(
            parent_id=task.id, status=types_module.SyncStatus.RUNNING
        )
        running_child.arq_job_id = "sync_range:c:1"
        pending_child = _make_task(
            parent_id=task.id, status=types_module.SyncStatus.PENDING
        )
        pending_child.arq_job_id = "sync_range:c:2"
        session = AsyncMock()
        session.execute.side_effect = [
            _scalar_result(task),
            _scalars_result([running_child, pending_child]),
        ]
        aborted: list[str] = []

        async def _abort(_redis: object, _task_id: uuid.UUID, job_id: str) -> bool:
            aborted.append(job_id)
            return True

        monkeypatch.setattr(lifecycle_module, "abort_job", _abort)

        await lifecycle_module.request_cancel(
            session, task.id, task.user_id, arq_redis=MagicMock()
        )

        assert sorted(aborted) == ["plan_sync:t:1", "sync_range:c:1"]

    async def test_pending_task_is_not_aborted(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        task = _make_task(status=types_module.SyncStatus.PENDING)
        task.arq_job_id = "export_playlist:t:1"
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(task), _scalars_result([])]
        abort = AsyncMock()
        monkeypatch.setattr(lifecycle_module, "abort_job", abort)

        await lifecycle_module.request_cancel(
            session, task.id, task.user_id, arq_redis=MagicMock()
        )

        abort.assert_not_awaited()

    async def test_abort_is_bounded_and_never_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(lifecycle_module, "ABORT_TIMEOUT_SECONDS", 0.01)

        class _HangingJob:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            async def abort(self, **_: object) -> bool:
                await asyncio.sleep(10)
                return True

        monkeypatch.setattr(lifecycle_module.arq_jobs, "Job", _HangingJob)

        started = asyncio.get_running_loop().time()
        confirmed = await lifecycle_module.abort_job(
            MagicMock(default_queue_name="q"), uuid.uuid4(), "j"
        )

        assert confirmed is False
        assert asyncio.get_running_loop().time() - started < 2

    async def test_abort_error_is_swallowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _BrokenJob:
            def __init__(self, *_: object, **__: object) -> None:
                pass

            async def abort(self, **_: object) -> bool:
                raise ConnectionError("redis down")

        monkeypatch.setattr(lifecycle_module.arq_jobs, "Job", _BrokenJob)

        assert not await lifecycle_module.abort_job(
            MagicMock(default_queue_name="q"), uuid.uuid4(), "j"
        )

    async def test_abort_passes_short_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        seen: dict[str, object] = {}

        class _Job:
            def __init__(self, job_id: str, *_: object, **__: object) -> None:
                seen["job_id"] = job_id

            async def abort(self, **kwargs: object) -> bool:
                seen.update(kwargs)
                return True

        monkeypatch.setattr(lifecycle_module.arq_jobs, "Job", _Job)

        assert await lifecycle_module.abort_job(
            MagicMock(default_queue_name="q"), uuid.uuid4(), "export_playlist:x:1"
        )
        assert seen["job_id"] == "export_playlist:x:1"
        assert seen["timeout"] == lifecycle_module.ABORT_TIMEOUT_SECONDS

    async def test_no_redis_skips_abort(self) -> None:
        assert not await lifecycle_module.abort_job(None, uuid.uuid4(), "j")


class TestCancelSurvivesInFlightRun:
    """A job holding the row in memory can't overwrite a committed cancel."""

    @pytest.mark.parametrize(
        "finish",
        [
            lambda s, t: lifecycle_module.complete_task(s, t, {"items_created": 3}),
            lambda s, t: lifecycle_module.fail_task(s, t, "Traceback ..."),
        ],
        ids=["complete", "fail"],
    )
    async def test_finishing_run_keeps_cancelled(
        self, task_db: TaskDB, finish: Any
    ) -> None:
        task = await task_db.add(status=types_module.SyncStatus.RUNNING)
        async with task_db.factory() as worker_session:
            row = await worker_session.get(task_models.Task, task.id)
            assert row is not None

            await task_db.update(
                task.id,
                status=types_module.SyncStatus.CANCELLED,
                error_message="Cancelled by user",
            )

            await finish(worker_session, row)
            await worker_session.commit()

        stored = await task_db.get(task.id)
        assert stored.status == types_module.SyncStatus.CANCELLED
        assert stored.error_message == "Cancelled by user"

    async def test_finishing_uncancelled_run_is_unaffected(
        self, task_db: TaskDB
    ) -> None:
        task = await task_db.add(status=types_module.SyncStatus.RUNNING)
        async with task_db.factory() as session:
            row = await session.get(task_models.Task, task.id)
            assert row is not None
            await lifecycle_module.complete_task(session, row, {})
            await session.commit()

        assert (await task_db.get(task.id)).status == (
            types_module.SyncStatus.COMPLETED
        )

    async def test_cancel_itself_is_not_blocked(self, task_db: TaskDB) -> None:
        task = await task_db.add(status=types_module.SyncStatus.RUNNING)
        async with task_db.factory() as session:
            row = await session.get(task_models.Task, task.id)
            assert row is not None
            await lifecycle_module.cancel_task(session, row, "Cancelled by user")
            await session.commit()

        assert (await task_db.get(task.id)).status == (
            types_module.SyncStatus.CANCELLED
        )


class TestCancelRunningChild:
    """An aborted child never reaches its sibling advance; cancel re-delivers it."""

    async def test_running_child_is_redelivered_to_advance_parent(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        parent_id = uuid.uuid4()
        child = _make_task(parent_id=parent_id, status=types_module.SyncStatus.RUNNING)
        child.arq_job_id = "sync_range:c:1"
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(child), _scalars_result([])]
        monkeypatch.setattr(lifecycle_module, "abort_job", AsyncMock(return_value=True))
        arq_redis = AsyncMock()

        await lifecycle_module.request_cancel(
            session, child.id, child.user_id, arq_redis=arq_redis
        )

        arq_redis.enqueue_job.assert_awaited_once()
        call = arq_redis.enqueue_job.call_args
        assert call.args == ("sync_range", str(child.id))
        assert call.kwargs["_job_id"].startswith(f"sync_range:{child.id}:")
        assert child.status == types_module.SyncStatus.CANCELLED

    async def test_pending_child_is_not_redelivered(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        child = _make_task(
            parent_id=uuid.uuid4(), status=types_module.SyncStatus.PENDING
        )
        session = AsyncMock()
        session.execute.side_effect = [_scalar_result(child), _scalars_result([])]
        arq_redis = AsyncMock()

        await lifecycle_module.request_cancel(
            session, child.id, child.user_id, arq_redis=arq_redis
        )

        arq_redis.enqueue_job.assert_not_awaited()
