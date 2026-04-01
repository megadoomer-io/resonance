"""Tests for the arq worker module."""

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.models.task as task_module
import resonance.types as types_module
import resonance.worker as worker_module

# ---------------------------------------------------------------------------
# Smoke tests: functions are callable and WorkerSettings is configured
# ---------------------------------------------------------------------------


class TestWorkerSettings:
    """Verify WorkerSettings has the expected attributes."""

    def test_functions_registered(self) -> None:
        coroutines = [f.coroutine for f in worker_module.WorkerSettings.functions]
        assert worker_module.plan_sync in coroutines
        assert worker_module.sync_range in coroutines

    def test_lifecycle_hooks(self) -> None:
        assert worker_module.WorkerSettings.on_startup is worker_module.startup
        assert worker_module.WorkerSettings.on_shutdown is worker_module.shutdown

    def test_max_jobs(self) -> None:
        assert worker_module.WorkerSettings.max_jobs == 10

    def test_job_timeout(self) -> None:
        assert worker_module.WorkerSettings.job_timeout == 300

    def test_redis_settings_is_instance(self) -> None:
        """redis_settings is an arq RedisSettings instance."""
        import arq.connections as arq_connections

        assert isinstance(
            worker_module.WorkerSettings.redis_settings,
            arq_connections.RedisSettings,
        )


class TestFunctionsCallable:
    """Verify that plan_sync and sync_range are async callables."""

    def test_plan_sync_is_coroutine_function(self) -> None:
        import inspect

        assert inspect.iscoroutinefunction(worker_module.plan_sync)

    def test_sync_range_is_coroutine_function(self) -> None:
        import inspect

        assert inspect.iscoroutinefunction(worker_module.sync_range)


# ---------------------------------------------------------------------------
# _check_parent_completion tests
# ---------------------------------------------------------------------------


def _make_task(
    *,
    task_id: uuid.UUID | None = None,
    parent_id: uuid.UUID | None = None,
    status: types_module.SyncStatus = types_module.SyncStatus.COMPLETED,
    result: dict[str, object] | None = None,
) -> task_module.SyncTask:
    """Create a SyncTask instance for testing."""
    task = task_module.SyncTask(
        id=task_id or uuid.uuid4(),
        user_id=uuid.uuid4(),
        service_connection_id=uuid.uuid4(),
        parent_id=parent_id,
        task_type=types_module.SyncTaskType.TIME_RANGE,
        status=status,
        result=result or {},
    )
    return task


class TestCheckParentCompletion:
    """Tests for _check_parent_completion."""

    @pytest.mark.asyncio
    async def test_no_parent_returns_early(self) -> None:
        """When task has no parent_id, nothing happens."""
        task = _make_task(parent_id=None)
        session = AsyncMock()
        log = MagicMock()

        await worker_module._check_parent_completion(session, task, log)

        # Should not query the database at all
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_pending_siblings_skips_parent_update(self) -> None:
        """When siblings are still pending, parent is not updated."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        # First query: count of non-terminal siblings returns 1
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 1
        session.execute.return_value = pending_result

        await worker_module._check_parent_completion(session, task, log)

        # Should have queried once (pending count) and not committed
        assert session.execute.call_count == 1
        session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_all_children_completed_marks_parent_completed(self) -> None:
        """When all children are done and none failed, parent is COMPLETED."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        # Build mock parent
        parent_task = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )

        # Child tasks for aggregation
        child1 = _make_task(
            parent_id=parent_id,
            result={"items_created": 10, "items_updated": 5},
        )
        child2 = _make_task(
            parent_id=parent_id,
            result={"items_created": 20, "items_updated": 3},
        )

        # Mock execute calls in order:
        # 1. pending count -> 0
        # 2. load parent task
        # 3. failed count -> 0
        # 4. load all children
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task

        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        children_scalars = MagicMock()
        children_scalars.all.return_value = [child1, child2]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        await worker_module._check_parent_completion(session, task, log)

        assert parent_task.status == types_module.SyncStatus.COMPLETED
        assert parent_task.result["items_created"] == 30
        assert parent_task.result["items_updated"] == 8
        assert parent_task.completed_at is not None
        session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_aggregates_results_from_children(self) -> None:
        """Parent result sums items_created/items_updated from children."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        parent_task = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )

        child1 = _make_task(
            parent_id=parent_id,
            result={"items_created": 15, "items_updated": 7},
        )
        child2 = _make_task(
            parent_id=parent_id,
            result={"items_created": 25, "items_updated": 13},
        )
        child3 = _make_task(
            parent_id=parent_id,
            result={"items_created": 5, "items_updated": 0},
        )

        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task

        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        children_scalars = MagicMock()
        children_scalars.all.return_value = [child1, child2, child3]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        await worker_module._check_parent_completion(session, task, log)

        assert parent_task.result["items_created"] == 45
        assert parent_task.result["items_updated"] == 20
        assert parent_task.result["children_completed"] == 3
        assert parent_task.result["children_failed"] == 0

    @pytest.mark.asyncio
    async def test_parent_completed_at_is_set(self) -> None:
        """Parent completed_at timestamp is set when all children finish."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        parent_task = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )
        assert parent_task.completed_at is None

        child1 = _make_task(
            parent_id=parent_id,
            result={"items_created": 1, "items_updated": 0},
        )

        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task

        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        children_scalars = MagicMock()
        children_scalars.all.return_value = [child1]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        await worker_module._check_parent_completion(session, task, log)

        assert parent_task.completed_at is not None
        assert parent_task.status == types_module.SyncStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_no_op_when_parent_not_found(self) -> None:
        """Gracefully handles parent_id pointing to a nonexistent record."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        # pending count -> 0 (all children done)
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        # parent lookup returns None
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [
            pending_result,
            parent_result,
        ]

        await worker_module._check_parent_completion(session, task, log)

        # Should not commit since parent was not found
        session.commit.assert_not_called()
        # Should have queried exactly twice (pending count + parent lookup)
        assert session.execute.call_count == 2

    @pytest.mark.asyncio
    async def test_failed_children_marks_parent_failed(self) -> None:
        """When any child failed, parent is FAILED with error message."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        parent_task = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )

        child1 = _make_task(
            parent_id=parent_id,
            status=types_module.SyncStatus.COMPLETED,
            result={"items_created": 10, "items_updated": 0},
        )
        child2 = _make_task(
            parent_id=parent_id,
            status=types_module.SyncStatus.FAILED,
            result={},
        )

        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task

        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 1

        children_scalars = MagicMock()
        children_scalars.all.return_value = [child1, child2]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        await worker_module._check_parent_completion(session, task, log)

        assert parent_task.status == types_module.SyncStatus.FAILED
        assert parent_task.error_message == "1 child task(s) failed"
        assert parent_task.result["items_created"] == 10
        assert parent_task.result["children_failed"] == 1
        session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# plan_sync tests
# ---------------------------------------------------------------------------


def _mock_session_factory(session: AsyncMock) -> MagicMock:
    """Create a mock async_sessionmaker that yields the given session.

    The real async_sessionmaker.__call__ returns an async context manager
    (not a coroutine), so we use MagicMock with __aenter__/__aexit__.
    """
    ctx_manager = AsyncMock()
    ctx_manager.__aenter__.return_value = session
    ctx_manager.__aexit__.return_value = False
    factory = MagicMock()
    factory.return_value = ctx_manager
    return factory


class TestPlanSync:
    """Tests for plan_sync."""

    @pytest.mark.asyncio
    async def test_task_not_found_logs_error(self) -> None:
        """When the task doesn't exist, logs an error and returns."""
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        session.execute.return_value = result_mock

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "settings": MagicMock(),
            "connector_registry": MagicMock(),
            "arq_redis": AsyncMock(),
        }

        await worker_module.plan_sync(ctx, str(uuid.uuid4()))

        # Should not have committed (no task found)
        session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# sync_range tests
# ---------------------------------------------------------------------------


class TestSyncRange:
    """Tests for sync_range."""

    @pytest.mark.asyncio
    async def test_task_not_found_logs_error(self) -> None:
        """When the task doesn't exist, logs an error and returns."""
        session = AsyncMock()
        result_mock = MagicMock()
        result_mock.scalar_one_or_none.return_value = None
        session.execute.return_value = result_mock

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "settings": MagicMock(),
            "connector_registry": MagicMock(),
            "arq_redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(uuid.uuid4()))

        session.commit.assert_not_called()
