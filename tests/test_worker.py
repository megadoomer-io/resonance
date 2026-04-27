"""Tests for the arq worker module."""

import datetime
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.models.task as task_module
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.types as types_module
import resonance.worker as worker_module

# ---------------------------------------------------------------------------
# WorkerContext TypedDict tests
# ---------------------------------------------------------------------------


class TestWorkerContext:
    """Tests for the WorkerContext TypedDict."""

    def test_has_expected_keys(self) -> None:
        annotations = worker_module.WorkerContext.__annotations__
        expected_keys = {
            "settings",
            "engine",
            "session_factory",
            "connector_registry",
            "strategies",
            "redis",
        }
        assert set(annotations.keys()) == expected_keys


# ---------------------------------------------------------------------------
# Smoke tests: functions are callable and WorkerSettings is configured
# ---------------------------------------------------------------------------


class TestWorkerSettings:
    """Verify WorkerSettings has the expected attributes."""

    def test_functions_registered(self) -> None:
        funcs = worker_module.WorkerSettings.functions
        assert len(funcs) == 7
        names = {f.name for f in funcs}
        assert names == {
            "plan_sync",
            "sync_range",
            "run_bulk_job",
            "sync_calendar_feed",
            "generate_playlist",
            "discover_tracks_for_artist",
            "score_and_build_playlist",
        }

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
) -> task_module.Task:
    """Create a Task instance for testing."""
    task = task_module.Task(
        id=task_id or uuid.uuid4(),
        user_id=uuid.uuid4(),
        service_connection_id=uuid.uuid4(),
        parent_id=parent_id,
        task_type=types_module.TaskType.TIME_RANGE,
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

        await worker_module._check_parent_completion(session, task, AsyncMock(), log)

        # Should not query the database at all
        session.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_pending_siblings_enqueues_next(self) -> None:
        """When siblings are still pending, next one is enqueued."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        next_task = _make_task(
            parent_id=parent_id,
            status=types_module.SyncStatus.PENDING,
        )
        session = AsyncMock()
        log = MagicMock()
        arq_redis = AsyncMock()

        # 1. pending count -> 1
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 1

        # 2. next pending sibling
        next_result = MagicMock()
        next_result.scalar_one_or_none.return_value = next_task

        session.execute.side_effect = [pending_result, next_result]

        await worker_module._check_parent_completion(session, task, arq_redis, log)

        # Should enqueue the next sibling, not update parent
        arq_redis.enqueue_job.assert_called_once_with(
            "sync_range", str(next_task.id), _job_id=f"sync_range:{next_task.id}"
        )
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

        await worker_module._check_parent_completion(session, task, AsyncMock(), log)

        assert parent_task.status == types_module.SyncStatus.COMPLETED
        assert parent_task.result["items_created"] == 30
        assert parent_task.result["items_updated"] == 8
        assert parent_task.completed_at is not None
        # Two commits: parent completion + post-sync entity resolution task
        assert session.commit.call_count == 2

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

        await worker_module._check_parent_completion(session, task, AsyncMock(), log)

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

        await worker_module._check_parent_completion(session, task, AsyncMock(), log)

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

        await worker_module._check_parent_completion(session, task, AsyncMock(), log)

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

        await worker_module._check_parent_completion(session, task, AsyncMock(), log)

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
            "strategies": {},
            "redis": AsyncMock(),
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
            "strategies": {},
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(uuid.uuid4()))

        session.commit.assert_not_called()


# ---------------------------------------------------------------------------
# Strategy dispatch tests
# ---------------------------------------------------------------------------


class TestPlanSyncStrategyDispatch:
    """Tests for plan_sync strategy dispatch."""

    @pytest.mark.asyncio
    async def test_empty_plan_marks_task_completed(self) -> None:
        """When strategy.plan() returns [], task is COMPLETED with zero counts."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.PENDING,
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id

        session = AsyncMock()

        # 1. _load_task returns the task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        session.execute.side_effect = [task_result, conn_result]

        # Strategy returns empty plan
        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.plan.return_value = []

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "strategies": {types_module.ServiceType.SPOTIFY: mock_strategy},
            "connector_registry": mock_connector_registry,
            "redis": AsyncMock(),
        }

        await worker_module.plan_sync(ctx, str(task_id))

        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result == {"items_created": 0, "items_updated": 0}
        assert task.completed_at is not None

    @pytest.mark.asyncio
    async def test_no_strategy_marks_task_failed(self) -> None:
        """When no strategy exists for the service type, task is FAILED."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.PENDING,
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id

        session = AsyncMock()

        # 1. _load_task returns the task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        session.execute.side_effect = [task_result, conn_result]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "strategies": {},  # Empty — no strategy for SPOTIFY
            "connector_registry": MagicMock(),
            "redis": AsyncMock(),
        }

        await worker_module.plan_sync(ctx, str(task_id))

        assert task.status == types_module.SyncStatus.FAILED
        assert "No sync strategy" in str(task.error_message)
        assert task.completed_at is not None


# ---------------------------------------------------------------------------
# sync_range watermark resume on retry
# ---------------------------------------------------------------------------


class TestSyncRangeWatermarkResume:
    """Tests for watermark resume when sync_range retries a RUNNING task."""

    @pytest.mark.asyncio
    async def test_retry_injects_watermark_before_execute(self) -> None:
        """RUNNING task (arq retry) gets max_ts from watermark."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        # Task is already RUNNING — simulates arq retry after crash
        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.RUNNING,
            params={"username": "testuser", "min_ts": 1700000000},
            progress_current=15000,
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.LISTENBRAINZ
        connection.id = conn_id
        connection.sync_watermark = {
            "listens": {
                "newest_synced_at": 1712000000,
                "oldest_synced_at": 1700500000,
            }
        }

        session = AsyncMock()

        # 1. _load_task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        # 3-6. _check_parent_completion mocks
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0
        parent_task = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task
        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0
        children_scalars = MagicMock()
        children_scalars.all.return_value = [task]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            task_result,
            conn_result,
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        # Capture what params the strategy sees at execute time
        captured_params: dict[str, object] = {}

        async def capture_execute(
            _session: Any,
            _task: Any,
            _connector: Any,
            _connection: Any,
        ) -> dict[str, object]:
            captured_params.update(_task.params)
            return {"items_created": 0, "items_updated": 0}

        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "parallel"
        mock_strategy.execute.side_effect = capture_execute

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {
                types_module.ServiceType.LISTENBRAINZ: mock_strategy,
            },
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        # Strategy should have received max_ts and items_so_far
        assert captured_params["max_ts"] == 1700500000
        assert captured_params["items_so_far"] == 15000
        assert captured_params["username"] == "testuser"
        assert captured_params["min_ts"] == 1700000000

    @pytest.mark.asyncio
    async def test_pending_task_does_not_inject_watermark(self) -> None:
        """PENDING task (normal first run) does not get watermark."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        # Task is PENDING — normal first execution
        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"username": "testuser", "min_ts": 1700000000},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.LISTENBRAINZ
        connection.id = conn_id
        connection.sync_watermark = {
            "listens": {
                "newest_synced_at": 1712000000,
                "oldest_synced_at": 1700500000,
            }
        }

        session = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0
        parent_task = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task
        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0
        children_scalars = MagicMock()
        children_scalars.all.return_value = [task]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            task_result,
            conn_result,
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        captured_params: dict[str, object] = {}

        async def capture_execute(
            _session: Any,
            _task: Any,
            _connector: Any,
            _connection: Any,
        ) -> dict[str, object]:
            captured_params.update(_task.params)
            return {"items_created": 0, "items_updated": 0}

        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "parallel"
        mock_strategy.execute.side_effect = capture_execute

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {
                types_module.ServiceType.LISTENBRAINZ: mock_strategy,
            },
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        # Should NOT have max_ts — this is a fresh execution
        assert "max_ts" not in captured_params


# ---------------------------------------------------------------------------
# Deferral tests
# ---------------------------------------------------------------------------


class TestSyncRangeDeferral:
    """Tests for DeferRequest handling in sync_range."""

    @pytest.mark.asyncio
    async def test_defer_request_sets_deferred_status(self) -> None:
        """When strategy raises DeferRequest, task becomes DEFERRED."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": "saved_tracks"},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id

        session = AsyncMock()

        # 1. _load_task returns the task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        # 3. _check_parent_completion: pending count (DEFERRED is non-terminal)
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 1

        # 4. next pending sibling -> None (only this deferred task is non-terminal)
        next_result = MagicMock()
        next_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [
            task_result,
            conn_result,
            pending_result,
            next_result,
        ]

        # Strategy that raises DeferRequest
        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.side_effect = sync_base.DeferRequest(
            retry_after=120.0,
            resume_params={"data_type": "saved_tracks", "offset": 50},
        )

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        arq_redis = AsyncMock()

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.SPOTIFY: mock_strategy},
            "redis": arq_redis,
        }

        await worker_module.sync_range(ctx, str(task_id))

        assert task.status == types_module.SyncStatus.DEFERRED
        assert task.deferred_until is not None
        assert task.params["offset"] == 50
        # Should have enqueued a deferred re-run
        arq_redis.enqueue_job.assert_any_call(
            "sync_range",
            str(task_id),
            _job_id=f"sync_range:{task_id}",
            _defer_by=datetime.timedelta(seconds=120.0),
        )


# ---------------------------------------------------------------------------
# ShutdownRequest tests
# ---------------------------------------------------------------------------


class TestSyncRangeShutdown:
    """Tests for ShutdownRequest handling in sync_range."""

    @pytest.mark.asyncio
    async def test_shutdown_request_checkpoints_task_as_pending(self) -> None:
        """ShutdownRequest reverts task to PENDING with resume params."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": "saved_tracks"},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id

        session = AsyncMock()

        # 1. _load_task returns the task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        session.execute.side_effect = [task_result, conn_result]

        # Strategy that raises ShutdownRequest
        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.side_effect = sync_base.ShutdownRequest(
            resume_params={"data_type": "saved_tracks", "offset": 50},
        )

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        arq_redis = AsyncMock()

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.SPOTIFY: mock_strategy},
            "redis": arq_redis,
        }

        await worker_module.sync_range(ctx, str(task_id))

        assert task.status == types_module.SyncStatus.PENDING
        assert task.params["offset"] == 50
        assert task.params["data_type"] == "saved_tracks"
        assert task.started_at is None
        session.commit.assert_called()
        # Should NOT enqueue anything — task stays PENDING for next worker startup
        arq_redis.enqueue_job.assert_not_called()


# ---------------------------------------------------------------------------
# Watermark write-back tests
# ---------------------------------------------------------------------------


class TestSyncRangeWatermarkWrite:
    """Tests for watermark write-back after successful sync_range."""

    @pytest.mark.asyncio
    async def test_writes_watermark_to_connection(self) -> None:
        """On success, watermark from result is saved to connection.sync_watermark."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": "saved_tracks"},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id
        connection.sync_watermark = {}

        session = AsyncMock()

        # 1. _load_task returns the task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        # 3. _check_parent_completion: pending count
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        # 4. load parent task
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )

        # 5. failed count
        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        # 6. children
        children_scalars = MagicMock()
        children_scalars.all.return_value = [task]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            task_result,
            conn_result,
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        watermark_data = {"last_offset": 100, "snapshot_id": "abc123"}
        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.return_value = {
            "items_created": 50,
            "items_updated": 10,
            "watermark": watermark_data,
        }

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.SPOTIFY: mock_strategy},
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        assert connection.sync_watermark == {"saved_tracks": watermark_data}

    @pytest.mark.asyncio
    async def test_no_watermark_in_result_skips_write(self) -> None:
        """When result has no 'watermark' key, connection is not modified."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": "saved_tracks"},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id
        connection.sync_watermark = {}

        session = AsyncMock()

        # 1. _load_task returns the task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        # 3. _check_parent_completion: pending count
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        # 4. load parent task
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )

        # 5. failed count
        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        # 6. children
        children_scalars = MagicMock()
        children_scalars.all.return_value = [task]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            task_result,
            conn_result,
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.return_value = {
            "items_created": 50,
            "items_updated": 10,
        }

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.SPOTIFY: mock_strategy},
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        # sync_watermark should remain empty — no watermark in result
        assert connection.sync_watermark == {}

    @pytest.mark.asyncio
    async def test_listenbrainz_uses_listens_key(self) -> None:
        """ListenBrainz tasks write watermark under 'listens' key."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={},  # ListenBrainz tasks have no data_type param
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.LISTENBRAINZ
        connection.id = conn_id
        connection.sync_watermark = {}

        session = AsyncMock()

        # 1. _load_task returns the task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        # 3. _check_parent_completion: pending count
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        # 4. load parent task
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = _make_task(
            task_id=parent_id,
            status=types_module.SyncStatus.RUNNING,
        )

        # 5. failed count
        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        # 6. children
        children_scalars = MagicMock()
        children_scalars.all.return_value = [task]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            task_result,
            conn_result,
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        watermark_data = {"max_ts": 1700000000}
        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.return_value = {
            "items_created": 100,
            "items_updated": 0,
            "watermark": watermark_data,
        }

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.LISTENBRAINZ: mock_strategy},
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        assert connection.sync_watermark == {"listens": watermark_data}


# ---------------------------------------------------------------------------
# Orphaned task re-enqueue tests
# ---------------------------------------------------------------------------


class TestReenqueueOrphanedTasks:
    """Tests for _reenqueue_orphaned_tasks."""

    @pytest.mark.asyncio
    async def test_reenqueues_pending_sync_job(self) -> None:
        """PENDING SYNC_JOB is re-enqueued as plan_sync."""
        task = _make_task(
            status=types_module.SyncStatus.PENDING,
        )
        task.task_type = types_module.TaskType.SYNC_JOB

        session = AsyncMock()
        # 1. PENDING tasks query
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = [task]
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars
        # 2. DEFERRED tasks query
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query (children of terminal parents)
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — none
        running_scalars = MagicMock()
        running_scalars.all.return_value = []
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        # 5. Children count for SYNC_JOB -> 0 (no children yet)
        children_count_result = MagicMock()
        children_count_result.scalar_one.return_value = 0

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
            children_count_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        arq_redis.enqueue_job.assert_called_once_with(
            "plan_sync", str(task.id), _job_id=f"plan_sync:{task.id}"
        )

    @pytest.mark.asyncio
    async def test_reenqueues_pending_time_range(self) -> None:
        """PENDING TIME_RANGE is re-enqueued as sync_range."""
        task = _make_task(
            status=types_module.SyncStatus.PENDING,
        )
        # _make_task already sets task_type to TIME_RANGE

        session = AsyncMock()
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = [task]
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query (children of terminal parents)
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — none
        running_scalars = MagicMock()
        running_scalars.all.return_value = []
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        arq_redis.enqueue_job.assert_called_once_with(
            "sync_range", str(task.id), _job_id=f"sync_range:{task.id}"
        )

    @pytest.mark.asyncio
    async def test_reenqueues_expired_deferred_task(self) -> None:
        """DEFERRED task with past deferred_until is reset and re-enqueued."""
        task = _make_task(
            status=types_module.SyncStatus.DEFERRED,
        )
        task.deferred_until = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)

        session = AsyncMock()
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = []
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = [task]
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query (children of terminal parents)
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — none
        running_scalars = MagicMock()
        running_scalars.all.return_value = []
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        assert task.status == types_module.SyncStatus.PENDING
        arq_redis.enqueue_job.assert_called_once_with(
            "sync_range", str(task.id), _job_id=f"sync_range:{task.id}"
        )
        session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_skips_sync_job_that_already_has_children(self) -> None:
        """PENDING SYNC_JOB with existing children is not re-enqueued."""
        parent = _make_task(status=types_module.SyncStatus.PENDING)
        parent.task_type = types_module.TaskType.SYNC_JOB

        session = AsyncMock()

        # 1. PENDING tasks query — returns the parent SYNC_JOB
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = [parent]
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars

        # 2. DEFERRED tasks query — none
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query — none
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — none
        running_scalars = MagicMock()
        running_scalars.all.return_value = []
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        # 5. Children check for the SYNC_JOB — returns count=1
        children_count_result = MagicMock()
        children_count_result.scalar_one.return_value = 1

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
            children_count_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        # Should NOT re-enqueue the parent (it already has children)
        arq_redis.enqueue_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_reenqueues_running_task(self) -> None:
        """RUNNING tasks should be reset to PENDING and re-enqueued."""
        task = _make_task(
            status=types_module.SyncStatus.RUNNING,
        )
        # _make_task sets task_type to TIME_RANGE by default
        task.started_at = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)

        session = AsyncMock()

        # 1. PENDING tasks query — none
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = []
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars

        # 2. DEFERRED tasks query — none
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query — none
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — returns the stuck task
        running_scalars = MagicMock()
        running_scalars.all.return_value = [task]
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        # 5. Connection query for watermark resume — no watermark
        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.sync_watermark = {}
        conn_result = MagicMock()
        conn_result.scalar_one_or_none.return_value = connection

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
            conn_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        # Task should be reset to PENDING with started_at preserved
        assert task.status == types_module.SyncStatus.PENDING
        assert task.started_at == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        # Should be re-enqueued as sync_range (TIME_RANGE task)
        arq_redis.enqueue_job.assert_called_once_with(
            "sync_range", str(task.id), _job_id=f"sync_range:{task.id}"
        )
        session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_running_listenbrainz_task_resumes_from_watermark(self) -> None:
        """RUNNING ListenBrainz task gets max_ts and items_so_far injected."""
        conn_id = uuid.uuid4()
        task = _make_task(
            status=types_module.SyncStatus.RUNNING,
        )
        task.service_connection_id = conn_id
        task.started_at = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        task.progress_current = 29000
        task.params = {"username": "testuser", "min_ts": 1700000000}

        session = AsyncMock()

        # 1-3: empty queries for PENDING, DEFERRED, stale
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = []
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — returns the stuck task
        running_scalars = MagicMock()
        running_scalars.all.return_value = [task]
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        # 5. Connection query — ListenBrainz with watermark showing page 29
        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.LISTENBRAINZ
        connection.sync_watermark = {
            "listens": {
                "newest_synced_at": 1712000000,
                "oldest_synced_at": 1700500000,
            }
        }
        conn_result = MagicMock()
        conn_result.scalar_one_or_none.return_value = connection

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
            conn_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        # max_ts should be injected from watermark's oldest_synced_at
        assert task.params["max_ts"] == 1700500000
        # items_so_far should be injected from progress_current
        assert task.params["items_so_far"] == 29000
        # Original params preserved
        assert task.params["username"] == "testuser"
        assert task.params["min_ts"] == 1700000000
        assert task.status == types_module.SyncStatus.PENDING
        # started_at preserved for UI continuity
        assert task.started_at == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)

    @pytest.mark.asyncio
    async def test_running_listenbrainz_task_without_watermark_unchanged(self) -> None:
        """RUNNING ListenBrainz task with no watermark keeps original params."""
        conn_id = uuid.uuid4()
        task = _make_task(
            status=types_module.SyncStatus.RUNNING,
        )
        task.service_connection_id = conn_id
        task.started_at = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        task.params = {"username": "testuser", "min_ts": None}

        session = AsyncMock()

        pending_scalars = MagicMock()
        pending_scalars.all.return_value = []
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        running_scalars = MagicMock()
        running_scalars.all.return_value = [task]
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        # Connection with empty watermark
        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.LISTENBRAINZ
        connection.sync_watermark = {}
        conn_result = MagicMock()
        conn_result.scalar_one_or_none.return_value = connection

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
            conn_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        # Params should be unchanged — no watermark to inject
        assert "max_ts" not in task.params
        assert task.params == {"username": "testuser", "min_ts": None}

    @pytest.mark.asyncio
    async def test_running_task_connection_not_found_still_reenqueues(self) -> None:
        """Connection not found still re-enqueues without watermark."""
        task = _make_task(
            status=types_module.SyncStatus.RUNNING,
        )
        task.started_at = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        task.params = {"username": "testuser", "min_ts": None}

        session = AsyncMock()

        pending_scalars = MagicMock()
        pending_scalars.all.return_value = []
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        running_scalars = MagicMock()
        running_scalars.all.return_value = [task]
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        # Connection not found
        conn_result = MagicMock()
        conn_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
            conn_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        # Should still be re-enqueued, just without watermark injection
        assert task.status == types_module.SyncStatus.PENDING
        arq_redis.enqueue_job.assert_called_once_with(
            "sync_range", str(task.id), _job_id=f"sync_range:{task.id}"
        )

    @pytest.mark.asyncio
    async def test_pending_calendar_sync_reenqueued(self) -> None:
        """PENDING CALENDAR_SYNC task is re-enqueued as sync_calendar_feed."""
        connection_id = uuid.uuid4()
        task = _make_task(
            status=types_module.SyncStatus.PENDING,
        )
        task.task_type = types_module.TaskType.CALENDAR_SYNC
        task.service_connection_id = connection_id
        task.params = {}

        session = AsyncMock()

        # 1. PENDING tasks query — returns the calendar sync task
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = [task]
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars

        # 2. DEFERRED tasks query — none
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query — none
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — none
        running_scalars = MagicMock()
        running_scalars.all.return_value = []
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        arq_redis.enqueue_job.assert_called_once_with(
            "sync_calendar_feed",
            str(connection_id),
            str(task.id),
            _job_id=f"sync_calendar_feed:{task.id}",
        )

    @pytest.mark.asyncio
    async def test_running_calendar_sync_reset_and_reenqueued(self) -> None:
        """RUNNING CALENDAR_SYNC task is reset to PENDING and re-enqueued."""
        connection_id = uuid.uuid4()
        task = _make_task(
            status=types_module.SyncStatus.RUNNING,
        )
        task.task_type = types_module.TaskType.CALENDAR_SYNC
        task.service_connection_id = connection_id
        task.params = {}
        task.started_at = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)

        session = AsyncMock()

        # 1. PENDING tasks query — none
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = []
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars

        # 2. DEFERRED tasks query — none
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query — none
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — returns the stuck CALENDAR_SYNC task
        running_scalars = MagicMock()
        running_scalars.all.return_value = [task]
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        # No watermark resume needed for CALENDAR_SYNC (not TIME_RANGE)

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        # Task should be reset to PENDING
        assert task.status == types_module.SyncStatus.PENDING
        # started_at preserved for UI continuity
        assert task.started_at == datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        # Should be re-enqueued with correct args
        arq_redis.enqueue_job.assert_called_once_with(
            "sync_calendar_feed",
            str(connection_id),
            str(task.id),
            _job_id=f"sync_calendar_feed:{task.id}",
        )
        session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_pending_bulk_job_reenqueued(self) -> None:
        """PENDING BULK_JOB task is re-enqueued as run_bulk_job."""
        task = _make_task(
            status=types_module.SyncStatus.PENDING,
        )
        task.task_type = types_module.TaskType.BULK_JOB
        task.params = {"operation": "dedup_all"}

        session = AsyncMock()

        # 1. PENDING tasks query — returns the bulk job task
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = [task]
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars

        # 2. DEFERRED tasks query — none
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query — none
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — none
        running_scalars = MagicMock()
        running_scalars.all.return_value = []
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        arq_redis.enqueue_job.assert_called_once_with(
            "run_bulk_job",
            str(task.id),
            _job_id=f"run_bulk_job:{task.id}",
        )

    @pytest.mark.asyncio
    async def test_no_orphans_does_nothing(self) -> None:
        """No orphaned tasks means no enqueue calls."""
        session = AsyncMock()
        pending_scalars = MagicMock()
        pending_scalars.all.return_value = []
        pending_result = MagicMock()
        pending_result.scalars.return_value = pending_scalars
        deferred_scalars = MagicMock()
        deferred_scalars.all.return_value = []
        deferred_result = MagicMock()
        deferred_result.scalars.return_value = deferred_scalars

        # 3. Stale tasks query (children of terminal parents)
        stale_scalars = MagicMock()
        stale_scalars.all.return_value = []
        stale_result = MagicMock()
        stale_result.scalars.return_value = stale_scalars

        # 4. RUNNING tasks query — none
        running_scalars = MagicMock()
        running_scalars.all.return_value = []
        running_result = MagicMock()
        running_result.scalars.return_value = running_scalars

        session.execute.side_effect = [
            pending_result,
            deferred_result,
            stale_result,
            running_result,
        ]

        arq_redis = AsyncMock()

        await worker_module._reenqueue_orphaned_tasks(
            _mock_session_factory(session), arq_redis
        )

        arq_redis.enqueue_job.assert_not_called()


# ---------------------------------------------------------------------------
# Worker shutdown tests
# ---------------------------------------------------------------------------


class TestWorkerShutdown:
    """Tests for worker shutdown hook."""

    @pytest.mark.asyncio
    async def test_shutdown_sets_event(self) -> None:
        """Shutdown sets the shutdown_requested event."""
        sync_base.shutdown_requested.clear()
        try:
            mock_engine = AsyncMock()
            ctx: dict[str, Any] = {
                "engine": mock_engine,
                "redis": AsyncMock(),
            }
            await worker_module.shutdown(ctx)
            assert sync_base.shutdown_requested.is_set()
        finally:
            sync_base.shutdown_requested.clear()

    @pytest.mark.asyncio
    async def test_shutdown_disposes_engine(self) -> None:
        """Shutdown disposes the database engine."""
        sync_base.shutdown_requested.clear()
        try:
            mock_engine = AsyncMock()
            ctx: dict[str, Any] = {
                "engine": mock_engine,
                "redis": AsyncMock(),
            }
            await worker_module.shutdown(ctx)
            mock_engine.dispose.assert_awaited_once()
        finally:
            sync_base.shutdown_requested.clear()


# ---------------------------------------------------------------------------
# generate_playlist tests
# ---------------------------------------------------------------------------


class TestGeneratePlaylist:
    """Tests for the generate_playlist worker function."""

    @pytest.mark.asyncio
    async def test_creates_discovery_tasks_for_artists(self) -> None:
        """Artists with no library tracks get TRACK_DISCOVERY child tasks."""
        task_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        artist1_id = uuid.uuid4()
        artist2_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        # 1. _load_task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load profile
        import resonance.models.generator as generator_models

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        profile.parameter_values = {}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        # 3. Query EventArtist — returns two confirmed artists
        import resonance.models.concert as concert_models

        ea1 = MagicMock(spec=concert_models.EventArtist)
        ea1.artist_id = artist1_id
        ea2 = MagicMock(spec=concert_models.EventArtist)
        ea2.artist_id = artist2_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea1, ea2]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        # 4. Query EventArtistCandidate with ACCEPTED — none
        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        # 5. Query Artist objects for service_links
        import resonance.models.music as music_models

        mock_artist1 = MagicMock(spec=music_models.Artist)
        mock_artist1.id = artist1_id
        mock_artist1.name = "Artist One"
        mock_artist1.service_links = {"listenbrainz": "mb-id-1"}
        mock_artist2 = MagicMock(spec=music_models.Artist)
        mock_artist2.id = artist2_id
        mock_artist2.name = "Artist Two"
        mock_artist2.service_links = {"listenbrainz": "mb-id-2"}
        artist_scalars = MagicMock()
        artist_scalars.all.return_value = [mock_artist1, mock_artist2]
        artist_result = MagicMock()
        artist_result.scalars.return_value = artist_scalars

        # 6-7. ListeningEvent count per artist — both return 0 (no library tracks)
        listen_count_1 = MagicMock()
        listen_count_1.scalar_one.return_value = 0
        listen_count_2 = MagicMock()
        listen_count_2.scalar_one.return_value = 0

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_count_1,
            listen_count_2,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": MagicMock(),
            "redis": arq_redis,
        }

        await worker_module.generate_playlist(ctx, str(task_id))

        # Should have called session.add for: 2 discovery tasks + 1 scoring task = 3
        assert session.add.call_count == 3

        # Verify the added tasks
        added_tasks = [call.args[0] for call in session.add.call_args_list]
        discovery_tasks = [
            t
            for t in added_tasks
            if t.task_type == types_module.TaskType.TRACK_DISCOVERY
        ]
        scoring_tasks = [
            t for t in added_tasks if t.task_type == types_module.TaskType.TRACK_SCORING
        ]
        assert len(discovery_tasks) == 2
        assert len(scoring_tasks) == 1

    @pytest.mark.asyncio
    async def test_enqueues_first_discovery_task(self) -> None:
        """Only the first discovery task is enqueued (sequential dispatch)."""
        task_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        artist_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        # 1. _load_task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load profile
        import resonance.models.generator as generator_models

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        profile.parameter_values = {}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        # 3. One confirmed EventArtist
        import resonance.models.concert as concert_models

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = artist_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        # 4. No accepted candidates
        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        # 5. Artist objects
        import resonance.models.music as music_models

        mock_artist = MagicMock(spec=music_models.Artist)
        mock_artist.id = artist_id
        mock_artist.name = "Test Artist"
        mock_artist.service_links = None
        artist_scalars = MagicMock()
        artist_scalars.all.return_value = [mock_artist]
        artist_result = MagicMock()
        artist_result.scalars.return_value = artist_scalars

        # 6. No library tracks
        listen_count = MagicMock()
        listen_count.scalar_one.return_value = 0

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_count,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": MagicMock(),
            "redis": arq_redis,
        }

        await worker_module.generate_playlist(ctx, str(task_id))

        # Should enqueue only the first child (the discovery task)
        assert arq_redis.enqueue_job.call_count == 1
        call_args = arq_redis.enqueue_job.call_args
        assert call_args.args[0] == "discover_tracks_for_artist"

    @pytest.mark.asyncio
    async def test_no_discovery_when_all_artists_have_library_tracks(self) -> None:
        """When all artists have library tracks, skip discovery, enqueue scoring."""
        task_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        artist_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        # 1. _load_task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load profile
        import resonance.models.generator as generator_models

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        profile.parameter_values = {}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        # 3. One confirmed EventArtist
        import resonance.models.concert as concert_models

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = artist_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        # 4. No accepted candidates
        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        # 5. Artist objects
        import resonance.models.music as music_models

        mock_artist = MagicMock(spec=music_models.Artist)
        mock_artist.id = artist_id
        mock_artist.name = "Known Artist"
        mock_artist.service_links = None
        artist_scalars = MagicMock()
        artist_scalars.all.return_value = [mock_artist]
        artist_result = MagicMock()
        artist_result.scalars.return_value = artist_scalars

        # 6. Artist has plenty of library tracks
        listen_count = MagicMock()
        listen_count.scalar_one.return_value = 25

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_count,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": MagicMock(),
            "redis": arq_redis,
        }

        await worker_module.generate_playlist(ctx, str(task_id))

        # Should create only 1 child task (scoring, no discovery)
        assert session.add.call_count == 1
        added_task = session.add.call_args.args[0]
        assert added_task.task_type == types_module.TaskType.TRACK_SCORING

        # Should directly enqueue the scoring task
        assert arq_redis.enqueue_job.call_count == 1
        call_args = arq_redis.enqueue_job.call_args
        assert call_args.args[0] == "score_and_build_playlist"


# ---------------------------------------------------------------------------
# discover_tracks_for_artist tests
# ---------------------------------------------------------------------------


class TestDiscoverTracksForArtist:
    """Tests for the discover_tracks_for_artist worker function."""

    @pytest.mark.asyncio
    async def test_calls_connector_discover_tracks(self) -> None:
        """Verify connector.discover_tracks is called with correct args."""
        task_id = uuid.uuid4()
        parent_id = uuid.uuid4()
        user_id = uuid.uuid4()
        artist_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_DISCOVERY,
            status=types_module.SyncStatus.PENDING,
            params={
                "artist_id": str(artist_id),
                "artist_name": "Test Artist",
                "service_links": {"listenbrainz": "mb-id-1"},
            },
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        # 1. _load_task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2-N. For each discovered track, we'd need upsert queries, but
        # let the connector return empty for simplicity
        mock_connector = AsyncMock()
        mock_connector.discover_tracks = AsyncMock(return_value=[])

        mock_registry = MagicMock()
        mock_registry.get_by_capability.return_value = [mock_connector]

        # _check_parent_completion mocks
        pending_count = MagicMock()
        pending_count.scalar_one.return_value = 1  # scoring task still pending
        next_pending_task = MagicMock(spec=task_module.Task)
        next_pending_task.id = uuid.uuid4()
        next_pending_task.task_type = types_module.TaskType.TRACK_SCORING
        next_pending_result = MagicMock()
        next_pending_result.scalar_one_or_none.return_value = next_pending_task

        session.execute.side_effect = [
            task_result,
            pending_count,
            next_pending_result,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_registry,
            "redis": arq_redis,
        }

        await worker_module.discover_tracks_for_artist(ctx, str(task_id))

        mock_connector.discover_tracks.assert_awaited_once_with(
            "Test Artist",
            {"listenbrainz": "mb-id-1"},
            limit=20,
        )

    @pytest.mark.asyncio
    async def test_marks_completed_with_result(self) -> None:
        """Task is marked COMPLETED with tracks_found count."""
        import resonance.connectors.base as base_module

        task_id = uuid.uuid4()
        parent_id = uuid.uuid4()
        user_id = uuid.uuid4()
        artist_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_DISCOVERY,
            status=types_module.SyncStatus.PENDING,
            params={
                "artist_id": str(artist_id),
                "artist_name": "Test Artist",
                "service_links": None,
            },
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        # 1. _load_task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # Connector returns 3 discovered tracks
        discovered = [
            base_module.DiscoveredTrack(
                external_id=f"ext-{i}",
                title=f"Track {i}",
                artist_name="Test Artist",
                artist_external_id="mb-id-1",
                service=types_module.ServiceType.LISTENBRAINZ,
                popularity_score=50,
            )
            for i in range(3)
        ]

        mock_connector = AsyncMock()
        mock_connector.discover_tracks = AsyncMock(return_value=discovered)

        mock_registry = MagicMock()
        mock_registry.get_by_capability.return_value = [mock_connector]

        # For each discovered track, we need:
        # - track lookup by service_links (returns None = new track)
        # - artist lookup
        track_not_found = MagicMock()
        track_not_found.scalar_one_or_none.return_value = None

        mock_artist = MagicMock()
        mock_artist.id = artist_id
        artist_found = MagicMock()
        artist_found.scalar_one_or_none.return_value = mock_artist

        # _check_parent_completion mocks
        pending_count = MagicMock()
        pending_count.scalar_one.return_value = 1
        next_pending_result = MagicMock()
        next_pending_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [
            task_result,
            # 3 tracks x 2 queries each
            track_not_found,
            artist_found,
            track_not_found,
            artist_found,
            track_not_found,
            artist_found,
            # _check_parent_completion
            pending_count,
            next_pending_result,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_registry,
            "redis": arq_redis,
        }

        await worker_module.discover_tracks_for_artist(ctx, str(task_id))

        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result is not None
        assert task.result["tracks_found"] == 3


# ---------------------------------------------------------------------------
# score_and_build_playlist tests
# ---------------------------------------------------------------------------


class TestScoreAndBuildPlaylist:
    """Tests for the score_and_build_playlist worker function."""

    @pytest.mark.asyncio
    async def test_creates_playlist_and_tracks(self) -> None:
        """Verify Playlist, PlaylistTrack, and GenerationRecord are created."""
        import resonance.models.generator as generator_models

        task_id = uuid.uuid4()
        parent_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        artist_id = uuid.uuid4()
        track_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_SCORING,
            status=types_module.SyncStatus.PENDING,
            params={
                "profile_id": str(profile_id),
                "event_id": str(event_id),
            },
        )

        # Build mock parent task
        parent_task = task_module.Task(
            id=parent_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.RUNNING,
            params={
                "profile_id": str(profile_id),
                "max_tracks": 30,
                "freshness_target": None,
            },
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        # 1. _load_task
        task_result_mock = MagicMock()
        task_result_mock.scalar_one_or_none.return_value = task

        # 2. Load profile
        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        profile.name = "My Concert Playlist"
        profile.parameter_values = {"familiarity": 70, "hit_depth": 40}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        # 3. Query EventArtist
        import resonance.models.concert as concert_models

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = artist_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        # 4. Query accepted candidates
        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        # 5. Query tracks for these artists
        import resonance.models.music as music_models

        mock_track = MagicMock(spec=music_models.Track)
        mock_track.id = track_id
        mock_track.title = "Test Song"
        mock_track.artist_id = artist_id
        mock_artist = MagicMock(spec=music_models.Artist)
        mock_artist.id = artist_id
        mock_artist.name = "Concert Artist"
        mock_track.artist = mock_artist
        track_scalars = MagicMock()
        track_scalars.all.return_value = [mock_track]
        track_result = MagicMock()
        track_result.scalars.return_value = track_scalars

        # 6. Query ListeningEvent counts (grouped)
        listen_rows: list[tuple[object, ...]] = [(track_id, 10)]
        listen_result = MagicMock()
        listen_result.all.return_value = listen_rows

        # 7. Query UserTrackRelation
        utr_scalars = MagicMock()
        utr_scalars.all.return_value = []
        utr_result = MagicMock()
        utr_result.scalars.return_value = utr_scalars

        # 8. Previous GenerationRecord — none
        prev_gen_result = MagicMock()
        prev_gen_result.scalar_one_or_none.return_value = None

        # 9. Load parent task for max_tracks/freshness_target
        parent_for_params_result = MagicMock()
        parent_for_params_result.scalar_one_or_none.return_value = parent_task

        # 10-13. _check_parent_completion mocks
        pending_count_mock = MagicMock()
        pending_count_mock.scalar_one.return_value = 0
        parent_result_mock = MagicMock()
        parent_result_mock.scalar_one_or_none.return_value = parent_task
        failed_count_mock = MagicMock()
        failed_count_mock.scalar_one.return_value = 0
        children_scalars_mock = MagicMock()
        children_scalars_mock.all.return_value = [task]
        children_result_mock = MagicMock()
        children_result_mock.scalars.return_value = children_scalars_mock

        session.execute.side_effect = [
            task_result_mock,
            profile_result,
            ea_result,
            eac_result,
            track_result,
            listen_result,
            utr_result,
            prev_gen_result,
            parent_for_params_result,
            # _check_parent_completion
            pending_count_mock,
            parent_result_mock,
            failed_count_mock,
            children_result_mock,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": MagicMock(),
            "redis": arq_redis,
        }

        await worker_module.score_and_build_playlist(ctx, str(task_id))

        # Task should be completed
        assert task.status == types_module.SyncStatus.COMPLETED

        # Should have added Playlist, PlaylistTrack(s), and GenerationRecord
        added_objects = [call.args[0] for call in session.add.call_args_list]

        import resonance.models.playlist as playlist_models

        playlists = [
            o for o in added_objects if isinstance(o, playlist_models.Playlist)
        ]
        playlist_tracks = [
            o for o in added_objects if isinstance(o, playlist_models.PlaylistTrack)
        ]
        gen_records = [
            o for o in added_objects if isinstance(o, generator_models.GenerationRecord)
        ]

        assert len(playlists) == 1
        assert len(playlist_tracks) >= 1
        assert len(gen_records) == 1

    @pytest.mark.asyncio
    async def test_marks_parent_completed(self) -> None:
        """After scoring, parent PLAYLIST_GENERATION task is marked complete."""
        import resonance.models.generator as generator_models

        task_id = uuid.uuid4()
        parent_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_SCORING,
            status=types_module.SyncStatus.PENDING,
            params={
                "profile_id": str(profile_id),
                "event_id": str(event_id),
            },
        )

        parent_task = task_module.Task(
            id=parent_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.RUNNING,
            params={
                "profile_id": str(profile_id),
                "max_tracks": 30,
                "freshness_target": None,
            },
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        # 1. _load_task
        task_result_mock = MagicMock()
        task_result_mock.scalar_one_or_none.return_value = task

        # 2. Load profile
        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        profile.name = "Concert Playlist"
        profile.parameter_values = {}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        # 3. No artists (empty event)
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = []
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        # 4. No candidates
        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        # 5. No tracks
        track_scalars = MagicMock()
        track_scalars.all.return_value = []
        track_result = MagicMock()
        track_result.scalars.return_value = track_scalars

        # 6. No listening events
        listen_result = MagicMock()
        listen_result.all.return_value = []

        # 7. No user track relations
        utr_scalars = MagicMock()
        utr_scalars.all.return_value = []
        utr_result = MagicMock()
        utr_result.scalars.return_value = utr_scalars

        # 8. No previous generation
        prev_gen_result = MagicMock()
        prev_gen_result.scalar_one_or_none.return_value = None

        # 9. Load parent task for max_tracks/freshness_target
        parent_for_params_result = MagicMock()
        parent_for_params_result.scalar_one_or_none.return_value = parent_task

        # 10-13. _check_parent_completion: all children done
        pending_count_mock = MagicMock()
        pending_count_mock.scalar_one.return_value = 0
        parent_result_mock = MagicMock()
        parent_result_mock.scalar_one_or_none.return_value = parent_task
        failed_count_mock = MagicMock()
        failed_count_mock.scalar_one.return_value = 0
        children_scalars_mock = MagicMock()
        children_scalars_mock.all.return_value = [task]
        children_result_mock = MagicMock()
        children_result_mock.scalars.return_value = children_scalars_mock

        session.execute.side_effect = [
            task_result_mock,
            profile_result,
            ea_result,
            eac_result,
            track_result,
            listen_result,
            utr_result,
            prev_gen_result,
            parent_for_params_result,
            pending_count_mock,
            parent_result_mock,
            failed_count_mock,
            children_result_mock,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": MagicMock(),
            "redis": arq_redis,
        }

        await worker_module.score_and_build_playlist(ctx, str(task_id))

        # Parent should be marked completed
        assert parent_task.status == types_module.SyncStatus.COMPLETED
        assert parent_task.completed_at is not None
