"""Tests for the arq worker module."""

import datetime
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.generators.pool as pool_module
import resonance.models.generator as generator_models
import resonance.models.task as task_module
import resonance.models.taste as taste_models
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
        assert len(funcs) == 13
        names = {f.name for f in funcs}
        assert names == {
            "plan_sync",
            "sync_range",
            "run_bulk_job",
            "sync_calendar_feed",
            "sync_concert_archives",
            "sync_concert_archives_chunk",
            "generate_playlist",
            "discover_tracks_for_artist",
            "score_and_build_playlist",
            "export_playlist",
            "backfill_mbids",
            "backfill_popularity",
            "enrich_related_artists",
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
    task_type: types_module.TaskType = types_module.TaskType.TIME_RANGE,
    status: types_module.SyncStatus = types_module.SyncStatus.COMPLETED,
    result: dict[str, object] | None = None,
) -> task_module.Task:
    """Create a Task instance for testing."""
    task = task_module.Task(
        id=task_id or uuid.uuid4(),
        user_id=uuid.uuid4(),
        service_connection_id=uuid.uuid4(),
        parent_id=parent_id,
        task_type=task_type,
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

        # Build mock parent — must be SYNC_JOB to trigger post-sync dedup
        parent_task = _make_task(
            task_id=parent_id,
            task_type=types_module.TaskType.SYNC_JOB,
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
        # Three commits: parent completion + post-sync dedup + reconcile event artists
        assert session.commit.call_count == 3

    @pytest.mark.asyncio
    async def test_aggregates_results_from_children(self) -> None:
        """Parent result sums items_created/items_updated from children."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        parent_task = _make_task(
            task_id=parent_id,
            task_type=types_module.TaskType.SYNC_JOB,
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
            task_type=types_module.TaskType.SYNC_JOB,
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
            task_type=types_module.TaskType.SYNC_JOB,
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

    @pytest.mark.asyncio
    async def test_generation_parent_skips_dedup(self) -> None:
        """PLAYLIST_GENERATION parent should not trigger post-sync dedup."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()
        arq_redis = AsyncMock()

        parent_task = _make_task(
            task_id=parent_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.RUNNING,
        )

        child1 = _make_task(
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_DISCOVERY,
            result={"tracks_found": 15},
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

        await worker_module._check_parent_completion(session, task, arq_redis, log)

        assert parent_task.status == types_module.SyncStatus.COMPLETED
        # Only one commit (parent completion) — no dedup task created
        session.commit.assert_called_once()
        arq_redis.enqueue_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_generation_parent_aggregates_generation_keys(self) -> None:
        """PLAYLIST_GENERATION parent aggregates tracks_found and playlist_id."""
        parent_id = uuid.uuid4()
        task = _make_task(parent_id=parent_id)
        session = AsyncMock()
        log = MagicMock()

        parent_task = _make_task(
            task_id=parent_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.RUNNING,
        )

        playlist_id = str(uuid.uuid4())
        child_discovery = _make_task(
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_DISCOVERY,
            result={"tracks_found": 10},
        )
        child_scoring = _make_task(
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_SCORING,
            result={
                "playlist_id": playlist_id,
                "tracks_selected": 8,
                "sources_summary": {"library": 5, "discovery": 3},
            },
        )

        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task

        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        children_scalars = MagicMock()
        children_scalars.all.return_value = [child_discovery, child_scoring]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            pending_result,
            parent_result,
            failed_result,
            children_result,
        ]

        await worker_module._check_parent_completion(session, task, AsyncMock(), log)

        assert parent_task.result["tracks_found"] == 10
        assert parent_task.result["playlist_id"] == playlist_id
        assert parent_task.result["tracks_selected"] == 8
        assert parent_task.result["sources_summary"] == {
            "library": 5,
            "discovery": 3,
        }
        assert parent_task.result["children_completed"] == 2
        assert parent_task.result["children_failed"] == 0
        # No items_created/items_updated keys for generation parents
        assert "items_created" not in parent_task.result


# ---------------------------------------------------------------------------
# plan_sync tests
# ---------------------------------------------------------------------------


def _not_cancelled_result(
    parent_id: uuid.UUID | None = None,
) -> MagicMock:
    """Create a mock result for is_cancelled() parent lookup.

    Returns a mock that simulates a RUNNING parent task so is_cancelled()
    returns False and the task proceeds normally.
    """
    parent_task = task_module.Task(
        id=parent_id or uuid.uuid4(),
        task_type=types_module.TaskType.SYNC_JOB,
        status=types_module.SyncStatus.RUNNING,
    )
    result = MagicMock()
    result.scalar_one_or_none.return_value = parent_task
    return result


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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
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

        session.execute.side_effect = [
            task_result,
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
            conn_result,
        ]

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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
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

        # 6. Batched library coverage — both artists have 0 library tracks
        listen_counts = MagicMock()
        listen_counts.all.return_value = []

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_counts,
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

        # Each discovery child carries max_tracks so its catalog fetch scales
        # with the playlist target. Parent has no max_tracks here, so the
        # dispatch default (50) is threaded through.
        for dt in discovery_tasks:
            assert dt.params["max_tracks"] == 50

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

        # 6. Batched library coverage — no library tracks
        listen_counts = MagicMock()
        listen_counts.all.return_value = []

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_counts,
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

        # 6. Batched library coverage — artist has plenty of tracks
        listen_counts = MagicMock()
        listen_counts.all.return_value = [(artist_id, 25)]

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_counts,
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

    @pytest.mark.asyncio
    async def test_adjacent_discovery_when_high_ratio_low_familiarity(self) -> None:
        """Adjacent library artists get discovery + persist when discovery-leaning.

        similar_artist_ratio > 0 AND familiarity < 50: the resolved (capped,
        ranked) adjacent library artists are added to the discovery fan-out and
        persisted onto the parent task params for scoring to read (issue #115).
        """
        import resonance.connectors.base as base_module
        import resonance.models.concert as concert_models
        import resonance.models.generator as generator_models
        import resonance.models.music as music_models

        task_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        target_id = uuid.uuid4()
        adjacent_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        # Discovery-leaning + adjacent ratio > 0
        profile.parameter_values = {"familiarity": 10, "similar_artist_ratio": 70}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = target_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        target_artist = MagicMock(spec=music_models.Artist)
        target_artist.id = target_id
        target_artist.name = "King Buffalo"
        target_artist.service_links = {"musicbrainz": {"id": "mbid-kb"}}
        artist_scalars = MagicMock()
        artist_scalars.all.return_value = [target_artist]
        artist_result = MagicMock()
        artist_result.scalars.return_value = artist_scalars

        # Library coverage: target is well-covered (no discovery needed by
        # coverage), but discovery_wanted=True still includes it.
        listen_counts = MagicMock()
        listen_counts.all.return_value = [(target_id, 25)]

        # Resolver's library query returns one adjacent artist as
        # (id, lowered_name, service_links). The neighbor "Elder" has no MBID,
        # so it matches the library by name and is treated as library-adjacent.
        resolver_result = MagicMock()
        resolver_result.all.return_value = [(adjacent_id, "elder", None)]

        # Adjacent artist objects loaded for the discovery children.
        adjacent_obj = MagicMock(spec=music_models.Artist)
        adjacent_obj.id = adjacent_id
        adjacent_obj.name = "Elder"
        adjacent_obj.service_links = None
        adjacent_obj_scalars = MagicMock()
        adjacent_obj_scalars.all.return_value = [adjacent_obj]
        adjacent_obj_result = MagicMock()
        adjacent_obj_result.scalars.return_value = adjacent_obj_scalars

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_counts,
            resolver_result,
            adjacent_obj_result,
        ]

        # Similar-artist connector returns one neighbor.
        connector = AsyncMock()
        connector.get_similar_artists.return_value = [
            {"name": "Elder", "mbid": None, "match": 0.9},
        ]
        registry = MagicMock()
        registry.get_by_capability.return_value = [connector]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": registry,
            "redis": arq_redis,
        }

        await worker_module.generate_playlist(ctx, str(task_id))

        registry.get_by_capability.assert_called_once_with(
            base_module.ConnectorCapability.SIMILAR_ARTISTS
        )

        # Discovery tasks created for BOTH the target and the adjacent artist.
        added_tasks = [call.args[0] for call in session.add.call_args_list]
        discovery_artist_ids = {
            t.params["artist_id"]
            for t in added_tasks
            if t.task_type == types_module.TaskType.TRACK_DISCOVERY
        }
        assert discovery_artist_ids == {str(target_id), str(adjacent_id)}

        # The capped, ranked adjacent set is persisted on the parent task params
        # so scoring reads the SAME set (resolve-once-and-persist).
        assert task.params["adjacent_artist_ids"] == [str(adjacent_id)]

    @pytest.mark.asyncio
    async def test_no_adjacent_discovery_when_ratio_zero(self) -> None:
        """ratio == 0: no adjacent resolution, no persisted set, behavior unchanged."""
        import resonance.models.concert as concert_models
        import resonance.models.generator as generator_models
        import resonance.models.music as music_models

        task_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        target_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        # Discovery-leaning but NO adjacent ratio.
        profile.parameter_values = {"familiarity": 10, "similar_artist_ratio": 0}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = target_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        target_artist = MagicMock(spec=music_models.Artist)
        target_artist.id = target_id
        target_artist.name = "King Buffalo"
        target_artist.service_links = None
        artist_scalars = MagicMock()
        artist_scalars.all.return_value = [target_artist]
        artist_result = MagicMock()
        artist_result.scalars.return_value = artist_scalars

        listen_counts = MagicMock()
        listen_counts.all.return_value = []

        # No resolver query expected; only the 6 base executes.
        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_counts,
        ]

        registry = MagicMock()

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": registry,
            "redis": arq_redis,
        }

        await worker_module.generate_playlist(ctx, str(task_id))

        # No similar-artist resolution path taken.
        registry.get_by_capability.assert_not_called()

        # Only the target discovery task + scoring task.
        added_tasks = [call.args[0] for call in session.add.call_args_list]
        discovery = [
            t
            for t in added_tasks
            if t.task_type == types_module.TaskType.TRACK_DISCOVERY
        ]
        assert len(discovery) == 1
        assert discovery[0].params["artist_id"] == str(target_id)
        # No adjacent set persisted (or persisted empty); never a populated list.
        assert task.params.get("adjacent_artist_ids", []) == []

    @pytest.mark.asyncio
    async def test_no_adjacent_discovery_when_not_discovery_wanted(self) -> None:
        """familiarity >= 50: adjacent discovery is skipped even if ratio > 0."""
        import resonance.models.concert as concert_models
        import resonance.models.generator as generator_models
        import resonance.models.music as music_models

        task_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        target_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        # Adjacent ratio > 0 but familiarity-leaning (not discovery).
        profile.parameter_values = {"familiarity": 80, "similar_artist_ratio": 70}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = target_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        target_artist = MagicMock(spec=music_models.Artist)
        target_artist.id = target_id
        target_artist.name = "King Buffalo"
        target_artist.service_links = None
        artist_scalars = MagicMock()
        artist_scalars.all.return_value = [target_artist]
        artist_result = MagicMock()
        artist_result.scalars.return_value = artist_scalars

        # Target well-covered: with familiarity>=50 it needs no discovery either.
        listen_counts = MagicMock()
        listen_counts.all.return_value = [(target_id, 25)]

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_counts,
        ]

        registry = MagicMock()

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": registry,
            "redis": arq_redis,
        }

        await worker_module.generate_playlist(ctx, str(task_id))

        # Not discovery-leaning -> no adjacent resolution.
        registry.get_by_capability.assert_not_called()
        assert task.params.get("adjacent_artist_ids", []) == []

    @pytest.mark.asyncio
    async def test_adjacent_discovery_capped_to_max(self) -> None:
        """Adjacent fan-out is capped to _MAX_ADJACENT_DISCOVERY by rank (#115)."""
        import resonance.models.concert as concert_models
        import resonance.models.generator as generator_models
        import resonance.models.music as music_models

        task_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        target_id = uuid.uuid4()
        # More adjacent artists than the cap.
        adjacent_ids = [
            uuid.uuid4() for _ in range(worker_module._MAX_ADJACENT_DISCOVERY + 4)
        ]

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        profile.parameter_values = {"familiarity": 10, "similar_artist_ratio": 90}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = target_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        target_artist = MagicMock(spec=music_models.Artist)
        target_artist.id = target_id
        target_artist.name = "King Buffalo"
        target_artist.service_links = None
        artist_scalars = MagicMock()
        artist_scalars.all.return_value = [target_artist]
        artist_result = MagicMock()
        artist_result.scalars.return_value = artist_scalars

        listen_counts = MagicMock()
        listen_counts.all.return_value = [(target_id, 25)]

        # Resolver returns library rows as (id, lowered_name, service_links);
        # the implementation orders them by neighbor (similarity) rank. All
        # neighbors lack an MBID, so each matches the library by name.
        resolver_result = MagicMock()
        resolver_result.all.return_value = [
            (aid, f"a{i}", None) for i, aid in enumerate(adjacent_ids)
        ]

        # Adjacent artist objects loaded for the capped discovery children.
        capped = adjacent_ids[: worker_module._MAX_ADJACENT_DISCOVERY]
        adjacent_objs = []
        for i, aid in enumerate(capped):
            obj = MagicMock(spec=music_models.Artist)
            obj.id = aid
            obj.name = f"a{i}"
            obj.service_links = None
            adjacent_objs.append(obj)
        adjacent_obj_scalars = MagicMock()
        adjacent_obj_scalars.all.return_value = adjacent_objs
        adjacent_obj_result = MagicMock()
        adjacent_obj_result.scalars.return_value = adjacent_obj_scalars

        session.execute.side_effect = [
            task_result,
            profile_result,
            ea_result,
            eac_result,
            artist_result,
            listen_counts,
            resolver_result,
            adjacent_obj_result,
        ]

        connector = AsyncMock()
        connector.get_similar_artists.return_value = [
            {"name": f"a{i}", "mbid": None, "match": 1.0 - i * 0.01}
            for i in range(len(adjacent_ids))
        ]
        registry = MagicMock()
        registry.get_by_capability.return_value = [connector]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": registry,
            "redis": arq_redis,
        }

        await worker_module.generate_playlist(ctx, str(task_id))

        # Persisted adjacent set is capped to the first _MAX_ADJACENT_DISCOVERY.
        persisted = task.params["adjacent_artist_ids"]
        assert len(persisted) == worker_module._MAX_ADJACENT_DISCOVERY
        assert persisted == [
            str(aid) for aid in adjacent_ids[: worker_module._MAX_ADJACENT_DISCOVERY]
        ]

        # And the discovery fan-out enqueues only the capped adjacent + target.
        added_tasks = [call.args[0] for call in session.add.call_args_list]
        discovery_artist_ids = {
            t.params["artist_id"]
            for t in added_tasks
            if t.task_type == types_module.TaskType.TRACK_DISCOVERY
        }
        expected = {str(target_id)} | {
            str(aid) for aid in adjacent_ids[: worker_module._MAX_ADJACENT_DISCOVERY]
        }
        assert discovery_artist_ids == expected


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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
            pending_count,
            next_pending_result,
        ]

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_registry,
            "redis": arq_redis,
        }

        await worker_module.discover_tracks_for_artist(ctx, str(task_id))

        # No max_tracks in params (legacy child) -> falls back to the playlist
        # default of 50, not the old hardcoded 20.
        mock_connector.discover_tracks.assert_awaited_once_with(
            "Test Artist",
            {"listenbrainz": "mb-id-1"},
            limit=50,
        )

    @pytest.mark.asyncio
    async def test_discovery_limit_scales_with_max_tracks(self) -> None:
        """The per-artist catalog fetch limit comes from the child's max_tracks.

        A single deep artist must be able to fill the whole playlist, so the
        discovery fetch scales with the requested length rather than a fixed cap.
        """
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
                "max_tracks": 120,
            },
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        mock_connector = AsyncMock()
        mock_connector.discover_tracks = AsyncMock(return_value=[])

        mock_registry = MagicMock()
        mock_registry.get_by_capability.return_value = [mock_connector]

        pending_count = MagicMock()
        pending_count.scalar_one.return_value = 1
        next_pending_task = MagicMock(spec=task_module.Task)
        next_pending_task.id = uuid.uuid4()
        next_pending_task.task_type = types_module.TaskType.TRACK_SCORING
        next_pending_result = MagicMock()
        next_pending_result.scalar_one_or_none.return_value = next_pending_task

        session.execute.side_effect = [
            task_result,
            _not_cancelled_result(parent_id),
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
            limit=120,
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
        mock_track.popularity_score = None
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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
            profile_result,
            ea_result,
            eac_result,
            parent_for_params_result,  # parent loaded early (adjacent + options)
            track_result,
            listen_result,
            utr_result,
            prev_gen_result,
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
            _not_cancelled_result(parent_id),  # is_cancelled parent lookup
            profile_result,
            ea_result,
            eac_result,
            parent_for_params_result,  # parent loaded early (adjacent + options)
            track_result,
            listen_result,
            utr_result,
            prev_gen_result,
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

    @pytest.mark.asyncio
    async def test_reads_persisted_adjacent_ids_no_reresolve(self) -> None:
        """Scoring reads parent params["adjacent_artist_ids"]; no re-resolve (#115).

        The pool is expanded to exactly target union persisted set; the cap bounds
        POOL MEMBERSHIP, not just discovery. Scoring must NOT call the similar-
        artist connectors again (resolve-once-and-persist).
        """
        import resonance.models.generator as generator_models
        import resonance.models.music as music_models
        import resonance.models.playlist as playlist_models

        task_id = uuid.uuid4()
        parent_id = uuid.uuid4()
        user_id = uuid.uuid4()
        profile_id = uuid.uuid4()
        event_id = uuid.uuid4()
        target_id = uuid.uuid4()
        adjacent_id = uuid.uuid4()
        target_track_id = uuid.uuid4()
        adjacent_track_id = uuid.uuid4()

        task = task_module.Task(
            id=task_id,
            user_id=user_id,
            parent_id=parent_id,
            task_type=types_module.TaskType.TRACK_SCORING,
            status=types_module.SyncStatus.PENDING,
            params={"profile_id": str(profile_id), "event_id": str(event_id)},
        )

        # Parent carries the persisted, capped adjacent set from fan-out.
        parent_task = task_module.Task(
            id=parent_id,
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.RUNNING,
            params={
                "profile_id": str(profile_id),
                "max_tracks": 30,
                "freshness_target": None,
                "adjacent_artist_ids": [str(adjacent_id)],
            },
        )

        session = AsyncMock()
        arq_redis = AsyncMock()

        task_result_mock = MagicMock()
        task_result_mock.scalar_one_or_none.return_value = task

        profile = MagicMock(spec=generator_models.GeneratorProfile)
        profile.id = profile_id
        profile.user_id = user_id
        profile.name = "Discovery Playlist"
        # Discovery-leaning with adjacent ratio so the OLD code would re-resolve.
        profile.parameter_values = {"familiarity": 10, "similar_artist_ratio": 70}
        profile.input_references = {"event_id": str(event_id)}
        profile_result = MagicMock()
        profile_result.scalar_one_or_none.return_value = profile

        import resonance.models.concert as concert_models

        ea = MagicMock(spec=concert_models.EventArtist)
        ea.artist_id = target_id
        ea_scalars = MagicMock()
        ea_scalars.all.return_value = [ea]
        ea_result = MagicMock()
        ea_result.scalars.return_value = ea_scalars

        eac_scalars = MagicMock()
        eac_scalars.all.return_value = []
        eac_result = MagicMock()
        eac_result.scalars.return_value = eac_scalars

        parent_for_params_result = MagicMock()
        parent_for_params_result.scalar_one_or_none.return_value = parent_task

        # The track query (target union adjacent) returns one track per artist.
        target_artist_obj = MagicMock(spec=music_models.Artist)
        target_artist_obj.id = target_id
        target_artist_obj.name = "King Buffalo"
        target_track = MagicMock(spec=music_models.Track)
        target_track.id = target_track_id
        target_track.title = "Target Song"
        target_track.artist_id = target_id
        target_track.popularity_score = None
        target_track.artist = target_artist_obj

        adjacent_artist_obj = MagicMock(spec=music_models.Artist)
        adjacent_artist_obj.id = adjacent_id
        adjacent_artist_obj.name = "Elder"
        adjacent_track = MagicMock(spec=music_models.Track)
        adjacent_track.id = adjacent_track_id
        adjacent_track.title = "Adjacent Song"
        adjacent_track.artist_id = adjacent_id
        adjacent_track.popularity_score = None
        adjacent_track.artist = adjacent_artist_obj

        track_scalars = MagicMock()
        track_scalars.all.return_value = [target_track, adjacent_track]
        track_result = MagicMock()
        track_result.scalars.return_value = track_scalars

        # Target track heard; adjacent track unheard (the whole point of #115).
        listen_result = MagicMock()
        listen_result.all.return_value = [(target_track_id, 12)]

        utr_scalars = MagicMock()
        utr_scalars.all.return_value = []
        utr_result = MagicMock()
        utr_result.scalars.return_value = utr_scalars

        prev_gen_result = MagicMock()
        prev_gen_result.scalar_one_or_none.return_value = None

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

        captured: dict[str, set[uuid.UUID]] = {}

        async def _capture_execute(stmt: Any) -> Any:
            # Capture the artist-id IN set used by the track query so we can
            # assert pool membership == target union persisted set. UUIDs render
            # without hyphens in compiled SQL, so match on .hex.
            compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
            if "FROM tracks" in compiled:
                ids: set[uuid.UUID] = set()
                for aid in (target_id, adjacent_id):
                    if aid.hex in compiled:
                        ids.add(aid)
                captured["track_query_ids"] = ids
            return next(_execute_iter)

        _execute_iter = iter(
            [
                task_result_mock,
                _not_cancelled_result(parent_id),
                profile_result,
                ea_result,
                eac_result,
                parent_for_params_result,
                track_result,
                listen_result,
                utr_result,
                prev_gen_result,
                pending_count_mock,
                parent_result_mock,
                failed_count_mock,
                children_result_mock,
            ]
        )
        session.execute.side_effect = _capture_execute

        registry = MagicMock()

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": registry,
            "redis": arq_redis,
        }

        await worker_module.score_and_build_playlist(ctx, str(task_id))

        # No re-resolution at scoring time; the persisted set is authoritative.
        registry.get_by_capability.assert_not_called()

        # Pool membership is exactly target union persisted adjacent set.
        assert captured["track_query_ids"] == {target_id, adjacent_id}

        # The adjacent (unheard) track entered the candidate pool as a non-target
        # discovery candidate, and was selected into the playlist.
        added_objects = [c.args[0] for c in session.add.call_args_list]
        playlist_tracks = [
            o for o in added_objects if isinstance(o, playlist_models.PlaylistTrack)
        ]
        selected_track_ids = {pt.track_id for pt in playlist_tracks}
        assert adjacent_track_id in selected_track_ids


# ---------------------------------------------------------------------------
# ExportPlaylist tests
# ---------------------------------------------------------------------------


class TestExportPlaylist:
    """Tests for the export_playlist worker function."""

    def test_export_playlist_is_coroutine_function(self) -> None:
        """export_playlist is an async function."""
        import inspect

        assert inspect.iscoroutinefunction(worker_module.export_playlist)

    def test_dispatch_entry_exists(self) -> None:
        """PLAYLIST_EXPORT is registered in _TASK_DISPATCH."""
        assert types_module.TaskType.PLAYLIST_EXPORT in worker_module._TASK_DISPATCH
        job_name, _ = worker_module._TASK_DISPATCH[
            types_module.TaskType.PLAYLIST_EXPORT
        ]
        assert job_name == "export_playlist"


class TestResolveSimilarLibraryArtists:
    """Tests for _resolve_similar_library_artists (issue #103, Mode 1).

    The resolver now returns an ORDERED list preserving provider similarity
    rank (issue #115) so callers can take the top-N adjacent artists.
    """

    @pytest.mark.anyio()
    async def test_matches_library_artists_excluding_targets(self) -> None:
        target = MagicMock()
        target.name = "King Buffalo"
        target.service_links = {"musicbrainz": {"id": "mbid-kb"}}

        connector = AsyncMock()
        connector.get_similar_artists.return_value = [
            {"name": "Elder", "mbid": None, "match": 0.9},
            {"name": "Pallbearer", "mbid": None, "match": 0.5},
        ]

        elder_id = uuid.uuid4()
        target_id = uuid.uuid4()
        # The library query returns (lowercased name, id) rows for a similar
        # artist and a target artist; the target must be filtered out via
        # exclude_ids.
        result = MagicMock()
        result.all.return_value = [("elder", elder_id), ("king buffalo", target_id)]
        session = AsyncMock()
        session.execute.return_value = result

        matched = await worker_module._resolve_similar_library_artists(
            session, [connector], [target], {target_id}
        )

        assert matched == [elder_id]
        connector.get_similar_artists.assert_awaited_once_with(
            "King Buffalo", mbid="mbid-kb", limit=30
        )

    @pytest.mark.anyio()
    async def test_no_similar_names_skips_query(self) -> None:
        target = MagicMock()
        target.name = "Obscure Band"
        target.service_links = None

        connector = AsyncMock()
        connector.get_similar_artists.return_value = []
        session = AsyncMock()

        matched = await worker_module._resolve_similar_library_artists(
            session, [connector], [target], set()
        )

        assert matched == []
        session.execute.assert_not_called()
        connector.get_similar_artists.assert_awaited_once_with(
            "Obscure Band", mbid=None, limit=30
        )

    @pytest.mark.anyio()
    async def test_unions_neighbors_across_providers(self) -> None:
        target = MagicMock()
        target.name = "King Buffalo"
        target.service_links = {"musicbrainz": {"id": "mbid-kb"}}

        # Two providers each contribute a distinct neighbor; both should be
        # queried and their results unioned.
        lastfm = AsyncMock()
        lastfm.get_similar_artists.return_value = [
            {"name": "Elder", "mbid": None, "match": 0.9},
        ]
        listenbrainz = AsyncMock()
        listenbrainz.get_similar_artists.return_value = [
            {"name": "Pallbearer", "mbid": "mbid-pb", "match": 0.8},
        ]

        elder_id = uuid.uuid4()
        pallbearer_id = uuid.uuid4()
        result = MagicMock()
        result.all.return_value = [
            ("elder", elder_id),
            ("pallbearer", pallbearer_id),
        ]
        session = AsyncMock()
        session.execute.return_value = result

        matched = await worker_module._resolve_similar_library_artists(
            session, [lastfm, listenbrainz], [target], set()
        )

        assert set(matched) == {elder_id, pallbearer_id}
        # Both providers are consulted...
        lastfm.get_similar_artists.assert_awaited_once_with(
            "King Buffalo", mbid="mbid-kb", limit=30
        )
        listenbrainz.get_similar_artists.assert_awaited_once_with(
            "King Buffalo", mbid="mbid-kb", limit=30
        )
        # ...and their neighbors are unioned into a single library query.
        session.execute.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_preserves_provider_similarity_rank(self) -> None:
        """First-seen order across providers/targets is preserved (issue #115)."""
        target = MagicMock()
        target.name = "King Buffalo"
        target.service_links = None

        connector = AsyncMock()
        # Providers return neighbors most-similar first. The resolver must
        # preserve that rank so callers can take the top-N adjacent artists.
        connector.get_similar_artists.return_value = [
            {"name": "Elder", "mbid": None, "match": 0.9},
            {"name": "Pallbearer", "mbid": None, "match": 0.7},
            {"name": "Yob", "mbid": None, "match": 0.5},
        ]

        elder_id = uuid.uuid4()
        pallbearer_id = uuid.uuid4()
        yob_id = uuid.uuid4()
        # The DB query returns rows in arbitrary order; the resolver reorders
        # them to match first-seen similarity rank from the providers.
        result = MagicMock()
        result.all.return_value = [
            ("yob", yob_id),
            ("elder", elder_id),
            ("pallbearer", pallbearer_id),
        ]
        session = AsyncMock()
        session.execute.return_value = result

        matched = await worker_module._resolve_similar_library_artists(
            session, [connector], [target], set()
        )

        assert matched == [elder_id, pallbearer_id, yob_id]

    @pytest.mark.anyio()
    async def test_dedups_preserving_first_seen_order(self) -> None:
        """A neighbor seen from multiple targets appears once, at first rank."""
        target_a = MagicMock()
        target_a.name = "King Buffalo"
        target_a.service_links = None
        target_b = MagicMock()
        target_b.name = "Sleep"
        target_b.service_links = None

        connector = AsyncMock()
        connector.get_similar_artists.side_effect = [
            # Neighbors of target A
            [
                {"name": "Elder", "mbid": None, "match": 0.9},
                {"name": "Yob", "mbid": None, "match": 0.4},
            ],
            # Neighbors of target B; Elder reappears (already seen)
            [
                {"name": "Pallbearer", "mbid": None, "match": 0.8},
                {"name": "Elder", "mbid": None, "match": 0.6},
            ],
        ]

        elder_id = uuid.uuid4()
        yob_id = uuid.uuid4()
        pallbearer_id = uuid.uuid4()
        result = MagicMock()
        result.all.return_value = [
            ("pallbearer", pallbearer_id),
            ("yob", yob_id),
            ("elder", elder_id),
        ]
        session = AsyncMock()
        session.execute.return_value = result

        matched = await worker_module._resolve_similar_library_artists(
            session, [connector], [target_a, target_b], set()
        )

        # Elder first (first-seen), then Yob, then Pallbearer; no duplicate.
        assert matched == [elder_id, yob_id, pallbearer_id]


class TestMaxAdjacentDiscovery:
    """The adjacent-discovery cap constant (issue #115)."""

    def test_constant_default(self) -> None:
        assert worker_module._MAX_ADJACENT_DISCOVERY == 10


class TestMergeAdjacentDiscoveryTargets:
    """Pure merge of capped adjacent ids into the discovery target set (#115)."""

    def test_appends_capped_adjacent_after_targets(self) -> None:
        t1, t2 = uuid.uuid4(), uuid.uuid4()
        a1, a2 = uuid.uuid4(), uuid.uuid4()
        merged = worker_module._merge_adjacent_discovery_targets([t1, t2], [a1, a2])
        assert merged == [t1, t2, a1, a2]

    def test_caps_adjacent_to_max(self) -> None:
        targets = [uuid.uuid4()]
        adjacent = [
            uuid.uuid4() for _ in range(worker_module._MAX_ADJACENT_DISCOVERY + 5)
        ]
        merged = worker_module._merge_adjacent_discovery_targets(targets, adjacent)
        adj_in_result = [m for m in merged if m in set(adjacent)]
        assert len(adj_in_result) == worker_module._MAX_ADJACENT_DISCOVERY
        # The first _MAX_ADJACENT_DISCOVERY by rank are kept.
        assert adj_in_result == adjacent[: worker_module._MAX_ADJACENT_DISCOVERY]

    def test_dedups_adjacent_against_targets(self) -> None:
        t1, t2 = uuid.uuid4(), uuid.uuid4()
        a1 = uuid.uuid4()
        # t2 reappears in the adjacent list; must not be enqueued twice.
        merged = worker_module._merge_adjacent_discovery_targets([t1, t2], [t2, a1])
        assert merged == [t1, t2, a1]

    def test_dedups_adjacent_internally(self) -> None:
        t1 = uuid.uuid4()
        a1 = uuid.uuid4()
        merged = worker_module._merge_adjacent_discovery_targets([t1], [a1, a1])
        assert merged == [t1, a1]

    def test_empty_adjacent_returns_targets(self) -> None:
        t1, t2 = uuid.uuid4(), uuid.uuid4()
        merged = worker_module._merge_adjacent_discovery_targets([t1, t2], [])
        assert merged == [t1, t2]


class TestArtistsNeedingDiscovery:
    """Gating for which target artists get an external catalog fetch (#110)."""

    def test_under_covered_artist_always_included(self) -> None:
        aid = uuid.uuid4()
        result = worker_module._artists_needing_discovery(
            [aid], {aid: 2}, discovery_wanted=False
        )
        assert result == [aid]

    def test_well_covered_artist_excluded_when_not_discovery(self) -> None:
        aid = uuid.uuid4()
        result = worker_module._artists_needing_discovery(
            [aid], {aid: 50}, discovery_wanted=False
        )
        assert result == []

    def test_well_covered_artist_included_when_discovery_wanted(self) -> None:
        aid = uuid.uuid4()
        result = worker_module._artists_needing_discovery(
            [aid], {aid: 50}, discovery_wanted=True
        )
        assert result == [aid]

    def test_missing_coverage_treated_as_zero(self) -> None:
        aid = uuid.uuid4()
        result = worker_module._artists_needing_discovery(
            [aid], {}, discovery_wanted=False
        )
        assert result == [aid]

    def test_boundary_at_min_library_tracks(self) -> None:
        # coverage == _MIN_LIBRARY_TRACKS is "covered" (gate is strictly less-than)
        aid = uuid.uuid4()
        result = worker_module._artists_needing_discovery(
            [aid], {aid: worker_module._MIN_LIBRARY_TRACKS}, discovery_wanted=False
        )
        assert result == []

    def test_discovery_wanted_includes_all(self) -> None:
        covered = uuid.uuid4()
        under = uuid.uuid4()
        result = worker_module._artists_needing_discovery(
            [covered, under], {covered: 50, under: 1}, discovery_wanted=True
        )
        assert set(result) == {covered, under}


class TestMaxImportedAdjacent:
    """The per-generation new-import ceiling constant (issue #115 Phase 2)."""

    def test_constant_default(self) -> None:
        assert worker_module._MAX_IMPORTED_ADJACENT == 10


class TestResolveAdjacentArtists:
    """_resolve_adjacent_artists splits neighbors into library + imports (#115 Ph2)."""

    @pytest.mark.anyio()
    async def test_splits_library_matches_and_import_candidates(self) -> None:
        target = MagicMock()
        target.name = "King Buffalo"
        target.service_links = {"musicbrainz": {"id": "mbid-kb"}}

        connector = AsyncMock()
        connector.get_similar_artists.return_value = [
            {"name": "Elder", "mbid": "mbid-elder", "match": 0.9},
            {"name": "Newcomer", "mbid": "mbid-new", "match": 0.5},
        ]

        elder_id = uuid.uuid4()
        # Library query returns only Elder (Newcomer is not in the library).
        result = MagicMock()
        result.all.return_value = [
            (elder_id, "elder", {"musicbrainz": {"id": "mbid-elder"}}),
        ]
        session = AsyncMock()
        session.execute.return_value = result

        resolution = await worker_module._resolve_adjacent_artists(
            session, [connector], [target], set()
        )

        assert resolution.library_ids == [elder_id]
        assert resolution.import_candidates == [("Newcomer", "mbid-new")]
        connector.get_similar_artists.assert_awaited_once_with(
            "King Buffalo", mbid="mbid-kb", limit=30
        )

    @pytest.mark.anyio()
    async def test_mbid_match_dedups_name_variant_to_library(self) -> None:
        """A neighbor whose MBID matches a library artist under a different name
        is a library match, not an import (decision d)."""
        target = MagicMock()
        target.name = "King Buffalo"
        target.service_links = None

        connector = AsyncMock()
        connector.get_similar_artists.return_value = [
            {"name": "Eldar", "mbid": "mbid-elder", "match": 0.9},
        ]

        elder_id = uuid.uuid4()
        # Library has the artist under canonical name "elder" with the same MBID.
        result = MagicMock()
        result.all.return_value = [
            (elder_id, "elder", {"musicbrainz": {"id": "mbid-elder"}}),
        ]
        session = AsyncMock()
        session.execute.return_value = result

        resolution = await worker_module._resolve_adjacent_artists(
            session, [connector], [target], set()
        )

        assert resolution.library_ids == [elder_id]
        assert resolution.import_candidates == []

    @pytest.mark.anyio()
    async def test_name_only_neighbor_is_not_an_import_candidate(self) -> None:
        target = MagicMock()
        target.name = "King Buffalo"
        target.service_links = None

        connector = AsyncMock()
        connector.get_similar_artists.return_value = [
            {"name": "NoMbid", "mbid": None, "match": 0.9},
        ]

        result = MagicMock()
        result.all.return_value = []  # not in the library
        session = AsyncMock()
        session.execute.return_value = result

        resolution = await worker_module._resolve_adjacent_artists(
            session, [connector], [target], set()
        )

        assert resolution.library_ids == []
        assert resolution.import_candidates == []

    @pytest.mark.anyio()
    async def test_excludes_target_ids_from_library_matches(self) -> None:
        kb = MagicMock()
        kb.name = "King Buffalo"
        kb.service_links = None
        sleep = MagicMock()
        sleep.name = "Sleep"
        sleep.service_links = None
        kb_id, sleep_id = uuid.uuid4(), uuid.uuid4()

        connector = AsyncMock()
        # King Buffalo's neighbors include the other target (Sleep) and Elder.
        connector.get_similar_artists.side_effect = [
            [
                {"name": "Sleep", "mbid": "mbid-sleep", "match": 0.9},
                {"name": "Elder", "mbid": "mbid-elder", "match": 0.6},
            ],
            [],  # Sleep's neighbors (none for simplicity)
        ]

        elder_id = uuid.uuid4()
        result = MagicMock()
        result.all.return_value = [
            (sleep_id, "sleep", {"musicbrainz": {"id": "mbid-sleep"}}),
            (elder_id, "elder", {"musicbrainz": {"id": "mbid-elder"}}),
        ]
        session = AsyncMock()
        session.execute.return_value = result

        resolution = await worker_module._resolve_adjacent_artists(
            session, [connector], [kb, sleep], {kb_id, sleep_id}
        )

        # Sleep (a target) is excluded; only Elder remains. Nothing imported.
        assert resolution.library_ids == [elder_id]
        assert resolution.import_candidates == []

    @pytest.mark.anyio()
    async def test_import_candidates_preserve_similarity_rank(self) -> None:
        target = MagicMock()
        target.name = "King Buffalo"
        target.service_links = None

        connector = AsyncMock()
        connector.get_similar_artists.return_value = [
            {"name": "First", "mbid": "mbid-1", "match": 0.9},
            {"name": "Second", "mbid": "mbid-2", "match": 0.7},
            {"name": "Third", "mbid": "mbid-3", "match": 0.5},
        ]

        result = MagicMock()
        result.all.return_value = []  # none in the library
        session = AsyncMock()
        session.execute.return_value = result

        resolution = await worker_module._resolve_adjacent_artists(
            session, [connector], [target], set()
        )

        assert resolution.library_ids == []
        assert resolution.import_candidates == [
            ("First", "mbid-1"),
            ("Second", "mbid-2"),
            ("Third", "mbid-3"),
        ]

    @pytest.mark.anyio()
    async def test_no_neighbors_returns_empty_without_query(self) -> None:
        target = MagicMock()
        target.name = "Obscure"
        target.service_links = None

        connector = AsyncMock()
        connector.get_similar_artists.return_value = []
        session = AsyncMock()

        resolution = await worker_module._resolve_adjacent_artists(
            session, [connector], [target], set()
        )

        assert resolution.library_ids == []
        assert resolution.import_candidates == []
        session.execute.assert_not_called()


class TestImportAdjacentCandidates:
    """_import_adjacent_candidates imports recommended artists (#115 Phase 2)."""

    @pytest.mark.anyio()
    async def test_imports_up_to_limit_in_order(self) -> None:
        a1, a2, a3 = MagicMock(), MagicMock(), MagicMock()
        a1.id, a2.id, a3.id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
        session = AsyncMock()
        connector = AsyncMock()
        log = MagicMock()
        candidates = [("A", "m1"), ("B", "m2"), ("C", "m3")]

        with patch.object(
            worker_module.artist_import_module,
            "import_artist_by_mbid",
            new=AsyncMock(side_effect=[a1, a2, a3]),
        ) as imp:
            result = await worker_module._import_adjacent_candidates(
                session, connector, candidates, limit=2, exclude_ids=set(), log=log
            )

        assert result == [a1.id, a2.id]
        assert imp.await_count == 2  # limit honored

    @pytest.mark.anyio()
    async def test_skips_unresolved_none(self) -> None:
        a1 = MagicMock()
        a1.id = uuid.uuid4()
        session = AsyncMock()
        connector = AsyncMock()
        log = MagicMock()
        candidates = [("A", "m1"), ("Ghost", "m2")]

        with patch.object(
            worker_module.artist_import_module,
            "import_artist_by_mbid",
            new=AsyncMock(side_effect=[a1, None]),
        ):
            result = await worker_module._import_adjacent_candidates(
                session, connector, candidates, limit=10, exclude_ids=set(), log=log
            )

        assert result == [a1.id]

    @pytest.mark.anyio()
    async def test_dedups_against_exclude_and_self(self) -> None:
        shared_id = uuid.uuid4()
        a_lib = MagicMock()
        a_lib.id = shared_id  # resolves to an already-selected library artist
        a_dup1 = MagicMock()
        a_dup1.id = uuid.uuid4()
        a_dup2 = MagicMock()
        a_dup2.id = a_dup1.id  # a second candidate resolves to the same artist
        session = AsyncMock()
        connector = AsyncMock()
        log = MagicMock()
        candidates = [("Lib", "m1"), ("Dup", "m2"), ("DupAgain", "m3")]

        with patch.object(
            worker_module.artist_import_module,
            "import_artist_by_mbid",
            new=AsyncMock(side_effect=[a_lib, a_dup1, a_dup2]),
        ):
            result = await worker_module._import_adjacent_candidates(
                session,
                connector,
                candidates,
                limit=10,
                exclude_ids={shared_id},
                log=log,
            )

        assert result == [a_dup1.id]  # library dup excluded; self-dup collapsed

    @pytest.mark.anyio()
    async def test_failure_is_logged_and_skipped(self) -> None:
        a2 = MagicMock()
        a2.id = uuid.uuid4()
        session = AsyncMock()
        connector = AsyncMock()
        log = MagicMock()
        candidates = [("Boom", "m1"), ("Ok", "m2")]

        with patch.object(
            worker_module.artist_import_module,
            "import_artist_by_mbid",
            new=AsyncMock(side_effect=[RuntimeError("mb 500"), a2]),
        ):
            result = await worker_module._import_adjacent_candidates(
                session, connector, candidates, limit=10, exclude_ids=set(), log=log
            )

        assert result == [a2.id]  # one failure does not abort the rest
        log.warning.assert_called_once()


# ---------------------------------------------------------------------------
# resolve_pool: shared target-resolution path (#128 T6/T14)
# ---------------------------------------------------------------------------


def _scalars_result(rows: list[Any]) -> MagicMock:
    """Build a mock execute() result whose .scalars().all() returns rows."""
    result = MagicMock()
    result.scalars.return_value.all.return_value = rows
    return result


class TestResolvePool:
    """resolve_pool turns input_references into a deduped, exclude-filtered pool."""

    async def test_legacy_event_id_shape(self) -> None:
        import resonance.generators.pool as pool_module

        a1, a2 = uuid.uuid4(), uuid.uuid4()
        eid = uuid.uuid4()
        ea = MagicMock()
        ea.artist_id = a1
        cand = MagicMock()
        cand.matched_artist_id = a2

        session = AsyncMock()
        session.execute.side_effect = [_scalars_result([ea]), _scalars_result([cand])]

        pool = await worker_module.resolve_pool(session, {"event_id": str(eid)})

        assert [r.artist_id for r in pool] == [a1, a2]
        assert all(r.via is pool_module.PoolProvenance.EVENT for r in pool)

    async def test_sources_event_shape(self) -> None:
        a1 = uuid.uuid4()
        eid = uuid.uuid4()
        ea = MagicMock()
        ea.artist_id = a1

        session = AsyncMock()
        session.execute.side_effect = [_scalars_result([ea]), _scalars_result([])]

        refs = {"sources": [{"kind": "event", "event_id": str(eid), "enabled": True}]}
        pool = await worker_module.resolve_pool(session, refs)

        assert [r.artist_id for r in pool] == [a1]

    async def test_artist_source_needs_no_query(self) -> None:
        import resonance.generators.pool as pool_module

        a3 = uuid.uuid4()
        session = AsyncMock()

        refs = {"sources": [{"kind": "artist", "artist_id": str(a3), "enabled": True}]}
        pool = await worker_module.resolve_pool(session, refs)

        assert [r.artist_id for r in pool] == [a3]
        assert pool[0].via is pool_module.PoolProvenance.ARTIST
        session.execute.assert_not_called()  # no event sources -> no DB hit

    async def test_exclude_applied_last(self) -> None:
        a1, a2 = uuid.uuid4(), uuid.uuid4()
        eid = uuid.uuid4()
        ea1, ea2 = MagicMock(), MagicMock()
        ea1.artist_id, ea2.artist_id = a1, a2

        session = AsyncMock()
        session.execute.side_effect = [
            _scalars_result([ea1, ea2]),
            _scalars_result([]),
        ]

        refs = {
            "sources": [{"kind": "event", "event_id": str(eid), "enabled": True}],
            "exclude_artist_ids": [str(a2)],
        }
        pool = await worker_module.resolve_pool(session, refs)

        assert [r.artist_id for r in pool] == [a1]  # a2 excluded

    async def test_disabled_source_skipped(self) -> None:
        eid = uuid.uuid4()
        session = AsyncMock()

        refs = {"sources": [{"kind": "event", "event_id": str(eid), "enabled": False}]}
        pool = await worker_module.resolve_pool(session, refs)

        assert pool == []
        session.execute.assert_not_called()  # disabled event -> no query

    async def test_multiple_events_batched(self) -> None:
        """Two event sources resolve in one query per table, not per event (T14)."""
        a1, a2 = uuid.uuid4(), uuid.uuid4()
        e1, e2 = uuid.uuid4(), uuid.uuid4()
        ea1, ea2 = MagicMock(), MagicMock()
        ea1.artist_id, ea2.artist_id = a1, a2

        session = AsyncMock()
        session.execute.side_effect = [
            _scalars_result([ea1, ea2]),  # both events' EventArtist rows, one query
            _scalars_result([]),  # both events' candidates, one query
        ]

        refs = {
            "sources": [
                {"kind": "event", "event_id": str(e1), "enabled": True},
                {"kind": "event", "event_id": str(e2), "enabled": True},
            ]
        }
        pool = await worker_module.resolve_pool(session, refs)

        assert {r.artist_id for r in pool} == {a1, a2}
        assert session.execute.call_count == 2  # batched: not 4

    async def test_dedup_across_event_and_artist(self) -> None:
        a1 = uuid.uuid4()
        eid = uuid.uuid4()
        ea = MagicMock()
        ea.artist_id = a1

        session = AsyncMock()
        session.execute.side_effect = [_scalars_result([ea]), _scalars_result([])]

        refs = {
            "sources": [
                {"kind": "event", "event_id": str(eid), "enabled": True},
                {"kind": "artist", "artist_id": str(a1), "enabled": True},
            ]
        }
        pool = await worker_module.resolve_pool(session, refs)

        assert [r.artist_id for r in pool] == [a1]  # event wins, artist dup dropped


# ---------------------------------------------------------------------------
# Related-artist enrichment (#133)
# ---------------------------------------------------------------------------


def _artist_ns(name: str = "Seed", **over: Any) -> Any:
    base: dict[str, Any] = {"id": uuid.uuid4(), "name": name, "service_links": {}}
    base.update(over)
    return MagicMock(**base)


class TestFetchSimilarWithStore:
    """_fetch_similar_with_store: stored-first, live fallback, refresh-if-old."""

    @pytest.mark.asyncio
    async def test_fresh_edges_returned_without_live_call(self) -> None:
        now = datetime.datetime.now(datetime.UTC)
        artist = _artist_ns()
        connector = AsyncMock()
        connector.service_type = types_module.ServiceType.LISTENBRAINZ
        row = MagicMock(neighbor_name="Elder", neighbor_mbid="m1", fetched_at=now)
        result = MagicMock()
        result.scalars.return_value.all.return_value = [row]
        session = AsyncMock()
        session.execute.return_value = result

        out = await worker_module._fetch_similar_with_store(
            session, connector, artist, limit=30, now=now
        )

        assert out == [{"name": "Elder", "mbid": "m1"}]
        connector.get_similar_artists.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_miss_fetches_live_and_records_named_only(self) -> None:
        now = datetime.datetime.now(datetime.UTC)
        artist = _artist_ns()
        connector = AsyncMock()
        connector.service_type = types_module.ServiceType.LISTENBRAINZ
        connector.get_similar_artists.return_value = [
            {"name": "Elder", "mbid": "m1"},
            {"name": "", "mbid": "skip"},  # nameless -> not recorded
        ]
        result = MagicMock()
        result.scalars.return_value.all.return_value = []  # no stored edges
        session = AsyncMock()
        session.execute.return_value = result

        out = await worker_module._fetch_similar_with_store(
            session, connector, artist, limit=30, now=now
        )

        connector.get_similar_artists.assert_awaited_once()
        assert out == [{"name": "Elder", "mbid": "m1"}, {"name": "", "mbid": "skip"}]
        added = [c.args[0] for c in session.add.call_args_list]
        assert len(added) == 1  # only the named neighbor recorded
        assert isinstance(added[0], taste_models.ArtistSimilarity)
        assert added[0].neighbor_name == "Elder"
        assert added[0].rank == 0

    @pytest.mark.asyncio
    async def test_stale_edges_trigger_refetch(self) -> None:
        now = datetime.datetime.now(datetime.UTC)
        stale = now - datetime.timedelta(days=60)
        artist = _artist_ns()
        connector = AsyncMock()
        connector.service_type = types_module.ServiceType.LISTENBRAINZ
        connector.get_similar_artists.return_value = [{"name": "Fresh", "mbid": "m2"}]
        row = MagicMock(neighbor_name="Old", neighbor_mbid="m1", fetched_at=stale)
        result = MagicMock()
        result.scalars.return_value.all.return_value = [row]
        session = AsyncMock()
        session.execute.return_value = result

        out = await worker_module._fetch_similar_with_store(
            session, connector, artist, limit=30, now=now
        )

        connector.get_similar_artists.assert_awaited_once()
        assert out == [{"name": "Fresh", "mbid": "m2"}]


class TestCollectRelated:
    """_collect_related: early-stop, library-first, import top-up."""

    @pytest.mark.asyncio
    async def test_early_stops_when_library_target_met(self) -> None:
        a, b = uuid.uuid4(), uuid.uuid4()
        seeds = [_artist_ns("s1"), _artist_ns("s2"), _artist_ns("s3")]
        resolve = AsyncMock(
            return_value=worker_module._AdjacentResolution(
                library_ids=[a, b], import_candidates=[]
            )
        )
        with patch.object(worker_module, "_resolve_adjacent_artists", resolve):
            out = await worker_module._collect_related(
                AsyncMock(),
                [AsyncMock()],
                None,
                seeds,
                set(),
                2,
                AsyncMock(),
                MagicMock(),
            )
        assert out == [a, b]
        resolve.assert_awaited_once()  # stopped after first seed

    @pytest.mark.asyncio
    async def test_tops_up_from_imports_when_short(self) -> None:
        a = uuid.uuid4()
        imp = uuid.uuid4()
        seeds = [_artist_ns("s1")]
        resolve = AsyncMock(
            return_value=worker_module._AdjacentResolution(
                library_ids=[a], import_candidates=[("New", "mbid-new")]
            )
        )
        imports = AsyncMock(return_value=[imp])
        lb = AsyncMock()
        with (
            patch.object(worker_module, "_resolve_adjacent_artists", resolve),
            patch.object(worker_module, "_import_adjacent_candidates", imports),
        ):
            out = await worker_module._collect_related(
                AsyncMock(),
                [AsyncMock()],
                lb,
                seeds,
                set(),
                3,
                AsyncMock(),
                MagicMock(),
            )
        assert out == [a, imp]
        imports.assert_awaited_once()
        assert imports.await_args.kwargs["limit"] == 2  # 3 target - 1 library

    @pytest.mark.asyncio
    async def test_no_imports_without_lb_connector(self) -> None:
        a = uuid.uuid4()
        seeds = [_artist_ns("s1")]
        resolve = AsyncMock(
            return_value=worker_module._AdjacentResolution(
                library_ids=[a], import_candidates=[("New", "mbid-new")]
            )
        )
        imports = AsyncMock()
        with (
            patch.object(worker_module, "_resolve_adjacent_artists", resolve),
            patch.object(worker_module, "_import_adjacent_candidates", imports),
        ):
            out = await worker_module._collect_related(
                AsyncMock(),
                [AsyncMock()],
                None,
                seeds,
                set(),
                3,
                AsyncMock(),
                MagicMock(),
            )
        assert out == [a]
        imports.assert_not_awaited()


def _enrich_ctx(session: AsyncMock, *, has_similar: bool = True) -> dict[str, Any]:
    registry = MagicMock()
    registry.get_by_capability.return_value = [AsyncMock()] if has_similar else []
    registry.get_base_connector.return_value = AsyncMock()
    return {
        "session_factory": _mock_session_factory(session),
        "settings": MagicMock(),
        "connector_registry": registry,
        "strategies": {},
        "redis": AsyncMock(),
    }


def _enrich_task(params: dict[str, Any]) -> task_module.Task:
    return task_module.Task(
        user_id=uuid.uuid4(),
        task_type=types_module.TaskType.RELATED_ARTIST_ENRICHMENT,
        status=types_module.SyncStatus.PENDING,
        params=params,
    )


class TestEnrichRelatedArtists:
    """enrich_related_artists orchestration."""

    @pytest.mark.asyncio
    async def test_task_not_found_returns(self) -> None:
        session = AsyncMock()
        load = MagicMock()
        load.scalar_one_or_none.return_value = None
        session.execute.return_value = load
        await worker_module.enrich_related_artists(
            _enrich_ctx(session), str(uuid.uuid4())
        )
        session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_profile_not_found_fails(self) -> None:
        task = _enrich_task(
            {"profile_id": str(uuid.uuid4()), "seed_artist_ids": "lineup", "n": 5}
        )
        session = AsyncMock()
        task_res = MagicMock()
        task_res.scalar_one_or_none.return_value = task
        profile_res = MagicMock()
        profile_res.scalar_one_or_none.return_value = None
        session.execute.side_effect = [task_res, profile_res]
        await worker_module.enrich_related_artists(
            _enrich_ctx(session), str(task.id or uuid.uuid4())
        )
        assert task.status == types_module.SyncStatus.FAILED

    @pytest.mark.asyncio
    async def test_no_connector_completes_with_message(self) -> None:
        profile = generator_models.GeneratorProfile(
            user_id=uuid.uuid4(),
            name="P",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references=pool_module.serialize_input_references(
                [pool_module.ArtistSource(artist_id=uuid.uuid4())]
            ),
        )
        task = _enrich_task(
            {"profile_id": str(uuid.uuid4()), "seed_artist_ids": "lineup", "n": 5}
        )
        session = AsyncMock()
        task_res = MagicMock()
        task_res.scalar_one_or_none.return_value = task
        profile_res = MagicMock()
        profile_res.scalar_one_or_none.return_value = profile
        artists_res = MagicMock()
        artists_res.scalars.return_value.all.return_value = []
        session.execute.side_effect = [task_res, profile_res, artists_res]
        with patch.object(worker_module, "resolve_pool", AsyncMock(return_value=[])):
            await worker_module.enrich_related_artists(
                _enrich_ctx(session, has_similar=False), str(task.id or uuid.uuid4())
            )
        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result["found"] == 0
        assert task.result["message"] == "no connector connected"

    @pytest.mark.asyncio
    async def test_lineup_success_replaces_scope(self) -> None:
        core_id = uuid.uuid4()
        new1, new2 = uuid.uuid4(), uuid.uuid4()
        profile = generator_models.GeneratorProfile(
            user_id=uuid.uuid4(),
            name="P",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references=pool_module.serialize_input_references(
                [pool_module.ArtistSource(artist_id=core_id)]
            ),
        )
        task = _enrich_task(
            {"profile_id": str(uuid.uuid4()), "seed_artist_ids": "lineup", "n": 5}
        )
        core_artist = _artist_ns("Core", id=core_id)
        session = AsyncMock()
        task_res = MagicMock()
        task_res.scalar_one_or_none.return_value = task
        profile_res = MagicMock()
        profile_res.scalar_one_or_none.return_value = profile
        artists_res = MagicMock()
        artists_res.scalars.return_value.all.return_value = [core_artist]
        session.execute.side_effect = [task_res, profile_res, artists_res]

        collect = AsyncMock(return_value=[new1, new2])
        with (
            patch.object(
                worker_module,
                "resolve_pool",
                AsyncMock(
                    return_value=[
                        pool_module.ResolvedArtist(
                            artist_id=core_id, via=pool_module.PoolProvenance.ARTIST
                        )
                    ]
                ),
            ),
            patch.object(worker_module, "_collect_related", collect),
        ):
            await worker_module.enrich_related_artists(
                _enrich_ctx(session), str(task.id or uuid.uuid4())
            )

        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result["found"] == 2
        assert task.progress_total == 5
        assert task.progress_current == 2
        # Lineup scope now holds the two discovered artists; core preserved.
        sources = pool_module.normalize_sources(profile.input_references)
        lineup_ids = {
            s.artist_id
            for s in sources
            if isinstance(s, pool_module.ArtistSource) and s.via_seed == "lineup"
        }
        assert lineup_ids == {new1, new2}
        assert pool_module.ArtistSource(artist_id=core_id, via_seed=None) in sources
        # Seed (core) is excluded from its own neighbor search.
        assert core_id in collect.await_args.args[4]

    @pytest.mark.asyncio
    async def test_per_seed_success_tags_each_scope(self) -> None:
        seed_id = uuid.uuid4()
        new = uuid.uuid4()
        profile = generator_models.GeneratorProfile(
            user_id=uuid.uuid4(),
            name="P",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references=pool_module.serialize_input_references(
                [pool_module.ArtistSource(artist_id=seed_id)]
            ),
        )
        task = _enrich_task(
            {"profile_id": str(uuid.uuid4()), "seed_artist_ids": [str(seed_id)], "n": 4}
        )
        seed_artist = _artist_ns("Seed", id=seed_id)
        session = AsyncMock()
        task_res = MagicMock()
        task_res.scalar_one_or_none.return_value = task
        profile_res = MagicMock()
        profile_res.scalar_one_or_none.return_value = profile
        artists_res = MagicMock()
        artists_res.scalars.return_value.all.return_value = [seed_artist]
        session.execute.side_effect = [task_res, profile_res, artists_res]

        with (
            patch.object(
                worker_module,
                "resolve_pool",
                AsyncMock(
                    return_value=[
                        pool_module.ResolvedArtist(
                            artist_id=seed_id, via=pool_module.PoolProvenance.ARTIST
                        )
                    ]
                ),
            ),
            patch.object(
                worker_module, "_collect_related", AsyncMock(return_value=[new])
            ),
        ):
            await worker_module.enrich_related_artists(
                _enrich_ctx(session), str(task.id or uuid.uuid4())
            )

        assert task.status == types_module.SyncStatus.COMPLETED
        assert task.result["found"] == 1
        sources = pool_module.normalize_sources(profile.input_references)
        tagged = {
            s.artist_id: s.via_seed
            for s in sources
            if isinstance(s, pool_module.ArtistSource)
        }
        assert tagged[new] == str(seed_id)  # discovered artist tagged by seed
        assert tagged[seed_id] is None  # original seed untouched
