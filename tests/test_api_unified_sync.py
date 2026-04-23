"""Tests for unified sync trigger endpoint.

POST /api/v1/sync/connection/{connection_id}
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx

import resonance.config as config_module
import resonance.connectors.registry as registry_module
import resonance.connectors.songkick as songkick_module
import resonance.connectors.test as test_connector_module
import resonance.middleware.session as session_middleware
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module


def _make_settings() -> config_module.Settings:
    """Create test settings with dummy credentials."""
    return config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        token_encryption_key="dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk=",
    )


class FakeRedis:
    """Minimal in-memory Redis replacement for session middleware tests."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._sets: dict[str, set[str]] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if self._store.pop(key, None) is not None:
                deleted += 1
            if self._sets.pop(key, None) is not None:
                deleted += 1
        return deleted

    async def sadd(self, key: str, *values: str) -> int:
        if key not in self._sets:
            self._sets[key] = set()
        self._sets[key].update(values)
        return len(values)

    async def smembers(self, key: str) -> set[bytes]:
        return {v.encode() for v in self._sets.get(key, set())}

    async def expire(self, key: str, ttl: int) -> bool:
        return key in self._store or key in self._sets

    async def aclose(self) -> None:
        pass

    def inject_session(self, session_id: str, data: dict[str, Any]) -> None:
        """Pre-populate a session for testing authenticated requests."""
        import json

        self._store[f"session:{session_id}"] = json.dumps(data)


class FakeScalarResult:
    """Fake result for scalar queries."""

    def __init__(self, value: Any = None) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value


class FakeAsyncSession:
    """Minimal async DB session stub for sync tests."""

    def __init__(self) -> None:
        self._execute_results: list[Any] = []
        self._execute_call_count = 0
        self._added: list[Any] = []

    def set_execute_results(self, results: list[Any]) -> None:
        self._execute_results = results

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._execute_call_count < len(self._execute_results):
            result = self._execute_results[self._execute_call_count]
            self._execute_call_count += 1
            return result
        return FakeScalarResult(None)

    def add(self, obj: Any) -> None:
        self._added.append(obj)

    async def flush(self) -> None:
        pass

    async def commit(self) -> None:
        pass

    async def __aenter__(self) -> FakeAsyncSession:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


class FakeSessionFactory:
    """Factory that produces FakeAsyncSession instances."""

    def __init__(self, session: FakeAsyncSession | None = None) -> None:
        self._session = session or FakeAsyncSession()

    def __call__(self) -> FakeAsyncSession:
        return self._session


def _make_session_cookie(secret_key: str, session_id: str = "test-session-id") -> str:
    """Create a signed session cookie value."""
    import itsdangerous

    signer = itsdangerous.TimestampSigner(secret_key)
    return signer.sign(session_id).decode("utf-8")


def _create_authenticated_app(
    user_id: uuid.UUID,
    db_session: FakeAsyncSession | None = None,
    *,
    register_connectors: bool = True,
) -> tuple[Any, FakeRedis]:
    """Create a test app with a pre-authenticated session and connector registry."""
    import fastapi

    import resonance.api.v1 as api_v1_module

    settings = _make_settings()
    fake_redis = FakeRedis()

    session_id = "test-session-id"
    fake_redis.inject_session(session_id, {"user_id": str(user_id)})

    application = fastapi.FastAPI(title="test", lifespan=None)
    application.state.settings = settings
    application.state.session_factory = FakeSessionFactory(db_session)
    application.state.arq_redis = AsyncMock()

    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )

    application.include_router(api_v1_module.router)

    registry = registry_module.ConnectorRegistry()
    if register_connectors:
        registry.register(songkick_module.SongkickConnector())
        registry.register(test_connector_module.TestConnector())
    application.state.connector_registry = registry

    return application, fake_redis


class TestTriggerSyncByConnection:
    """Tests for POST /api/v1/sync/connection/{connection_id}."""

    async def test_trigger_songkick_sync(self) -> None:
        """Songkick connection triggers sync_calendar_feed job."""
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        fake_conn = MagicMock(spec=user_models.ServiceConnection)
        fake_conn.id = conn_id
        fake_conn.user_id = user_id
        fake_conn.service_type = types_module.ServiceType.SONGKICK

        db_session = FakeAsyncSession()
        # 1st execute: find connection; 2nd execute: check running tasks
        db_session.set_execute_results(
            [FakeScalarResult(fake_conn), FakeScalarResult(None)]
        )

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(f"/api/v1/sync/connection/{conn_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert "task_id" in data

        # Verify a task was added to the session
        assert len(db_session._added) == 1
        task = db_session._added[0]
        assert isinstance(task, task_models.Task)
        assert task.task_type == types_module.TaskType.CALENDAR_SYNC
        assert task.service_connection_id == conn_id

        # Verify arq job was enqueued with correct function and args
        arq_redis = application.state.arq_redis
        arq_redis.enqueue_job.assert_called_once()
        call_args = arq_redis.enqueue_job.call_args
        assert call_args[0][0] == "sync_calendar_feed"
        assert call_args[0][1] == str(conn_id)  # connection_id
        assert call_args[0][2] == str(task.id)  # task_id
        assert call_args[1]["_job_id"] == f"sync_calendar_feed:{task.id}"

    async def test_trigger_oauth_sync(self) -> None:
        """OAuth connection (test connector) triggers plan_sync job."""
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        fake_conn = MagicMock(spec=user_models.ServiceConnection)
        fake_conn.id = conn_id
        fake_conn.user_id = user_id
        fake_conn.service_type = types_module.ServiceType.TEST

        db_session = FakeAsyncSession()
        db_session.set_execute_results(
            [FakeScalarResult(fake_conn), FakeScalarResult(None)]
        )

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(f"/api/v1/sync/connection/{conn_id}")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert "task_id" in data

        # Verify task type is SYNC_JOB for incremental sync
        assert len(db_session._added) == 1
        task = db_session._added[0]
        assert isinstance(task, task_models.Task)
        assert task.task_type == types_module.TaskType.SYNC_JOB
        assert task.service_connection_id == conn_id

        # Verify arq job enqueued as plan_sync with just task_id
        arq_redis = application.state.arq_redis
        arq_redis.enqueue_job.assert_called_once()
        call_args = arq_redis.enqueue_job.call_args
        assert call_args[0][0] == "plan_sync"
        assert call_args[0][1] == str(task.id)
        assert len(call_args[0]) == 2  # only function name + task_id
        assert call_args[1]["_job_id"] == f"plan_sync:{task.id}"

    async def test_connection_not_found(self) -> None:
        """Returns 404 when connection does not exist."""
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(f"/api/v1/sync/connection/{conn_id}")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_sync_already_running(self) -> None:
        """Returns 409 when a PENDING or RUNNING task exists for the connection."""
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        fake_conn = MagicMock(spec=user_models.ServiceConnection)
        fake_conn.id = conn_id
        fake_conn.user_id = user_id
        fake_conn.service_type = types_module.ServiceType.SONGKICK

        fake_running_task = MagicMock(spec=task_models.Task)
        fake_running_task.id = uuid.uuid4()
        fake_running_task.status = types_module.SyncStatus.RUNNING

        db_session = FakeAsyncSession()
        db_session.set_execute_results(
            [FakeScalarResult(fake_conn), FakeScalarResult(fake_running_task)]
        )

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(f"/api/v1/sync/connection/{conn_id}")

        assert response.status_code == 409
        assert "already in progress" in response.json()["detail"].lower()

    async def test_no_sync_config_returns_400(self) -> None:
        """Returns 400 when the service type has no connector config."""
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        fake_conn = MagicMock(spec=user_models.ServiceConnection)
        fake_conn.id = conn_id
        fake_conn.user_id = user_id
        fake_conn.service_type = types_module.ServiceType.BANDCAMP  # not registered

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(fake_conn)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(f"/api/v1/sync/connection/{conn_id}")

        assert response.status_code == 400
        assert "no sync config" in response.json()["detail"].lower()

    async def test_unauthenticated_returns_401(self) -> None:
        """Returns 401 when no session cookie is present."""
        import fastapi

        import resonance.api.v1 as api_v1_module

        settings = _make_settings()
        fake_redis = FakeRedis()
        application = fastapi.FastAPI(title="test", lifespan=None)
        application.state.settings = settings
        application.state.session_factory = FakeSessionFactory()
        application.state.arq_redis = AsyncMock()
        application.add_middleware(
            session_middleware.SessionMiddleware,
            redis=fake_redis,  # type: ignore[arg-type]
            secret_key=settings.session_secret_key,
        )
        application.include_router(api_v1_module.router)
        registry = registry_module.ConnectorRegistry()
        application.state.connector_registry = registry

        transport = httpx.ASGITransport(app=application)
        conn_id = uuid.uuid4()
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.post(f"/api/v1/sync/connection/{conn_id}")

        assert response.status_code == 401
