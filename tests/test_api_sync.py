"""Tests for sync API routes."""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.registry as registry_module
import resonance.middleware.session as session_middleware
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


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

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)

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


class FakeScalarsResult:
    """Fake result for scalars() queries returning lists."""

    def __init__(self, values: list[Any] | None = None) -> None:
        self._values = values or []

    def all(self) -> list[Any]:
        return self._values


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


def _create_test_app(db_session: FakeAsyncSession | None = None) -> Any:
    """Create a test app with fake Redis and optional DB session."""
    import fastapi

    import resonance.api.v1 as api_v1_module

    settings = _make_settings()
    fake_redis = FakeRedis()

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
    application.state.connector_registry = registry

    return application


def _create_authenticated_app(
    user_id: uuid.UUID, db_session: FakeAsyncSession | None = None
) -> tuple[Any, FakeRedis]:
    """Create a test app with a pre-authenticated session."""
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
    application.state.connector_registry = registry

    return application, fake_redis


def _make_session_cookie(secret_key: str, session_id: str = "test-session-id") -> str:
    """Create a signed session cookie value."""
    import itsdangerous

    signer = itsdangerous.TimestampSigner(secret_key)
    return signer.sign(session_id).decode("utf-8")


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    application = _create_test_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestSyncTrigger:
    """Tests for POST /api/v1/sync/{service}."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.post("/api/v1/sync/spotify")
        assert response.status_code == 401

    async def test_unknown_service_returns_404(self) -> None:
        user_id = uuid.uuid4()
        application, _redis = _create_authenticated_app(user_id)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/sync/nonexistent")

        assert response.status_code == 404

    async def test_no_connection_returns_400(self) -> None:
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()
        # Query for ServiceConnection returns None
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/sync/spotify")

        assert response.status_code == 400
        assert "No connection" in response.json()["detail"]

    async def test_already_running_returns_409(self) -> None:
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        fake_conn = MagicMock(spec=user_models.ServiceConnection)
        fake_conn.id = conn_id
        fake_conn.user_id = user_id
        fake_conn.service_type = types_module.ServiceType.SPOTIFY
        fake_conn.encrypted_access_token = "encrypted-token"

        fake_running_task = MagicMock(spec=task_models.SyncTask)
        fake_running_task.id = uuid.uuid4()
        fake_running_task.status = types_module.SyncStatus.RUNNING
        fake_running_task.task_type = types_module.SyncTaskType.SYNC_JOB

        db_session = FakeAsyncSession()
        # First: find connection; Second: find running sync task
        db_session.set_execute_results(
            [
                FakeScalarResult(fake_conn),
                FakeScalarResult(fake_running_task),
            ]
        )

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/sync/spotify")

        assert response.status_code == 409
        assert "already running" in response.json()["detail"].lower()

    async def test_deferred_task_returns_409(self) -> None:
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        fake_conn = MagicMock(spec=user_models.ServiceConnection)
        fake_conn.id = conn_id
        fake_conn.user_id = user_id
        fake_conn.service_type = types_module.ServiceType.SPOTIFY
        fake_conn.encrypted_access_token = "encrypted-token"

        fake_deferred_task = MagicMock(spec=task_models.SyncTask)
        fake_deferred_task.id = uuid.uuid4()
        fake_deferred_task.status = types_module.SyncStatus.DEFERRED
        fake_deferred_task.task_type = types_module.SyncTaskType.SYNC_JOB

        db_session = FakeAsyncSession()
        # First: find connection; Second: find deferred sync task
        db_session.set_execute_results(
            [
                FakeScalarResult(fake_conn),
                FakeScalarResult(fake_deferred_task),
            ]
        )

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/sync/spotify")

        assert response.status_code == 409
        assert "already running" in response.json()["detail"].lower()


class TestSyncStatus:
    """Tests for GET /api/v1/sync/status."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/api/v1/sync/status")
        assert response.status_code == 401

    async def test_returns_sync_tasks(self) -> None:
        user_id = uuid.uuid4()
        job_id = uuid.uuid4()

        fake_task = MagicMock(spec=task_models.SyncTask)
        fake_task.id = job_id
        fake_task.status = types_module.SyncStatus.COMPLETED
        fake_task.task_type = types_module.SyncTaskType.SYNC_JOB
        fake_task.progress_current = 10
        fake_task.progress_total = 10
        fake_task.result = {"items_created": 5, "items_updated": 3}
        fake_task.error_message = None
        fake_task.started_at = datetime.datetime(
            2026, 1, 1, 0, 0, 0, tzinfo=datetime.UTC
        )
        fake_task.completed_at = datetime.datetime(
            2026, 1, 1, 0, 1, 0, tzinfo=datetime.UTC
        )

        db_session = FakeAsyncSession()
        scalars_result = FakeScalarsResult([fake_task])
        execute_result = MagicMock()
        execute_result.scalars.return_value = scalars_result
        db_session.set_execute_results([execute_result])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.get("/api/v1/sync/status")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == str(job_id)
        assert data[0]["status"] == "completed"
        assert data[0]["task_type"] == "sync_job"
        assert data[0]["items_created"] == 5
        assert data[0]["items_updated"] == 3

    async def test_returns_empty_list_when_no_jobs(self) -> None:
        user_id = uuid.uuid4()

        db_session = FakeAsyncSession()
        scalars_result = FakeScalarsResult([])
        execute_result = MagicMock()
        execute_result.scalars.return_value = scalars_result
        db_session.set_execute_results([execute_result])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.get("/api/v1/sync/status")

        assert response.status_code == 200
        assert response.json() == []
