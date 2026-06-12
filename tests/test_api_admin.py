"""Tests for admin API routes."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.registry as registry_module
import resonance.middleware.session as session_middleware
import resonance.models.user as user_models
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _make_settings() -> config_module.Settings:
    """Create test settings with dummy credentials."""
    return config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
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
    """Minimal async DB session stub for admin tests."""

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


def _create_test_app(db_session: FakeAsyncSession | None = None) -> Any:
    """Create a test app with fake Redis and no auth."""
    import fastapi

    import resonance.api.v1 as api_v1_module

    settings = _make_settings()
    fake_redis = FakeRedis()

    application = fastapi.FastAPI(title="test", lifespan=None)
    application.state.settings = settings
    application.state.session_factory = FakeSessionFactory(db_session)

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
    user_id: uuid.UUID,
    db_session: FakeAsyncSession | None = None,
    user_role: str = "user",
) -> tuple[Any, FakeRedis]:
    """Create a test app with a pre-authenticated session."""
    import fastapi

    import resonance.api.v1 as api_v1_module

    settings = _make_settings()
    fake_redis = FakeRedis()

    session_id = "test-session-id"
    fake_redis.inject_session(
        session_id, {"user_id": str(user_id), "user_role": user_role}
    )

    application = fastapi.FastAPI(title="test", lifespan=None)
    application.state.settings = settings
    application.state.session_factory = FakeSessionFactory(db_session)

    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )

    application.include_router(api_v1_module.router)

    registry = registry_module.ConnectorRegistry()
    application.state.connector_registry = registry

    return application, fake_redis


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    application = _create_test_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class FakeResult:
    """Fake DB result supporting scalars().unique().all(), one(), scalar(), all()."""

    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []

    def unique(self) -> FakeResult:
        return self

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items

    def one(self) -> Any:
        return self._items[0]

    def scalar(self) -> Any:
        return self._items[0] if self._items else 0

    def scalar_one(self) -> Any:
        return self._items[0]

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


def _make_bearer_app(
    db_session: FakeAsyncSession | None = None,
    token: str = "test-admin-token",
) -> Any:
    """Create a test app that accepts bearer token auth."""
    import fastapi

    import resonance.api.v1 as api_v1_module

    settings = config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
        admin_api_token=token,
    )
    fake_redis = FakeRedis()

    application = fastapi.FastAPI(title="test", lifespan=None)
    application.state.settings = settings
    application.state.session_factory = FakeSessionFactory(db_session)

    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )

    application.include_router(api_v1_module.router)

    registry = registry_module.ConnectorRegistry()
    application.state.connector_registry = registry

    return application


class TestAdminTestConnect:
    """Tests for POST /api/v1/admin/test/connect."""

    async def test_requires_auth(self, client: httpx.AsyncClient) -> None:
        """Unauthenticated requests should return 401."""
        response = await client.post("/api/v1/admin/test/connect")
        assert response.status_code == 401

    async def test_requires_admin_role(self) -> None:
        """Regular users should get 403."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()

        application, _redis = _create_authenticated_app(
            user_id, db_session=db_session, user_role="user"
        )
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/admin/test/connect")

        assert response.status_code == 403

    async def test_admin_can_connect(self) -> None:
        """Admin users can connect the test service."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()
        # First execute: check for existing connection -> None
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(
            user_id, db_session=db_session, user_role="admin"
        )
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/admin/test/connect")

        assert response.status_code == 200
        assert response.json() == {"status": "connected"}
        assert len(db_session._added) == 1

    async def test_owner_can_connect(self) -> None:
        """Owner users can connect the test service."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(
            user_id, db_session=db_session, user_role="owner"
        )
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/admin/test/connect")

        assert response.status_code == 200
        assert response.json() == {"status": "connected"}

    async def test_already_connected(self) -> None:
        """Returns already_connected when test service is already linked."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()

        existing_conn = MagicMock(spec=user_models.ServiceConnection)
        existing_conn.service_type = types_module.ServiceType.TEST
        db_session.set_execute_results([FakeScalarResult(existing_conn)])

        application, _redis = _create_authenticated_app(
            user_id, db_session=db_session, user_role="admin"
        )
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post("/api/v1/admin/test/connect")

        assert response.status_code == 200
        assert response.json() == {"status": "already_connected"}
        assert len(db_session._added) == 0


class TestAdminAuth:
    """Auth tests for admin endpoints using router-level verify_admin_access."""

    async def test_bearer_token_auth(self) -> None:
        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeResult([])])
        application = _make_bearer_app(db_session=db_session)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                "/api/v1/admin/status",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 200

    async def test_invalid_bearer_token(self) -> None:
        application = _make_bearer_app()
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                "/api/v1/admin/status",
                headers={"Authorization": "Bearer wrong-token"},
            )
        assert response.status_code == 403

    async def test_unauthenticated(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/api/v1/admin/status")
        assert response.status_code == 401

    async def test_non_admin_session(self) -> None:
        user_id = uuid.uuid4()
        application, _redis = _create_authenticated_app(user_id, user_role="user")
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            response = await c.get("/api/v1/admin/status")
        assert response.status_code == 403


class TestAdminStatus:
    """Tests for GET /api/v1/admin/status."""

    async def test_returns_sync_jobs(self) -> None:
        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeResult([])])
        application = _make_bearer_app(db_session=db_session)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                "/api/v1/admin/status",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 200
        data = response.json()
        assert "sync_jobs" in data
        assert isinstance(data["sync_jobs"], list)


class TestAdminStats:
    """Tests for GET /api/v1/admin/stats."""

    async def test_returns_aggregate_fields(self) -> None:
        from collections import namedtuple

        DurRow = namedtuple("DurRow", ["with_duration", "without_duration"])
        db_session = FakeAsyncSession()
        db_session.set_execute_results(
            [
                FakeResult([10]),  # artists count
                FakeResult([20]),  # tracks count
                FakeResult([30]),  # events count
                FakeResult([DurRow(15, 5)]),  # duration breakdown
                FakeResult([]),  # events by service
                FakeResult([3]),  # duplicate artist groups
                FakeResult([7]),  # duplicate track groups
            ]
        )
        application = _make_bearer_app(db_session=db_session)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                "/api/v1/admin/stats",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 200
        data = response.json()
        assert "artists" in data
        assert "tracks" in data
        assert "events_total" in data
        assert "tracks_with_duration" in data
        assert "tracks_without_duration" in data
        assert "events_by_service" in data
        assert "duplicate_artist_groups" in data
        assert "duplicate_track_groups" in data


class TestAdminTaskDetail:
    """Tests for GET /api/v1/admin/tasks/{task_id}."""

    async def test_task_not_found(self) -> None:
        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeResult([])])
        application = _make_bearer_app(db_session=db_session)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                f"/api/v1/admin/tasks/{uuid.uuid4()}",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 404

    async def test_returns_task_detail(self) -> None:
        task = MagicMock()
        task.id = uuid.uuid4()
        task.status = types_module.SyncStatus.COMPLETED
        task.params = {"operation": "dedup_events"}
        task.progress_current = 100
        task.progress_total = 100
        task.result = {"items_deduped": 42}
        task.error_message = None
        task.started_at = None
        task.completed_at = None

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeResult([task])])
        application = _make_bearer_app(db_session=db_session)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                f"/api/v1/admin/tasks/{task.id}",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["task_id"] == str(task.id)
        assert data["status"] == "completed"
        assert data["operation"] == "dedup_events"
        assert data["result"] == {"items_deduped": 42}


class TestAdminTrackSearch:
    """Tests for GET /api/v1/admin/tracks."""

    async def test_empty_query(self) -> None:
        application = _make_bearer_app()
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                "/api/v1/admin/tracks",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 422

    async def test_returns_results(self) -> None:
        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeResult([])])
        application = _make_bearer_app(db_session=db_session)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                "/api/v1/admin/tracks?q=beatles",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 200
        data = response.json()
        assert data["query"] == "beatles"
        assert isinstance(data["results"], list)


class TestAdminDedup:
    """Tests for POST /api/v1/admin/dedup/{operation}."""

    async def test_invalid_operation(self) -> None:
        application = _make_bearer_app()
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.post(
                "/api/v1/admin/dedup/invalid",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 400

    async def test_enqueues_job(self) -> None:
        db_session = FakeAsyncSession()
        application = _make_bearer_app(db_session=db_session)

        fake_arq = MagicMock()
        fake_arq.enqueue_job = AsyncMock()
        application.state.arq_redis = fake_arq

        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.post(
                "/api/v1/admin/dedup/events",
                headers={"Authorization": "Bearer test-admin-token"},
            )
        assert response.status_code == 200
        data = response.json()
        assert "task_id" in data
        assert data["status"] == "started"
        assert len(db_session._added) == 1
