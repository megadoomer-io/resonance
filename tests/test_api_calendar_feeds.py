"""Tests for calendar feed API routes (unified ServiceConnection model)."""

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
    import datetime
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


class FakeScalarsResult:
    """Fake result for scalars() queries returning lists."""

    def __init__(self, values: list[Any] | None = None) -> None:
        self._values = values or []

    def all(self) -> list[Any]:
        return self._values


class FakeAsyncSession:
    """Minimal async DB session stub for calendar feed tests."""

    def __init__(self) -> None:
        self._execute_results: list[Any] = []
        self._execute_call_count = 0
        self._added: list[Any] = []
        self._deleted: list[Any] = []

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

    async def delete(self, obj: Any) -> None:
        self._deleted.append(obj)

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
    """Create a test app with fake Redis and no auth."""
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


def _make_fake_connection(
    user_id: uuid.UUID,
    service_type: types_module.ServiceType = types_module.ServiceType.SONGKICK,
    external_user_id: str | None = "mike123",
    url: str | None = None,
    label: str | None = None,
    enabled: bool = True,
    last_synced_at: datetime.datetime | None = None,
) -> MagicMock:
    """Create a fake ServiceConnection for testing."""
    conn = MagicMock(spec=user_models.ServiceConnection)
    conn.id = uuid.uuid4()
    conn.user_id = user_id
    conn.service_type = service_type
    conn.external_user_id = external_user_id
    conn.url = url
    conn.label = label
    conn.enabled = enabled
    conn.last_synced_at = last_synced_at
    return conn


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    application = _create_test_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestPostSongkickConnection:
    """Tests for POST /api/v1/calendar-feeds/songkick."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.post(
            "/api/v1/calendar-feeds/songkick",
            json={"username": "mike123"},
        )
        assert response.status_code == 401

    async def test_creates_connection(self) -> None:
        """Creates a Songkick ServiceConnection for the given username."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()
        # Check for existing connection -> None
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(
                "/api/v1/calendar-feeds/songkick",
                json={"username": "mike123"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["service_type"] == "songkick"
        assert data["external_user_id"] == "mike123"
        assert len(db_session._added) == 1

    async def test_duplicate_returns_409(self) -> None:
        """Returns 409 when connection already exists for this user+username."""
        user_id = uuid.uuid4()
        existing = _make_fake_connection(user_id)

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(existing)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(
                "/api/v1/calendar-feeds/songkick",
                json={"username": "mike123"},
            )

        assert response.status_code == 409
        assert "already exists" in response.json()["detail"].lower()


class TestPostGenericConnection:
    """Tests for POST /api/v1/calendar-feeds/ical."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.post(
            "/api/v1/calendar-feeds/ical",
            json={"url": "https://example.com/cal.ics"},
        )
        assert response.status_code == 401

    async def test_creates_one_connection(self) -> None:
        """Creates a single iCal ServiceConnection."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()
        # Check for existing -> None
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(
                "/api/v1/calendar-feeds/ical",
                json={"url": "https://example.com/cal.ics", "label": "My Calendar"},
            )

        assert response.status_code == 200
        data = response.json()
        assert data["service_type"] == "ical"
        assert data["url"] == "https://example.com/cal.ics"
        assert data["label"] == "My Calendar"
        assert data["enabled"] is True
        assert len(db_session._added) == 1

    async def test_duplicate_returns_409(self) -> None:
        """Returns 409 when connection with same URL already exists."""
        user_id = uuid.uuid4()
        existing = _make_fake_connection(
            user_id,
            service_type=types_module.ServiceType.ICAL,
            external_user_id=None,
            url="https://example.com/cal.ics",
        )

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(existing)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(
                "/api/v1/calendar-feeds/ical",
                json={"url": "https://example.com/cal.ics"},
            )

        assert response.status_code == 409
        assert "already exists" in response.json()["detail"].lower()


class TestListCalendarConnections:
    """Tests for GET /api/v1/calendar-feeds."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/api/v1/calendar-feeds")
        assert response.status_code == 401

    async def test_returns_user_connections(self) -> None:
        """Lists calendar connections for the authenticated user."""
        user_id = uuid.uuid4()
        conn1 = _make_fake_connection(
            user_id,
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id="test",
        )
        conn2 = _make_fake_connection(
            user_id,
            service_type=types_module.ServiceType.ICAL,
            external_user_id=None,
            url="https://example.com/cal.ics",
            label="My Calendar",
        )

        db_session = FakeAsyncSession()
        scalars_result = FakeScalarsResult([conn1, conn2])
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
            response = await c.get("/api/v1/calendar-feeds")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2

    async def test_returns_empty_list(self) -> None:
        """Returns empty list when user has no calendar connections."""
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
            response = await c.get("/api/v1/calendar-feeds")

        assert response.status_code == 200
        assert response.json() == []


class TestDeleteCalendarConnection:
    """Tests for DELETE /api/v1/calendar-feeds/{connection_id}."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        conn_id = uuid.uuid4()
        response = await client.delete(f"/api/v1/calendar-feeds/{conn_id}")
        assert response.status_code == 401

    async def test_deletes_owned_connection(self) -> None:
        """Deletes a connection owned by the authenticated user."""
        user_id = uuid.uuid4()
        conn = _make_fake_connection(user_id)

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(conn)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.delete(f"/api/v1/calendar-feeds/{conn.id}")

        assert response.status_code == 200
        assert response.json() == {"status": "deleted"}
        assert len(db_session._deleted) == 1

    async def test_not_found_returns_404(self) -> None:
        """Returns 404 when connection doesn't exist or isn't owned by user."""
        user_id = uuid.uuid4()

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            conn_id = uuid.uuid4()
            response = await c.delete(f"/api/v1/calendar-feeds/{conn_id}")

        assert response.status_code == 404


class TestDeleteSongkickConnection:
    """Tests for DELETE /api/v1/calendar-feeds/songkick/{username}."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.delete("/api/v1/calendar-feeds/songkick/mike123")
        assert response.status_code == 401

    async def test_deletes_connection(self) -> None:
        """Deletes the Songkick connection for the given username."""
        user_id = uuid.uuid4()
        conn = _make_fake_connection(user_id)

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(conn)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.delete("/api/v1/calendar-feeds/songkick/mike123")

        assert response.status_code == 200
        assert response.json() == {"status": "deleted"}
        assert len(db_session._deleted) == 1

    async def test_unknown_username_returns_404(self) -> None:
        """Returns 404 when no connection exists for the given username."""
        user_id = uuid.uuid4()

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.delete("/api/v1/calendar-feeds/songkick/unknownuser")

        assert response.status_code == 404
        assert "no songkick connection" in response.json()["detail"].lower()
