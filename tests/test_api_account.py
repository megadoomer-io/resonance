"""Tests for account API routes."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

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
    """Minimal async DB session stub for account tests."""

    def __init__(self) -> None:
        self._execute_results: list[Any] = []
        self._execute_call_count = 0
        self._deleted: list[Any] = []

    def set_execute_results(self, results: list[Any]) -> None:
        self._execute_results = results

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._execute_call_count < len(self._execute_results):
            result = self._execute_results[self._execute_call_count]
            self._execute_call_count += 1
            return result
        return FakeScalarResult(None)

    async def delete(self, obj: Any) -> None:
        self._deleted.append(obj)

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

    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )

    application.include_router(api_v1_module.router)

    registry = registry_module.ConnectorRegistry()
    application.state.connector_registry = registry

    return application


def _make_fake_user(
    user_id: uuid.UUID | None = None,
    display_name: str = "Test User",
    email: str | None = "test@example.com",
) -> MagicMock:
    """Create a fake User model instance."""
    user = MagicMock(spec=user_models.User)
    user.id = user_id or uuid.uuid4()
    user.display_name = display_name
    user.email = email
    return user


def _make_fake_connection(
    connection_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    service_type: types_module.ServiceType = types_module.ServiceType.SPOTIFY,
    external_user_id: str = "ext-user-1",
) -> MagicMock:
    """Create a fake ServiceConnection model instance."""
    conn = MagicMock(spec=user_models.ServiceConnection)
    conn.id = connection_id or uuid.uuid4()
    conn.user_id = user_id or uuid.uuid4()
    conn.service_type = service_type
    conn.external_user_id = external_user_id
    conn.connected_at = "2026-01-01T00:00:00+00:00"
    return conn


def _create_authenticated_app(
    user_id: uuid.UUID, db_session: FakeAsyncSession | None = None
) -> tuple[Any, FakeRedis]:
    """Create a test app with a pre-authenticated session.

    Returns the app and FakeRedis so callers can set up the session cookie.
    """
    import fastapi

    import resonance.api.v1 as api_v1_module

    settings = _make_settings()
    fake_redis = FakeRedis()

    # Pre-populate a session in Redis
    session_id = "test-session-id"
    fake_redis.inject_session(session_id, {"user_id": str(user_id)})

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


class TestAccountProfile:
    """Tests for GET /api/v1/account."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/api/v1/account")
        assert response.status_code == 401

    async def test_returns_profile(self) -> None:
        user_id = uuid.uuid4()
        fake_user = _make_fake_user(user_id=user_id)
        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(fake_user)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.get("/api/v1/account")

        assert response.status_code == 200
        data = response.json()
        assert data["id"] == str(user_id)
        assert data["display_name"] == "Test User"
        assert data["email"] == "test@example.com"

    async def test_user_not_found_returns_404(self) -> None:
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
            response = await c.get("/api/v1/account")

        assert response.status_code == 404


class TestAccountConnections:
    """Tests for GET /api/v1/account/connections."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/api/v1/account/connections")
        assert response.status_code == 401

    async def test_returns_connections_list(self) -> None:
        user_id = uuid.uuid4()
        conn1 = _make_fake_connection(user_id=user_id)
        conn2 = _make_fake_connection(
            user_id=user_id,
            service_type=types_module.ServiceType.LASTFM,
            external_user_id="ext-user-2",
        )

        db_session = FakeAsyncSession()
        # scalars() returns a ScalarsResult-like, which has .all()
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
            response = await c.get("/api/v1/account/connections")

        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2


class TestUnlinkConnection:
    """Tests for DELETE /api/v1/account/connections/{connection_id}."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        conn_id = uuid.uuid4()
        response = await client.delete(f"/api/v1/account/connections/{conn_id}")
        assert response.status_code == 401

    async def test_cannot_unlink_last_connection(self) -> None:
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        db_session = FakeAsyncSession()
        # First execute: count returns 1 (only one connection)
        count_result = MagicMock()
        count_result.scalar_one.return_value = 1
        db_session.set_execute_results([count_result])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.delete(f"/api/v1/account/connections/{conn_id}")

        assert response.status_code == 400
        assert "Cannot unlink last connected service" in response.json()["detail"]

    async def test_connection_not_found_returns_404(self) -> None:
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        db_session = FakeAsyncSession()
        # First execute: count returns 2
        count_result = MagicMock()
        count_result.scalar_one.return_value = 2
        # Second execute: connection lookup returns None
        db_session.set_execute_results([count_result, FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.delete(f"/api/v1/account/connections/{conn_id}")

        assert response.status_code == 404

    async def test_unlink_success(self) -> None:
        user_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        fake_conn = _make_fake_connection(connection_id=conn_id, user_id=user_id)

        db_session = FakeAsyncSession()
        # First execute: count returns 2
        count_result = MagicMock()
        count_result.scalar_one.return_value = 2
        # Second execute: connection found
        db_session.set_execute_results([count_result, FakeScalarResult(fake_conn)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.delete(f"/api/v1/account/connections/{conn_id}")

        assert response.status_code == 200
        assert response.json() == {"status": "unlinked"}
