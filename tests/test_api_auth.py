"""Tests for auth API routes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.connectors.spotify as spotify_module
import resonance.middleware.session as session_middleware
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _make_settings() -> config_module.Settings:
    """Create test settings with dummy credentials."""
    return config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        spotify_redirect_uri="http://localhost:8000/api/v1/auth/spotify/callback",
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


def _make_mock_spotify_connector() -> MagicMock:
    """Create a mock Spotify connector with auth capability."""
    connector = MagicMock(spec=spotify_module.SpotifyConnector)
    connector.service_type = types_module.ServiceType.SPOTIFY
    connector.capabilities = frozenset({base_module.ConnectorCapability.AUTHENTICATION})
    connector.has_capability = MagicMock(
        side_effect=lambda cap: cap in connector.capabilities
    )
    connector.get_auth_url = MagicMock(
        return_value="https://accounts.spotify.com/authorize?state=test"
    )
    connector.exchange_code = AsyncMock(
        return_value=base_module.TokenResponse(
            access_token="access-token-123",
            refresh_token="refresh-token-456",
            expires_in=3600,
            scope="user-read-email",
        )
    )
    connector.get_current_user = AsyncMock(
        return_value={"id": "spotify-user-1", "display_name": "Test User"}
    )
    return connector


class FakeAsyncSession:
    """Minimal async DB session stub for tests that don't need real DB."""

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        return MagicMock(scalar_one_or_none=MagicMock(return_value=None))

    def add(self, obj: Any) -> None:
        pass

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

    def __call__(self) -> FakeAsyncSession:
        return FakeAsyncSession()


def _create_test_app(
    connector: base_module.BaseConnector | None = None,
) -> Any:
    """Create a test app with fake Redis and an optional mock connector."""
    import fastapi

    import resonance.api.v1 as api_v1_module

    settings = _make_settings()
    fake_redis = FakeRedis()

    application = fastapi.FastAPI(title="test", lifespan=None)
    application.state.settings = settings
    application.state.session_factory = FakeSessionFactory()

    # Use fake Redis for session middleware
    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )

    # Register API routes
    application.include_router(api_v1_module.router)

    # Set up connector registry
    registry = registry_module.ConnectorRegistry()
    if connector is not None:
        registry.register(connector)
    application.state.connector_registry = registry

    # Health endpoint for sanity
    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return application


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Client with a mock Spotify connector registered."""
    connector = _make_mock_spotify_connector()
    application = _create_test_app(connector=connector)
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
async def client_no_connectors() -> AsyncIterator[httpx.AsyncClient]:
    """Client with no connectors registered."""
    application = _create_test_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestAuthInitiate:
    """Tests for GET /api/v1/auth/{service}."""

    async def test_spotify_auth_redirects(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/api/v1/auth/spotify", follow_redirects=False)
        assert response.status_code == 307
        location = response.headers["location"]
        assert "accounts.spotify.com/authorize" in location

    async def test_unknown_service_returns_404(self, client: httpx.AsyncClient) -> None:
        response = await client.get(
            "/api/v1/auth/unknown_service", follow_redirects=False
        )
        assert response.status_code == 404

    async def test_unregistered_service_returns_404(
        self,
        client_no_connectors: httpx.AsyncClient,
    ) -> None:
        response = await client_no_connectors.get(
            "/api/v1/auth/spotify", follow_redirects=False
        )
        assert response.status_code == 404

    async def test_service_without_auth_capability_returns_400(
        self,
    ) -> None:
        """A connector without AUTHENTICATION capability should return 400."""
        connector = MagicMock(spec=base_module.BaseConnector)
        connector.service_type = types_module.ServiceType.LASTFM
        connector.capabilities = frozenset(
            {base_module.ConnectorCapability.LISTENING_HISTORY}
        )
        connector.has_capability = MagicMock(
            side_effect=lambda cap: cap in connector.capabilities
        )

        application = _create_test_app(connector=connector)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get("/api/v1/auth/lastfm", follow_redirects=False)
            assert response.status_code == 400

    async def test_stores_state_in_session(self, client: httpx.AsyncClient) -> None:
        """Auth initiate should store oauth_state in the session."""
        response = await client.get("/api/v1/auth/spotify", follow_redirects=False)
        assert response.status_code == 307
        # Session cookie should be set (session was modified with oauth_state)
        set_cookie_headers = response.headers.get_list("set-cookie")
        assert any("session_id" in c for c in set_cookie_headers)


class TestAuthCallback:
    """Tests for GET /api/v1/auth/{service}/callback."""

    async def test_callback_without_state_returns_400(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get(
            "/api/v1/auth/spotify/callback?code=test&state=bad-state"
        )
        assert response.status_code == 400

    async def test_callback_unknown_service_returns_404(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get(
            "/api/v1/auth/unknown_service/callback?code=test&state=test"
        )
        assert response.status_code == 404


class TestAuthLogout:
    """Tests for POST /api/v1/auth/logout."""

    async def test_logout_returns_ok(self, client: httpx.AsyncClient) -> None:
        response = await client.post("/api/v1/auth/logout")
        assert response.status_code == 200
        assert response.json() == {"status": "logged_out"}
