"""Tests for POST /api/v1/calendar-feeds/songkick/lookup."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.registry as registry_module
import resonance.middleware.session as session_middleware

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


# ---------------------------------------------------------------------------
# Sample iCal data
# ---------------------------------------------------------------------------

SAMPLE_ATTENDANCE_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:Concert A
END:VEVENT
BEGIN:VEVENT
SUMMARY:Concert B
END:VEVENT
BEGIN:VEVENT
SUMMARY:Concert C
END:VEVENT
END:VCALENDAR
"""

SAMPLE_TRACKED_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:Artist X Tour
END:VEVENT
BEGIN:VEVENT
SUMMARY:Artist Y Tour
END:VEVENT
END:VCALENDAR
"""

EMPTY_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
END:VCALENDAR
"""


# ---------------------------------------------------------------------------
# Fake helpers (mirroring test_api_calendar_feeds.py)
# ---------------------------------------------------------------------------


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


class FakeAsyncSession:
    """Minimal async DB session stub."""

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
        return MagicMock(scalar_one_or_none=lambda: None)

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


def _make_settings() -> config_module.Settings:
    """Create test settings with dummy credentials."""
    return config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        token_encryption_key="dGVzdC1lbmNyeXB0aW9uLWtleS0xMjM0NTY3ODk=",
    )


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


# ---------------------------------------------------------------------------
# Mock helpers for httpx.AsyncClient
# ---------------------------------------------------------------------------


def _mock_response(
    status_code: int = 200,
    text: str = "",
) -> httpx.Response:
    """Build a real httpx.Response with the given status and body."""
    return httpx.Response(
        status_code=status_code,
        text=text,
        request=httpx.Request("GET", "https://www.songkick.com/fake"),
    )


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    application = _create_test_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSongkickLookup:
    """Tests for POST /api/v1/calendar-feeds/songkick/lookup."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        """Unauthenticated requests are rejected."""
        response = await client.post(
            "/api/v1/calendar-feeds/songkick/lookup",
            json={"username": "michael-dougherty"},
        )
        assert response.status_code == 401

    async def test_valid_username_returns_counts(self) -> None:
        """Valid Songkick username returns 200 with plans and tracked counts."""
        user_id = uuid.uuid4()
        application, _redis = _create_authenticated_app(user_id)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        mock_client_instance = AsyncMock()
        mock_client_instance.get = AsyncMock(
            side_effect=[
                _mock_response(200, SAMPLE_ATTENDANCE_ICAL),
                _mock_response(200, SAMPLE_TRACKED_ICAL),
            ]
        )
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            with patch(
                "resonance.api.v1.calendar_feeds.httpx.AsyncClient",
                return_value=mock_client_instance,
            ):
                response = await c.post(
                    "/api/v1/calendar-feeds/songkick/lookup",
                    json={"username": "michael-dougherty"},
                )

        assert response.status_code == 200
        data = response.json()
        assert data["username"] == "michael-dougherty"
        assert data["plans_count"] == 3
        assert data["tracked_artist_count"] == 2

    async def test_invalid_username_returns_404(self) -> None:
        """Songkick returning 404 yields HTTP 404."""
        user_id = uuid.uuid4()
        application, _redis = _create_authenticated_app(user_id)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        not_found_resp = _mock_response(404, "Not Found")
        mock_client_instance = AsyncMock()
        mock_client_instance.get = AsyncMock(return_value=not_found_resp)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            with patch(
                "resonance.api.v1.calendar_feeds.httpx.AsyncClient",
                return_value=mock_client_instance,
            ):
                response = await c.post(
                    "/api/v1/calendar-feeds/songkick/lookup",
                    json={"username": "nonexistent-user-xyz"},
                )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    async def test_songkick_server_error_returns_502(self) -> None:
        """Songkick returning 500 yields HTTP 502."""
        user_id = uuid.uuid4()
        application, _redis = _create_authenticated_app(user_id)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        error_resp = _mock_response(500, "Internal Server Error")
        mock_client_instance = AsyncMock()
        mock_client_instance.get = AsyncMock(return_value=error_resp)
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            with patch(
                "resonance.api.v1.calendar_feeds.httpx.AsyncClient",
                return_value=mock_client_instance,
            ):
                response = await c.post(
                    "/api/v1/calendar-feeds/songkick/lookup",
                    json={"username": "some-user"},
                )

        assert response.status_code == 502
        assert "unavailable" in response.json()["detail"].lower()

    async def test_songkick_connect_error_returns_502(self) -> None:
        """Network failure connecting to Songkick yields HTTP 502."""
        user_id = uuid.uuid4()
        application, _redis = _create_authenticated_app(user_id)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        mock_client_instance = AsyncMock()
        mock_client_instance.get = AsyncMock(
            side_effect=httpx.ConnectError("Connection refused")
        )
        mock_client_instance.__aenter__ = AsyncMock(return_value=mock_client_instance)
        mock_client_instance.__aexit__ = AsyncMock(return_value=False)

        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            with patch(
                "resonance.api.v1.calendar_feeds.httpx.AsyncClient",
                return_value=mock_client_instance,
            ):
                response = await c.post(
                    "/api/v1/calendar-feeds/songkick/lookup",
                    json={"username": "some-user"},
                )

        assert response.status_code == 502
        assert "unavailable" in response.json()["detail"].lower()
