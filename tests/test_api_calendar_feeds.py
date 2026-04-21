"""Tests for calendar feed API routes."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.registry as registry_module
import resonance.middleware.session as session_middleware
import resonance.models.concert as concert_models
import resonance.models.task as task_models
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


def _make_fake_feed(
    user_id: uuid.UUID,
    feed_type: types_module.FeedType = types_module.FeedType.SONGKICK_ATTENDANCE,
    url: str = "https://www.songkick.com/users/mike123/calendars.ics?filter=attendance",
    label: str | None = None,
    enabled: bool = True,
    last_synced_at: datetime.datetime | None = None,
) -> MagicMock:
    """Create a fake UserCalendarFeed for testing."""
    feed = MagicMock(spec=concert_models.UserCalendarFeed)
    feed.id = uuid.uuid4()
    feed.user_id = user_id
    feed.feed_type = feed_type
    feed.url = url
    feed.label = label
    feed.enabled = enabled
    feed.last_synced_at = last_synced_at
    return feed


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    application = _create_test_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestPostSongkickFeeds:
    """Tests for POST /api/v1/calendar-feeds/songkick."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.post(
            "/api/v1/calendar-feeds/songkick",
            json={"username": "mike123"},
        )
        assert response.status_code == 401

    async def test_creates_two_feeds(self) -> None:
        """Creates attendance and tracked_artist feeds for the given username."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()
        # First two queries: check for existing feeds -> None, None
        db_session.set_execute_results([FakeScalarResult(None), FakeScalarResult(None)])

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
        assert len(data) == 2
        assert len(db_session._added) == 2

        # Verify feed types
        feed_types = {item["feed_type"] for item in data}
        assert feed_types == {"songkick_attendance", "songkick_tracked_artist"}

        # Verify URLs
        urls = {item["url"] for item in data}
        assert (
            "https://www.songkick.com/users/mike123/calendars.ics?filter=attendance"
            in urls
        )
        assert (
            "https://www.songkick.com/users/mike123/calendars.ics?filter=tracked_artist"
            in urls
        )

    async def test_duplicate_returns_409(self) -> None:
        """Returns 409 when feeds already exist for this user+URL."""
        user_id = uuid.uuid4()
        existing_feed = _make_fake_feed(user_id)

        db_session = FakeAsyncSession()
        # First query finds an existing feed
        db_session.set_execute_results([FakeScalarResult(existing_feed)])

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
        assert "already exist" in response.json()["detail"].lower()


class TestPostGenericFeed:
    """Tests for POST /api/v1/calendar-feeds/ical."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.post(
            "/api/v1/calendar-feeds/ical",
            json={"url": "https://example.com/cal.ics"},
        )
        assert response.status_code == 401

    async def test_creates_one_feed(self) -> None:
        """Creates a single generic iCal feed."""
        user_id = uuid.uuid4()
        db_session = FakeAsyncSession()
        # Check for existing feed -> None
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
        assert data["feed_type"] == "ical_generic"
        assert data["url"] == "https://example.com/cal.ics"
        assert data["label"] == "My Calendar"
        assert data["enabled"] is True
        assert len(db_session._added) == 1

    async def test_duplicate_returns_409(self) -> None:
        """Returns 409 when feed with same URL already exists for this user."""
        user_id = uuid.uuid4()
        existing_feed = _make_fake_feed(
            user_id,
            feed_type=types_module.FeedType.ICAL_GENERIC,
            url="https://example.com/cal.ics",
        )

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(existing_feed)])

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


class TestListCalendarFeeds:
    """Tests for GET /api/v1/calendar-feeds."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/api/v1/calendar-feeds")
        assert response.status_code == 401

    async def test_returns_user_feeds(self) -> None:
        """Lists feeds for the authenticated user."""
        user_id = uuid.uuid4()
        feed1 = _make_fake_feed(
            user_id,
            feed_type=types_module.FeedType.SONGKICK_ATTENDANCE,
            url="https://songkick.com/users/test/calendars.ics?filter=attendance",
        )
        feed2 = _make_fake_feed(
            user_id,
            feed_type=types_module.FeedType.ICAL_GENERIC,
            url="https://example.com/cal.ics",
            label="My Calendar",
        )

        db_session = FakeAsyncSession()
        scalars_result = FakeScalarsResult([feed1, feed2])
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
        """Returns empty list when user has no feeds."""
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


class TestDeleteCalendarFeed:
    """Tests for DELETE /api/v1/calendar-feeds/{feed_id}."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        feed_id = uuid.uuid4()
        response = await client.delete(f"/api/v1/calendar-feeds/{feed_id}")
        assert response.status_code == 401

    async def test_deletes_owned_feed(self) -> None:
        """Deletes a feed owned by the authenticated user."""
        user_id = uuid.uuid4()
        feed = _make_fake_feed(user_id)

        db_session = FakeAsyncSession()
        db_session.set_execute_results([FakeScalarResult(feed)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.delete(f"/api/v1/calendar-feeds/{feed.id}")

        assert response.status_code == 200
        assert response.json() == {"status": "deleted"}
        assert len(db_session._deleted) == 1

    async def test_not_found_returns_404(self) -> None:
        """Returns 404 when feed doesn't exist or isn't owned by user."""
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
            feed_id = uuid.uuid4()
            response = await c.delete(f"/api/v1/calendar-feeds/{feed_id}")

        assert response.status_code == 404


class TestSyncCalendarFeed:
    """Tests for POST /api/v1/calendar-feeds/{feed_id}/sync."""

    async def test_unauthenticated_returns_401(self, client: httpx.AsyncClient) -> None:
        feed_id = uuid.uuid4()
        response = await client.post(f"/api/v1/calendar-feeds/{feed_id}/sync")
        assert response.status_code == 401

    async def test_triggers_sync(self) -> None:
        """Creates a task and enqueues an arq job."""
        user_id = uuid.uuid4()
        feed = _make_fake_feed(user_id)

        db_session = FakeAsyncSession()
        # First query: find feed; Second: check for running sync -> None
        db_session.set_execute_results([FakeScalarResult(feed), FakeScalarResult(None)])

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(f"/api/v1/calendar-feeds/{feed.id}/sync")

        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "started"
        assert "task_id" in data
        assert len(db_session._added) == 1

        # Verify arq job was enqueued
        application.state.arq_redis.enqueue_job.assert_called_once()
        call_args = application.state.arq_redis.enqueue_job.call_args
        assert call_args[0][0] == "sync_calendar_feed"

    async def test_feed_not_found_returns_404(self) -> None:
        """Returns 404 when feed doesn't exist or isn't owned by user."""
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
            feed_id = uuid.uuid4()
            response = await c.post(f"/api/v1/calendar-feeds/{feed_id}/sync")

        assert response.status_code == 404

    async def test_already_running_returns_409(self) -> None:
        """Returns 409 when a sync is already in progress for this feed."""
        user_id = uuid.uuid4()
        feed = _make_fake_feed(user_id)

        fake_running_task = MagicMock(spec=task_models.Task)
        fake_running_task.id = uuid.uuid4()
        fake_running_task.status = types_module.SyncStatus.RUNNING

        db_session = FakeAsyncSession()
        # First: find feed; Second: find running sync task
        db_session.set_execute_results(
            [FakeScalarResult(feed), FakeScalarResult(fake_running_task)]
        )

        application, _redis = _create_authenticated_app(user_id, db_session=db_session)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", cookies={"session_id": cookie}
        ) as c:
            response = await c.post(f"/api/v1/calendar-feeds/{feed.id}/sync")

        assert response.status_code == 409
        assert "already running" in response.json()["detail"].lower()
