"""Tests for data API endpoints: events, artists, tracks, history."""

from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import httpx
import pytest

import resonance.config as config_module
import resonance.middleware.session as session_middleware

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


PAGE_SIZE = 50


def _make_settings() -> config_module.Settings:
    return config_module.Settings(
        spotify_client_id="test-id",
        spotify_client_secret="test-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
    )


class FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._sets: dict[str, set[str]] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> int:
        return sum(1 for k in keys if self._store.pop(k, None) is not None)

    async def sadd(self, key: str, *values: str) -> int:
        self._sets.setdefault(key, set()).update(values)
        return len(values)

    async def smembers(self, key: str) -> set[bytes]:
        return {v.encode() for v in self._sets.get(key, set())}

    async def expire(self, key: str, ttl: int) -> bool:
        return key in self._store or key in self._sets

    async def aclose(self) -> None:
        pass

    def inject_session(self, session_id: str, data: dict[str, Any]) -> None:
        import json

        self._store[f"session:{session_id}"] = json.dumps(data)


class FakeResult:
    """Fake DB result supporting scalars().all(), unique().scalars().all(),
    and scalar_one_or_none() chains."""

    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []

    def unique(self) -> FakeResult:
        return self

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


class FakeAsyncSession:
    def __init__(self) -> None:
        self._results: list[Any] = []
        self._call_count = 0

    def set_results(self, results: list[Any]) -> None:
        self._results = results

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._call_count < len(self._results):
            result = self._results[self._call_count]
            self._call_count += 1
            return result
        return FakeResult()

    async def __aenter__(self) -> FakeAsyncSession:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


class FakeSessionFactory:
    def __init__(self, session: FakeAsyncSession | None = None) -> None:
        self._session = session or FakeAsyncSession()

    def __call__(self) -> FakeAsyncSession:
        return self._session


def _make_session_cookie(secret_key: str) -> str:
    import itsdangerous

    signer = itsdangerous.TimestampSigner(secret_key)
    return signer.sign("test-session-id").decode("utf-8")


def _create_app(
    user_id: uuid.UUID,
    db_session: FakeAsyncSession | None = None,
) -> Any:
    import fastapi

    import resonance.api.v1 as api_v1_module
    import resonance.connectors.registry as registry_module

    settings = _make_settings()
    fake_redis = FakeRedis()
    fake_redis.inject_session(
        "test-session-id", {"user_id": str(user_id), "user_role": "owner"}
    )

    app = fastapi.FastAPI(title="test", lifespan=None)
    app.state.settings = settings
    app.state.session_factory = FakeSessionFactory(db_session)
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(api_v1_module.router)
    app.state.connector_registry = registry_module.ConnectorRegistry()

    return app


def _make_venue(
    **overrides: Any,
) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "The Fillmore",
        "address": "1805 Geary Blvd",
        "city": "San Francisco",
        "state": "CA",
        "postal_code": "94115",
        "country": "US",
        "service_links": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_event(
    **overrides: Any,
) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "title": "Red Pears / Together PANGEA",
        "event_date": datetime.date(2026, 5, 15),
        "source_service": "songkick",
        "external_id": "sk-123",
        "external_url": "https://songkick.com/events/123",
        "service_links": None,
        "venue": _make_venue(),
        "artists": [],
        "artist_candidates": [],
        "created_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_artist(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "The Red Pears",
        "origin": None,
        "service_links": {"musicbrainz": "mb-123"},
        "created_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_track(**overrides: Any) -> SimpleNamespace:
    artist = overrides.pop("artist", None) or _make_artist()
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "title": "Hands On",
        "artist_id": artist.id,
        "artist": artist,
        "duration_ms": 195000,
        "service_links": {"spotify": "sp-456"},
        "created_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_listening_event(**overrides: Any) -> SimpleNamespace:
    track = overrides.pop("track", None) or _make_track()
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "user_id": uuid.uuid4(),
        "track_id": track.id,
        "track": track,
        "source_service": "lastfm",
        "listened_at": datetime.datetime(2026, 4, 27, 14, 30, tzinfo=datetime.UTC),
        "created_at": datetime.datetime(2026, 4, 27, 14, 30, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 4, 27, 14, 30, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_event_artist(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "event_id": uuid.uuid4(),
        "artist_id": uuid.uuid4(),
        "position": 0,
        "raw_name": "The Red Pears",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_event_candidate(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "event_id": uuid.uuid4(),
        "raw_name": "Unknown Artist",
        "matched_artist_id": None,
        "position": 0,
        "confidence_score": 0,
        "status": "pending",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# --- Fixtures ---


@pytest.fixture
def user_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
async def authed_client(user_id: uuid.UUID) -> AsyncIterator[httpx.AsyncClient]:
    """Authenticated client with empty DB."""
    db = FakeAsyncSession()
    app = _create_app(user_id, db)
    settings = _make_settings()
    cookie = _make_session_cookie(settings.session_secret_key)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={"session_id": cookie},
    ) as c:
        yield c


@pytest.fixture
async def unauthed_client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    import fastapi

    import resonance.api.v1 as api_v1_module
    import resonance.connectors.registry as registry_module

    settings = _make_settings()
    fake_redis = FakeRedis()

    app = fastapi.FastAPI(title="test", lifespan=None)
    app.state.settings = settings
    app.state.session_factory = FakeSessionFactory()
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(api_v1_module.router)
    app.state.connector_registry = registry_module.ConnectorRegistry()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# --- Events ---


class TestEventsAuth:
    async def test_list_requires_auth(self, unauthed_client: httpx.AsyncClient) -> None:
        resp = await unauthed_client.get("/api/v1/events")
        assert resp.status_code == 401

    async def test_detail_requires_auth(
        self, unauthed_client: httpx.AsyncClient
    ) -> None:
        resp = await unauthed_client.get(f"/api/v1/events/{uuid.uuid4()}")
        assert resp.status_code == 401


class TestEventsList:
    async def test_returns_empty_list(self, authed_client: httpx.AsyncClient) -> None:
        resp = await authed_client.get("/api/v1/events")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []
        assert data["page"] == 1
        assert data["has_next"] is False

    async def test_returns_events_with_venue(self, user_id: uuid.UUID) -> None:
        venue = _make_venue(name="Great American Music Hall", city="San Francisco")
        event = _make_event(title="Live at GAMH", venue=venue)

        db = FakeAsyncSession()
        db.set_results([FakeResult([event])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/events")

        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["title"] == "Live at GAMH"
        assert items[0]["venue"]["name"] == "Great American Music Hall"

    async def test_pagination_has_next(self, user_id: uuid.UUID) -> None:
        events = [_make_event(title=f"Event {i}") for i in range(PAGE_SIZE + 1)]

        db = FakeAsyncSession()
        db.set_results([FakeResult(events)])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/events")

        data = resp.json()
        assert len(data["items"]) == PAGE_SIZE
        assert data["has_next"] is True


class TestEventDetail:
    async def test_returns_event(self, user_id: uuid.UUID) -> None:
        event_id = uuid.uuid4()
        artist_link = _make_event_artist(event_id=event_id, raw_name="Together PANGEA")
        candidate = _make_event_candidate(
            event_id=event_id, raw_name="Opener TBD", status="pending"
        )
        event = _make_event(
            id=event_id,
            artists=[artist_link],
            artist_candidates=[candidate],
        )

        db = FakeAsyncSession()
        db.set_results([FakeResult([event])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get(f"/api/v1/events/{event_id}")

        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(event_id)
        assert len(data["artists"]) == 1
        assert data["artists"][0]["raw_name"] == "Together PANGEA"
        assert len(data["artist_candidates"]) == 1

    async def test_returns_404_when_not_found(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        resp = await authed_client.get(f"/api/v1/events/{uuid.uuid4()}")
        assert resp.status_code == 404


# --- Artists ---


class TestArtistsAuth:
    async def test_list_requires_auth(self, unauthed_client: httpx.AsyncClient) -> None:
        resp = await unauthed_client.get("/api/v1/artists")
        assert resp.status_code == 401

    async def test_detail_requires_auth(
        self, unauthed_client: httpx.AsyncClient
    ) -> None:
        resp = await unauthed_client.get(f"/api/v1/artists/{uuid.uuid4()}")
        assert resp.status_code == 401


class TestArtistsList:
    async def test_returns_empty_list(self, authed_client: httpx.AsyncClient) -> None:
        resp = await authed_client.get("/api/v1/artists")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []

    async def test_returns_artists(self, user_id: uuid.UUID) -> None:
        artist = _make_artist(name="Together PANGEA")

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/artists")

        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["name"] == "Together PANGEA"

    async def test_search_by_name(self, user_id: uuid.UUID) -> None:
        artist = _make_artist(name="The Red Pears")

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/artists?q=red+pears")

        assert resp.status_code == 200


class TestArtistDetail:
    async def test_returns_artist(self, user_id: uuid.UUID) -> None:
        artist_id = uuid.uuid4()
        artist = _make_artist(id=artist_id, name="The Red Pears")

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get(f"/api/v1/artists/{artist_id}")

        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(artist_id)
        assert data["name"] == "The Red Pears"

    async def test_returns_404_when_not_found(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        resp = await authed_client.get(f"/api/v1/artists/{uuid.uuid4()}")
        assert resp.status_code == 404


# --- Tracks ---


class TestTracksAuth:
    async def test_list_requires_auth(self, unauthed_client: httpx.AsyncClient) -> None:
        resp = await unauthed_client.get("/api/v1/tracks")
        assert resp.status_code == 401

    async def test_detail_requires_auth(
        self, unauthed_client: httpx.AsyncClient
    ) -> None:
        resp = await unauthed_client.get(f"/api/v1/tracks/{uuid.uuid4()}")
        assert resp.status_code == 401


class TestTracksList:
    async def test_returns_empty_list(self, authed_client: httpx.AsyncClient) -> None:
        resp = await authed_client.get("/api/v1/tracks")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []

    async def test_returns_tracks_with_artist(self, user_id: uuid.UUID) -> None:
        artist = _make_artist(name="The Red Pears")
        track = _make_track(title="Hands On", artist=artist, duration_ms=195000)

        db = FakeAsyncSession()
        db.set_results([FakeResult([track])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/tracks")

        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["title"] == "Hands On"
        assert items[0]["artist_name"] == "The Red Pears"
        assert items[0]["duration_ms"] == 195000

    async def test_search_by_title(self, user_id: uuid.UUID) -> None:
        track = _make_track(title="Hands On")

        db = FakeAsyncSession()
        db.set_results([FakeResult([track])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/tracks?q=hands")

        assert resp.status_code == 200


class TestTrackDetail:
    async def test_returns_track(self, user_id: uuid.UUID) -> None:
        track_id = uuid.uuid4()
        artist = _make_artist(name="The Red Pears")
        track = _make_track(id=track_id, title="Hands On", artist=artist)

        db = FakeAsyncSession()
        db.set_results([FakeResult([track])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get(f"/api/v1/tracks/{track_id}")

        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(track_id)
        assert data["title"] == "Hands On"
        assert data["artist"]["name"] == "The Red Pears"

    async def test_returns_404_when_not_found(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        resp = await authed_client.get(f"/api/v1/tracks/{uuid.uuid4()}")
        assert resp.status_code == 404


# --- History ---


class TestHistoryAuth:
    async def test_list_requires_auth(self, unauthed_client: httpx.AsyncClient) -> None:
        resp = await unauthed_client.get("/api/v1/history")
        assert resp.status_code == 401


class TestHistoryList:
    async def test_returns_empty_list(self, authed_client: httpx.AsyncClient) -> None:
        resp = await authed_client.get("/api/v1/history")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []

    async def test_returns_listening_events(self, user_id: uuid.UUID) -> None:
        artist = _make_artist(name="The Red Pears")
        track = _make_track(title="Hands On", artist=artist)
        listen = _make_listening_event(
            user_id=user_id, track=track, source_service="lastfm"
        )

        db = FakeAsyncSession()
        db.set_results([FakeResult([listen])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/history")

        items = resp.json()["items"]
        assert len(items) == 1
        assert items[0]["track"]["title"] == "Hands On"
        assert items[0]["track"]["artist_name"] == "The Red Pears"
        assert items[0]["source_service"] == "lastfm"
