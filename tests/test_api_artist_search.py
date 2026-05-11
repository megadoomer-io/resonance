"""Tests for artist search-external and import API endpoints."""

from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.crypto as crypto_module
import resonance.middleware.session as session_middleware
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


# --- Test infrastructure ---


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
    """Fake DB result supporting scalars().all() and scalar_one_or_none()."""

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
        self.added: list[Any] = []
        self._committed = False

    def set_results(self, results: list[Any]) -> None:
        self._results = results

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._call_count < len(self._results):
            result = self._results[self._call_count]
            self._call_count += 1
            return result
        return FakeResult()

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        pass

    async def commit(self) -> None:
        self._committed = True

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
    *,
    registry: registry_module.ConnectorRegistry | None = None,
) -> Any:
    import fastapi

    import resonance.api.v1 as api_v1_module

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
    app.state.connector_registry = registry or registry_module.ConnectorRegistry()

    return app


def _make_artist(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "Coldplay",
        "origin": None,
        "disambiguation": "",
        "artist_type": "Group",
        "area": "London",
        "begin_year": 1996,
        "end_year": None,
        "service_links": {
            "musicbrainz": {"id": "cc197bad-dc9c-440d-a5b5-d52ba2e14234"},
        },
        "created_at": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_lb_connector_mock() -> AsyncMock:
    """Create a mock ListenBrainz connector with search and lookup methods."""
    mock = AsyncMock(spec=base_module.BaseConnector)
    mock.service_type = types_module.ServiceType.LISTENBRAINZ
    mock.capabilities = frozenset({base_module.ConnectorCapability.AUTHENTICATION})
    mock.parse_url = AsyncMock(return_value=None)
    mock.search_artists = AsyncMock(return_value=[])
    mock.get_artist_by_mbid = AsyncMock(return_value=None)
    return mock


def _make_spotify_connector_mock() -> AsyncMock:
    """Create a mock Spotify connector with search methods."""
    mock = AsyncMock(spec=base_module.BaseConnector)
    mock.service_type = types_module.ServiceType.SPOTIFY
    mock.capabilities = frozenset({base_module.ConnectorCapability.AUTHENTICATION})
    mock.parse_url = AsyncMock(return_value=None)
    mock.search_artists = AsyncMock(return_value=[])
    return mock


# --- Fixtures ---


@pytest.fixture
def user_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
async def unauthed_client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    import fastapi

    import resonance.api.v1 as api_v1_module

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


# --- search-external ---


class TestSearchExternalAuth:
    async def test_requires_auth(self, unauthed_client: httpx.AsyncClient) -> None:
        resp = await unauthed_client.get("/api/v1/artists/search-external?q=Coldplay")
        assert resp.status_code == 401


class TestSearchExternalMusicBrainz:
    async def test_search_by_query_returns_results(self, user_id: uuid.UUID) -> None:
        mb_results = [
            {
                "mbid": "cc197bad-dc9c-440d-a5b5-d52ba2e14234",
                "name": "Coldplay",
                "disambiguation": "",
                "artist_type": "Group",
                "area": "London",
                "begin_year": 1996,
                "end_year": None,
            }
        ]
        lb_mock = _make_lb_connector_mock()
        lb_mock.search_artists = AsyncMock(return_value=mb_results)

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        # The endpoint queries for local MBID matches — return empty (not imported)
        db.set_results([FakeResult([])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/artists/search-external?q=Coldplay")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "Coldplay"
        assert data[0]["mbid"] == "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        assert data[0]["already_imported"] is False
        assert data[0]["local_artist_id"] is None

    async def test_search_explicit_service_param(self, user_id: uuid.UUID) -> None:
        mb_results = [
            {
                "mbid": "cc197bad-dc9c-440d-a5b5-d52ba2e14234",
                "name": "Coldplay",
                "disambiguation": "",
                "artist_type": "Group",
                "area": "London",
                "begin_year": 1996,
                "end_year": None,
            }
        ]
        lb_mock = _make_lb_connector_mock()
        lb_mock.search_artists = AsyncMock(return_value=mb_results)

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get(
                "/api/v1/artists/search-external?q=Coldplay&services=musicbrainz"
            )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1

    async def test_url_musicbrainz_returns_single_result(
        self, user_id: uuid.UUID
    ) -> None:
        mbid = "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        mb_artist = {
            "mbid": mbid,
            "name": "Coldplay",
            "disambiguation": "",
            "artist_type": "Group",
            "area": "London",
            "begin_year": 1996,
            "end_year": None,
        }

        lb_mock = _make_lb_connector_mock()
        # parse_url is a static method on the real connector; mock it
        lb_mock.parse_url = lambda url: mbid  # type: ignore[assignment]
        lb_mock.get_artist_by_mbid = AsyncMock(return_value=mb_artist)

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get(
                f"/api/v1/artists/search-external?url=https://musicbrainz.org/artist/{mbid}"
            )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["mbid"] == mbid
        assert data[0]["name"] == "Coldplay"

    async def test_url_spotify_returns_result(self, user_id: uuid.UUID) -> None:
        spotify_id = "4gzpq5DPGxSnKTe4SA8HAU"

        sp_mock = _make_spotify_connector_mock()
        sp_mock.parse_url = lambda url: spotify_id  # type: ignore[assignment]
        sp_mock.search_artists = AsyncMock(
            return_value=[{"spotify_id": spotify_id, "name": "Coldplay"}]
        )

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.SPOTIFY] = sp_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        # First query: check for existing Spotify connection (for token)
        encryption_key = _make_settings().token_encryption_key
        encrypted_token = crypto_module.encrypt_token(
            "test-access-token", encryption_key
        )
        db.set_results(
            [
                FakeResult(
                    [
                        SimpleNamespace(
                            encrypted_access_token=encrypted_token,
                            token_expires_at=datetime.datetime(
                                2099, 1, 1, tzinfo=datetime.UTC
                            ),
                        )
                    ]
                ),
            ]
        )

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get(
                f"/api/v1/artists/search-external?url=https://open.spotify.com/artist/{spotify_id}"
            )

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "Coldplay"
        assert data[0]["service"] == "spotify"
        assert data[0]["spotify_id"] == spotify_id

    async def test_already_imported_flag(self, user_id: uuid.UUID) -> None:
        local_artist_id = uuid.uuid4()
        mbid = "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        mb_results = [
            {
                "mbid": mbid,
                "name": "Coldplay",
                "disambiguation": "",
                "artist_type": "Group",
                "area": "London",
                "begin_year": 1996,
                "end_year": None,
            }
        ]
        lb_mock = _make_lb_connector_mock()
        lb_mock.search_artists = AsyncMock(return_value=mb_results)

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        local_artist = _make_artist(
            id=local_artist_id,
            service_links={"musicbrainz": {"id": mbid}},
        )
        db = FakeAsyncSession()
        # The MBID lookup returns a local match
        db.set_results([FakeResult([local_artist])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get("/api/v1/artists/search-external?q=Coldplay")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["already_imported"] is True
        assert data[0]["local_artist_id"] == str(local_artist_id)

    async def test_requires_q_or_url(self, user_id: uuid.UUID) -> None:
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
            resp = await c.get("/api/v1/artists/search-external")

        assert resp.status_code == 400


# --- import ---


class TestImportAuth:
    async def test_requires_auth(self, unauthed_client: httpx.AsyncClient) -> None:
        resp = await unauthed_client.post(
            "/api/v1/artists/import",
            json={"mbid": "abc", "name": "Test"},
        )
        assert resp.status_code == 401


class TestImportArtist:
    async def test_creates_new_artist(self, user_id: uuid.UUID) -> None:
        mbid = "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        db = FakeAsyncSession()
        # First query: check if MBID already exists locally — no match
        db.set_results([FakeResult([])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                "/api/v1/artists/import",
                json={
                    "mbid": mbid,
                    "name": "Coldplay",
                    "disambiguation": "",
                    "artist_type": "Group",
                    "area": "London",
                    "begin_year": 1996,
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "Coldplay"
        assert data["disambiguation"] == ""
        assert data["artist_type"] == "Group"
        assert data["area"] == "London"
        assert data["begin_year"] == 1996
        assert data["service_links"]["musicbrainz"]["id"] == mbid
        # Should have added an artist to the session
        assert len(db.added) == 1

    async def test_returns_existing_artist_if_mbid_exists(
        self, user_id: uuid.UUID
    ) -> None:
        mbid = "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        existing_id = uuid.uuid4()
        existing = _make_artist(
            id=existing_id,
            name="Coldplay",
            service_links={"musicbrainz": {"id": mbid}},
        )

        db = FakeAsyncSession()
        # MBID lookup returns the existing artist
        db.set_results([FakeResult([existing])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                "/api/v1/artists/import",
                json={"mbid": mbid, "name": "Coldplay"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["id"] == str(existing_id)
        # Should NOT have added a new artist
        assert len(db.added) == 0

    async def test_populates_musicbrainz_service_link(self, user_id: uuid.UUID) -> None:
        mbid = "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        db = FakeAsyncSession()
        db.set_results([FakeResult([])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                "/api/v1/artists/import",
                json={"mbid": mbid, "name": "Coldplay"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["service_links"]["musicbrainz"]["id"] == mbid

    async def test_populates_spotify_service_link(self, user_id: uuid.UUID) -> None:
        mbid = "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        spotify_id = "4gzpq5DPGxSnKTe4SA8HAU"
        db = FakeAsyncSession()
        db.set_results([FakeResult([])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                "/api/v1/artists/import",
                json={
                    "mbid": mbid,
                    "name": "Coldplay",
                    "service_ids": {"spotify": spotify_id},
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["service_links"]["spotify"]["id"] == spotify_id
        assert data["service_links"]["musicbrainz"]["id"] == mbid
