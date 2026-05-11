"""Tests for lazy artist enrichment endpoint."""

from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.middleware.session as session_middleware
import resonance.types as types_module
import resonance.ui.routes as ui_routes_module

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
        self._flushed = False

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
        self._flushed = True

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
    app.include_router(ui_routes_module.router)
    app.state.connector_registry = registry or registry_module.ConnectorRegistry()

    return app


def _make_artist(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "Radiohead",
        "origin": None,
        "disambiguation": None,
        "artist_type": None,
        "area": None,
        "begin_year": None,
        "end_year": None,
        "service_links": {
            "musicbrainz": {"id": "a74b1b7f-71a5-4011-9441-d0b5e4122711"},
        },
        "created_at": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_lb_connector_mock() -> AsyncMock:
    """Create a mock ListenBrainz connector with get_artist_by_mbid."""
    mock = AsyncMock(spec=base_module.BaseConnector)
    mock.service_type = types_module.ServiceType.LISTENBRAINZ
    mock.capabilities = frozenset({base_module.ConnectorCapability.AUTHENTICATION})
    mock.parse_url = AsyncMock(return_value=None)
    mock.search_artists = AsyncMock(return_value=[])
    mock.get_artist_by_mbid = AsyncMock(return_value=None)
    return mock


# --- Fixtures ---


@pytest.fixture
def user_id() -> uuid.UUID:
    return uuid.uuid4()


# --- Tests ---


class TestArtistEnrichAuth:
    """Enrichment endpoint requires authentication."""

    async def test_requires_auth(self) -> None:
        settings = _make_settings()
        fake_redis = FakeRedis()
        import fastapi

        app = fastapi.FastAPI(title="test", lifespan=None)
        app.state.settings = settings
        app.state.session_factory = FakeSessionFactory()
        app.add_middleware(
            session_middleware.SessionMiddleware,
            redis=fake_redis,  # type: ignore[arg-type]
            secret_key=settings.session_secret_key,
        )
        app.include_router(ui_routes_module.router)
        app.state.connector_registry = registry_module.ConnectorRegistry()

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test", follow_redirects=False
        ) as client:
            artist_id = uuid.uuid4()
            resp = await client.get(f"/partials/artist-enrich/{artist_id}")
            assert resp.status_code == 307
            assert resp.headers["location"] == "/login"


class TestArtistEnrichWithMBID:
    """Artist with MBID and null disambiguation gets enriched."""

    async def test_enriches_artist_from_musicbrainz(self, user_id: uuid.UUID) -> None:
        mbid = "a74b1b7f-71a5-4011-9441-d0b5e4122711"
        artist = _make_artist(
            disambiguation=None,
            service_links={"musicbrainz": {"id": mbid}},
        )

        lb_mock = _make_lb_connector_mock()
        lb_mock.get_artist_by_mbid = AsyncMock(
            return_value={
                "mbid": mbid,
                "name": "Radiohead",
                "disambiguation": "English rock band",
                "artist_type": "Group",
                "area": "Abingdon",
                "begin_year": 1985,
                "end_year": None,
            }
        )

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                f"/partials/artist-enrich/{artist.id}",
                cookies={"session_id": cookie},
            )
            assert resp.status_code == 200
            assert "English rock band" in resp.text

        lb_mock.get_artist_by_mbid.assert_awaited_once_with(mbid)
        assert artist.disambiguation == "English rock band"
        assert artist.artist_type == "Group"
        assert artist.area == "Abingdon"
        assert artist.begin_year == 1985
        assert artist.end_year is None
        assert "enrichment_requested_at" in artist.service_links["musicbrainz"]


class TestArtistAlreadyEnriched:
    """Artist with non-null disambiguation skips MB lookup."""

    async def test_already_enriched_skips_api_call(self, user_id: uuid.UUID) -> None:
        mbid = "a74b1b7f-71a5-4011-9441-d0b5e4122711"
        artist = _make_artist(
            disambiguation="English rock band",
            artist_type="Group",
            area="Abingdon",
            service_links={"musicbrainz": {"id": mbid}},
        )

        lb_mock = _make_lb_connector_mock()
        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                f"/partials/artist-enrich/{artist.id}",
                cookies={"session_id": cookie},
            )
            assert resp.status_code == 200
            assert "English rock band" in resp.text

        lb_mock.get_artist_by_mbid.assert_not_awaited()


class TestArtistNoMBID:
    """Artist with no MBID skips MB lookup."""

    async def test_no_mbid_skips_api_call(self, user_id: uuid.UUID) -> None:
        artist = _make_artist(
            disambiguation=None,
            service_links={},
        )

        lb_mock = _make_lb_connector_mock()
        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                f"/partials/artist-enrich/{artist.id}",
                cookies={"session_id": cookie},
            )
            assert resp.status_code == 200

        lb_mock.get_artist_by_mbid.assert_not_awaited()


class TestStaleEnrichmentRequest:
    """Enrichment request older than 3 minutes is retried."""

    async def test_stale_enrichment_retries(self, user_id: uuid.UUID) -> None:
        stale_time = (
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
        ).isoformat()
        artist = _make_artist(
            disambiguation=None,
            service_links={
                "musicbrainz": {
                    "id": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
                    "enrichment_requested_at": stale_time,
                },
            },
        )

        lb_mock = _make_lb_connector_mock()
        lb_mock.get_artist_by_mbid = AsyncMock(
            return_value={
                "mbid": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
                "name": "Radiohead",
                "disambiguation": "English rock band",
                "artist_type": "Group",
                "area": "Abingdon",
                "begin_year": 1985,
                "end_year": None,
            }
        )

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                f"/partials/artist-enrich/{artist.id}",
                cookies={"session_id": cookie},
            )
            assert resp.status_code == 200
            assert "English rock band" in resp.text

        lb_mock.get_artist_by_mbid.assert_awaited_once()
        assert artist.disambiguation == "English rock band"


class TestRecentEnrichmentRequest:
    """Recent enrichment request (<3 min) with null disambiguation is skipped."""

    async def test_recent_enrichment_skips(self, user_id: uuid.UUID) -> None:
        recent_time = (
            datetime.datetime.now(datetime.UTC) - datetime.timedelta(seconds=30)
        ).isoformat()
        artist = _make_artist(
            disambiguation=None,
            service_links={
                "musicbrainz": {
                    "id": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
                    "enrichment_requested_at": recent_time,
                },
            },
        )

        lb_mock = _make_lb_connector_mock()
        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                f"/partials/artist-enrich/{artist.id}",
                cookies={"session_id": cookie},
            )
            assert resp.status_code == 200

        lb_mock.get_artist_by_mbid.assert_not_awaited()
        # Disambiguation should remain None
        assert artist.disambiguation is None


class TestArtistNotFound:
    """Non-existent artist returns empty HTML."""

    async def test_unknown_artist_returns_empty(self, user_id: uuid.UUID) -> None:
        db = FakeAsyncSession()
        db.set_results([FakeResult([])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                f"/partials/artist-enrich/{uuid.uuid4()}",
                cookies={"session_id": cookie},
            )
            assert resp.status_code == 200
            assert resp.text == ""


class TestArtistEnrichLegacyMBID:
    """Artist with legacy listenbrainz MBID format also gets enriched."""

    async def test_legacy_mbid_triggers_enrichment(self, user_id: uuid.UUID) -> None:
        artist = _make_artist(
            disambiguation=None,
            service_links={
                "listenbrainz": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
            },
        )

        lb_mock = _make_lb_connector_mock()
        lb_mock.get_artist_by_mbid = AsyncMock(
            return_value={
                "mbid": "a74b1b7f-71a5-4011-9441-d0b5e4122711",
                "name": "Radiohead",
                "disambiguation": "English rock band",
                "artist_type": "Group",
                "area": "Abingdon",
                "begin_year": 1985,
                "end_year": None,
            }
        )

        registry = registry_module.ConnectorRegistry()
        registry._connectors[types_module.ServiceType.LISTENBRAINZ] = lb_mock  # type: ignore[assignment]

        db = FakeAsyncSession()
        db.set_results([FakeResult([artist])])

        app = _create_app(user_id, db, registry=registry)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)

        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://test"
        ) as client:
            resp = await client.get(
                f"/partials/artist-enrich/{artist.id}",
                cookies={"session_id": cookie},
            )
            assert resp.status_code == 200
            assert "English rock band" in resp.text

        lb_mock.get_artist_by_mbid.assert_awaited_once_with(
            "a74b1b7f-71a5-4011-9441-d0b5e4122711"
        )
        assert artist.disambiguation == "English rock band"
