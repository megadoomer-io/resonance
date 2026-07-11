"""Tests for entity matching API endpoints: artist search, candidates, merge."""

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


# --- Test infrastructure (mirrors test_api_data.py) ---


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

    def scalar_one(self) -> Any:
        if not self._items:
            msg = "No result found"
            raise Exception(msg)
        return self._items[0]


class FakeAsyncSession:
    def __init__(self) -> None:
        self._results: list[Any] = []
        self._call_count = 0
        self._added: list[Any] = []
        self._deleted: list[Any] = []

    def set_results(self, results: list[Any]) -> None:
        self._results = results

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._call_count < len(self._results):
            result = self._results[self._call_count]
            self._call_count += 1
            return result
        return FakeResult()

    def add(self, obj: Any) -> None:
        self._added.append(obj)

    async def delete(self, obj: Any) -> None:
        self._deleted.append(obj)

    async def commit(self) -> None:
        pass

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


def _make_event(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "title": "Red Pears / Together PANGEA",
        "event_date": datetime.date(2026, 5, 15),
        "source_service": "songkick",
        "external_id": "sk-123",
        "external_url": "https://songkick.com/events/123",
        "service_links": None,
        "venue": None,
        "artists": [],
        "artist_candidates": [],
        "created_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
        "updated_at": datetime.datetime(2026, 4, 1, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_candidate(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "event_id": uuid.uuid4(),
        "raw_name": "Unknown Artist",
        "normalized_raw_name": "unknown artist",
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


# --- Artist Search ---


class TestArtistSearch:
    async def test_returns_results(self, user_id: uuid.UUID) -> None:
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
            resp = await c.get("/api/v1/artists/search?q=Red+Pears")

        assert resp.status_code == 200
        data = resp.json()
        assert len(data["items"]) == 1
        assert data["items"][0]["name"] == "The Red Pears"

    async def test_accepts_seed_ids_and_returns_genres(
        self, user_id: uuid.UUID
    ) -> None:
        # The #136 wiring contract: the seed_artist_ids query param binds (no 422)
        # and each result carries a `genres` list for picker disambiguation.
        artist = _make_artist(name="Nite")
        tag = SimpleNamespace(
            artist_id=artist.id, tag="black metal", genre_mbid="g1", count=9
        )
        db = FakeAsyncSession()
        # Query order: exact-name, substring, in-library, seed-load (#152 on-demand
        # seed tag fetch -- no seed rows here, so no LB fetch), tags.
        db.set_results(
            [
                FakeResult([artist]),
                FakeResult([]),
                FakeResult([]),
                FakeResult([]),
                FakeResult([tag]),
            ]
        )

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.get(
                f"/api/v1/artists/search?q=Nite&seed_artist_ids={uuid.uuid4()}"
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["items"][0]["name"] == "Nite"
        assert data["items"][0]["genres"] == ["black metal"]

    async def test_requires_q_param(self, authed_client: httpx.AsyncClient) -> None:
        resp = await authed_client.get("/api/v1/artists/search")
        assert resp.status_code == 422

    async def test_returns_empty_when_no_match(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        resp = await authed_client.get("/api/v1/artists/search?q=nonexistent")
        assert resp.status_code == 200
        data = resp.json()
        assert data["items"] == []

    async def test_requires_auth(self) -> None:
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
            resp = await c.get("/api/v1/artists/search?q=test")
        assert resp.status_code == 401


# --- Candidate Accept/Reject ---


class TestCandidateAccept:
    async def test_returns_404_for_missing_candidate(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        event_id = uuid.uuid4()
        candidate_id = uuid.uuid4()
        resp = await authed_client.post(
            f"/api/v1/events/{event_id}/candidates/{candidate_id}/accept"
        )
        assert resp.status_code == 404

    async def test_returns_400_when_no_matched_artist(self, user_id: uuid.UUID) -> None:
        event_id = uuid.uuid4()
        candidate = _make_candidate(
            event_id=event_id,
            matched_artist_id=None,
        )

        db = FakeAsyncSession()
        # _get_candidate query returns the candidate
        db.set_results([FakeResult([candidate])])

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
                f"/api/v1/events/{event_id}/candidates/{candidate.id}/accept"
            )

        assert resp.status_code == 400


class TestCandidateReject:
    async def test_returns_404_for_missing_candidate(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        event_id = uuid.uuid4()
        candidate_id = uuid.uuid4()
        resp = await authed_client.post(
            f"/api/v1/events/{event_id}/candidates/{candidate_id}/reject"
        )
        assert resp.status_code == 404


def _make_event_artist(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "event_id": uuid.uuid4(),
        "artist_id": uuid.uuid4(),
        "position": 0,
        "raw_name": "Some Artist",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestPatchCandidate:
    async def test_patch_confidence_score(self, user_id: uuid.UUID) -> None:
        event_id = uuid.uuid4()
        candidate = _make_candidate(
            event_id=event_id,
            confidence_score=90,
            status="pending",
        )

        db = FakeAsyncSession()
        db.set_results([FakeResult([candidate])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.patch(
                f"/api/v1/events/{event_id}/candidates/{candidate.id}",
                json={"confidence_score": 100},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "updated"
        assert candidate.confidence_score == 100

    async def test_patch_status_to_accepted_creates_event_artist(
        self, user_id: uuid.UUID
    ) -> None:
        event_id = uuid.uuid4()
        artist_id = uuid.uuid4()
        candidate = _make_candidate(
            event_id=event_id,
            matched_artist_id=artist_id,
            status="pending",
            raw_name="Together PANGEA",
        )

        db = FakeAsyncSession()
        db.set_results(
            [
                FakeResult([candidate]),  # _get_candidate
                FakeResult(),  # EventArtist lookup (none exists)
            ]
        )

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.patch(
                f"/api/v1/events/{event_id}/candidates/{candidate.id}",
                json={"status": "accepted"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "updated"
        assert data["event_artist_id"] is not None
        assert len(db._added) == 1

        import resonance.models.concert as concert_models

        assert isinstance(db._added[0], concert_models.EventArtist)

    async def test_patch_status_to_accepted_without_artist_returns_400(
        self, user_id: uuid.UUID
    ) -> None:
        event_id = uuid.uuid4()
        candidate = _make_candidate(
            event_id=event_id,
            matched_artist_id=None,
            status="pending",
        )

        db = FakeAsyncSession()
        db.set_results([FakeResult([candidate])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.patch(
                f"/api/v1/events/{event_id}/candidates/{candidate.id}",
                json={"status": "accepted"},
            )

        assert resp.status_code == 400

    async def test_patch_status_from_accepted_removes_event_artist(
        self, user_id: uuid.UUID
    ) -> None:
        event_id = uuid.uuid4()
        artist_id = uuid.uuid4()
        candidate = _make_candidate(
            event_id=event_id,
            matched_artist_id=artist_id,
            status="accepted",
        )
        ea = _make_event_artist(event_id=event_id, artist_id=artist_id)

        db = FakeAsyncSession()
        db.set_results(
            [
                FakeResult([candidate]),  # _get_candidate
                FakeResult([ea]),  # EventArtist lookup for removal
            ]
        )

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.patch(
                f"/api/v1/events/{event_id}/candidates/{candidate.id}",
                json={"status": "rejected"},
            )

        assert resp.status_code == 200
        assert len(db._deleted) == 1
        assert db._deleted[0] is ea

    async def test_patch_accept_idempotent(self, user_id: uuid.UUID) -> None:
        event_id = uuid.uuid4()
        artist_id = uuid.uuid4()
        ea = _make_event_artist(event_id=event_id, artist_id=artist_id)
        candidate = _make_candidate(
            event_id=event_id,
            matched_artist_id=artist_id,
            status="accepted",
        )

        db = FakeAsyncSession()
        db.set_results(
            [
                FakeResult([candidate]),  # _get_candidate
                FakeResult([ea]),  # EventArtist lookup (already exists)
            ]
        )

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.patch(
                f"/api/v1/events/{event_id}/candidates/{candidate.id}",
                json={"status": "accepted"},
            )

        assert resp.status_code == 200
        assert len(db._added) == 0
        assert len(db._deleted) == 0

    async def test_patch_validates_artist_exists(self, user_id: uuid.UUID) -> None:
        event_id = uuid.uuid4()
        candidate = _make_candidate(event_id=event_id, status="pending")

        db = FakeAsyncSession()
        db.set_results(
            [
                FakeResult([candidate]),  # _get_candidate
                FakeResult(),  # Artist lookup (not found)
            ]
        )

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.patch(
                f"/api/v1/events/{event_id}/candidates/{candidate.id}",
                json={"matched_artist_id": str(uuid.uuid4())},
            )

        assert resp.status_code == 404
        assert "Artist not found" in resp.json()["detail"]

    async def test_patch_returns_404_for_missing_candidate(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        event_id = uuid.uuid4()
        candidate_id = uuid.uuid4()
        resp = await authed_client.patch(
            f"/api/v1/events/{event_id}/candidates/{candidate_id}",
            json={"confidence_score": 50},
        )
        assert resp.status_code == 404

    async def test_patch_empty_body_is_noop(self, user_id: uuid.UUID) -> None:
        event_id = uuid.uuid4()
        candidate = _make_candidate(
            event_id=event_id,
            status="pending",
            confidence_score=90,
        )

        db = FakeAsyncSession()
        db.set_results([FakeResult([candidate])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.patch(
                f"/api/v1/events/{event_id}/candidates/{candidate.id}",
                json={},
            )

        assert resp.status_code == 200
        assert resp.json()["status"] == "updated"
        assert candidate.confidence_score == 90


class TestCreateCandidate:
    async def test_returns_404_for_missing_event(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        event_id = uuid.uuid4()
        artist_id = uuid.uuid4()
        resp = await authed_client.post(
            f"/api/v1/events/{event_id}/candidates",
            json={"artist_id": str(artist_id)},
        )
        assert resp.status_code == 404


# --- Merge Preview/Confirm ---


class TestMergePreview:
    async def test_returns_404_for_missing_artist(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        artist_id = uuid.uuid4()
        other_id = uuid.uuid4()
        resp = await authed_client.post(
            f"/api/v1/matching/artists/{artist_id}/merge/{other_id}"
        )
        assert resp.status_code == 404

    async def test_preview_shape(self, user_id: uuid.UUID) -> None:
        canonical = _make_artist(name="The Red Pears")
        other = _make_artist(name="Red Pears")

        db = FakeAsyncSession()
        # Queries: canonical artist, other artist, canonical tracks count,
        # other tracks count, canonical events count, other events count,
        # canonical listens count, other listens count
        db.set_results(
            [
                FakeResult([canonical]),
                FakeResult([other]),
                FakeResult([5]),
                FakeResult([3]),
                FakeResult([2]),
                FakeResult([1]),
                FakeResult([100]),
                FakeResult([50]),
            ]
        )

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
                f"/api/v1/matching/artists/{canonical.id}/merge/{other.id}"
            )

        assert resp.status_code == 200
        data = resp.json()
        assert "canonical" in data
        assert "other" in data
        assert "merged_service_links" in data
        assert data["canonical"]["name"] == "The Red Pears"
        assert data["other"]["name"] == "Red Pears"
        assert data["canonical"]["tracks"] == 5
        assert data["other"]["tracks"] == 3


class TestMergeConfirm:
    async def test_returns_404_for_missing_artist(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        artist_id = uuid.uuid4()
        other_id = uuid.uuid4()
        resp = await authed_client.post(
            f"/api/v1/matching/artists/{artist_id}/merge/{other_id}/confirm"
        )
        assert resp.status_code == 404

    async def test_track_merge_returns_404_for_missing_track(
        self, authed_client: httpx.AsyncClient
    ) -> None:
        track_id = uuid.uuid4()
        other_id = uuid.uuid4()
        resp = await authed_client.post(
            f"/api/v1/matching/tracks/{track_id}/merge/{other_id}/confirm"
        )
        assert resp.status_code == 404
