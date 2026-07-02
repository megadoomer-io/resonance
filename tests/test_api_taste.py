"""Tests for the taste / genre-discovery API (#154 Arc 2, Phase 1).

The aggregation's load-bearing logic is the per-mbid representative-label pick and
the final ordering; those are unit-tested against a fake session that replays the
two canned query results. Auth + response contract are covered via HTTP.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any

import httpx
import sqlalchemy.ext.asyncio as sa_async

import resonance.api.v1.taste as taste_module
import resonance.config as config_module
import resonance.middleware.session as session_middleware
import resonance.models as models
import resonance.models.music as music_models

# --- fakes ---


class _FakeRows:
    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    def all(self) -> list[Any]:
        return self._rows


class _FakeDB:
    """Replays canned per-call results for successive ``execute`` calls."""

    def __init__(self, *results: list[Any]) -> None:
        self._results = list(results)
        self._i = 0

    async def execute(self, *args: Any, **kwargs: Any) -> _FakeRows:
        rows = self._results[self._i] if self._i < len(self._results) else []
        self._i += 1
        return _FakeRows(rows)

    async def __aenter__(self) -> _FakeDB:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


def _stat(genre_mbid: str, artist_count: int, total_votes: int) -> SimpleNamespace:
    return SimpleNamespace(
        genre_mbid=genre_mbid, artist_count=artist_count, total_votes=total_votes
    )


def _label(genre_mbid: str, tag: str, n: int) -> SimpleNamespace:
    return SimpleNamespace(genre_mbid=genre_mbid, tag=tag, n=n)


# --- get_top_genres: aggregation shaping ---


class TestGetTopGenres:
    async def test_orders_by_artist_count_desc(self) -> None:
        db = _FakeDB(
            [_stat("mbid-a", 10, 100), _stat("mbid-b", 3, 40)],
            [_label("mbid-a", "metal", 10), _label("mbid-b", "punk", 3)],
        )
        out = await taste_module.get_top_genres(db)  # type: ignore[arg-type]
        assert [g["genre_mbid"] for g in out] == ["mbid-a", "mbid-b"]
        assert out[0]["label"] == "metal"
        assert out[0]["artist_count"] == 10
        assert out[0]["total_votes"] == 100

    async def test_label_is_most_common_tag_variant(self) -> None:
        # Same genre_mbid, two label spellings; the one on more artists wins.
        db = _FakeDB(
            [_stat("mbid-a", 12, 90)],
            [_label("mbid-a", "Death Metal", 3), _label("mbid-a", "death metal", 9)],
        )
        out = await taste_module.get_top_genres(db)  # type: ignore[arg-type]
        assert out[0]["label"] == "death metal"

    async def test_label_tie_breaks_alphabetically(self) -> None:
        db = _FakeDB(
            [_stat("mbid-a", 8, 50)],
            [_label("mbid-a", "punk", 4), _label("mbid-a", "emo", 4)],
        )
        out = await taste_module.get_top_genres(db)  # type: ignore[arg-type]
        assert out[0]["label"] == "emo"

    async def test_empty_library_returns_empty(self) -> None:
        db = _FakeDB([])  # no stats rows -> short-circuits before label query
        out = await taste_module.get_top_genres(db)  # type: ignore[arg-type]
        assert out == []

    async def test_missing_label_falls_back_to_mbid(self) -> None:
        # Stats row with no matching label row -> label defaults to the mbid.
        db = _FakeDB([_stat("mbid-x", 2, 5)], [])
        out = await taste_module.get_top_genres(db)  # type: ignore[arg-type]
        assert out[0]["label"] == "mbid-x"

    async def test_counts_are_ints(self) -> None:
        db = _FakeDB([_stat("mbid-a", 1, 7)], [_label("mbid-a", "jazz", 1)])
        out = await taste_module.get_top_genres(db)  # type: ignore[arg-type]
        assert isinstance(out[0]["artist_count"], int)
        assert isinstance(out[0]["total_votes"], int)


# --- HTTP contract + auth ---


def _make_settings() -> config_module.Settings:
    return config_module.Settings(
        spotify_client_id="test-id",
        spotify_client_secret="test-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
    )


class _FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> int:
        return sum(1 for k in keys if self._store.pop(k, None) is not None)

    async def aclose(self) -> None:
        pass

    def inject_session(self, session_id: str, data: dict[str, Any]) -> None:
        import json

        self._store[f"session:{session_id}"] = json.dumps(data)


class _SessionFactory:
    def __init__(self, db: Any) -> None:
        self._db = db

    def __call__(self) -> Any:
        return self._db


def _cookie(secret_key: str) -> str:
    import itsdangerous

    return itsdangerous.TimestampSigner(secret_key).sign("test-session-id").decode()


def _build_app(user_id: uuid.UUID | None, db: Any) -> Any:
    import fastapi

    import resonance.api.v1 as api_v1_module
    import resonance.connectors.registry as registry_module

    settings = _make_settings()
    redis = _FakeRedis()
    if user_id is not None:
        redis.inject_session(
            "test-session-id", {"user_id": str(user_id), "user_role": "owner"}
        )
    app = fastapi.FastAPI(title="test", lifespan=None)
    app.state.settings = settings
    app.state.session_factory = _SessionFactory(db)
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(api_v1_module.router)
    app.state.connector_registry = registry_module.ConnectorRegistry()
    return app


class TestTasteGenresEndpoint:
    async def test_requires_auth(self) -> None:
        app = _build_app(None, _FakeDB())
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            resp = await c.get("/api/v1/taste/genres")
        assert resp.status_code == 401

    async def test_returns_genres(self) -> None:
        user_id = uuid.uuid4()
        db = _FakeDB(
            [_stat("mbid-a", 5, 20), _stat("mbid-b", 2, 8)],
            [_label("mbid-a", "metal", 5), _label("mbid-b", "punk", 2)],
        )
        app = _build_app(user_id, db)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://t",
            cookies={"session_id": _cookie(_make_settings().session_secret_key)},
        ) as c:
            resp = await c.get("/api/v1/taste/genres")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2
        assert data["items"][0]["label"] == "metal"
        assert data["items"][0]["artist_count"] == 5


# --- real-SQL integration (async SQLite) ---
#
# The fakes above exercise only the Python roll-up. These run get_top_genres'
# actual SQL against a real (in-memory async SQLite) engine, so the load-bearing
# claim -- GROUP BY genre_mbid with COUNT(DISTINCT artist_id) dedupes tag-string
# variants of one genre on one artist -- is genuinely tested, not just asserted.


async def _seeded_engine() -> sa_async.async_sessionmaker[sa_async.AsyncSession]:
    engine = sa_async.create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    return sa_async.async_sessionmaker(engine, expire_on_commit=False)


def _artist(name: str) -> music_models.Artist:
    return music_models.Artist(id=uuid.uuid4(), name=name)


def _tag(artist_id: uuid.UUID, tag: str, genre_mbid: str | None, count: int) -> Any:
    return music_models.ArtistTag(
        id=uuid.uuid4(),
        artist_id=artist_id,
        tag=tag,
        genre_mbid=genre_mbid,
        count=count,
        source="test",
    )


class TestGetTopGenresRealSQL:
    async def test_variants_on_one_artist_count_once(self) -> None:
        # ONE artist tagged with two spellings of the SAME genre_mbid must count
        # as a single artist -- this is the exact double-count the design prevents
        # and the case the fake-based tests cannot exercise.
        maker = await _seeded_engine()
        metal = "11111111-1111-1111-1111-111111111111"
        async with maker() as db:
            a = _artist("solo")
            db.add(a)
            await db.flush()
            db.add_all([_tag(a.id, "metal", metal, 9), _tag(a.id, "Metal", metal, 2)])
            await db.commit()
            out = await taste_module.get_top_genres(db)
        assert len(out) == 1
        assert out[0]["genre_mbid"] == metal
        assert out[0]["artist_count"] == 1  # NOT 2 -- the dedupe under test
        # Both variants tie at n=1 (same single artist); the deterministic pick
        # falls to the raw-tag tiebreak, and "Metal" < "metal" in ASCII.
        assert out[0]["label"] == "Metal"
        assert out[0]["total_votes"] == 11  # 9 + 2

    async def test_ordering_labels_and_folksonomy_exclusion(self) -> None:
        maker = await _seeded_engine()
        metal = "11111111-1111-1111-1111-111111111111"
        punk = "22222222-2222-2222-2222-222222222222"
        async with maker() as db:
            artists = [_artist(f"a{i}") for i in range(5)]
            db.add_all(artists)
            await db.flush()
            db.add_all(
                [
                    _tag(artists[0].id, "metal", metal, 9),
                    _tag(artists[1].id, "metal", metal, 5),
                    _tag(artists[2].id, "Metal", metal, 2),  # variant, 3rd artist
                    _tag(artists[3].id, "punk", punk, 7),
                    _tag(artists[4].id, "seen live", None, 3),  # folksonomy -> excluded
                ]
            )
            await db.commit()
            out = await taste_module.get_top_genres(db)
        assert [g["genre_mbid"] for g in out] == [metal, punk]
        assert out[0]["artist_count"] == 3  # three distinct metal artists
        assert out[0]["label"] == "metal"  # borne by 2 artists vs "Metal" by 1
        assert out[0]["total_votes"] == 16
        assert out[1]["artist_count"] == 1
        assert out[1]["label"] == "punk"

    async def test_empty_library(self) -> None:
        maker = await _seeded_engine()
        async with maker() as db:
            out = await taste_module.get_top_genres(db)
        assert out == []

    async def test_limit_cutoff_is_deterministic_on_ties(self) -> None:
        # Three genres each on exactly one artist (tied artist_count). The LIMIT=2
        # cut must be stable across calls -- the SQL genre_mbid tie-break guarantees
        # the same two survive every time.
        maker = await _seeded_engine()
        g1, g2, g3 = (str(uuid.UUID(int=i)) for i in (1, 2, 3))
        async with maker() as db:
            arts = [_artist(f"a{i}") for i in range(3)]
            db.add_all(arts)
            await db.flush()
            db.add_all(
                [
                    _tag(arts[0].id, "g1", g1, 1),
                    _tag(arts[1].id, "g2", g2, 1),
                    _tag(arts[2].id, "g3", g3, 1),
                ]
            )
            await db.commit()
            first = [g["genre_mbid"] for g in await taste_module.get_top_genres(db, 2)]
            second = [g["genre_mbid"] for g in await taste_module.get_top_genres(db, 2)]
        assert len(first) == 2
        assert first == second  # deterministic cutoff
