"""Tests for the dev-environment seeding endpoint (POST /api/v1/admin/seed)."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import fastapi
import httpx
import pytest
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.api.v1 as api_v1_module
import resonance.config as config_module
import resonance.middleware.session as session_middleware
import resonance.models as models
import resonance.models.music as music_models
import resonance.models.taste as taste_models
import resonance.models.user as user_models
import resonance.types as types_module
from tests.test_api_admin import FakeRedis

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

_TOKEN = "seed-test-token"
_AUTH = {"Authorization": f"Bearer {_TOKEN}"}
_MBID = "8f6bd1e4-fbe1-4f50-aa9b-94c450ec0f11"


@pytest.fixture
async def maker() -> AsyncIterator[sa_async.async_sessionmaker[sa_async.AsyncSession]]:
    engine = sa_async.create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    try:
        yield sa_async.async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def _client(
    maker: sa_async.async_sessionmaker[sa_async.AsyncSession], *, enabled: bool
) -> httpx.AsyncClient:
    settings = config_module.Settings(admin_api_token=_TOKEN, dev_seed_enabled=enabled)
    app = fastapi.FastAPI(lifespan=None)
    app.state.settings = settings
    app.state.session_factory = maker
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=FakeRedis(),  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(api_v1_module.router)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


async def _post(
    maker: sa_async.async_sessionmaker[sa_async.AsyncSession],
    body: Any,
    *,
    enabled: bool = True,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    async with _client(maker, enabled=enabled) as c:
        return await c.post(
            "/api/v1/admin/seed",
            json=body,
            headers=_AUTH if headers is None else headers,
        )


async def _all(
    maker: sa_async.async_sessionmaker[sa_async.AsyncSession], model: Any
) -> list[Any]:
    async with maker() as db:
        return list((await db.execute(sa.select(model))).scalars().all())


class TestGating:
    async def test_disabled_by_default(self) -> None:
        assert config_module.Settings().dev_seed_enabled is False

    async def test_disabled_returns_404(self, maker: Any) -> None:
        resp = await _post(maker, {"artists": [{"name": "A"}]}, enabled=False)
        assert resp.status_code == 404
        assert await _all(maker, music_models.Artist) == []

    async def test_disabled_404s_before_validating_the_body(self, maker: Any) -> None:
        resp = await _post(maker, {"bogus": 1}, enabled=False)
        assert resp.status_code == 404

    async def test_requires_admin(self, maker: Any) -> None:
        resp = await _post(maker, {}, headers={})
        assert resp.status_code == 401
        resp = await _post(maker, {}, headers={"Authorization": "Bearer wrong"})
        assert resp.status_code == 403


class TestValidation:
    @pytest.mark.parametrize(
        "body",
        [
            {"unknown": []},
            {"artists": [{"name": ""}]},
            {"artists": [{"name": "A", "tags": [{"tag": "x", "count": -1}]}]},
            {"artists": [{"name": "A", "tracks": [{"title": "t", "listens": [{}]}]}]},
            {
                "artists": [
                    {
                        "name": "A",
                        "similar": [{"neighbor_name": "B", "connector": "nope"}],
                    }
                ]
            },
            {"user": {"display_name": "u", "role": "superuser"}},
            # A second owner would make "listens go to the owner" ambiguous.
            {"user": {"display_name": "u", "role": "owner"}},
            {"user": {"display_name": "u", "email": "e" * 256}},
            {
                "artists": [
                    {"name": "A", "tags": [{"tag": "x", "genre_mbid": "g" * 65}]}
                ]
            },
            {"artists": [{"name": "A", "tags": [{"tag": "x", "source": "s" * 65}]}]},
            {"artists": [{"name": "A", "tags": [{"tag": "x", "source": ""}]}]},
            {"artists": [{"name": "A", "tags": [{"tag": "x", "count": 2**31}]}]},
            {
                "artists": [
                    {
                        "name": "A",
                        "similar": [{"neighbor_name": "B", "neighbor_mbid": "m" * 65}],
                    }
                ]
            },
            {
                "artists": [
                    {"name": "A", "similar": [{"neighbor_name": "B", "rank": 2**31}]}
                ]
            },
            {"artists": [{"name": "A", "tracks": [{"title": "t", "duration_ms": -1}]}]},
            {
                "artists": [
                    {"name": "A", "tracks": [{"title": "t", "duration_ms": 2**31}]}
                ]
            },
            {
                "artists": [
                    {
                        "name": "A",
                        "tracks": [
                            {
                                "title": "t",
                                "listens": [{"listened_at": "2026-01-01T12:00:00"}],
                            }
                        ],
                    }
                ]
            },
        ],
    )
    async def test_rejects_invalid_scenarios(self, maker: Any, body: Any) -> None:
        resp = await _post(maker, body)
        assert resp.status_code == 422


class TestEntities:
    async def test_artists_keep_service_links_verbatim(self, maker: Any) -> None:
        legacy = {"listenbrainz": _MBID.upper()}
        canonical = {"musicbrainz": {"id": "aaaaaaaa-0000-0000-0000-000000000001"}}
        resp = await _post(
            maker,
            {
                "artists": [
                    {"name": "Legacy", "service_links": legacy},
                    {"name": "Canonical", "service_links": canonical},
                    {"name": "Bare"},
                ]
            },
        )
        assert resp.status_code == 200, resp.text
        assert resp.json()["counts"]["artists"] == 3
        by_name = {a.name: a for a in await _all(maker, music_models.Artist)}
        assert by_name["Legacy"].service_links == legacy
        assert by_name["Legacy"].musicbrainz_id == _MBID
        assert by_name["Canonical"].service_links == canonical
        assert by_name["Bare"].service_links is None

    async def test_tags(self, maker: Any) -> None:
        resp = await _post(
            maker,
            {
                "artists": [
                    {
                        "name": "A",
                        "tags": [
                            {"tag": "metal", "genre_mbid": "g-1", "count": 5},
                            {"tag": "seen live"},
                        ],
                    }
                ]
            },
        )
        assert resp.status_code == 200, resp.text
        tags = {t.tag: t for t in await _all(maker, music_models.ArtistTag)}
        assert tags["metal"].genre_mbid == "g-1"
        assert tags["metal"].count == 5
        assert tags["seen live"].genre_mbid is None
        assert tags["seen live"].source == "musicbrainz"
        artist_id = resp.json()["artists"][0]["id"]
        assert {str(t.artist_id) for t in tags.values()} == {artist_id}

    async def test_similarity_edges(self, maker: Any) -> None:
        resp = await _post(
            maker,
            {
                "artists": [
                    {
                        "name": "A",
                        "similar": [
                            {"neighbor_name": "B", "neighbor_mbid": _MBID, "rank": 1},
                            {
                                "neighbor_name": "C",
                                "rank": 2,
                                "connector": "listenbrainz",
                            },
                        ],
                    }
                ]
            },
        )
        assert resp.status_code == 200, resp.text
        edges = {
            e.neighbor_name: e for e in await _all(maker, taste_models.ArtistSimilarity)
        }
        assert edges["B"].neighbor_mbid == _MBID
        assert edges["B"].connector == types_module.ServiceType.LASTFM
        assert edges["C"].connector == types_module.ServiceType.LISTENBRAINZ
        assert edges["C"].rank == 2

    async def test_tracks_and_listens_go_to_the_owner(self, maker: Any) -> None:
        resp = await _post(
            maker,
            {
                "artists": [
                    {
                        "name": "A",
                        "tracks": [
                            {
                                "title": "One",
                                "duration_ms": 1000,
                                "popularity_score": 40,
                                "listens": [
                                    {"listened_at": "2026-01-01T00:00:00Z"},
                                    {
                                        "listened_at": "2026-01-02T00:00:00Z",
                                        "source_service": "lastfm",
                                    },
                                ],
                            },
                            {"title": "Two"},
                        ],
                    }
                ]
            },
        )
        assert resp.status_code == 200, resp.text
        data = resp.json()
        assert data["counts"] == {
            "artists": 1,
            "tags": 0,
            "similar": 0,
            "tracks": 2,
            "listens": 2,
        }
        users = await _all(maker, user_models.User)
        assert [u.role for u in users] == [types_module.UserRole.OWNER]
        assert data["user_id"] == str(users[0].id)
        tracks = {t.title: t for t in await _all(maker, music_models.Track)}
        assert tracks["One"].popularity_score == 40
        assert sorted(data["artists"][0]["track_ids"]) == sorted(
            str(t.id) for t in tracks.values()
        )
        listens = await _all(maker, music_models.ListeningEvent)
        assert {e.user_id for e in listens} == {users[0].id}
        assert {e.track_id for e in listens} == {tracks["One"].id}
        assert {e.source_service for e in listens} == {
            types_module.ServiceType.LISTENBRAINZ,
            types_module.ServiceType.LASTFM,
        }

    async def test_reuses_an_existing_owner(self, maker: Any) -> None:
        owner_id = uuid.uuid4()
        async with maker() as db:
            db.add(
                user_models.User(
                    id=owner_id, display_name="Me", role=types_module.UserRole.OWNER
                )
            )
            await db.commit()
        resp = await _post(maker, {"artists": []})
        assert resp.json()["user_id"] == str(owner_id)
        assert len(await _all(maker, user_models.User)) == 1

    async def test_creates_a_named_user_for_listens(self, maker: Any) -> None:
        resp = await _post(
            maker,
            {
                "user": {"display_name": "Tester", "email": "t@example.com"},
                "artists": [
                    {
                        "name": "A",
                        "tracks": [
                            {
                                "title": "T",
                                "listens": [{"listened_at": "2026-01-01T00:00:00Z"}],
                            }
                        ],
                    }
                ],
            },
        )
        assert resp.status_code == 200, resp.text
        users = await _all(maker, user_models.User)
        assert len(users) == 1
        assert users[0].display_name == "Tester"
        assert users[0].role == types_module.UserRole.USER
        listens = await _all(maker, music_models.ListeningEvent)
        assert listens[0].user_id == users[0].id


class TestConstraints:
    async def test_duplicate_mbid_is_refused_and_rolled_back(self, maker: Any) -> None:
        resp = await _post(
            maker,
            {
                "artists": [
                    {"name": "One", "service_links": {"musicbrainz": {"id": _MBID}}},
                    {"name": "Two", "service_links": {"listenbrainz": _MBID}},
                ]
            },
        )
        assert resp.status_code == 409
        assert await _all(maker, music_models.Artist) == []

    async def test_duplicate_tag_is_refused(self, maker: Any) -> None:
        resp = await _post(
            maker, {"artists": [{"name": "A", "tags": [{"tag": "x"}, {"tag": "x"}]}]}
        )
        assert resp.status_code == 409
        assert await _all(maker, music_models.Artist) == []
