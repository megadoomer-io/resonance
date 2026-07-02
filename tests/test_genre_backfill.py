"""Tests for the artist genre-tag backfill (#136 genre model)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
import sqlalchemy as sa

import resonance.config as config_module
import resonance.models.music as music_module
import resonance.services.artist_tags as artist_tags_module
import resonance.sync.backfill as backfill_module


def _settings() -> config_module.Settings:
    return config_module.Settings(mbid_mapper_batch_size=50)


def _artist(mbid: str | None) -> SimpleNamespace:
    links = {"musicbrainz": {"id": mbid}} if mbid else None
    return SimpleNamespace(
        id=uuid.uuid4(), service_links=links, genre_attempted_at=None
    )


class _FakeResult:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def scalars(self) -> _FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items


class _RoutingSession:
    """Fake session: Select -> next batch, Delete -> noop; records adds/commits."""

    def __init__(self, select_batches: list[list[Any]]) -> None:
        self._batches = list(select_batches)
        self.added: list[music_module.ArtistTag] = []
        self.deletes = 0
        self.commits = 0

    async def execute(self, stmt: Any, *a: Any, **k: Any) -> Any:
        if isinstance(stmt, sa.Delete):
            self.deletes += 1
            return SimpleNamespace()
        # Treat everything else as the SELECT.
        batch = self._batches.pop(0) if self._batches else []
        return _FakeResult(batch)

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def commit(self) -> None:
        self.commits += 1


def _client(tags: dict[str, list[artist_tags_module.ArtistTagResult]]) -> Any:
    return SimpleNamespace(fetch_tags=AsyncMock(return_value=tags))


def _tag(name: str, count: int = 1, genre_mbid: str | None = None) -> Any:
    return artist_tags_module.ArtistTagResult(
        tag=name, count=count, genre_mbid=genre_mbid
    )


class TestRunGenreBackfill:
    @pytest.mark.anyio()
    async def test_happy_path_stores_tags_and_stamps(self) -> None:
        art = _artist("33333333-3333-3333-3333-333333333333")
        session = _RoutingSession([[art], []])
        client = _client(
            {
                "33333333-3333-3333-3333-333333333333": [
                    _tag("rock", 5, "g-rock"),
                    _tag("indie", 2),
                ]
            }
        )

        counts = await backfill_module.run_genre_backfill(session, _settings(), client)

        assert counts.candidates == 1
        assert counts.updated == 1
        assert counts.no_tags == 0
        assert art.genre_attempted_at is not None  # stamped
        assert session.deletes == 1  # wholesale-replace issued
        stored = {t.tag: t for t in session.added}
        assert stored["rock"].genre_mbid == "g-rock"
        assert stored["rock"].count == 5
        assert stored["indie"].genre_mbid is None

    @pytest.mark.anyio()
    async def test_no_tags_still_stamps_attempted(self) -> None:
        art = _artist("33333333-3333-3333-3333-333333333333")
        session = _RoutingSession([[art], []])
        client = _client({})  # LB returned nothing for this MBID

        counts = await backfill_module.run_genre_backfill(session, _settings(), client)

        assert counts.no_tags == 1
        assert counts.updated == 0
        assert art.genre_attempted_at is not None  # attempted, no tags
        assert session.added == []
        assert session.deletes == 1  # still clears any stale tags

    @pytest.mark.anyio()
    async def test_transient_leaves_unattempted(self) -> None:
        art = _artist("33333333-3333-3333-3333-333333333333")
        session = _RoutingSession([[art], []])
        client = SimpleNamespace(
            fetch_tags=AsyncMock(
                side_effect=artist_tags_module.ArtistTagsUnavailableError("down")
            )
        )

        counts = await backfill_module.run_genre_backfill(session, _settings(), client)

        assert counts.transient == 1
        assert counts.updated == 0
        assert art.genre_attempted_at is None  # retried next run (CRITICAL)
        assert session.added == []

    @pytest.mark.anyio()
    async def test_negative_count_clamped_to_zero(self) -> None:
        art = _artist("33333333-3333-3333-3333-333333333333")
        session = _RoutingSession([[art], []])
        client = _client(
            {"33333333-3333-3333-3333-333333333333": [_tag("weird", -3, "g")]}
        )

        await backfill_module.run_genre_backfill(session, _settings(), client)

        assert session.added[0].count == 0  # CHECK(count >= 0) protected

    @pytest.mark.anyio()
    async def test_invalid_mbid_not_fetched_but_stamped(self) -> None:
        # A malformed MBID must NOT reach fetch_tags (it would 400 the batch);
        # the artist is stamped attempted-no-tags instead.
        good = _artist("a74b1b7f-71a5-4011-9441-d0b5e4122711")
        bad = _artist("not-a-uuid")
        session = _RoutingSession([[good, bad], []])
        fetch = AsyncMock(return_value={"a74b1b7f-71a5-4011-9441-d0b5e4122711": []})
        client = SimpleNamespace(fetch_tags=fetch)

        counts = await backfill_module.run_genre_backfill(session, _settings(), client)

        # Only the valid MBID was sent to the endpoint.
        fetch.assert_awaited_once_with(["a74b1b7f-71a5-4011-9441-d0b5e4122711"])
        assert good.genre_attempted_at is not None
        assert bad.genre_attempted_at is not None  # stamped, not re-scanned
        assert counts.candidates == 2

    @pytest.mark.anyio()
    async def test_all_invalid_batch_skips_fetch(self) -> None:
        bad = _artist("garbage")
        session = _RoutingSession([[bad], []])
        fetch = AsyncMock(return_value={})
        client = SimpleNamespace(fetch_tags=fetch)

        counts = await backfill_module.run_genre_backfill(session, _settings(), client)

        fetch.assert_not_awaited()  # no valid MBID -> no 400 risk
        assert bad.genre_attempted_at is not None
        assert counts.no_tags == 1

    @pytest.mark.anyio()
    async def test_multi_batch_pagination(self) -> None:
        a1 = _artist("11111111-1111-1111-1111-111111111111")
        a2 = _artist("22222222-2222-2222-2222-222222222222")
        session = _RoutingSession([[a1], [a2], []])
        client = _client(
            {
                "11111111-1111-1111-1111-111111111111": [_tag("rock")],
                "22222222-2222-2222-2222-222222222222": [_tag("jazz")],
            }
        )

        counts = await backfill_module.run_genre_backfill(session, _settings(), client)

        assert counts.candidates == 2
        assert counts.updated == 2
        assert session.commits == 2  # one per populated batch
