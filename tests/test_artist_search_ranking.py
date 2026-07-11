"""Tests for artist search disambiguation ranking (#136 Arc 1 phase 4)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest

import resonance.api.v1.artists as artists_module
import resonance.sync.backfill as backfill_module


def _artist(name: str) -> SimpleNamespace:
    return SimpleNamespace(id=uuid.uuid4(), name=name)


def _tag(genre_mbid: str | None, count: int) -> SimpleNamespace:
    return SimpleNamespace(genre_mbid=genre_mbid, count=count)


def _rank(
    candidates: list[Any],
    in_library: set[uuid.UUID],
    tags: dict[uuid.UUID, list[Any]],
    seeds: list[list[tuple[str | None, float]]],
) -> list[str]:
    ranked = artists_module.rank_search_candidates(candidates, in_library, tags, seeds)
    return [a.name for a in ranked]


class TestRankSearchCandidates:
    def test_in_library_beats_cold_match(self) -> None:
        cold = _artist("Nite")  # electronic collision, alphabetically same
        mine = _artist("Nite")  # the one we have tracks for
        order = _rank([cold, mine], {mine.id}, {}, [])
        assert order[0] == "Nite"  # both named Nite; the in-library one is first
        ranked = artists_module.rank_search_candidates([cold, mine], {mine.id}, {}, [])
        assert ranked[0] is mine

    def test_metal_seed_prefers_metal_over_electronic(self) -> None:
        # Neither in library; genre affinity to metal seeds breaks the tie.
        metal = _artist("Nite")
        electronic = _artist("Nite")
        tags = {
            metal.id: [_tag("g-metal", 9), _tag("g-thrash", 2)],
            electronic.id: [_tag("g-house", 8), _tag("g-techno", 3)],
        }
        seeds = [[("g-metal", 8), ("g-black", 3)], [("g-metal", 5)]]
        ranked = artists_module.rank_search_candidates(
            [electronic, metal], set(), tags, seeds
        )
        assert ranked[0] is metal

    def test_unknown_genre_outranks_confirmed_mismatch(self) -> None:
        # untagged (could be the right band) must beat a known off-genre artist.
        untagged = _artist("Nite")
        wrong = _artist("Nite")
        tags = {wrong.id: [_tag("g-polka", 5)]}  # untagged has no tags
        seeds = [[("g-metal", 8)]]
        ranked = artists_module.rank_search_candidates(
            [wrong, untagged], set(), tags, seeds
        )
        assert ranked[0] is untagged

    def test_no_seeds_falls_back_to_in_library_then_name(self) -> None:
        b = _artist("Beta")
        a = _artist("Alpha")
        order = _rank([b, a], set(), {}, [])
        assert order == ["Alpha", "Beta"]

    def test_in_library_dominates_genre(self) -> None:
        # An in-library off-genre artist still beats a cold on-genre one: the
        # library signal is primary (you listen to the one you mean).
        cold_metal = _artist("X")
        mine_pop = _artist("X")
        tags = {
            cold_metal.id: [_tag("g-metal", 9)],
            mine_pop.id: [_tag("g-pop", 9)],
        }
        seeds = [[("g-metal", 9)]]
        ranked = artists_module.rank_search_candidates(
            [cold_metal, mine_pop], {mine_pop.id}, tags, seeds
        )
        assert ranked[0] is mine_pop


class _FakeScalars:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def all(self) -> list[Any]:
        return self._items


class _FakeSeedResult:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def scalars(self) -> _FakeScalars:
        return _FakeScalars(self._items)


class _FakeDB:
    """Minimal async session: SELECT returns seeds; records commit/rollback."""

    def __init__(self, seeds: list[Any]) -> None:
        self._seeds = seeds
        self.commits = 0
        self.rollbacks = 0

    async def execute(self, *a: Any, **k: Any) -> _FakeSeedResult:
        return _FakeSeedResult(self._seeds)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


def _fake_request() -> Any:
    settings = SimpleNamespace(mbid_mapper_batch_size=50)
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(settings=settings))
    )


class TestEnsureSeedTags:
    """_ensure_seed_tags orchestration: commit on success, degrade on failure (#152)."""

    @pytest.mark.anyio()
    async def test_commits_when_tags_written(self, monkeypatch: Any) -> None:
        db = _FakeDB([SimpleNamespace(id=uuid.uuid4())])
        monkeypatch.setattr(
            backfill_module, "fetch_and_persist_tags", AsyncMock(return_value=True)
        )
        await artists_module._ensure_seed_tags(_fake_request(), db, [uuid.uuid4()])
        assert db.commits == 1
        assert db.rollbacks == 0

    @pytest.mark.anyio()
    async def test_no_commit_when_endpoint_unavailable(self, monkeypatch: Any) -> None:
        # fetch_and_persist_tags returns False (LB down) -> nothing written, no commit.
        db = _FakeDB([SimpleNamespace(id=uuid.uuid4())])
        monkeypatch.setattr(
            backfill_module, "fetch_and_persist_tags", AsyncMock(return_value=False)
        )
        await artists_module._ensure_seed_tags(_fake_request(), db, [uuid.uuid4()])
        assert db.commits == 0
        assert db.rollbacks == 0

    @pytest.mark.anyio()
    async def test_rolls_back_and_degrades_on_http_error(
        self, monkeypatch: Any
    ) -> None:
        # A hard HTTP error must not fail the search: roll back, do not commit.
        db = _FakeDB([SimpleNamespace(id=uuid.uuid4())])
        monkeypatch.setattr(
            backfill_module,
            "fetch_and_persist_tags",
            AsyncMock(side_effect=httpx.HTTPError("boom")),
        )
        await artists_module._ensure_seed_tags(_fake_request(), db, [uuid.uuid4()])
        assert db.commits == 0
        assert db.rollbacks == 1
