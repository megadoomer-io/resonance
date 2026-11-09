"""Enrich and calendar sync hold no row locks across external calls (#199)."""

from __future__ import annotations

import asyncio
import datetime
import uuid
from typing import TYPE_CHECKING, Any

import pytest
import sqlalchemy as sa

import resonance.models.music as music_models
import resonance.models.taste as taste_models
import resonance.types as types_module
import resonance.worker as worker_module

if TYPE_CHECKING:
    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration


async def _seed_artist(db: TaskDB, **fields: Any) -> music_models.Artist:
    artist = music_models.Artist(id=uuid.uuid4(), name="Seed", **fields)
    async with db.factory() as session:
        session.add(artist)
        await session.commit()
    return artist


class _Neighbors:
    service_type = types_module.ServiceType.LISTENBRAINZ

    def __init__(self, names: list[str]) -> None:
        self.names = names

    async def get_similar_artists(
        self, name: str, *, mbid: str | None, limit: int
    ) -> list[dict[str, Any]]:
        return [{"name": n, "mbid": None} for n in self.names]


async def test_concurrent_similarity_refresh_does_not_conflict(
    pg_task_db: TaskDB,
) -> None:
    """Two refreshes of one source both insert the same edges; neither raises."""
    artist = await _seed_artist(pg_task_db)
    now = datetime.datetime.now(datetime.UTC)
    hold_a = asyncio.Event()

    async with pg_task_db.factory() as a, pg_task_db.factory() as b:
        commit_a = a.commit
        calls = 0

        async def held_commit() -> None:
            nonlocal calls
            calls += 1
            if calls == 2:  # A's edge write stays uncommitted until released
                await hold_a.wait()
            await commit_a()

        a.commit = held_commit  # type: ignore[method-assign]
        connector = _Neighbors(["Elder", "Kyuss"])
        run_a = asyncio.create_task(
            worker_module._fetch_similar_with_store(
                a, connector, artist, limit=30, now=now
            )
        )
        while calls < 2:
            await asyncio.sleep(0.01)
        # B's insert waits on A's uncommitted unique keys, then skips them.
        run_b = asyncio.create_task(
            worker_module._fetch_similar_with_store(
                b, connector, artist, limit=30, now=now
            )
        )
        await asyncio.sleep(0.2)
        hold_a.set()
        await asyncio.gather(run_a, run_b)

    async with pg_task_db.factory() as session:
        names = (
            await session.execute(
                sa.select(taste_models.ArtistSimilarity.neighbor_name).where(
                    taste_models.ArtistSimilarity.source_artist_id == artist.id
                )
            )
        ).scalars()
        assert sorted(names) == ["Elder", "Kyuss"]
