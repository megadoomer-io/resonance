"""MBID lookup and convergent import on Postgres via artists.musicbrainz_id (#203)."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock

import pytest
import sqlalchemy as sa

import resonance.config as config_module
import resonance.models.music as music_models
import resonance.services.artist_import as artist_import
import resonance.sync.backfill as backfill_module
import resonance.types as types_module

if TYPE_CHECKING:
    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration


async def _seed(db: TaskDB, name: str, links: Any) -> uuid.UUID:
    artist = music_models.Artist(id=uuid.uuid4(), name=name, service_links=links)
    async with db.factory() as session:
        session.add(artist)
        await session.commit()
    return artist.id


@pytest.mark.parametrize(
    ("links", "query"),
    [
        ({"musicbrainz": {"id": "abc-1"}}, "abc-1"),
        ({"listenbrainz": "abc-1"}, "abc-1"),
        ({"musicbrainz": {"id": "ABC-1"}}, " abc-1 "),
        ({"listenbrainz": "abc-1"}, "ABC-1"),
    ],
)
async def test_find_local_artist_by_mbid_uses_the_column(
    pg_task_db: TaskDB, links: dict[str, Any], query: str
) -> None:
    artist_id = await _seed(pg_task_db, "Elder", links)

    async with pg_task_db.factory() as session:
        found = await artist_import.find_local_artist_by_mbid(session, query)

    assert found is not None
    assert found.id == artist_id


class _BarrierConnector:
    """Resolves an MBID only once every caller has asked, so all race to insert."""

    def __init__(self, parties: int) -> None:
        self._barrier = asyncio.Barrier(parties)

    async def get_artist_by_mbid(self, mbid: str) -> dict[str, Any]:
        await asyncio.wait_for(self._barrier.wait(), timeout=10)
        return {"mbid": mbid, "name": "Elder"}


async def _count_artists(db: TaskDB) -> int:
    async with db.factory() as session:
        return int(
            (
                await session.execute(
                    sa.select(sa.func.count()).select_from(music_models.Artist)
                )
            ).scalar_one()
        )


async def test_concurrent_imports_converge_on_one_artist(pg_task_db: TaskDB) -> None:
    connector = _BarrierConnector(parties=2)

    async def import_once(mbid: str) -> music_models.Artist | None:
        async with pg_task_db.factory() as session:
            return await artist_import.import_artist_by_mbid(session, connector, mbid)

    first, second = await asyncio.gather(
        import_once("race-mbid"), import_once("RACE-MBID")
    )

    assert first is not None
    assert second is not None
    assert first.id == second.id
    assert await _count_artists(pg_task_db) == 1


async def test_insert_keeps_the_callers_transaction(pg_task_db: TaskDB) -> None:
    """Losing the race rolls back only the savepoint, not earlier writes."""
    winner_id = await _seed(pg_task_db, "Elder", {"listenbrainz": "dup"})

    async with pg_task_db.factory() as session:
        bystander = music_models.Artist(id=uuid.uuid4(), name="Bystander")
        session.add(bystander)
        await session.flush()
        artist, created = await artist_import.insert_artist_or_get_winner(
            session,
            music_models.Artist(
                name="Elder", service_links={"musicbrainz": {"id": "DUP"}}
            ),
        )
        await session.commit()

    assert not created
    assert artist.id == winner_id
    assert await _count_artists(pg_task_db) == 2


async def test_other_integrity_errors_propagate(pg_task_db: TaskDB) -> None:
    existing = await _seed(pg_task_db, "Elder", None)

    async with pg_task_db.factory() as session:
        with pytest.raises(sa.exc.IntegrityError):
            await artist_import.insert_artist_or_get_winner(
                session,
                music_models.Artist(
                    id=existing, name="Clash", service_links={"listenbrainz": "z"}
                ),
            )


async def test_backfill_skips_an_mbid_held_by_another_artist(
    pg_task_db: TaskDB,
) -> None:
    holder = await _seed(pg_task_db, "Portishead", {"musicbrainz": {"id": "p-1"}})
    async with pg_task_db.factory() as session:
        await session.execute(
            sa.update(music_models.Artist)
            .where(music_models.Artist.id == holder)
            .values(mb_attempted_at=sa.func.now())
        )
        await session.commit()
    candidate = await _seed(pg_task_db, "Portishead", {"lastfm": "p"})
    connector = AsyncMock()
    connector.search_artists = AsyncMock(
        return_value=[{"mbid": "P-1", "name": "Portishead"}]
    )

    async with pg_task_db.factory() as session:
        counts = await backfill_module.backfill_artists(
            session, config_module.Settings(), connector, harvested={}
        )

    assert counts.collision == 1
    assert counts.matched == 0
    async with pg_task_db.factory() as session:
        row = await session.get(music_models.Artist, candidate)
        assert row is not None
        assert row.service_links == {"lastfm": "p"}
        assert row.musicbrainz_id is None
        assert row.mb_match_status == types_module.MatchStatus.MATCHED
