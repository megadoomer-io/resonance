"""MBID lookup and convergent import on Postgres via artists.musicbrainz_id (#203)."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import pytest

import resonance.models.music as music_models
import resonance.services.artist_import as artist_import

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
