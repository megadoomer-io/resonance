"""Export bookkeeping must not read as a playlist content change (#190)."""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

import pytest
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.models as models
import resonance.models.playlist as playlist_models
import resonance.worker as worker_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

Factory = sa_async.async_sessionmaker[sa_async.AsyncSession]

_BEFORE = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)
_EXPORTED = datetime.datetime(2020, 1, 2, tzinfo=datetime.UTC)


@pytest.fixture
async def factory() -> AsyncIterator[Factory]:
    engine = sa_async.create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    try:
        yield sa_async.async_sessionmaker(engine, expire_on_commit=False)
    finally:
        await engine.dispose()


def _aware(value: datetime.datetime) -> datetime.datetime:
    return value if value.tzinfo else value.replace(tzinfo=datetime.UTC)


async def _seed(factory: Factory) -> uuid.UUID:
    playlist_id = uuid.uuid4()
    async with factory() as session:
        session.add(
            playlist_models.Playlist(
                id=playlist_id, user_id=uuid.uuid4(), name="PL", updated_at=_BEFORE
            )
        )
        await session.commit()
    return playlist_id


async def _export(factory: Factory, playlist_id: uuid.UUID) -> None:
    async with factory() as session:
        playlist = await session.get(playlist_models.Playlist, playlist_id)
        assert playlist is not None
        await worker_module._merge_spotify_export_link(
            session,
            playlist,
            "conn",
            {"playlist_id": "sp", "exported_at": _EXPORTED.isoformat()},
        )
        await session.commit()


async def _is_stale(factory: Factory, playlist_id: uuid.UUID) -> bool:
    """The page's staleness rule: content changed after the last export."""
    async with factory() as session:
        row = (
            await session.execute(
                sa.select(
                    playlist_models.Playlist.updated_at,
                    playlist_models.Playlist.service_links,
                ).where(playlist_models.Playlist.id == playlist_id)
            )
        ).one()
    exported_at = datetime.datetime.fromisoformat(
        row.service_links["spotify"]["conn"]["exported_at"]
    )
    return _aware(row.updated_at) > exported_at


async def test_export_link_write_keeps_updated_at(factory: Factory) -> None:
    playlist_id = await _seed(factory)
    await _export(factory, playlist_id)
    assert not await _is_stale(factory, playlist_id)


async def test_content_change_after_export_reads_stale(factory: Factory) -> None:
    playlist_id = await _seed(factory)
    await _export(factory, playlist_id)
    async with factory() as session:
        playlist = await session.get(playlist_models.Playlist, playlist_id)
        assert playlist is not None
        playlist.name = "Renamed"
        await session.commit()
    assert await _is_stale(factory, playlist_id)
