"""Backfill batches make their external calls outside a transaction (#197).

A worker session idle inside a transaction past the timeout is ended by
Postgres, so slow per-item external calls must not run inside one.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any

import pytest
import sqlalchemy as sa

import resonance.config as config_module
import resonance.database as database_module
import resonance.models as models
import resonance.models.music as music_models
import resonance.sync.backfill as backfill_module
import resonance.types as types_module
import resonance.worker as worker_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import sqlalchemy.ext.asyncio as sa_async

    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration

_SLOW = 1.5  # past the 1s idle timeout, per external call


@pytest.fixture
async def factory(
    pg_task_db: TaskDB,
) -> AsyncIterator[sa_async.async_sessionmaker[sa_async.AsyncSession]]:
    async with pg_task_db.factory() as session:
        path = str((await session.execute(sa.text("SHOW search_path"))).scalar_one())
    settings = config_module.Settings(worker_idle_tx_timeout_seconds=1)
    engine = database_module.create_async_engine(
        settings,
        server_settings={
            **worker_module.worker_server_settings(settings),
            "search_path": path.split(",")[0].strip('" '),
        },
    )
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    try:
        yield database_module.create_session_factory(engine)
    finally:
        await engine.dispose()


def _settings() -> config_module.Settings:
    return config_module.Settings(mbid_mapper_batch_size=2)


class _SlowSearch:
    async def search_artists(self, name: str, limit: int = 1) -> list[dict[str, Any]]:
        await asyncio.sleep(_SLOW)
        return [{"mbid": str(uuid.uuid4()), "name": name}]


class _SlowPopularity:
    async def get_recording_popularity(self, mbids: list[str]) -> dict[str, int]:
        await asyncio.sleep(_SLOW)
        return dict.fromkeys(mbids, 1000)


async def test_backfill_artists_survives_slow_searches(
    factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
) -> None:
    async with factory() as session:
        session.add_all(
            music_models.Artist(id=uuid.uuid4(), name=f"Band {i}") for i in range(3)
        )
        await session.commit()

    async with factory() as session:
        counts = await backfill_module.backfill_artists(
            session, _settings(), _SlowSearch(), harvested={}
        )

    assert counts.matched == 3
    async with factory() as session:
        statuses = (
            await session.execute(sa.select(music_models.Artist.mb_match_status))
        ).scalars()
        assert set(statuses) == {types_module.MatchStatus.MATCHED}


async def test_popularity_backfill_survives_slow_fetch(
    factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
) -> None:
    artist = music_models.Artist(id=uuid.uuid4(), name="Band")
    async with factory() as session:
        session.add(artist)
        session.add_all(
            music_models.Track(
                id=uuid.uuid4(),
                title=f"Song {i}",
                artist_id=artist.id,
                service_links={"musicbrainz": {"id": str(uuid.uuid4())}},
            )
            for i in range(3)
        )
        await session.commit()

    async with factory() as session:
        counts = await backfill_module.run_popularity_backfill(
            session, _settings(), _SlowPopularity()
        )

    assert counts.updated == 3
