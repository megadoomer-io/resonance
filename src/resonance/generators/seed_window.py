"""Seed-window DB helper: top listened artists in a time range (#rediscovery).

The DB half of the reusable seed-window primitive. Its pure counterpart --
``pool.resolve_window_bounds`` -- turns a :class:`pool.ListeningWindow` into
concrete ``(start, end)`` bounds; this module turns those bounds into the user's
top seed artists.

It lives in its own lightweight module (sqlalchemy + music models only, no
connectors/arq) so both the worker generation path and the web seed-preview
endpoint can import it without pulling in the whole worker import graph.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa

import resonance.models.music as music_models

if TYPE_CHECKING:
    import datetime
    import uuid

    import sqlalchemy.ext.asyncio as sa_async


async def seed_artists_in_window(
    session: sa_async.AsyncSession,
    user_id: uuid.UUID,
    start: datetime.datetime,
    end: datetime.datetime,
    limit: int,
) -> list[uuid.UUID]:
    """Top artists by distinct-track listens in ``[start, end]`` (#rediscovery).

    Ranks by ``COUNT(DISTINCT track_id)`` per artist (not raw scrobbles), matching
    the ``track_coverage`` definition elsewhere, so one repeat-played track can't
    dominate the seed set. Bounds are inclusive on both ends. Returns artist ids
    highest-count-first, capped at ``limit``.
    """
    result = await session.execute(
        sa.select(
            music_models.Track.artist_id,
            sa.func.count(sa.distinct(music_models.ListeningEvent.track_id)).label(
                "cnt"
            ),
        )
        .join(
            music_models.Track,
            music_models.Track.id == music_models.ListeningEvent.track_id,
        )
        .where(
            music_models.ListeningEvent.user_id == user_id,
            music_models.ListeningEvent.listened_at >= start,
            music_models.ListeningEvent.listened_at <= end,
        )
        .group_by(music_models.Track.artist_id)
        .order_by(sa.desc("cnt"))
        .limit(limit)
    )
    return [row[0] for row in result.all()]
