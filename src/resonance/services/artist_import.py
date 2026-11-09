"""Resolve and import artists by MusicBrainz ID.

Centralizes the "find-or-create a local Artist row from an MBID" flow so both
the HTTP import endpoint (``api/v1/artists.py``) and the background worker
(adjacent-artist discovery, issue #115 Phase 2) share one dedup + create path.

Dedup is MBID-first, on ``Artist.musicbrainz_id`` (normalized from the
canonical ``service_links["musicbrainz"]["id"]`` or the legacy flat
``service_links["listenbrainz"]``).
"""

from __future__ import annotations

import typing
from typing import Any

import sqlalchemy as sa
import structlog

import resonance.models.music as music_models
import resonance.services.artist_utils as artist_utils

if typing.TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger(__name__)


async def find_local_artist_by_mbid(
    session: sa_async.AsyncSession, mbid: str
) -> music_models.Artist | None:
    """Find a local artist by MBID via the normalized ``musicbrainz_id`` column.

    The column covers the canonical ``service_links["musicbrainz"]["id"]`` and
    the legacy flat ``service_links["listenbrainz"]`` storage, case-insensitively.
    Until the unique index exists a shared MBID can match several rows; the
    oldest wins rather than raising.

    Args:
        session: Async DB session.
        mbid: The MusicBrainz artist ID.

    Returns:
        The matching Artist, or None.
    """
    normalized = artist_utils.normalize_mbid(mbid)
    if normalized is None:
        return None
    stmt = (
        sa.select(music_models.Artist)
        .where(music_models.Artist.musicbrainz_id == normalized)
        .order_by(music_models.Artist.created_at, music_models.Artist.id)
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def import_artist_by_mbid(
    session: sa_async.AsyncSession,
    connector: Any,
    mbid: str,
) -> music_models.Artist | None:
    """Resolve an artist by MBID and create a local Artist row if needed.

    Dedups by MBID first: if an artist with this MBID already exists it is
    returned without a MusicBrainz call. Otherwise the MBID is resolved via the
    connector (``get_artist_by_mbid``), an Artist is created with canonical
    ``service_links`` and returned.

    **This commits the caller's session**, including any writes the caller has
    pending: once before the MusicBrainz call (so no transaction is held across
    it, #199) and once after the insert or the re-check that finds a concurrent
    import. Flush or commit your own work first, and don't call this mid-way
    through a change that must stay atomic. The insert runs in a savepoint, so a
    failed insert rolls back only the savepoint.

    Args:
        session: Async DB session.
        connector: A connector exposing ``get_artist_by_mbid`` (the
            ListenBrainz/MusicBrainz connector).
        mbid: MusicBrainz artist identifier.

    Returns:
        The existing or newly-created Artist, or None if the MBID could not be
        resolved on MusicBrainz.
    """
    existing = await find_local_artist_by_mbid(session, mbid)
    if existing is not None:
        return existing

    await session.commit()
    data = await connector.get_artist_by_mbid(mbid)
    if data is None:
        logger.warning("artist_import_mbid_unresolved", mbid=mbid)
        return None

    # Another writer may have imported it while we were fetching.
    existing = await find_local_artist_by_mbid(session, mbid)
    if existing is not None:
        await session.commit()
        return existing

    artist = music_models.Artist(
        name=data["name"],
        disambiguation=data.get("disambiguation") or None,
        artist_type=data.get("artist_type") or None,
        area=data.get("area") or None,
        begin_year=data.get("begin_year"),
        end_year=data.get("end_year"),
        service_links={"musicbrainz": {"id": mbid}},
    )
    async with session.begin_nested():
        session.add(artist)
    await session.commit()
    logger.info(
        "artist_imported_by_mbid",
        mbid=mbid,
        name=artist.name,
        artist_id=str(artist.id),
    )
    return artist
