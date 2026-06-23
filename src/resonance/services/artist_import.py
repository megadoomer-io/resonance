"""Resolve and import artists by MusicBrainz ID.

Centralizes the "find-or-create a local Artist row from an MBID" flow so both
the HTTP import endpoint (``api/v1/artists.py``) and the background worker
(adjacent-artist discovery, issue #115 Phase 2) share one dedup + create path.

Dedup is MBID-first and checks both the canonical
``service_links["musicbrainz"]["id"]`` location and the legacy flat
``service_links["listenbrainz"]`` location.
"""

from __future__ import annotations

import typing
from typing import Any

import sqlalchemy as sa
import structlog

import resonance.models.music as music_models

if typing.TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger(__name__)


async def find_local_artist_by_mbid(
    session: sa_async.AsyncSession, mbid: str
) -> music_models.Artist | None:
    """Find a local artist by MBID, checking canonical and legacy locations.

    Matches both the canonical ``service_links["musicbrainz"]["id"]`` and the
    legacy flat ``service_links["listenbrainz"]`` storage so an artist imported
    under either convention is found.

    Args:
        session: Async DB session.
        mbid: The MusicBrainz artist ID.

    Returns:
        The matching Artist, or None.
    """
    stmt = sa.select(music_models.Artist).where(
        sa.or_(
            music_models.Artist.service_links["musicbrainz"]["id"].as_string() == mbid,
            music_models.Artist.service_links["listenbrainz"].as_string() == mbid,
        )
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
    ``service_links``, flushed so it gets an ID, and returned. The caller owns
    the surrounding transaction (this does not commit).

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

    data = await connector.get_artist_by_mbid(mbid)
    if data is None:
        logger.warning("artist_import_mbid_unresolved", mbid=mbid)
        return None

    artist = music_models.Artist(
        name=data["name"],
        disambiguation=data.get("disambiguation") or None,
        artist_type=data.get("artist_type") or None,
        area=data.get("area") or None,
        begin_year=data.get("begin_year"),
        end_year=data.get("end_year"),
        service_links={"musicbrainz": {"id": mbid}},
    )
    session.add(artist)
    await session.flush()
    logger.info(
        "artist_imported_by_mbid",
        mbid=mbid,
        name=artist.name,
        artist_id=str(artist.id),
    )
    return artist
