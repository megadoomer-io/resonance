"""Upsert helpers for syncing external service data into the database."""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.connectors.base as base_module
import resonance.models as models_module
import resonance.types as types_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


async def _upsert_artist(
    session: AsyncSession, artist_data: base_module.ArtistData
) -> bool:
    """Find artist by service_links JSON lookup, create if not found.

    Supports MBID-based cross-service matching for ListenBrainz artists:
    if the artist has an MBID, checks both listenbrainz and musicbrainz
    service_links keys before falling back to name matching.

    Args:
        session: The async database session.
        artist_data: Artist data from the connector.

    Returns:
        True if created, False if existing artist was found/updated.
    """
    service_key = artist_data.service.value

    # 1. Check service-specific ID in service_links (existing behavior)
    if artist_data.external_id:
        stmt = sa.select(models_module.Artist).where(
            models_module.Artist.service_links[service_key].as_string()
            == artist_data.external_id
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing is not None:
            existing.name = artist_data.name
            return False

    # 2. If this is a ListenBrainz artist with an MBID, check if any
    #    existing artist already has this MBID stored under another key
    if (
        artist_data.service == types_module.ServiceType.LISTENBRAINZ
        and artist_data.external_id
    ):
        for check_key in ["listenbrainz", "musicbrainz"]:
            if check_key == service_key:
                continue  # already checked above
            stmt = sa.select(models_module.Artist).where(
                models_module.Artist.service_links[check_key].as_string()
                == artist_data.external_id
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            if existing is not None:
                # Merge service_links
                links = dict(existing.service_links or {})
                links[service_key] = artist_data.external_id
                existing.service_links = links
                existing.name = artist_data.name
                return False

    # 3. Fall back to exact name match
    stmt = sa.select(models_module.Artist).where(
        models_module.Artist.name == artist_data.name
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing is not None:
        # Merge service_links — always record the service, even without an ID
        links = dict(existing.service_links or {})
        links[service_key] = artist_data.external_id
        existing.service_links = links
        return False

    # 4. Create new — always record the source service
    artist = models_module.Artist(
        id=uuid.uuid4(),
        name=artist_data.name,
        service_links={service_key: artist_data.external_id},
    )
    session.add(artist)
    return True


async def _upsert_artist_from_track(
    session: AsyncSession, track_data: base_module.TrackData
) -> None:
    """Ensure the artist from a track exists in the database.

    Args:
        session: The async database session.
        track_data: Track data containing artist information.
    """
    artist_data = base_module.ArtistData(
        external_id=track_data.artist_external_id,
        name=track_data.artist_name,
        service=track_data.service,
    )
    await _upsert_artist(session, artist_data)


async def _upsert_track(
    session: AsyncSession, track_data: base_module.TrackData
) -> bool:
    """Find track by service_links, create if not found.

    Supports MBID-based cross-service matching for ListenBrainz tracks,
    using the same pattern as _upsert_artist.

    Args:
        session: The async database session.
        track_data: Track data from the connector.

    Returns:
        True if created, False if existing track was found.
    """
    service_key = track_data.service.value

    # 1. Check service-specific ID in service_links
    if track_data.external_id:
        stmt = sa.select(models_module.Track).where(
            models_module.Track.service_links[service_key].as_string()
            == track_data.external_id
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing is not None:
            return False

    # 2. MBID cross-service check for ListenBrainz
    if (
        track_data.service == types_module.ServiceType.LISTENBRAINZ
        and track_data.external_id
    ):
        for check_key in ["listenbrainz", "musicbrainz"]:
            if check_key == service_key:
                continue
            stmt = sa.select(models_module.Track).where(
                models_module.Track.service_links[check_key].as_string()
                == track_data.external_id
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            if existing is not None:
                links = dict(existing.service_links or {})
                links[service_key] = track_data.external_id
                existing.service_links = links
                return False

    # 3. Fall back to title + artist name match
    stmt = sa.select(models_module.Track).where(
        models_module.Track.title == track_data.title,
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing is not None:
        # Always record the source service, even without an ID
        links = dict(existing.service_links or {})
        links[service_key] = track_data.external_id
        existing.service_links = links
        return False

    # 4. Look up artist for the new track
    artist: models_module.Artist | None = None

    # Try by service_links first
    if track_data.artist_external_id:
        artist_stmt = sa.select(models_module.Artist).where(
            models_module.Artist.service_links[service_key].as_string()
            == track_data.artist_external_id
        )
        artist_result = await session.execute(artist_stmt)
        artist = artist_result.scalar_one_or_none()

    # Fall back to name match
    if artist is None and track_data.artist_name:
        artist_stmt = sa.select(models_module.Artist).where(
            models_module.Artist.name == track_data.artist_name
        )
        artist_result = await session.execute(artist_stmt)
        artist = artist_result.scalar_one_or_none()

    if artist is None:
        logger.warning(
            "Could not find artist for track %r by %r — skipping",
            track_data.title,
            track_data.artist_name,
        )
        return False

    artist_id = artist.id

    track = models_module.Track(
        id=uuid.uuid4(),
        title=track_data.title,
        artist_id=artist_id,
        service_links={service_key: track_data.external_id},
    )
    session.add(track)
    return True


async def _upsert_user_artist_relation(
    session: AsyncSession,
    user_id: uuid.UUID,
    artist_data: base_module.ArtistData,
    connection_id: uuid.UUID,
) -> None:
    """Create a FOLLOW relation if not already present.

    Args:
        session: The async database session.
        user_id: The user's ID.
        artist_data: Artist data from the connector.
        connection_id: The service connection ID.
    """
    if not artist_data.external_id:
        return

    artist_stmt = sa.select(models_module.Artist).where(
        models_module.Artist.service_links[artist_data.service.value].as_string()
        == artist_data.external_id
    )
    artist_result = await session.execute(artist_stmt)
    artist = artist_result.scalar_one_or_none()

    if artist is None:
        return

    check_stmt = sa.select(models_module.UserArtistRelation).where(
        models_module.UserArtistRelation.user_id == user_id,
        models_module.UserArtistRelation.artist_id == artist.id,
        models_module.UserArtistRelation.relation_type
        == types_module.ArtistRelationType.FOLLOW,
        models_module.UserArtistRelation.source_service == artist_data.service,
    )
    check_result = await session.execute(check_stmt)
    if check_result.scalar_one_or_none() is not None:
        return

    relation = models_module.UserArtistRelation(
        id=uuid.uuid4(),
        user_id=user_id,
        artist_id=artist.id,
        relation_type=types_module.ArtistRelationType.FOLLOW,
        source_service=artist_data.service,
        source_connection_id=connection_id,
    )
    session.add(relation)


async def _upsert_user_track_relation(
    session: AsyncSession,
    user_id: uuid.UUID,
    track_data: base_module.TrackData,
    connection_id: uuid.UUID,
) -> None:
    """Create a LIKE relation if not already present.

    Args:
        session: The async database session.
        user_id: The user's ID.
        track_data: Track data from the connector.
        connection_id: The service connection ID.
    """
    if not track_data.external_id:
        return

    track_stmt = sa.select(models_module.Track).where(
        models_module.Track.service_links[track_data.service.value].as_string()
        == track_data.external_id
    )
    track_result = await session.execute(track_stmt)
    track = track_result.scalar_one_or_none()

    if track is None:
        return

    check_stmt = sa.select(models_module.UserTrackRelation).where(
        models_module.UserTrackRelation.user_id == user_id,
        models_module.UserTrackRelation.track_id == track.id,
        models_module.UserTrackRelation.relation_type
        == types_module.TrackRelationType.LIKE,
        models_module.UserTrackRelation.source_service == track_data.service,
    )
    check_result = await session.execute(check_stmt)
    if check_result.scalar_one_or_none() is not None:
        return

    relation = models_module.UserTrackRelation(
        id=uuid.uuid4(),
        user_id=user_id,
        track_id=track.id,
        relation_type=types_module.TrackRelationType.LIKE,
        source_service=track_data.service,
        source_connection_id=connection_id,
    )
    session.add(relation)


async def _upsert_listening_event(
    session: AsyncSession,
    user_id: uuid.UUID,
    track_data: base_module.TrackData,
    played_at: str,
) -> None:
    """Create a listening event if not a duplicate.

    Args:
        session: The async database session.
        user_id: The user's ID.
        track_data: Track data from the connector.
        played_at: ISO 8601 timestamp of when the track was played.
    """
    service_key = track_data.service.value
    track: models_module.Track | None = None

    # 1. Try service_links lookup if we have an external_id
    if track_data.external_id:
        track_stmt = sa.select(models_module.Track).where(
            models_module.Track.service_links[service_key].as_string()
            == track_data.external_id
        )
        track_result = await session.execute(track_stmt)
        track = track_result.scalar_one_or_none()

    # 2. Fall back to title match (handles tracks without external IDs)
    if track is None:
        track_stmt = sa.select(models_module.Track).where(
            models_module.Track.title == track_data.title,
        )
        track_result = await session.execute(track_stmt)
        track = track_result.scalar_one_or_none()

    if track is None:
        return

    listened_at = datetime.datetime.fromisoformat(played_at)

    check_stmt = sa.select(models_module.ListeningEvent).where(
        models_module.ListeningEvent.user_id == user_id,
        models_module.ListeningEvent.track_id == track.id,
        models_module.ListeningEvent.listened_at == listened_at,
    )
    check_result = await session.execute(check_stmt)
    if check_result.scalar_one_or_none() is not None:
        return

    event = models_module.ListeningEvent(
        id=uuid.uuid4(),
        user_id=user_id,
        track_id=track.id,
        source_service=track_data.service,
        listened_at=listened_at,
    )
    session.add(event)
