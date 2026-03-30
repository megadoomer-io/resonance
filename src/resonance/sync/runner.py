"""Sync runner for pulling external service data into the database."""

from __future__ import annotations

import datetime
import logging
import traceback
import uuid
from typing import TYPE_CHECKING, Protocol

import sqlalchemy as sa

import resonance.connectors.base as base_module
import resonance.connectors.spotify as spotify_module
import resonance.models as models_module
import resonance.types as types_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class SyncableConnector(Protocol):
    """Protocol for connectors that support sync operations."""

    service_type: types_module.ServiceType

    async def get_followed_artists(
        self, access_token: str
    ) -> list[base_module.SpotifyArtistData]: ...

    async def get_saved_tracks(
        self, access_token: str
    ) -> list[base_module.SpotifyTrackData]: ...

    async def get_recently_played(
        self, access_token: str
    ) -> list[spotify_module.PlayedTrackItem]: ...


async def run_sync(
    job: models_module.SyncJob,
    connector: SyncableConnector,
    session: AsyncSession,
    access_token: str,
) -> None:
    """Execute a sync job, pulling data from the connector into the database.

    Args:
        job: The sync job to execute.
        connector: The service connector to fetch data from.
        session: The async database session.
        access_token: OAuth access token for the connector.
    """
    try:
        job.status = types_module.SyncStatus.RUNNING
        job.started_at = datetime.datetime.now(datetime.UTC)
        await session.commit()

        items_created = 0
        items_updated = 0

        artists = await connector.get_followed_artists(access_token)
        for artist_data in artists:
            created = await _upsert_artist(session, artist_data)
            if created:
                items_created += 1
            else:
                items_updated += 1
            await _upsert_user_artist_relation(
                session, job.user_id, artist_data, job.service_connection_id
            )

        saved_tracks = await connector.get_saved_tracks(access_token)
        for track_data in saved_tracks:
            await _upsert_artist_from_track(session, track_data)
            created = await _upsert_track(session, track_data)
            if created:
                items_created += 1
            else:
                items_updated += 1
            await _upsert_user_track_relation(
                session, job.user_id, track_data, job.service_connection_id
            )

        recently_played = await connector.get_recently_played(access_token)
        for played_item in recently_played:
            await _upsert_artist_from_track(session, played_item.track)
            await _upsert_track(session, played_item.track)
            await _upsert_listening_event(
                session,
                job.user_id,
                played_item.track,
                played_item.played_at,
            )

        job.items_created = items_created
        job.items_updated = items_updated
        job.status = types_module.SyncStatus.COMPLETED
        job.completed_at = datetime.datetime.now(datetime.UTC)
        await session.commit()

    except Exception:
        logger.exception("Sync job failed: %s", job.id)
        job.status = types_module.SyncStatus.FAILED
        job.error_message = traceback.format_exc()
        job.completed_at = datetime.datetime.now(datetime.UTC)
        await session.commit()


async def _upsert_artist(
    session: AsyncSession, artist_data: base_module.SpotifyArtistData
) -> bool:
    """Find artist by service_links JSON lookup, create if not found.

    Args:
        session: The async database session.
        artist_data: Artist data from the connector.

    Returns:
        True if created, False if existing artist was found/updated.
    """
    stmt = sa.select(models_module.Artist).where(
        models_module.Artist.service_links[artist_data.service.value].as_string()
        == artist_data.external_id
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing is not None:
        existing.name = artist_data.name
        return False

    artist = models_module.Artist(
        id=uuid.uuid4(),
        name=artist_data.name,
        service_links={artist_data.service.value: artist_data.external_id},
    )
    session.add(artist)
    return True


async def _upsert_artist_from_track(
    session: AsyncSession, track_data: base_module.SpotifyTrackData
) -> None:
    """Ensure the artist from a track exists in the database.

    Args:
        session: The async database session.
        track_data: Track data containing artist information.
    """
    artist_data = base_module.SpotifyArtistData(
        external_id=track_data.artist_external_id,
        name=track_data.artist_name,
        service=track_data.service,
    )
    await _upsert_artist(session, artist_data)


async def _upsert_track(
    session: AsyncSession, track_data: base_module.SpotifyTrackData
) -> bool:
    """Find track by service_links, create if not found.

    Args:
        session: The async database session.
        track_data: Track data from the connector.

    Returns:
        True if created, False if existing track was found.
    """
    stmt = sa.select(models_module.Track).where(
        models_module.Track.service_links[track_data.service.value].as_string()
        == track_data.external_id
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing is not None:
        return False

    artist_stmt = sa.select(models_module.Artist).where(
        models_module.Artist.service_links[track_data.service.value].as_string()
        == track_data.artist_external_id
    )
    artist_result = await session.execute(artist_stmt)
    artist = artist_result.scalar_one_or_none()

    artist_id = artist.id if artist is not None else uuid.uuid4()

    track = models_module.Track(
        id=uuid.uuid4(),
        title=track_data.title,
        artist_id=artist_id,
        service_links={track_data.service.value: track_data.external_id},
    )
    session.add(track)
    return True


async def _upsert_user_artist_relation(
    session: AsyncSession,
    user_id: uuid.UUID,
    artist_data: base_module.SpotifyArtistData,
    connection_id: uuid.UUID,
) -> None:
    """Create a FOLLOW relation if not already present.

    Args:
        session: The async database session.
        user_id: The user's ID.
        artist_data: Artist data from the connector.
        connection_id: The service connection ID.
    """
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
    track_data: base_module.SpotifyTrackData,
    connection_id: uuid.UUID,
) -> None:
    """Create a LIKE relation if not already present.

    Args:
        session: The async database session.
        user_id: The user's ID.
        track_data: Track data from the connector.
        connection_id: The service connection ID.
    """
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
    track_data: base_module.SpotifyTrackData,
    played_at: str,
) -> None:
    """Create a listening event if not a duplicate.

    Args:
        session: The async database session.
        user_id: The user's ID.
        track_data: Track data from the connector.
        played_at: ISO 8601 timestamp of when the track was played.
    """
    track_stmt = sa.select(models_module.Track).where(
        models_module.Track.service_links[track_data.service.value].as_string()
        == track_data.external_id
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
