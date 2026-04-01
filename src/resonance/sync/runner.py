"""Sync runner for pulling external service data into the database."""

from __future__ import annotations

import datetime
import logging
import traceback
import uuid
from typing import TYPE_CHECKING, Protocol

import sqlalchemy as sa

import resonance.connectors.base as base_module
import resonance.connectors.listenbrainz as listenbrainz_module
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
    ) -> list[base_module.ArtistData]: ...

    async def get_saved_tracks(
        self, access_token: str
    ) -> list[base_module.TrackData]: ...

    async def get_recently_played(
        self, access_token: str
    ) -> list[spotify_module.PlayedTrackItem]: ...


async def run_sync(
    job: models_module.SyncJob,
    connector: SyncableConnector | listenbrainz_module.ListenBrainzConnector,
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

        # Look up the connection for username (needed by ListenBrainz)
        conn_result = await session.execute(
            sa.select(models_module.ServiceConnection).where(
                models_module.ServiceConnection.id == job.service_connection_id
            )
        )
        connection = conn_result.scalar_one()

        if isinstance(connector, listenbrainz_module.ListenBrainzConnector):
            items_created, items_updated = await _sync_listenbrainz(
                job, connector, session, connection.external_user_id
            )
        else:
            items_created, items_updated = await _sync_spotify(
                job, connector, session, access_token
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


async def _sync_spotify(
    job: models_module.SyncJob,
    connector: SyncableConnector,
    session: AsyncSession,
    access_token: str,
) -> tuple[int, int]:
    """Sync data from Spotify.

    Args:
        job: The sync job being executed.
        connector: The Spotify connector.
        session: The async database session.
        access_token: OAuth access token.

    Returns:
        Tuple of (items_created, items_updated).
    """
    items_created = 0
    items_updated = 0

    artists = await connector.get_followed_artists(access_token)
    for artist_data in artists:
        with session.no_autoflush:
            created = await _upsert_artist(session, artist_data)
            await session.flush()
            if created:
                items_created += 1
            else:
                items_updated += 1
            await _upsert_user_artist_relation(
                session, job.user_id, artist_data, job.service_connection_id
            )

    saved_tracks = await connector.get_saved_tracks(access_token)
    for track_data in saved_tracks:
        with session.no_autoflush:
            await _upsert_artist_from_track(session, track_data)
            await session.flush()
            created = await _upsert_track(session, track_data)
            await session.flush()
            if created:
                items_created += 1
            else:
                items_updated += 1
            await _upsert_user_track_relation(
                session, job.user_id, track_data, job.service_connection_id
            )

    recently_played = await connector.get_recently_played(access_token)
    for played_item in recently_played:
        with session.no_autoflush:
            await _upsert_artist_from_track(session, played_item.track)
            await session.flush()
            await _upsert_track(session, played_item.track)
            await session.flush()
            await _upsert_listening_event(
                session,
                job.user_id,
                played_item.track,
                played_item.played_at,
            )

    return items_created, items_updated


async def _sync_listenbrainz(
    job: models_module.SyncJob,
    connector: listenbrainz_module.ListenBrainzConnector,
    session: AsyncSession,
    username: str,
) -> tuple[int, int]:
    """Sync listening history from ListenBrainz.

    Args:
        job: The sync job being executed.
        connector: The ListenBrainz connector.
        session: The async database session.
        username: The ListenBrainz username.

    Returns:
        Tuple of (items_created, items_updated).
    """
    items_created = 0
    items_updated = 0
    max_ts: int | None = None

    while True:
        listens = await connector.get_listens(username, max_ts=max_ts, count=100)
        if not listens:
            break

        for listen in listens:
            with session.no_autoflush:
                await _upsert_artist_from_track(session, listen.track)
                await session.flush()
                await _upsert_track(session, listen.track)
                await session.flush()
                played_at = datetime.datetime.fromtimestamp(
                    listen.listened_at, tz=datetime.UTC
                ).isoformat()
                await _upsert_listening_event(
                    session, job.user_id, listen.track, played_at
                )
            items_created += 1

        # Use the oldest listen's timestamp for next page
        max_ts = listens[-1].listened_at
        await session.commit()  # commit per page

    return items_created, items_updated


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
        # Merge service_links
        links = dict(existing.service_links or {})
        if artist_data.external_id:
            links[service_key] = artist_data.external_id
        existing.service_links = links
        return False

    # 4. Create new
    artist = models_module.Artist(
        id=uuid.uuid4(),
        name=artist_data.name,
        service_links=(
            {service_key: artist_data.external_id} if artist_data.external_id else {}
        ),
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
        links = dict(existing.service_links or {})
        if track_data.external_id:
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
        service_links=(
            {service_key: track_data.external_id} if track_data.external_id else {}
        ),
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

    # For tracks without an external_id, we can't reliably look up the track
    if not track_data.external_id:
        return

    track_stmt = sa.select(models_module.Track).where(
        models_module.Track.service_links[service_key].as_string()
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
