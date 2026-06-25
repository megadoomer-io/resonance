"""Upsert helpers for syncing external service data into the database."""

from __future__ import annotations

import bisect
import datetime
import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg_dialect
import structlog

import resonance.connectors.base as base_module
import resonance.models as models_module
import resonance.types as types_module

if TYPE_CHECKING:
    import collections.abc

    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


async def bulk_fetch_artists(
    session: AsyncSession,
    service_key: str,
    external_ids: set[str],
) -> dict[str, models_module.Artist]:
    """Fetch all artists matching service_links IDs in one query.

    Args:
        session: The async database session.
        service_key: The service type value (e.g., "listenbrainz").
        external_ids: Set of external IDs to look up.

    Returns:
        A dict mapping external_id -> Artist for items found.
    """
    if not external_ids:
        return {}
    valid_ids = {eid for eid in external_ids if eid}
    if not valid_ids:
        return {}
    stmt = sa.select(models_module.Artist).where(
        models_module.Artist.service_links[service_key].as_string().in_(valid_ids)
    )
    result = await session.execute(stmt)
    artists = result.scalars().all()
    return {
        a.service_links.get(service_key, ""): a
        for a in artists
        if a.service_links and service_key in a.service_links
    }


async def bulk_fetch_tracks(
    session: AsyncSession,
    service_key: str,
    external_ids: set[str],
) -> dict[str, models_module.Track]:
    """Fetch all tracks matching service_links IDs in one query.

    Args:
        session: The async database session.
        service_key: The service type value (e.g., "listenbrainz").
        external_ids: Set of external IDs to look up.

    Returns:
        A dict mapping external_id -> Track for items found.
    """
    if not external_ids:
        return {}
    valid_ids = {eid for eid in external_ids if eid}
    if not valid_ids:
        return {}
    stmt = sa.select(models_module.Track).where(
        models_module.Track.service_links[service_key].as_string().in_(valid_ids)
    )
    result = await session.execute(stmt)
    tracks = result.scalars().all()
    return {
        t.service_links.get(service_key, ""): t
        for t in tracks
        if t.service_links and service_key in t.service_links
    }


async def _upsert_artist(
    session: AsyncSession,
    artist_data: base_module.ArtistData,
    *,
    artist_cache: dict[str, models_module.Artist] | None = None,
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

    # 0. Check bulk-prefetch cache (fast path, avoids per-item DB query)
    if (
        artist_cache is not None
        and artist_data.external_id
        and artist_data.external_id in artist_cache
    ):
        cached = artist_cache[artist_data.external_id]
        cached.name = artist_data.name
        return False

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
    #    (flat musicbrainz or nested musicbrainz.id — flat listenbrainz
    #    is already checked in step 1)
    if (
        artist_data.service == types_module.ServiceType.LISTENBRAINZ
        and artist_data.external_id
    ):
        cross_checks = [
            models_module.Artist.service_links["musicbrainz"].as_string(),
            models_module.Artist.service_links["musicbrainz"]["id"].as_string(),
        ]
        for check_expr in cross_checks:
            stmt = sa.select(models_module.Artist).where(
                check_expr == artist_data.external_id
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
    stmt = (
        sa.select(models_module.Artist)
        .where(models_module.Artist.name == artist_data.name)
        .limit(1)
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
    session: AsyncSession,
    track_data: base_module.TrackData,
    *,
    artist_cache: dict[str, models_module.Artist] | None = None,
) -> None:
    """Ensure the artist from a track exists in the database.

    Args:
        session: The async database session.
        track_data: Track data containing artist information.
        artist_cache: Optional pre-fetched artist cache to avoid per-item queries.
    """
    artist_data = base_module.ArtistData(
        external_id=track_data.artist_external_id,
        name=track_data.artist_name,
        service=track_data.service,
    )
    await _upsert_artist(session, artist_data, artist_cache=artist_cache)


async def _upsert_track(
    session: AsyncSession,
    track_data: base_module.TrackData,
    *,
    track_cache: dict[str, models_module.Track] | None = None,
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

    # 0. Check bulk-prefetch cache (fast path, avoids per-item DB query)
    if (
        track_cache is not None
        and track_data.external_id
        and track_data.external_id in track_cache
    ):
        # Update duration if we have it and the cached track doesn't
        cached = track_cache[track_data.external_id]
        if track_data.duration_ms and not cached.duration_ms:
            cached.duration_ms = track_data.duration_ms
        return False

    # 1. Check service-specific ID in service_links
    if track_data.external_id:
        stmt = sa.select(models_module.Track).where(
            models_module.Track.service_links[service_key].as_string()
            == track_data.external_id
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing is not None:
            if track_data.duration_ms and not existing.duration_ms:
                existing.duration_ms = track_data.duration_ms
            return False

    # 2. MBID cross-service check for ListenBrainz
    #    (flat musicbrainz or nested musicbrainz.id — flat listenbrainz
    #    is already checked in step 1)
    if (
        track_data.service == types_module.ServiceType.LISTENBRAINZ
        and track_data.external_id
    ):
        cross_checks = [
            models_module.Track.service_links["musicbrainz"].as_string(),
            models_module.Track.service_links["musicbrainz"]["id"].as_string(),
        ]
        for check_expr in cross_checks:
            stmt = sa.select(models_module.Track).where(
                check_expr == track_data.external_id
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()
            if existing is not None:
                links = dict(existing.service_links or {})
                links[service_key] = track_data.external_id
                existing.service_links = links
                if track_data.duration_ms and not existing.duration_ms:
                    existing.duration_ms = track_data.duration_ms
                return False

    # 3. Fall back to title + artist name match
    stmt = (
        sa.select(models_module.Track)
        .where(models_module.Track.title == track_data.title)
        .limit(1)
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing is not None:
        # Always record the source service, even without an ID
        links = dict(existing.service_links or {})
        links[service_key] = track_data.external_id
        existing.service_links = links
        if track_data.duration_ms and not existing.duration_ms:
            existing.duration_ms = track_data.duration_ms
        return False

    # 4. Look up artist for the new track
    artist: models_module.Artist | None = None

    # Try by service_links first
    if track_data.artist_external_id:
        artist_stmt = (
            sa.select(models_module.Artist)
            .where(
                models_module.Artist.service_links[service_key].as_string()
                == track_data.artist_external_id
            )
            .limit(1)
        )
        artist_result = await session.execute(artist_stmt)
        artist = artist_result.scalar_one_or_none()

    # Fall back to name match
    if artist is None and track_data.artist_name:
        artist_stmt = (
            sa.select(models_module.Artist)
            .where(models_module.Artist.name == track_data.artist_name)
            .limit(1)
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
        duration_ms=track_data.duration_ms,
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
        track_stmt = (
            sa.select(models_module.Track)
            .where(
                models_module.Track.service_links[service_key].as_string()
                == track_data.external_id
            )
            .limit(1)
        )
        track_result = await session.execute(track_stmt)
        track = track_result.scalar_one_or_none()

    # 2. Fall back to title + artist name match
    if track is None:
        track_stmt = (
            sa.select(models_module.Track)
            .join(models_module.Artist)
            .where(
                models_module.Track.title == track_data.title,
                models_module.Artist.name == track_data.artist_name,
            )
            .limit(1)
        )
        track_result = await session.execute(track_stmt)
        track = track_result.scalar_one_or_none()

    if track is None:
        return

    listened_at = datetime.datetime.fromisoformat(played_at)

    # Fuzzy dedup: skip insert if a matching event exists within a window.
    window = datetime.timedelta(seconds=_dedup_window_seconds(track.duration_ms))
    check_stmt = (
        sa.select(models_module.ListeningEvent)
        .where(
            models_module.ListeningEvent.user_id == user_id,
            models_module.ListeningEvent.track_id == track.id,
            models_module.ListeningEvent.listened_at >= listened_at - window,
            models_module.ListeningEvent.listened_at <= listened_at + window,
        )
        .limit(1)
    )
    check_result = await session.execute(check_stmt)
    if check_result.scalar_one_or_none() is not None:
        return

    stmt = (
        pg_dialect.insert(models_module.ListeningEvent)
        .values(
            id=uuid.uuid4(),
            user_id=user_id,
            track_id=track.id,
            source_service=track_data.service,
            listened_at=listened_at,
        )
        .on_conflict_do_nothing(
            constraint="uq_listening_events_user_track_time",
        )
    )
    await session.execute(stmt)


def _dedup_window_seconds(duration_ms: int | None) -> int:
    """Half-window (seconds) for fuzzy listening-event dedup.

    Services record different moments for the same play (track start vs
    scrobble point), so two events for one track within ~one track length are
    treated as the same listen. Falls back to 10 minutes when duration is
    unknown. Shared by the per-listen and batched upsert paths.
    """
    default_dedup_seconds = 600  # 10 minutes
    if duration_ms:
        return duration_ms // 1000 + 60  # duration + 60s buffer
    return default_dedup_seconds


def _select_new_events(
    resolved: collections.abc.Sequence[tuple[uuid.UUID, datetime.datetime, int | None]],
    existing: dict[uuid.UUID, list[datetime.datetime]],
) -> list[tuple[uuid.UUID, datetime.datetime]]:
    """Pick which resolved events to insert, dropping fuzzy-window dups (#6).

    Pure dedup core (no I/O), so the tricky part is unit-testable. Given
    ``resolved`` ``(track_id, listened_at, duration_ms)`` events and ``existing``
    (per-track sorted anchor timestamps already in the DB), returns the
    survivors. Events are processed in time order so an accepted event becomes
    an anchor for later ones in the same batch -- without that, two same-page
    near-duplicates would both survive (the per-listen path avoided this by
    querying the live DB between single-row inserts).

    Mutates ``existing`` in place, inserting each accepted timestamp as a new
    anchor.
    """
    survivors: list[tuple[uuid.UUID, datetime.datetime]] = []
    for track_id, listened_at, duration_ms in sorted(resolved, key=lambda row: row[1]):
        window = datetime.timedelta(seconds=_dedup_window_seconds(duration_ms))
        series = existing.setdefault(track_id, [])
        idx = bisect.bisect_left(series, listened_at)
        is_dup = (idx < len(series) and series[idx] - listened_at <= window) or (
            idx > 0 and listened_at - series[idx - 1] <= window
        )
        if is_dup:
            continue
        series.insert(idx, listened_at)
        survivors.append((track_id, listened_at))
    return survivors


async def _resolve_track_by_title_artist(
    session: AsyncSession,
    track_data: base_module.TrackData,
) -> models_module.Track | None:
    """Resolve a track by exact title + artist name (the event path's fallback)."""
    stmt = (
        sa.select(models_module.Track)
        .join(models_module.Artist)
        .where(
            models_module.Track.title == track_data.title,
            models_module.Artist.name == track_data.artist_name,
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def upsert_listening_events_batch(
    session: AsyncSession,
    user_id: uuid.UUID,
    events: collections.abc.Sequence[tuple[base_module.TrackData, int]],
    *,
    service_type: types_module.ServiceType,
) -> int:
    """Insert one page of listening events, deduped, in a few queries (#6).

    Replaces per-listen :func:`_upsert_listening_event` on the hot sync path
    (the LB backfill ran ~3-4 queries per listen, x1000/page). For a page it:

    1. resolves tracks from a single bulk service_links fetch (run *after* the
       caller's track-upsert pass, so newly-created tracks are included),
       falling back to a title+artist lookup only for the rare miss;
    2. loads existing events spanning the page in one range query, widened by
       the largest dedup window so a near-duplicate straddling a page boundary
       is still caught;
    3. applies the same fuzzy cross-service dedup window as the per-listen path,
       crucially also against events accepted earlier *in this same page* (each
       accepted event becomes an anchor, matching the serial path's behavior --
       otherwise two same-page near-duplicates would both survive); and
    4. bulk-inserts the survivors with ON CONFLICT DO NOTHING on the exact
       (user, track, time) unique constraint.

    Args:
        session: Active database session.
        user_id: The listening user's ID.
        events: ``(track_data, listened_at_epoch)`` pairs for the page.
        service_type: The source service for these listens.

    Returns:
        The number of events actually inserted (excludes dedup-skipped and
        exact-conflict rows).
    """
    if not events:
        return 0
    service_key = service_type.value

    # 1. Resolve tracks. Re-fetch here (after the caller's track-upsert pass)
    #    because that pass does not add newly-created tracks to its cache.
    external_ids = {td.external_id for td, _ in events if td.external_id}
    track_cache = await bulk_fetch_tracks(session, service_key, external_ids)

    # (track_id, listened_at, duration_ms)
    resolved: list[tuple[uuid.UUID, datetime.datetime, int | None]] = []
    for track_data, listened_at_epoch in events:
        listened_at = datetime.datetime.fromtimestamp(
            listened_at_epoch, tz=datetime.UTC
        )
        track = (
            track_cache.get(track_data.external_id) if track_data.external_id else None
        )
        if track is None:
            track = await _resolve_track_by_title_artist(session, track_data)
        if track is not None:
            resolved.append((track.id, listened_at, track.duration_ms))

    if not resolved:
        return 0

    # 2. Load existing events for the page's tracks across a window-widened
    #    time span (one query). The widening preserves cross-page-boundary
    #    dedup that the per-listen path got for free by querying the live DB.
    track_ids = {tid for tid, _, _ in resolved}
    max_window = max(_dedup_window_seconds(dur) for _, _, dur in resolved)
    widen = datetime.timedelta(seconds=max_window)
    min_at = min(at for _, at, _ in resolved) - widen
    max_at = max(at for _, at, _ in resolved) + widen
    existing_result = await session.execute(
        sa.select(
            models_module.ListeningEvent.track_id,
            models_module.ListeningEvent.listened_at,
        ).where(
            models_module.ListeningEvent.user_id == user_id,
            models_module.ListeningEvent.track_id.in_(track_ids),
            models_module.ListeningEvent.listened_at >= min_at,
            models_module.ListeningEvent.listened_at <= max_at,
        )
    )
    # Per-track sorted anchor timestamps from rows already in the DB.
    anchors: dict[uuid.UUID, list[datetime.datetime]] = {}
    for tid, at in existing_result.all():
        anchors.setdefault(tid, []).append(at)
    for series in anchors.values():
        series.sort()

    # 3. Fuzzy-dedup (pure, unit-tested core) then build the insert rows.
    survivors = _select_new_events(resolved, anchors)
    to_insert: list[dict[str, object]] = [
        {
            "id": uuid.uuid4(),
            "user_id": user_id,
            "track_id": track_id,
            "source_service": service_type,
            "listened_at": listened_at,
        }
        for track_id, listened_at in survivors
    ]

    if not to_insert:
        return 0

    # 4. One bulk insert; the unique constraint absorbs any exact collisions.
    stmt = (
        pg_dialect.insert(models_module.ListeningEvent)
        .values(to_insert)
        .on_conflict_do_nothing(constraint="uq_listening_events_user_track_time")
        .returning(models_module.ListeningEvent.id)
    )
    result = await session.execute(stmt)
    return len(result.all())
