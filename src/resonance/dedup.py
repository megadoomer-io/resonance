"""Entity deduplication — find and merge duplicate artists and tracks.

The core merge functions are reusable by:
- Batch dedup jobs (admin endpoint)
- Inline upsert enhancement (future)
- User-managed merge (#42, future)

Merge priority for picking the canonical record:
1. Record with an MBID in service_links (authoritative ID)
2. Record with more service_links entries (richer data)
3. Oldest record by created_at (tiebreaker)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.models as models_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


@dataclass
class MergeStats:
    """Counts of records affected by a merge operation."""

    artists_merged: int = 0
    tracks_merged: int = 0
    tracks_repointed: int = 0
    events_repointed: int = 0
    artist_relations_repointed: int = 0
    artist_relations_deleted: int = 0
    track_relations_repointed: int = 0
    track_relations_deleted: int = 0


def _pick_canonical(
    a: models_module.Artist,
    b: models_module.Artist,
) -> tuple[models_module.Artist, models_module.Artist]:
    """Pick the canonical record from two duplicates.

    Priority: MBID holder > more service_links > oldest created_at.

    Returns:
        (canonical, duplicate) tuple.
    """
    a_links = a.service_links or {}
    b_links = b.service_links or {}

    # 1. MBID holder wins (any non-empty value in service_links)
    a_has_mbid = any(
        k in ("musicbrainz", "listenbrainz") and v for k, v in a_links.items()
    )
    b_has_mbid = any(
        k in ("musicbrainz", "listenbrainz") and v for k, v in b_links.items()
    )
    if a_has_mbid and not b_has_mbid:
        return a, b
    if b_has_mbid and not a_has_mbid:
        return b, a

    # 2. More service_links wins
    if len(a_links) > len(b_links):
        return a, b
    if len(b_links) > len(a_links):
        return b, a

    # 3. Oldest wins
    if a.created_at <= b.created_at:
        return a, b
    return b, a


def _pick_canonical_track(
    a: models_module.Track,
    b: models_module.Track,
) -> tuple[models_module.Track, models_module.Track]:
    """Pick the canonical track from two duplicates.

    Same priority as artists: MBID > more links > oldest.
    Also prefers the one with duration_ms set.
    """
    a_links = a.service_links or {}
    b_links = b.service_links or {}

    a_has_mbid = any(
        k in ("musicbrainz", "listenbrainz") and v for k, v in a_links.items()
    )
    b_has_mbid = any(
        k in ("musicbrainz", "listenbrainz") and v for k, v in b_links.items()
    )
    if a_has_mbid and not b_has_mbid:
        return a, b
    if b_has_mbid and not a_has_mbid:
        return b, a

    if len(a_links) > len(b_links):
        return a, b
    if len(b_links) > len(a_links):
        return b, a

    # Prefer the one with duration
    if a.duration_ms and not b.duration_ms:
        return a, b
    if b.duration_ms and not a.duration_ms:
        return b, a

    if a.created_at <= b.created_at:
        return a, b
    return b, a


async def merge_artists(
    session: AsyncSession,
    canonical: models_module.Artist,
    duplicate: models_module.Artist,
) -> MergeStats:
    """Merge a duplicate artist into a canonical one.

    - Merges service_links
    - Re-points tracks to canonical
    - Re-points user_artist_relations (deletes conflicts)
    - Deletes the duplicate

    Caller must commit.
    """
    stats = MergeStats()
    log = logger.bind(
        canonical_id=str(canonical.id),
        duplicate_id=str(duplicate.id),
        artist_name=canonical.name,
    )

    # Merge service_links
    canonical_links = dict(canonical.service_links or {})
    for k, v in (duplicate.service_links or {}).items():
        if v and k not in canonical_links:
            canonical_links[k] = v
    canonical.service_links = canonical_links

    # Re-point tracks
    result = await session.execute(
        sa.update(models_module.Track)
        .where(models_module.Track.artist_id == duplicate.id)
        .values(artist_id=canonical.id)
    )
    stats.tracks_repointed = result.rowcount if hasattr(result, "rowcount") else 0

    # Re-point user_artist_relations (handle unique constraint conflicts)
    rels = (
        (
            await session.execute(
                sa.select(models_module.UserArtistRelation).where(
                    models_module.UserArtistRelation.artist_id == duplicate.id
                )
            )
        )
        .scalars()
        .all()
    )

    for rel in rels:
        # Check if canonical already has this relation
        conflict = (
            await session.execute(
                sa.select(models_module.UserArtistRelation).where(
                    models_module.UserArtistRelation.user_id == rel.user_id,
                    models_module.UserArtistRelation.artist_id == canonical.id,
                    models_module.UserArtistRelation.relation_type == rel.relation_type,
                    models_module.UserArtistRelation.source_service
                    == rel.source_service,
                )
            )
        ).scalar_one_or_none()

        if conflict:
            await session.delete(rel)
            stats.artist_relations_deleted += 1
        else:
            rel.artist_id = canonical.id
            stats.artist_relations_repointed += 1

    # Delete the duplicate
    await session.execute(
        sa.delete(models_module.Artist).where(models_module.Artist.id == duplicate.id)
    )
    stats.artists_merged = 1

    log.info(
        "artist_merged",
        tracks_repointed=stats.tracks_repointed,
        relations_repointed=stats.artist_relations_repointed,
        relations_deleted=stats.artist_relations_deleted,
    )
    return stats


async def merge_tracks(
    session: AsyncSession,
    canonical: models_module.Track,
    duplicate: models_module.Track,
) -> MergeStats:
    """Merge a duplicate track into a canonical one.

    - Merges service_links and duration_ms
    - Re-points listening_events (deletes timestamp conflicts)
    - Re-points user_track_relations (deletes conflicts)
    - Deletes the duplicate

    Caller must commit.
    """
    stats = MergeStats()
    log = logger.bind(
        canonical_id=str(canonical.id),
        duplicate_id=str(duplicate.id),
        track_title=canonical.title,
    )

    # Merge service_links
    canonical_links = dict(canonical.service_links or {})
    for k, v in (duplicate.service_links or {}).items():
        if v and k not in canonical_links:
            canonical_links[k] = v
    canonical.service_links = canonical_links

    # Merge duration
    if not canonical.duration_ms and duplicate.duration_ms:
        canonical.duration_ms = duplicate.duration_ms

    # Re-point listening_events (handle unique constraint on user+track+time)
    events = (
        (
            await session.execute(
                sa.select(models_module.ListeningEvent).where(
                    models_module.ListeningEvent.track_id == duplicate.id
                )
            )
        )
        .scalars()
        .all()
    )

    for event in events:
        # Check for conflict (same user, canonical track, same timestamp)
        conflict = (
            await session.execute(
                sa.select(models_module.ListeningEvent)
                .where(
                    models_module.ListeningEvent.user_id == event.user_id,
                    models_module.ListeningEvent.track_id == canonical.id,
                    models_module.ListeningEvent.listened_at == event.listened_at,
                )
                .limit(1)
            )
        ).scalar_one_or_none()

        if conflict:
            await session.delete(event)
            stats.events_repointed -= 1  # net negative = deleted
        else:
            event.track_id = canonical.id
            stats.events_repointed += 1

    # Re-point user_track_relations
    rels = (
        (
            await session.execute(
                sa.select(models_module.UserTrackRelation).where(
                    models_module.UserTrackRelation.track_id == duplicate.id
                )
            )
        )
        .scalars()
        .all()
    )

    for rel in rels:
        rel_conflict = (
            await session.execute(
                sa.select(models_module.UserTrackRelation).where(
                    models_module.UserTrackRelation.user_id == rel.user_id,
                    models_module.UserTrackRelation.track_id == canonical.id,
                    models_module.UserTrackRelation.relation_type == rel.relation_type,
                    models_module.UserTrackRelation.source_service
                    == rel.source_service,
                )
            )
        ).scalar_one_or_none()

        if rel_conflict:
            await session.delete(rel)
            stats.track_relations_deleted += 1
        else:
            rel.track_id = canonical.id
            stats.track_relations_repointed += 1

    # Delete the duplicate
    await session.execute(
        sa.delete(models_module.Track).where(models_module.Track.id == duplicate.id)
    )
    stats.tracks_merged = 1

    log.info(
        "track_merged",
        events_repointed=stats.events_repointed,
        relations_repointed=stats.track_relations_repointed,
        relations_deleted=stats.track_relations_deleted,
    )
    return stats


async def find_and_merge_duplicate_artists(
    session: AsyncSession,
) -> MergeStats:
    """Find all duplicate artists and merge them.

    Candidates: artists with the same name (case-insensitive).
    """
    total = MergeStats()

    # Find duplicate name groups
    result = await session.execute(
        sa.text(
            "SELECT LOWER(name) as lname, COUNT(*) as cnt "
            "FROM artists "
            "GROUP BY LOWER(name) "
            "HAVING COUNT(*) > 1 "
            "ORDER BY cnt DESC"
        )
    )
    groups = result.all()
    logger.info("artist_dedup_candidates", groups=len(groups))

    for row in groups:
        lower_name = row[0]
        # Fetch all artists with this name
        artists_result = await session.execute(
            sa.select(models_module.Artist)
            .where(sa.func.lower(models_module.Artist.name) == lower_name)
            .order_by(models_module.Artist.created_at)
        )
        artists = list(artists_result.scalars().all())

        if len(artists) < 2:
            continue

        # Pick canonical and merge all others into it
        canonical = artists[0]
        for other in artists[1:]:
            canonical, dup = _pick_canonical(canonical, other)
            stats = await merge_artists(session, canonical, dup)
            total.artists_merged += stats.artists_merged
            total.tracks_repointed += stats.tracks_repointed
            total.artist_relations_repointed += stats.artist_relations_repointed
            total.artist_relations_deleted += stats.artist_relations_deleted

        await session.flush()

    await session.commit()
    logger.info("artist_dedup_completed", stats=total)
    return total


async def find_and_merge_duplicate_tracks(
    session: AsyncSession,
) -> MergeStats:
    """Find all duplicate tracks and merge them.

    Candidates: tracks with the same title AND same artist_id.
    """
    total = MergeStats()

    result = await session.execute(
        sa.text(
            "SELECT LOWER(title), artist_id, COUNT(*) as cnt "
            "FROM tracks "
            "GROUP BY LOWER(title), artist_id "
            "HAVING COUNT(*) > 1 "
            "ORDER BY cnt DESC"
        )
    )
    groups = result.all()
    logger.info("track_dedup_candidates", groups=len(groups))

    for row in groups:
        lower_title = row[0]
        artist_id = row[1]

        tracks_result = await session.execute(
            sa.select(models_module.Track)
            .where(
                sa.func.lower(models_module.Track.title) == lower_title,
                models_module.Track.artist_id == artist_id,
            )
            .order_by(models_module.Track.created_at)
        )
        tracks = list(tracks_result.scalars().all())

        if len(tracks) < 2:
            continue

        canonical = tracks[0]
        for other in tracks[1:]:
            canonical, dup = _pick_canonical_track(canonical, other)
            stats = await merge_tracks(session, canonical, dup)
            total.tracks_merged += stats.tracks_merged
            total.events_repointed += stats.events_repointed
            total.track_relations_repointed += stats.track_relations_repointed
            total.track_relations_deleted += stats.track_relations_deleted

        await session.flush()

    await session.commit()
    logger.info("track_dedup_completed", stats=total)
    return total
