"""Entity deduplication — find and merge duplicate artists, tracks, venues, events.

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

import collections
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.models as models_module
import resonance.normalize as normalize_module
import resonance.services.artist_utils as artist_utils

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()

_ATTENDANCE_PRIORITY = {"GOING": 3, "INTERESTED": 2, "NOT_GOING": 1}


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
    event_artists_repointed: int = 0
    event_artists_deleted: int = 0
    candidates_repointed: int = 0
    venues_merged: int = 0
    events_venue_repointed: int = 0
    concerts_merged: int = 0
    concert_candidates_repointed: int = 0
    concert_candidates_deleted: int = 0
    concert_artists_repointed: int = 0
    concert_artists_deleted: int = 0
    attendance_repointed: int = 0
    attendance_deleted: int = 0


def pick_canonical(
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

    # 1. MBID holder wins (canonical or legacy location)
    a_has_mbid = artist_utils.has_mbid(a_links)
    b_has_mbid = artist_utils.has_mbid(b_links)
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


def pick_canonical_track(
    a: models_module.Track,
    b: models_module.Track,
) -> tuple[models_module.Track, models_module.Track]:
    """Pick the canonical track from two duplicates.

    Same priority as artists: MBID > more links > oldest.
    Also prefers the one with duration_ms set.
    """
    a_links = a.service_links or {}
    b_links = b.service_links or {}

    a_has_mbid = artist_utils.has_mbid(a_links)
    b_has_mbid = artist_utils.has_mbid(b_links)
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

    # Re-point confirmed event artists (handle unique constraint conflicts)
    event_artists = (
        (
            await session.execute(
                sa.select(models_module.EventArtist).where(
                    models_module.EventArtist.artist_id == duplicate.id
                )
            )
        )
        .scalars()
        .all()
    )

    for ea in event_artists:
        ea_conflict = (
            await session.execute(
                sa.select(models_module.EventArtist).where(
                    models_module.EventArtist.event_id == ea.event_id,
                    models_module.EventArtist.artist_id == canonical.id,
                )
            )
        ).scalar_one_or_none()

        if ea_conflict:
            await session.delete(ea)
            stats.event_artists_deleted += 1
        else:
            ea.artist_id = canonical.id
            stats.event_artists_repointed += 1

    # Re-point candidate matches
    candidate_result = await session.execute(
        sa.update(models_module.EventArtistCandidate)
        .where(models_module.EventArtistCandidate.matched_artist_id == duplicate.id)
        .values(matched_artist_id=canonical.id)
    )
    stats.candidates_repointed = (
        candidate_result.rowcount if hasattr(candidate_result, "rowcount") else 0
    )

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
        event_artists_repointed=stats.event_artists_repointed,
        event_artists_deleted=stats.event_artists_deleted,
        candidates_repointed=stats.candidates_repointed,
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
            canonical, dup = pick_canonical(canonical, other)
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
            canonical, dup = pick_canonical_track(canonical, other)
            stats = await merge_tracks(session, canonical, dup)
            total.tracks_merged += stats.tracks_merged
            total.events_repointed += stats.events_repointed
            total.track_relations_repointed += stats.track_relations_repointed
            total.track_relations_deleted += stats.track_relations_deleted

        await session.flush()

    await session.commit()
    logger.info("track_dedup_completed", stats=total)
    return total


async def delete_cross_service_duplicate_events(
    session: AsyncSession,
) -> int:
    """Delete duplicate cross-service listening events.

    For each pair of events on the same track by the same user from
    different services within the dedup window, deletes the later one.
    The window is track duration + 60s, or 10 minutes when duration
    is unknown.

    Returns:
        Number of events deleted.
    """
    result = await session.execute(
        sa.text(
            "DELETE FROM listening_events "
            "WHERE id IN ("
            "  SELECT e2.id "
            "  FROM listening_events e1 "
            "  JOIN listening_events e2 "
            "    ON e1.track_id = e2.track_id "
            "    AND e1.user_id = e2.user_id "
            "    AND e1.source_service != e2.source_service "
            "    AND e1.listened_at < e2.listened_at "
            "  JOIN tracks t ON e1.track_id = t.id "
            "  WHERE e2.listened_at - e1.listened_at < "
            "    CASE "
            "      WHEN t.duration_ms IS NOT NULL "
            "      THEN make_interval(secs => t.duration_ms / 1000 + 60) "
            "      ELSE interval '10 minutes' "
            "    END"
            ")"
        )
    )
    deleted = result.rowcount if hasattr(result, "rowcount") else 0
    await session.commit()
    logger.info("event_dedup_completed", events_deleted=deleted)
    return deleted


async def dedup_all(session: AsyncSession) -> dict[str, int]:
    """Run all dedup operations in sequence.

    Order matters:
    1. Venues first — so event grouping by venue_id works after merge.
    2. Concert events — cross-source event dedup relies on deduped venues.
    3. Artists — artist merges affect track grouping (title + artist_id).
    4. Tracks — track merges affect listening event dedup.
    5. Listening events — cross-service listening event dedup.

    Args:
        session: Active database session.

    Returns:
        Combined stats from all operations.
    """
    venue_stats = await find_and_merge_duplicate_venues(session)
    concert_stats = await find_and_merge_duplicate_concerts(session)
    artist_stats = await find_and_merge_duplicate_artists(session)
    track_stats = await find_and_merge_duplicate_tracks(session)
    events_deleted = await delete_cross_service_duplicate_events(session)

    result: dict[str, int] = {
        "venues_merged": venue_stats.venues_merged,
        "events_venue_repointed": venue_stats.events_venue_repointed,
        "concerts_merged": concert_stats.concerts_merged,
        "concert_candidates_repointed": concert_stats.concert_candidates_repointed,
        "concert_candidates_deleted": concert_stats.concert_candidates_deleted,
        "concert_artists_repointed": concert_stats.concert_artists_repointed,
        "concert_artists_deleted": concert_stats.concert_artists_deleted,
        "attendance_repointed": concert_stats.attendance_repointed,
        "attendance_deleted": concert_stats.attendance_deleted,
        "artists_merged": artist_stats.artists_merged,
        "tracks_repointed": artist_stats.tracks_repointed,
        "artist_relations_repointed": artist_stats.artist_relations_repointed,
        "artist_relations_deleted": artist_stats.artist_relations_deleted,
        "tracks_merged": track_stats.tracks_merged,
        "events_repointed": track_stats.events_repointed,
        "track_relations_repointed": track_stats.track_relations_repointed,
        "track_relations_deleted": track_stats.track_relations_deleted,
        "events_deleted": events_deleted,
    }

    logger.info("dedup_all_complete", **result)
    return result


# ---------------------------------------------------------------------------
# Venue dedup
# ---------------------------------------------------------------------------


def pick_canonical_venue(
    a: models_module.Venue,
    b: models_module.Venue,
) -> tuple[models_module.Venue, models_module.Venue]:
    """Pick the canonical venue from two duplicates.

    Priority: more non-null location fields > more service_links > oldest.
    """

    def _location_field_count(v: models_module.Venue) -> int:
        count = 0
        for attr in ("address", "postal_code"):
            if getattr(v, attr, None):
                count += 1
        return count

    a_fields = _location_field_count(a)
    b_fields = _location_field_count(b)
    if a_fields > b_fields:
        return a, b
    if b_fields > a_fields:
        return b, a

    a_links = a.service_links or {}
    b_links = b.service_links or {}
    if len(a_links) > len(b_links):
        return a, b
    if len(b_links) > len(a_links):
        return b, a

    if a.created_at <= b.created_at:
        return a, b
    return b, a


async def merge_venues(
    session: AsyncSession,
    canonical: models_module.Venue,
    duplicate: models_module.Venue,
) -> MergeStats:
    """Merge a duplicate venue into a canonical one.

    Caller must commit.
    """
    stats = MergeStats()
    log = logger.bind(
        canonical_id=str(canonical.id),
        duplicate_id=str(duplicate.id),
        venue_name=canonical.name,
    )

    # Merge service_links
    canonical_links = dict(canonical.service_links or {})
    for k, v in (duplicate.service_links or {}).items():
        if v and k not in canonical_links:
            canonical_links[k] = v
    canonical.service_links = canonical_links

    # Fill null fields from duplicate
    for attr in ("address", "postal_code"):
        if not getattr(canonical, attr) and getattr(duplicate, attr):
            setattr(canonical, attr, getattr(duplicate, attr))

    # Re-point events
    result = await session.execute(
        sa.update(models_module.Event)
        .where(models_module.Event.venue_id == duplicate.id)
        .values(venue_id=canonical.id)
    )
    stats.events_venue_repointed = result.rowcount if hasattr(result, "rowcount") else 0

    # Delete the duplicate
    await session.execute(
        sa.delete(models_module.Venue).where(models_module.Venue.id == duplicate.id)
    )
    stats.venues_merged = 1

    log.info(
        "venue_merged",
        events_repointed=stats.events_venue_repointed,
    )
    return stats


async def find_and_merge_duplicate_venues(
    session: AsyncSession,
) -> MergeStats:
    """Find all duplicate venues (by normalized name+location) and merge them."""
    total = MergeStats()

    result = await session.execute(sa.select(models_module.Venue))
    all_venues = list(result.scalars().all())

    # Group by normalized key
    groups: dict[tuple[str, ...], list[models_module.Venue]] = collections.defaultdict(
        list
    )
    for venue in all_venues:
        key = (
            normalize_module.normalize_name(venue.name),
            normalize_module.normalize_name(venue.city or ""),
            normalize_module.normalize_name(venue.state or ""),
            normalize_module.normalize_name(venue.country or ""),
        )
        groups[key].append(venue)

    dup_groups = {k: v for k, v in groups.items() if len(v) > 1}
    logger.info("venue_dedup_candidates", groups=len(dup_groups))

    for venues in dup_groups.values():
        venues.sort(key=lambda v: v.created_at)
        canonical = venues[0]
        for other in venues[1:]:
            canonical, dup = pick_canonical_venue(canonical, other)
            stats = await merge_venues(session, canonical, dup)
            total.venues_merged += stats.venues_merged
            total.events_venue_repointed += stats.events_venue_repointed

        await session.flush()

    await session.commit()
    logger.info("venue_dedup_completed", stats=total)
    return total


# ---------------------------------------------------------------------------
# Cross-source concert event dedup
# ---------------------------------------------------------------------------


def pick_canonical_event(
    a: models_module.Event,
    b: models_module.Event,
) -> tuple[models_module.Event, models_module.Event]:
    """Pick the canonical event from two cross-source duplicates.

    Priority: more confirmed EventArtists > more candidates >
    more service_links > has external_url > oldest.
    """
    a_artists = len(getattr(a, "artists", []) or [])
    b_artists = len(getattr(b, "artists", []) or [])
    if a_artists > b_artists:
        return a, b
    if b_artists > a_artists:
        return b, a

    a_cands = len(getattr(a, "artist_candidates", []) or [])
    b_cands = len(getattr(b, "artist_candidates", []) or [])
    if a_cands > b_cands:
        return a, b
    if b_cands > a_cands:
        return b, a

    a_links = a.service_links or {}
    b_links = b.service_links or {}
    if len(a_links) > len(b_links):
        return a, b
    if len(b_links) > len(a_links):
        return b, a

    a_url = 1 if a.external_url else 0
    b_url = 1 if b.external_url else 0
    if a_url > b_url:
        return a, b
    if b_url > a_url:
        return b, a

    if a.created_at <= b.created_at:
        return a, b
    return b, a


async def merge_events(
    session: AsyncSession,
    canonical: models_module.Event,
    duplicate: models_module.Event,
) -> MergeStats:
    """Merge a duplicate concert event into a canonical one.

    Handles cascading unique constraints on EventArtist, EventArtistCandidate,
    and UserEventAttendance. Caller must commit.
    """
    stats = MergeStats()
    log = logger.bind(
        canonical_id=str(canonical.id),
        duplicate_id=str(duplicate.id),
        event_title=canonical.title,
    )

    # Merge service_links
    canonical_links = dict(canonical.service_links or {})
    for k, v in (duplicate.service_links or {}).items():
        if v and k not in canonical_links:
            canonical_links[k] = v
    canonical.service_links = canonical_links

    # Re-point EventArtist records
    dup_artists = (
        (
            await session.execute(
                sa.select(models_module.EventArtist).where(
                    models_module.EventArtist.event_id == duplicate.id
                )
            )
        )
        .scalars()
        .all()
    )

    for ea in dup_artists:
        ea_conflict = (
            await session.execute(
                sa.select(models_module.EventArtist).where(
                    models_module.EventArtist.event_id == canonical.id,
                    models_module.EventArtist.artist_id == ea.artist_id,
                )
            )
        ).scalar_one_or_none()

        if ea_conflict:
            await session.delete(ea)
            stats.concert_artists_deleted += 1
        else:
            ea.event_id = canonical.id
            stats.concert_artists_repointed += 1

    # Re-point EventArtistCandidate records
    dup_candidates = (
        (
            await session.execute(
                sa.select(models_module.EventArtistCandidate).where(
                    models_module.EventArtistCandidate.event_id == duplicate.id
                )
            )
        )
        .scalars()
        .all()
    )

    for cand in dup_candidates:
        cand_conflict = (
            await session.execute(
                sa.select(models_module.EventArtistCandidate).where(
                    models_module.EventArtistCandidate.event_id == canonical.id,
                    models_module.EventArtistCandidate.raw_name == cand.raw_name,
                )
            )
        ).scalar_one_or_none()

        if cand_conflict:
            if cand.matched_artist_id and not cand_conflict.matched_artist_id:
                cand_conflict.matched_artist_id = cand.matched_artist_id
            if cand.confidence_score > cand_conflict.confidence_score:
                cand_conflict.confidence_score = cand.confidence_score
            if _candidate_status_rank(cand.status) > _candidate_status_rank(
                cand_conflict.status
            ):
                cand_conflict.status = cand.status
            await session.delete(cand)
            stats.concert_candidates_deleted += 1
        else:
            cand.event_id = canonical.id
            stats.concert_candidates_repointed += 1

    # Re-point UserEventAttendance
    dup_attendance = (
        (
            await session.execute(
                sa.select(models_module.UserEventAttendance).where(
                    models_module.UserEventAttendance.event_id == duplicate.id
                )
            )
        )
        .scalars()
        .all()
    )

    for att in dup_attendance:
        att_conflict = (
            await session.execute(
                sa.select(models_module.UserEventAttendance).where(
                    models_module.UserEventAttendance.user_id == att.user_id,
                    models_module.UserEventAttendance.event_id == canonical.id,
                )
            )
        ).scalar_one_or_none()

        if att_conflict:
            att_prio = _ATTENDANCE_PRIORITY.get(str(att.status), 0)
            conflict_prio = _ATTENDANCE_PRIORITY.get(str(att_conflict.status), 0)
            if att_prio > conflict_prio:
                att_conflict.status = att.status
            await session.delete(att)
            stats.attendance_deleted += 1
        else:
            att.event_id = canonical.id
            stats.attendance_repointed += 1

    # Keep richer title
    auto_generated = canonical.title.startswith(
        "Concert on "
    ) and not duplicate.title.startswith("Concert on ")
    if auto_generated or len(duplicate.title) > len(canonical.title):
        canonical.title = duplicate.title

    # Delete the duplicate
    await session.execute(
        sa.delete(models_module.Event).where(models_module.Event.id == duplicate.id)
    )
    stats.concerts_merged = 1

    log.info(
        "concert_merged",
        artists_repointed=stats.concert_artists_repointed,
        artists_deleted=stats.concert_artists_deleted,
        candidates_repointed=stats.concert_candidates_repointed,
        candidates_deleted=stats.concert_candidates_deleted,
        attendance_repointed=stats.attendance_repointed,
        attendance_deleted=stats.attendance_deleted,
    )
    return stats


def _candidate_status_rank(status: object) -> int:
    """Rank candidate status for conflict resolution (higher = better)."""
    name = status.name if hasattr(status, "name") else str(status)
    return {"ACCEPTED": 3, "PENDING": 2, "REJECTED": 1}.get(name, 0)


async def find_and_merge_duplicate_concerts(
    session: AsyncSession,
) -> MergeStats:
    """Find cross-source duplicate concert events and merge them.

    Groups events by (event_date, venue_id) and merges groups where
    multiple source services are present. For groups with 3+ events,
    uses artist name overlap to confirm matches.
    """
    total = MergeStats()

    result = await session.execute(
        sa.text(
            "SELECT event_date, venue_id, COUNT(*) as cnt "
            "FROM events "
            "WHERE venue_id IS NOT NULL "
            "GROUP BY event_date, venue_id "
            "HAVING COUNT(DISTINCT source_service) > 1 "
            "ORDER BY cnt DESC"
        )
    )
    groups = result.all()
    logger.info("concert_dedup_candidates", groups=len(groups))

    for row in groups:
        event_date = row[0]
        venue_id = row[1]

        events_result = await session.execute(
            sa.select(models_module.Event)
            .options(
                sa.orm.selectinload(models_module.Event.artists),
                sa.orm.selectinload(models_module.Event.artist_candidates),
            )
            .where(
                models_module.Event.event_date == event_date,
                models_module.Event.venue_id == venue_id,
            )
            .order_by(models_module.Event.created_at)
        )
        events = list(events_result.scalars().all())

        if len(events) < 2:
            continue

        # For groups of exactly 2, merge directly.
        # For 3+, verify artist overlap before merging.
        if len(events) == 2:
            canonical, dup = pick_canonical_event(events[0], events[1])
            stats = await merge_events(session, canonical, dup)
            _accumulate_concert_stats(total, stats)
        else:
            merged_ids: set[object] = set()
            for i, ev_a in enumerate(events):
                if ev_a.id in merged_ids:
                    continue
                for ev_b in events[i + 1 :]:
                    if ev_b.id in merged_ids:
                        continue
                    if _artist_overlap_sufficient(ev_a, ev_b):
                        canonical, dup = pick_canonical_event(ev_a, ev_b)
                        stats = await merge_events(session, canonical, dup)
                        _accumulate_concert_stats(total, stats)
                        merged_ids.add(dup.id)

        await session.flush()

    await session.commit()
    logger.info("concert_dedup_completed", stats=total)
    return total


def _artist_overlap_sufficient(
    a: models_module.Event,
    b: models_module.Event,
) -> bool:
    """Check if two events have enough artist name overlap to be duplicates."""
    a_names = {
        normalize_module.normalize_name(c.raw_name) for c in (a.artist_candidates or [])
    }
    b_names = {
        normalize_module.normalize_name(c.raw_name) for c in (b.artist_candidates or [])
    }

    if not a_names or not b_names:
        return True

    overlap = len(a_names & b_names)
    total = len(a_names | b_names)
    return overlap / total >= 0.5 if total > 0 else True


def _accumulate_concert_stats(total: MergeStats, stats: MergeStats) -> None:
    total.concerts_merged += stats.concerts_merged
    total.concert_candidates_repointed += stats.concert_candidates_repointed
    total.concert_candidates_deleted += stats.concert_candidates_deleted
    total.concert_artists_repointed += stats.concert_artists_repointed
    total.concert_artists_deleted += stats.concert_artists_deleted
    total.attendance_repointed += stats.attendance_repointed
    total.attendance_deleted += stats.attendance_deleted
