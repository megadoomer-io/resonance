"""Upsert helpers for syncing concert data into the database."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.concerts.ical as ical_module
import resonance.concerts.parser as parser_module
import resonance.models.concert as concert_models
import resonance.models.music as music_models
import resonance.normalize as normalize_module
import resonance.types as types_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


async def upsert_venue(
    session: AsyncSession,
    venue_data: ical_module.VenueData,
) -> concert_models.Venue:
    """Look up a venue by (name, city, state, country); create if missing.

    Args:
        session: The async database session.
        venue_data: Parsed venue data from the iCal feed.

    Returns:
        The existing or newly created Venue.
    """
    norm_state = normalize_module.normalize_state(venue_data.state or "")
    norm_country = normalize_module.normalize_country(venue_data.country or "")

    stmt = sa.select(concert_models.Venue).where(
        sa.func.lower(concert_models.Venue.name) == (venue_data.name or "").lower(),
        sa.func.lower(concert_models.Venue.city) == (venue_data.city or "").lower(),
    )
    result = await session.execute(stmt)
    for existing in result.scalars().all():
        if (
            normalize_module.normalize_state(existing.state or "") == norm_state
            and normalize_module.normalize_country(existing.country or "")
            == norm_country
        ):
            return existing

    venue = concert_models.Venue(
        id=uuid.uuid4(),
        name=venue_data.name,
        city=venue_data.city,
        state=venue_data.state,
        country=venue_data.country,
    )
    session.add(venue)
    logger.info("created_venue", name=venue_data.name, city=venue_data.city)
    return venue


async def upsert_event(
    session: AsyncSession,
    parsed: ical_module.ParsedEvent,
    source_service: types_module.ServiceType,
    venue: concert_models.Venue | None,
) -> tuple[concert_models.Event, bool]:
    """Look up an event by (source_service, external_id); create or update.

    Args:
        session: The async database session.
        parsed: Parsed event data from the iCal feed.
        source_service: The service this event originated from.
        venue: The resolved venue for this event, or None.

    Returns:
        A tuple of (event, created) where created is True if a new event was
        inserted, False if an existing event was updated.
    """
    venue_id = venue.id if venue is not None else None

    stmt = sa.select(concert_models.Event).where(
        concert_models.Event.source_service == source_service,
        concert_models.Event.external_id == parsed.external_id,
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing is not None:
        existing.title = parsed.title
        existing.event_date = parsed.event_date
        existing.external_url = parsed.external_url
        existing.venue_id = venue_id
        return existing, False

    event = concert_models.Event(
        id=uuid.uuid4(),
        title=parsed.title,
        event_date=parsed.event_date,
        venue_id=venue_id,
        source_service=source_service,
        external_id=parsed.external_id,
        external_url=parsed.external_url,
    )
    session.add(event)
    logger.info("created_event", title=parsed.title, external_id=parsed.external_id)
    return event, True


async def upsert_candidates(
    session: AsyncSession,
    event: concert_models.Event,
    candidates: list[parser_module.ArtistCandidate],
) -> int:
    """Create EventArtistCandidate rows, skipping duplicates.

    Args:
        session: The async database session.
        event: The event these candidates belong to.
        candidates: Parsed artist candidates from the event title.

    Returns:
        The number of newly created candidate rows.
    """
    created_count = 0
    for candidate in candidates:
        normalized = normalize_module.normalize_name(candidate.name)
        stmt = sa.select(concert_models.EventArtistCandidate).where(
            concert_models.EventArtistCandidate.event_id == event.id,
            concert_models.EventArtistCandidate.normalized_raw_name == normalized,
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()
        if existing is not None:
            continue

        row = concert_models.EventArtistCandidate(
            id=uuid.uuid4(),
            event_id=event.id,
            raw_name=candidate.name,
            position=candidate.position,
            confidence_score=candidate.confidence,
            status=types_module.CandidateStatus.PENDING,
        )
        session.add(row)
        created_count += 1

    return created_count


_ATTENDANCE_MAP: dict[str, types_module.AttendanceStatus] = {
    "going": types_module.AttendanceStatus.GOING,
    "interested": types_module.AttendanceStatus.INTERESTED,
}


async def upsert_attendance(
    session: AsyncSession,
    user_id: uuid.UUID,
    event: concert_models.Event,
    status_str: str,
    source_service: types_module.ServiceType,
) -> None:
    """Create or update a user's attendance for an event.

    Args:
        session: The async database session.
        user_id: The user's ID.
        event: The event to record attendance for.
        status_str: Raw status string ("going" or "interested").
        source_service: The service this attendance originated from.
    """
    status = _ATTENDANCE_MAP.get(status_str, types_module.AttendanceStatus.NOT_GOING)

    stmt = sa.select(concert_models.UserEventAttendance).where(
        concert_models.UserEventAttendance.user_id == user_id,
        concert_models.UserEventAttendance.event_id == event.id,
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing is not None:
        existing.status = status
        return

    attendance = concert_models.UserEventAttendance(
        id=uuid.uuid4(),
        user_id=user_id,
        event_id=event.id,
        status=status,
        source_service=source_service,
    )
    session.add(attendance)


_AUTO_ACCEPT_THRESHOLD = 80


async def match_candidates_to_artists(
    session: AsyncSession,
    event: concert_models.Event,
) -> int:
    """Match pending EventArtistCandidates to existing Artists by name.

    Performs a case-insensitive name lookup against the artists table. Matched
    candidates with confidence >= 80 are auto-accepted and an EventArtist
    record is created. Lower-confidence matches are left PENDING for manual
    review.

    Args:
        session: The async database session.
        event: The event whose candidates should be matched.

    Returns:
        The number of candidates that were matched to an artist.
    """
    candidates_stmt = sa.select(concert_models.EventArtistCandidate).where(
        concert_models.EventArtistCandidate.event_id == event.id,
        concert_models.EventArtistCandidate.status
        == types_module.CandidateStatus.PENDING,
        concert_models.EventArtistCandidate.matched_artist_id.is_(None),
    )
    candidates_result = await session.execute(candidates_stmt)
    candidates = candidates_result.scalars().all()

    matched_count = 0
    for candidate in candidates:
        artist_stmt = sa.select(music_models.Artist).where(
            sa.func.lower(music_models.Artist.name) == candidate.normalized_raw_name,
        )
        artist_result = await session.execute(artist_stmt)
        artist = artist_result.scalar_one_or_none()

        if artist is not None:
            candidate.matched_artist_id = artist.id
            matched_count += 1

            if candidate.confidence_score >= _AUTO_ACCEPT_THRESHOLD:
                existing_ea = await session.execute(
                    sa.select(concert_models.EventArtist.id).where(
                        concert_models.EventArtist.event_id == event.id,
                        concert_models.EventArtist.artist_id == artist.id,
                    )
                )
                if existing_ea.scalar_one_or_none() is None:
                    session.add(
                        concert_models.EventArtist(
                            event_id=event.id,
                            artist_id=artist.id,
                            position=candidate.position,
                            raw_name=candidate.raw_name,
                        )
                    )
                    candidate.status = types_module.CandidateStatus.ACCEPTED

    return matched_count


_VENUE_CONFIDENCE = 100
_EVENT_CONFIDENCE = 100


async def upsert_venue_candidate(
    session: AsyncSession,
    venue_data: ical_module.VenueData,
    source_service: types_module.ServiceType,
    external_id: str,
) -> concert_models.VenueCandidate:
    """Create or update a VenueCandidate from parsed venue data.

    Args:
        session: The async database session.
        venue_data: Parsed venue data from the source.
        source_service: The source service.
        external_id: External ID for this venue in the source.

    Returns:
        The existing or newly created VenueCandidate.
    """
    stmt = sa.select(concert_models.VenueCandidate).where(
        concert_models.VenueCandidate.source_service == source_service,
        concert_models.VenueCandidate.external_id == external_id,
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing is not None:
        existing.name = venue_data.name
        existing.city = venue_data.city
        existing.state = venue_data.state
        existing.country = venue_data.country
        return existing

    candidate = concert_models.VenueCandidate(
        id=uuid.uuid4(),
        source_service=source_service,
        external_id=external_id,
        name=venue_data.name,
        city=venue_data.city,
        state=venue_data.state,
        country=venue_data.country,
    )
    session.add(candidate)
    return candidate


async def resolve_venue_candidate(
    session: AsyncSession,
    candidate: concert_models.VenueCandidate,
) -> concert_models.Venue:
    """Auto-resolve a VenueCandidate to an existing or new Venue.

    Uses normalized name comparison. If a matching Venue exists (and is not
    excluded), links the candidate to it. Otherwise creates a new Venue.

    Args:
        session: The async database session.
        candidate: The VenueCandidate to resolve.

    Returns:
        The resolved Venue.
    """
    if candidate.resolved_venue_id is not None:
        result = await session.execute(
            sa.select(concert_models.Venue).where(
                concert_models.Venue.id == candidate.resolved_venue_id
            )
        )
        existing = result.scalar_one_or_none()
        if existing is not None:
            return existing

    norm_name = normalize_module.normalize_name(candidate.name)
    norm_city = normalize_module.normalize_name(candidate.city or "")
    norm_state = normalize_module.normalize_state(candidate.state or "")
    norm_country = normalize_module.normalize_country(candidate.country or "")

    venues_result = await session.execute(
        sa.select(concert_models.Venue).where(
            sa.func.lower(concert_models.Venue.city) == norm_city,
        )
    )
    potential_matches = venues_result.scalars().all()

    for venue in potential_matches:
        if (
            normalize_module.normalize_name(venue.name) == norm_name
            and normalize_module.normalize_state(venue.state or "") == norm_state
            and normalize_module.normalize_country(venue.country or "") == norm_country
        ):
            if await _is_excluded(session, "venue", venue.id, candidate):
                continue
            candidate.resolved_venue_id = venue.id
            candidate.confidence_score = _VENUE_CONFIDENCE
            candidate.status = types_module.CandidateStatus.AUTO_ACCEPTED
            return venue

    venue = concert_models.Venue(
        id=uuid.uuid4(),
        name=candidate.name,
        city=candidate.city,
        state=candidate.state,
        country=candidate.country,
    )
    session.add(venue)
    candidate.resolved_venue_id = venue.id
    candidate.confidence_score = _VENUE_CONFIDENCE
    candidate.status = types_module.CandidateStatus.AUTO_ACCEPTED
    logger.info("created_venue", name=candidate.name, city=candidate.city)
    return venue


async def upsert_event_candidate(
    session: AsyncSession,
    parsed: ical_module.ParsedEvent,
    source_service: types_module.ServiceType,
    venue_candidate: concert_models.VenueCandidate | None,
) -> concert_models.EventCandidate:
    """Create or update an EventCandidate from parsed event data.

    Args:
        session: The async database session.
        parsed: Parsed event data from the source.
        source_service: The source service.
        venue_candidate: The VenueCandidate for this event's venue, or None.

    Returns:
        The existing or newly created EventCandidate.
    """
    stmt = sa.select(concert_models.EventCandidate).where(
        concert_models.EventCandidate.source_service == source_service,
        concert_models.EventCandidate.external_id == parsed.external_id,
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()

    if existing is not None:
        existing.title = parsed.title
        existing.event_date = parsed.event_date
        existing.external_url = parsed.external_url
        existing.venue_candidate_id = venue_candidate.id if venue_candidate else None
        existing.attendance_status = parsed.attendance_status
        return existing

    candidate = concert_models.EventCandidate(
        id=uuid.uuid4(),
        source_service=source_service,
        external_id=parsed.external_id,
        external_url=parsed.external_url,
        title=parsed.title,
        event_date=parsed.event_date,
        venue_candidate_id=venue_candidate.id if venue_candidate else None,
        attendance_status=parsed.attendance_status,
    )
    session.add(candidate)
    return candidate


async def resolve_event_candidate(
    session: AsyncSession,
    candidate: concert_models.EventCandidate,
    venue: concert_models.Venue | None,
) -> tuple[concert_models.Event, bool]:
    """Auto-resolve an EventCandidate to an existing or new Event.

    Matches by (event_date, venue_id) across sources. Also updates
    the resolved Event on the events table for backward compatibility.

    Args:
        session: The async database session.
        candidate: The EventCandidate to resolve.
        venue: The resolved Venue, or None.

    Returns:
        A tuple of (event, created) where created is True for new events.
    """
    venue_id = venue.id if venue is not None else None

    if candidate.resolved_event_id is not None:
        result = await session.execute(
            sa.select(concert_models.Event).where(
                concert_models.Event.id == candidate.resolved_event_id
            )
        )
        existing = result.scalar_one_or_none()
        if existing is not None:
            existing.title = candidate.title
            existing.event_date = candidate.event_date
            existing.external_url = candidate.external_url
            existing.venue_id = venue_id
            return existing, False

    if venue_id is not None:
        match_stmt = sa.select(concert_models.Event).where(
            concert_models.Event.event_date == candidate.event_date,
            concert_models.Event.venue_id == venue_id,
        )
        match_result = await session.execute(match_stmt)
        potential_match = match_result.scalar_one_or_none()

        if potential_match is not None and not await _is_excluded(
            session, "event", potential_match.id, candidate
        ):
            candidate.resolved_event_id = potential_match.id
            candidate.confidence_score = _EVENT_CONFIDENCE
            candidate.status = types_module.CandidateStatus.AUTO_ACCEPTED
            return potential_match, False

    event = concert_models.Event(
        id=uuid.uuid4(),
        title=candidate.title,
        event_date=candidate.event_date,
        venue_id=venue_id,
        source_service=candidate.source_service,
        external_id=candidate.external_id,
        external_url=candidate.external_url,
    )
    session.add(event)
    candidate.resolved_event_id = event.id
    candidate.confidence_score = _EVENT_CONFIDENCE
    candidate.status = types_module.CandidateStatus.AUTO_ACCEPTED
    logger.info(
        "created_event",
        title=candidate.title,
        external_id=candidate.external_id,
    )
    return event, True


async def _is_excluded(
    session: AsyncSession,
    entity_type: str,
    entity_id: uuid.UUID,
    candidate: concert_models.VenueCandidate | concert_models.EventCandidate,
) -> bool:
    """Check if resolving a candidate to an entity would violate an exclusion."""
    resolved_id = getattr(candidate, "resolved_venue_id", None) or getattr(
        candidate, "resolved_event_id", None
    )
    if resolved_id is None:
        return False

    a_id = min(entity_id, resolved_id)
    b_id = max(entity_id, resolved_id)

    result = await session.execute(
        sa.select(concert_models.EntityExclusion.id).where(
            concert_models.EntityExclusion.entity_type == entity_type,
            concert_models.EntityExclusion.entity_a_id == a_id,
            concert_models.EntityExclusion.entity_b_id == b_id,
        )
    )
    return result.scalar_one_or_none() is not None


async def reconcile_unmatched_candidates(
    session: AsyncSession,
) -> int:
    """Re-match all unmatched EventArtistCandidates against the Artist catalog.

    Finds all PENDING candidates with no matched_artist_id and attempts
    to match them. Intended as a post-sync hook so that newly synced
    artists get linked to existing concert events.

    Args:
        session: The async database session.

    Returns:
        The number of candidates that were newly matched.
    """
    # Find distinct events that have unmatched candidates
    event_ids_stmt = (
        sa.select(concert_models.EventArtistCandidate.event_id)
        .where(
            concert_models.EventArtistCandidate.matched_artist_id.is_(None),
            concert_models.EventArtistCandidate.status
            == types_module.CandidateStatus.PENDING,
        )
        .distinct()
    )
    event_ids_result = await session.execute(event_ids_stmt)
    event_ids = [row[0] for row in event_ids_result.all()]

    if not event_ids:
        return 0

    events_stmt = sa.select(concert_models.Event).where(
        concert_models.Event.id.in_(event_ids)
    )
    events_result = await session.execute(events_stmt)
    events = events_result.scalars().all()

    total_matched = 0
    for event in events:
        matched = await match_candidates_to_artists(session, event)
        total_matched += matched

    if total_matched > 0:
        await session.commit()
        logger.info(
            "reconcile_unmatched_completed",
            events_checked=len(events),
            candidates_matched=total_matched,
        )

    return total_matched
