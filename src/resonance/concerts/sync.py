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
    stmt = sa.select(concert_models.Venue).where(
        concert_models.Venue.name == venue_data.name,
        concert_models.Venue.city == venue_data.city,
        concert_models.Venue.state == venue_data.state,
        concert_models.Venue.country == venue_data.country,
    )
    result = await session.execute(stmt)
    existing = result.scalar_one_or_none()
    if existing is not None:
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
        stmt = sa.select(concert_models.EventArtistCandidate).where(
            concert_models.EventArtistCandidate.event_id == event.id,
            concert_models.EventArtistCandidate.raw_name == candidate.name,
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
    status = _ATTENDANCE_MAP.get(status_str, types_module.AttendanceStatus.NONE)

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


async def match_candidates_to_artists(
    session: AsyncSession,
    event: concert_models.Event,
) -> int:
    """Match pending EventArtistCandidates to existing Artists by name.

    Performs a case-insensitive name lookup against the artists table. Matched
    candidates have their ``matched_artist_id`` set but status remains PENDING
    so the user can still accept or reject the match.

    Args:
        session: The async database session.
        event: The event whose candidates should be matched.

    Returns:
        The number of candidates that were matched to an artist.
    """
    # Load all PENDING candidates for this event
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
            sa.func.lower(music_models.Artist.name)
            == sa.func.lower(candidate.raw_name),
        )
        artist_result = await session.execute(artist_stmt)
        artist = artist_result.scalar_one_or_none()

        if artist is not None:
            candidate.matched_artist_id = artist.id
            matched_count += 1

    return matched_count
