"""iCal feed parser — parses VCALENDAR data into structured event data."""

from __future__ import annotations

import datetime

import icalendar
import pydantic

import resonance.concerts.parser as parser_module
import resonance.types as types_module


class VenueData(pydantic.BaseModel):
    """Parsed venue information from iCal LOCATION."""

    name: str
    city: str | None = None
    state: str | None = None
    country: str | None = None


class ParsedEvent(pydantic.BaseModel):
    """A parsed event from an iCal feed."""

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    title: str
    event_date: datetime.date
    venue: VenueData | None = None
    external_id: str
    external_url: str | None = None
    artist_candidates: list[parser_module.ArtistCandidate] = []
    attendance_status: str | None = None


def _parse_songkick_location(location: str) -> VenueData | None:
    """Parse Songkick LOCATION format: 'Venue, City, State, Country'.

    Args:
        location: The raw LOCATION string from a Songkick iCal event.

    Returns:
        Parsed venue data, or None if the location string is empty.
    """
    parts = [p.strip() for p in location.split(", ")]
    if not parts or not parts[0]:
        return None

    name = parts[0]
    city = parts[1] if len(parts) > 1 else None
    state = parts[2] if len(parts) > 2 else None
    country = parts[3] if len(parts) > 3 else None

    return VenueData(name=name, city=city, state=state, country=country)


def _is_songkick_feed(feed_type: types_module.FeedType) -> bool:
    """Check whether a feed type is a Songkick feed."""
    return feed_type in {
        types_module.FeedType.SONGKICK_ATTENDANCE,
        types_module.FeedType.SONGKICK_TRACKED_ARTIST,
    }


def _extract_date(dtstart: datetime.date | datetime.datetime) -> datetime.date:
    """Normalize DTSTART to a date, handling both date and datetime values."""
    if isinstance(dtstart, datetime.datetime):
        return dtstart.date()
    return dtstart


def parse_ical_feed(
    ical_text: str, feed_type: types_module.FeedType
) -> list[ParsedEvent]:
    """Parse iCal text into structured event data.

    For Songkick feeds: extracts artist candidates from SUMMARY and
    attendance from DESCRIPTION.
    For generic feeds: stores raw SUMMARY only, no artist extraction.

    Args:
        ical_text: Raw iCal/VCALENDAR text to parse.
        feed_type: The type of feed, which controls parsing behavior.

    Returns:
        List of parsed events extracted from the calendar.
    """
    cal = icalendar.Calendar.from_ical(ical_text)
    songkick = _is_songkick_feed(feed_type)
    events: list[ParsedEvent] = []

    for component in cal.walk():
        if component.name != "VEVENT":
            continue

        summary = str(component.get("SUMMARY", ""))
        dtstart_value = component.get("DTSTART")
        event_date = (
            _extract_date(dtstart_value.dt) if dtstart_value else datetime.date.today()
        )
        uid = str(component.get("UID", ""))
        url_value = component.get("URL")
        external_url = str(url_value) if url_value else None
        location_value = component.get("LOCATION")
        description = str(component.get("DESCRIPTION", ""))

        # Venue parsing
        venue: VenueData | None = None
        if songkick and location_value:
            venue = _parse_songkick_location(str(location_value))

        # Artist candidate extraction (Songkick only)
        artist_candidates: list[parser_module.ArtistCandidate] = []
        if songkick:
            artist_candidates = parser_module.parse_songkick_summary(summary)

        # Attendance status (Songkick attendance feeds only)
        attendance_status: str | None = None
        if feed_type == types_module.FeedType.SONGKICK_ATTENDANCE:
            attendance_status = parser_module.parse_songkick_attendance(description)

        events.append(
            ParsedEvent(
                title=summary,
                event_date=event_date,
                venue=venue,
                external_id=uid,
                external_url=external_url,
                artist_candidates=artist_candidates,
                attendance_status=attendance_status,
            )
        )

    return events
