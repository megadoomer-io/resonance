"""Concert Archives CSV parser — parses Concert Archives export files."""

from __future__ import annotations

import csv
import datetime
import io
import re

import pydantic

import resonance.concerts.ical as ical_module
import resonance.concerts.parser as parser_module

_DATE_IN_FILENAME = re.compile(r"(\d{2})-(\d{2})-(\d{4})")
_USERNAME_FROM_FILENAME = re.compile(r"^(.+?)\s*-\s*Concert Archives Export")
_USERNAME_FROM_URL = re.compile(r"concertarchives\.org/([^/]+)/concerts/")

_REQUIRED_HEADERS = frozenset(
    {
        "Start Date",
        "End Date",
        "Status",
        "Concert Name",
        "Bands Seen",
        "Bands Not Seen",
        "Venue",
        "Location",
        "URL",
    }
)

_SENTINEL_DATE = datetime.date(1970, 1, 1)


class ParseResult(pydantic.BaseModel):
    """Result of parsing a Concert Archives CSV export."""

    events: list[ical_module.ParsedEvent]
    warnings: list[str]


def parse_export_date(filename: str) -> datetime.date | None:
    """Extract the export date from a Concert Archives filename.

    Args:
        filename: The CSV filename, e.g.
            "user - Concert Archives Export - 05-19-2026.csv".

    Returns:
        The parsed date, or None if no MM-DD-YYYY pattern is found.
    """
    match = _DATE_IN_FILENAME.search(filename)
    if not match:
        return None
    month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    return datetime.date(year, month, day)


def parse_username(filename: str, urls: list[str]) -> str | None:
    """Extract the Concert Archives username from filename or URL.

    Tries the filename first, then falls back to the first matching URL.

    Args:
        filename: The CSV filename.
        urls: List of Concert Archives URLs from the CSV rows.

    Returns:
        The username, or None if it cannot be determined.
    """
    match = _USERNAME_FROM_FILENAME.search(filename)
    if match:
        return match.group(1).strip()

    for url in urls:
        match = _USERNAME_FROM_URL.search(url)
        if match:
            return match.group(1)

    return None


def parse_location(location: str) -> ical_module.VenueData | None:
    """Parse a Concert Archives location string into venue data.

    Concert Archives locations are comma-separated: "City, State, Country"
    or "City, Country". The venue name comes from a separate column, so
    name is set to empty string.

    Args:
        location: The raw location string, e.g.
            "San Francisco, California, United States".

    Returns:
        Parsed venue data with name="", or None for empty strings.
    """
    stripped = location.strip()
    if not stripped:
        return None

    parts = [p.strip() for p in stripped.split(",")]

    if len(parts) == 1:
        return ical_module.VenueData(name="", city=parts[0])

    if len(parts) == 2:
        return ical_module.VenueData(name="", city=parts[0], country=parts[1])

    # 3+ parts: City, State, Country
    return ical_module.VenueData(
        name="", city=parts[0], state=parts[1], country=parts[2]
    )


def _normalize_segment(value: str | None) -> str:
    """Normalize a string segment for use in an external ID.

    Lowercases, strips, and collapses whitespace to hyphens.

    Args:
        value: The string to normalize, or None.

    Returns:
        Normalized string, or empty string for None/empty input.
    """
    if not value:
        return ""
    return re.sub(r"\s+", "-", value.strip().lower())


def generate_external_id(
    event_date: datetime.date, venue_name: str | None, city: str | None
) -> str:
    """Generate a deterministic external ID for a Concert Archives event.

    Format: {date}_{normalized_venue}_{normalized_city}

    Args:
        event_date: The event date.
        venue_name: The venue name (may be None).
        city: The city name (may be None).

    Returns:
        A deterministic string identifier.
    """
    date_str = event_date.isoformat()
    venue_part = _normalize_segment(venue_name)
    city_part = _normalize_segment(city)
    return f"{date_str}_{venue_part}_{city_part}"


def parse_artists(
    bands_seen: str, bands_not_seen: str
) -> list[parser_module.ArtistCandidate]:
    """Parse Concert Archives band fields into artist candidates.

    Concatenates both fields and splits on " / " (space-slash-space).
    All candidates get confidence 90 and sequential positions.

    Args:
        bands_seen: Comma-separated bands the user saw.
        bands_not_seen: Comma-separated bands the user didn't see.

    Returns:
        List of ArtistCandidate with name, position, and confidence.
    """
    candidates: list[parser_module.ArtistCandidate] = []
    position = 0

    for field in (bands_seen, bands_not_seen):
        stripped = field.strip()
        if not stripped:
            continue
        parts = stripped.split(" / ")
        for part in parts:
            name = part.strip()
            if not name:
                continue
            candidates.append(
                parser_module.ArtistCandidate(
                    name=name, position=position, confidence=90
                )
            )
            position += 1

    return candidates


def _synthesize_title(
    concert_name: str,
    headliner: str | None,
    venue_name: str,
    event_date: datetime.date,
) -> str:
    """Synthesize an event title when Concert Name is absent.

    Args:
        concert_name: The Concert Name column value.
        headliner: The first artist name, if any.
        venue_name: The venue name from the Venue column.
        event_date: The event date for fallback title.

    Returns:
        A human-readable title string.
    """
    if concert_name.strip():
        return concert_name.strip()

    if headliner and venue_name.strip():
        return f"{headliner} at {venue_name.strip()}"

    if headliner:
        return headliner

    return f"Concert on {event_date.isoformat()}"


def parse_csv(content: str) -> ParseResult:
    """Parse a Concert Archives CSV export into structured event data.

    Args:
        content: The raw CSV content as a string.

    Returns:
        ParseResult containing events and any warnings.

    Raises:
        ValueError: If required headers are missing.
    """
    reader = csv.DictReader(io.StringIO(content))
    actual_headers = set(reader.fieldnames or [])
    missing = _REQUIRED_HEADERS - actual_headers
    if missing:
        raise ValueError(f"Missing required headers: {', '.join(sorted(missing))}")

    events: list[ical_module.ParsedEvent] = []
    warnings: list[str] = []

    for row_num, row in enumerate(reader, start=2):
        # Parse date
        date_str = (row.get("Start Date") or "").strip()
        if date_str:
            try:
                parsed_dt = datetime.datetime.strptime(date_str, "%m/%d/%Y")
                event_date = parsed_dt.date()
            except ValueError:
                event_date = _SENTINEL_DATE
                warnings.append(
                    f"Row {row_num}: invalid date format "
                    f"'{date_str}', using sentinel date"
                )
        else:
            event_date = _SENTINEL_DATE
            warnings.append(
                f"Row {row_num}: missing date, using sentinel date 1970-01-01"
            )

        # Parse venue and location
        venue_name = (row.get("Venue") or "").strip()
        location_str = (row.get("Location") or "").strip()
        location_data = parse_location(location_str)

        venue: ical_module.VenueData | None = None
        if venue_name or location_data:
            venue = ical_module.VenueData(
                name=venue_name,
                city=location_data.city if location_data else None,
                state=location_data.state if location_data else None,
                country=location_data.country if location_data else None,
            )

        city = venue.city if venue else None

        # Generate external ID
        external_id = generate_external_id(
            event_date, venue_name if venue_name else None, city
        )

        # Parse artists
        bands_seen = (row.get("Bands Seen") or "").strip()
        bands_not_seen = (row.get("Bands Not Seen") or "").strip()
        artist_candidates = parse_artists(bands_seen, bands_not_seen)

        # Determine headliner for title synthesis
        headliner = artist_candidates[0].name if artist_candidates else None

        # Synthesize title
        concert_name = (row.get("Concert Name") or "").strip()
        title = _synthesize_title(concert_name, headliner, venue_name, event_date)

        # Attendance status
        status = (row.get("Status") or "").strip()
        attendance_status: str | None = None
        if status in {"Past", "Upcoming"}:
            attendance_status = "going"

        # External URL
        external_url = (row.get("URL") or "").strip() or None

        events.append(
            ical_module.ParsedEvent(
                title=title,
                event_date=event_date,
                venue=venue,
                external_id=external_id,
                external_url=external_url,
                artist_candidates=artist_candidates,
                attendance_status=attendance_status,
            )
        )

    return ParseResult(events=events, warnings=warnings)
