"""Songkick event title parser — extracts artists, venues, and attendance."""

from __future__ import annotations

import dataclasses
import re


@dataclasses.dataclass(frozen=True)
class ArtistCandidate:
    """An artist extracted from a Songkick event title."""

    name: str
    position: int
    confidence: int


def _split_artist_list(raw: str) -> list[str]:
    """Split a comma/and-separated artist string into individual names.

    Handles Oxford comma (", and ") as a list separator, then falls back
    to splitting on ", " and finally " and ".
    """
    # Normalize Oxford comma: ", and " → ", "
    normalized = raw.replace(", and ", ", ")

    # Try splitting on ", " first
    parts = [p.strip() for p in normalized.split(", ") if p.strip()]
    if len(parts) > 1:
        # Each part may still contain " and " for the last pair
        expanded: list[str] = []
        for part in parts:
            sub = [s.strip() for s in part.split(" and ") if s.strip()]
            expanded.extend(sub)
        return expanded

    # Single part — try " and " as separator
    parts = [p.strip() for p in raw.split(" and ") if p.strip()]
    return parts


# Matches a date parenthetical like "(11 May 26)" or "(01 Jan 27)"
_DATE_PATTERN = re.compile(r"\s*\(\d{2}\s+\w+\s+\d{2}\)")


def parse_songkick_summary(summary: str) -> list[ArtistCandidate]:
    """Parse a Songkick SUMMARY field into a list of artist candidates.

    Songkick SUMMARY format:
        "Artist1, Artist2, and Artist3 at Venue (DD Mon YY) with Support1 and Support2"

    Args:
        summary: The raw SUMMARY string from a Songkick iCal event.

    Returns:
        List of ArtistCandidate with name, position, and confidence.
        Empty list if the summary cannot be parsed.
    """
    if not summary:
        return []

    # Count occurrences of " at " to detect ambiguity
    at_count = summary.count(" at ")
    if at_count == 0:
        return []

    ambiguous = at_count > 1
    confidence = 30 if ambiguous else 90

    # Split on the last " at " to separate artists from venue+date
    last_at_idx = summary.rfind(" at ")
    artists_part = summary[:last_at_idx]
    venue_and_rest = summary[last_at_idx + 4 :]  # skip " at "

    # Check for support acts: everything after the date parenthetical ")" + " with "
    support_part = ""
    date_match = _DATE_PATTERN.search(venue_and_rest)
    if date_match:
        after_date = venue_and_rest[date_match.end() :]
        if after_date.startswith(" with "):
            support_part = after_date[6:]  # skip " with "

    # Parse headliners
    headliners = _split_artist_list(artists_part)

    # Parse support acts
    support_acts: list[str] = []
    if support_part:
        support_acts = _split_artist_list(support_part)

    # Build result with sequential positions
    all_artists = headliners + support_acts
    result = [
        ArtistCandidate(name=name, position=pos, confidence=confidence)
        for pos, name in enumerate(all_artists)
    ]

    return result


def parse_songkick_venue(summary: str) -> str | None:
    """Extract the venue name from a Songkick SUMMARY field.

    Splits on the last ` at `, then strips the date suffix ``(DD Mon YY)``
    and anything that follows it.

    Args:
        summary: The raw SUMMARY string from a Songkick iCal event.

    Returns:
        The venue name, or ``None`` if no ` at ` delimiter is found.
    """
    if " at " not in summary:
        return None

    last_at_idx = summary.rfind(" at ")
    venue_and_rest = summary[last_at_idx + 4 :]

    # Strip date parenthetical and anything after it
    date_match = _DATE_PATTERN.search(venue_and_rest)
    venue = venue_and_rest[: date_match.start()] if date_match else venue_and_rest

    return venue.strip() or None


def parse_songkick_attendance(description: str) -> str | None:
    """Parse the Songkick DESCRIPTION field for attendance status.

    Args:
        description: The raw DESCRIPTION string from a Songkick iCal event.

    Returns:
        ``"going"`` if the user is attending, ``"interested"`` if tracking,
        or ``None`` if the status cannot be determined.
    """
    if not description:
        return None

    if description.startswith("You're going."):
        return "going"
    if description.startswith("You're tracking this event."):
        return "interested"

    return None
