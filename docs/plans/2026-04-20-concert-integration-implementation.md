# Concert Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Import concert/live event data from Songkick iCal feeds and generic iCal URLs, model events with venues and artist associations, and link concert artists to existing Artist entities.

**Architecture:** Calendar feeds are fetched as plain HTTP GET (no OAuth). A new `concerts/` package handles iCal parsing, Songkick SUMMARY parsing, and event/venue upserts. A new `sync_calendar_feed` arq task orchestrates the sync pipeline using the existing Task model. Data models follow existing patterns (UUID PKs, TimestampMixin, `native_enum=False`, unique constraints).

**Tech Stack:** Python 3.14, SQLAlchemy 2.0 (async), `icalendar` library, FastAPI, arq, pytest

**Design Doc:** [docs/plans/2026-04-20-concert-integration-design.md](2026-04-20-concert-integration-design.md)

---

### Task 1: Types & Enums

Add new enum values and types needed by concert models.

**Files:**
- Modify: `src/resonance/types.py`
- Modify: `tests/test_models.py`

**Step 1: Add new enums to types.py**

Add `ServiceType.ICAL` and three new enum classes:

```python
# In ServiceType (after SOUNDCLOUD, before TEST):
ICAL = "ical"

# New enums at end of file:

class FeedType(enum.StrEnum):
    """Types of calendar feeds."""
    SONGKICK_ATTENDANCE = "songkick_attendance"
    SONGKICK_TRACKED_ARTIST = "songkick_tracked_artist"
    ICAL_GENERIC = "ical_generic"


class AttendanceStatus(enum.StrEnum):
    """User attendance status for an event."""
    GOING = "going"
    INTERESTED = "interested"
    NONE = "none"


class CandidateStatus(enum.StrEnum):
    """Status of an artist-to-event candidate."""
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class TaskType(enum.StrEnum):
    # existing values...
    CALENDAR_SYNC = "calendar_sync"  # add to existing enum
```

**Step 2: Update enum tests**

In `tests/test_models.py`:
- Update `TestServiceType.test_service_type_count` — now 9 (added ICAL)
- Add `assert types_module.ServiceType.ICAL == "ical"` to `test_service_type_values`
- Add `TestFeedType`, `TestAttendanceStatus`, `TestCandidateStatus` test classes
- Add `CALENDAR_SYNC` to `TestTaskType` (if exists) or `TestSyncStatus`

**Step 3: Run tests**

```bash
uv run pytest tests/test_models.py -v
```

**Step 4: Lint & type check**

```bash
uv run ruff check src/resonance/types.py tests/test_models.py && uv run mypy src/resonance/types.py
```

**Step 5: Commit**

```bash
git add src/resonance/types.py tests/test_models.py
git commit -m "feat: add concert-related enum types (FeedType, AttendanceStatus, CandidateStatus)"
```

---

### Task 2: Concert Data Models

Create SQLAlchemy models for Venue, Event, EventArtistCandidate, EventArtist, UserEventAttendance, and UserCalendarFeed.

**Files:**
- Create: `src/resonance/models/concert.py`
- Modify: `src/resonance/models/__init__.py`
- Modify: `tests/test_models.py`

**Step 1: Write failing model tests**

Add test classes to `tests/test_models.py` following existing patterns (see `TestArtistModel`, `TestTrackModel`). Test for each model:
- `test_table_name`
- `test_expected_columns` (verify column name set)
- `test_<column>_is_<type>` for enum/FK columns
- `test_unique_constraint` for each unique constraint
- Test relationships where applicable

Models to test:

| Model | Table | Unique Constraint |
|-------|-------|-------------------|
| Venue | `venues` | `(name, city, state, country)` |
| Event | `events` | `(source_service, external_id)` |
| EventArtistCandidate | `event_artist_candidates` | `(event_id, raw_name)` |
| EventArtist | `event_artists` | `(event_id, artist_id)` |
| UserEventAttendance | `user_event_attendance` | `(user_id, event_id)` |
| UserCalendarFeed | `user_calendar_feeds` | `(user_id, url)` |

Also add export tests in `TestModelsPackageExports`.

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_models.py -v -k "Venue or Event or Calendar or Attendance or Candidate"
```

Expected: ImportError — `concert` module doesn't exist yet.

**Step 3: Write models in `src/resonance/models/concert.py`**

Follow patterns from `music.py` and `taste.py`:
- `from __future__ import annotations`
- UUID PKs with `default=uuid.uuid4`
- `TimestampMixin` on all models
- Enums with `native_enum=False`
- JSON columns for `service_links`
- FK with `ondelete="CASCADE"`

```python
"""Concert domain models: events, venues, and artist associations."""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class Venue(base_module.TimestampMixin, base_module.Base):
    """A physical location where live events happen."""

    __tablename__ = "venues"
    __table_args__ = (
        sa.UniqueConstraint(
            "name", "city", "state", "country",
            name="uq_venues_name_location",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    address: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(512), nullable=True, default=None
    )
    city: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    state: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    postal_code: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(32), nullable=True, default=None
    )
    country: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(2), nullable=True, default=None
    )
    service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        sa.JSON, nullable=True, default=None
    )

    events: orm.Mapped[list[Event]] = orm.relationship(
        back_populates="venue"
    )


class Event(base_module.TimestampMixin, base_module.Base):
    """A live music event at a specific venue on a specific date."""

    __tablename__ = "events"
    __table_args__ = (
        sa.UniqueConstraint(
            "source_service", "external_id",
            name="uq_events_source_external",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    title: orm.Mapped[str] = orm.mapped_column(sa.String(1024), nullable=False)
    event_date: orm.Mapped[datetime.date] = orm.mapped_column(
        sa.Date, nullable=False
    )
    venue_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("venues.id", ondelete="SET NULL"), nullable=True
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
    external_id: orm.Mapped[str] = orm.mapped_column(
        sa.String(512), nullable=False
    )
    external_url: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(1024), nullable=True, default=None
    )
    service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        sa.JSON, nullable=True, default=None
    )

    venue: orm.Mapped[Venue | None] = orm.relationship(
        back_populates="events"
    )
    artist_candidates: orm.Mapped[list[EventArtistCandidate]] = orm.relationship(
        back_populates="event", cascade="all, delete-orphan"
    )
    artists: orm.Mapped[list[EventArtist]] = orm.relationship(
        back_populates="event", cascade="all, delete-orphan"
    )


class EventArtistCandidate(base_module.TimestampMixin, base_module.Base):
    """Staged artist-to-event association pending user review."""

    __tablename__ = "event_artist_candidates"
    __table_args__ = (
        sa.UniqueConstraint(
            "event_id", "raw_name",
            name="uq_event_artist_candidates_event_name",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    event_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    raw_name: orm.Mapped[str] = orm.mapped_column(
        sa.String(512), nullable=False
    )
    matched_artist_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="SET NULL"), nullable=True
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    confidence_score: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    status: orm.Mapped[types_module.CandidateStatus] = orm.mapped_column(
        sa.Enum(types_module.CandidateStatus, native_enum=False),
        nullable=False,
        default=types_module.CandidateStatus.PENDING,
    )

    event: orm.Mapped[Event] = orm.relationship(
        back_populates="artist_candidates"
    )


class EventArtist(base_module.TimestampMixin, base_module.Base):
    """Confirmed artist-to-event link (created when candidate is accepted)."""

    __tablename__ = "event_artists"
    __table_args__ = (
        sa.UniqueConstraint(
            "event_id", "artist_id",
            name="uq_event_artists_event_artist",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    event_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE"), nullable=False
    )
    position: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    raw_name: orm.Mapped[str] = orm.mapped_column(
        sa.String(512), nullable=False
    )

    event: orm.Mapped[Event] = orm.relationship(
        back_populates="artists"
    )


class UserEventAttendance(base_module.TimestampMixin, base_module.Base):
    """Per-user attendance status for an event."""

    __tablename__ = "user_event_attendance"
    __table_args__ = (
        sa.UniqueConstraint(
            "user_id", "event_id",
            name="uq_user_event_attendance_user_event",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    event_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    status: orm.Mapped[types_module.AttendanceStatus] = orm.mapped_column(
        sa.Enum(types_module.AttendanceStatus, native_enum=False), nullable=False
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )


class UserCalendarFeed(base_module.TimestampMixin, base_module.Base):
    """Configured iCal feed URL for a user."""

    __tablename__ = "user_calendar_feeds"
    __table_args__ = (
        sa.UniqueConstraint(
            "user_id", "url",
            name="uq_user_calendar_feeds_user_url",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    feed_type: orm.Mapped[types_module.FeedType] = orm.mapped_column(
        sa.Enum(types_module.FeedType, native_enum=False), nullable=False
    )
    url: orm.Mapped[str] = orm.mapped_column(
        sa.String(2048), nullable=False
    )
    label: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    last_synced_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    enabled: orm.Mapped[bool] = orm.mapped_column(
        sa.Boolean, nullable=False, default=True
    )
```

**Step 4: Update `models/__init__.py`**

Add imports and `__all__` entries for all 6 new models.

**Step 5: Run tests**

```bash
uv run pytest tests/test_models.py -v
```

**Step 6: Lint & type check**

```bash
uv run ruff check src/resonance/models/concert.py && uv run mypy src/resonance/models/concert.py
```

**Step 7: Commit**

```bash
git add src/resonance/models/concert.py src/resonance/models/__init__.py tests/test_models.py
git commit -m "feat: add concert data models (Venue, Event, candidates, attendance, feeds)"
```

---

### Task 3: Songkick SUMMARY Parser

Pure function that extracts artist names, positions, and confidence from Songkick event titles.

**Files:**
- Create: `src/resonance/concerts/__init__.py`
- Create: `src/resonance/concerts/parser.py`
- Create: `tests/test_songkick_parser.py`

**Step 1: Write failing tests**

Create `tests/test_songkick_parser.py` with test cases from the design doc. The parser returns a list of `ArtistCandidate` dataclasses with `name`, `position`, and `confidence`.

Test cases:
```python
"""Tests for Songkick event title parser."""

import resonance.concerts.parser as parser_module


class TestParseSongkickSummary:
    """Test artist extraction from Songkick SUMMARY format."""

    def test_single_artist_at_venue(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Puscifer at Golden Gate Theatre (11 May 26)"
        )
        assert len(result) == 1
        assert result[0].name == "Puscifer"
        assert result[0].position == 0
        assert result[0].confidence == 90

    def test_two_artists_with_and(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Puscifer and Dave Hill at Golden Gate Theatre (11 May 26)"
        )
        assert len(result) == 2
        assert result[0].name == "Puscifer"
        assert result[1].name == "Dave Hill"
        assert result[0].position == 0
        assert result[1].position == 1

    def test_multiple_headliners_with_support(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Lagwagon, Strung Out, and Swingin' Utters at The Fillmore (16 May 26) with Western Addiction"
        )
        assert len(result) == 4
        assert result[0].name == "Lagwagon"
        assert result[1].name == "Strung Out"
        assert result[2].name == "Swingin' Utters"
        assert result[3].name == "Western Addiction"
        # First three are headliners (position 0,1,2), last is support
        assert [r.position for r in result] == [0, 1, 2, 3]

    def test_headliner_with_multiple_support(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Sleepbomb at Bottom of the Hill (04 Jun 26) with Hazzard's Cure and Ominess"
        )
        assert len(result) == 3
        assert result[0].name == "Sleepbomb"
        assert result[1].name == "Hazzard's Cure"
        assert result[2].name == "Ominess"

    def test_no_at_delimiter_returns_empty(self) -> None:
        result = parser_module.parse_songkick_summary("Just a random string")
        assert result == []

    def test_ambiguous_multiple_at_low_confidence(self) -> None:
        # "Panic! at the Disco at Venue" has multiple " at "
        result = parser_module.parse_songkick_summary(
            "Panic! at the Disco at The Forum (01 Jan 27)"
        )
        # Should still attempt parsing but with low confidence
        assert len(result) >= 1
        assert result[0].confidence == 30

    def test_venue_extraction(self) -> None:
        result = parser_module.parse_songkick_venue(
            "Puscifer at Golden Gate Theatre (11 May 26)"
        )
        assert result == "Golden Gate Theatre"

    def test_venue_extraction_strips_date(self) -> None:
        result = parser_module.parse_songkick_venue(
            "Artist at The Fillmore (16 May 26)"
        )
        assert result == "The Fillmore"

    def test_attendance_going(self) -> None:
        status = parser_module.parse_songkick_attendance(
            "You're going.\n\nMore details: https://..."
        )
        assert status == "going"

    def test_attendance_tracking(self) -> None:
        status = parser_module.parse_songkick_attendance(
            "You're tracking this event.\n\nMore details: https://..."
        )
        assert status == "interested"

    def test_attendance_unknown(self) -> None:
        status = parser_module.parse_songkick_attendance("Some random description")
        assert status is None
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_songkick_parser.py -v
```

Expected: ImportError — module doesn't exist.

**Step 3: Implement the parser**

Create `src/resonance/concerts/__init__.py` (empty) and `src/resonance/concerts/parser.py`:

```python
"""Songkick event title parser — extracts artist names from SUMMARY field."""

from __future__ import annotations

import dataclasses
import re


@dataclasses.dataclass(frozen=True)
class ArtistCandidate:
    """A parsed artist name candidate from a Songkick event title."""

    name: str
    position: int
    confidence: int


def parse_songkick_summary(summary: str) -> list[ArtistCandidate]:
    """Extract artist names from a Songkick SUMMARY string.

    Format: "Artist1, Artist2, and Artist3 at Venue (DD Mon YY) with Support1 and Support2"

    Returns:
        List of ArtistCandidate with name, position, and confidence score.
        Empty list if the format cannot be parsed.
    """
    # Split on " at " — artists are left, venue+date is right
    at_parts = summary.split(" at ")
    if len(at_parts) < 2:
        return []

    # Multiple " at " = ambiguous (e.g., "Panic! at the Disco at Venue")
    confidence = 90 if len(at_parts) == 2 else 30

    # For ambiguous cases, try last " at " as the venue split
    artists_str = " at ".join(at_parts[:-1])
    venue_and_rest = at_parts[-1]

    # Check for " with " support acts after the venue
    # The venue+date is before " with ", support acts after
    support_str = ""
    with_match = re.search(r"\)\s+with\s+", venue_and_rest)
    if with_match:
        support_str = venue_and_rest[with_match.end():]

    # Parse headliners from the artists string
    headliners = _split_artist_list(artists_str)
    candidates: list[ArtistCandidate] = []
    for i, name in enumerate(headliners):
        candidates.append(ArtistCandidate(name=name.strip(), position=i, confidence=confidence))

    # Parse support acts
    if support_str:
        support_acts = _split_artist_list(support_str)
        for name in support_acts:
            candidates.append(
                ArtistCandidate(
                    name=name.strip(),
                    position=len(candidates),
                    confidence=confidence,
                )
            )

    return candidates


def _split_artist_list(text: str) -> list[str]:
    """Split a comma/and-separated artist list.

    Handles: "A", "A and B", "A, B, and C", "A, B, and C"
    """
    # Replace Oxford comma pattern: ", and " → ","
    text = re.sub(r",\s+and\s+", ", ", text)
    # Split on ", " first
    parts = [p.strip() for p in text.split(", ") if p.strip()]
    # If only one part, try splitting on " and "
    if len(parts) == 1:
        parts = [p.strip() for p in parts[0].split(" and ") if p.strip()]
    return parts


def parse_songkick_venue(summary: str) -> str | None:
    """Extract venue name from a Songkick SUMMARY string.

    Returns:
        Venue name, or None if format can't be parsed.
    """
    at_parts = summary.split(" at ")
    if len(at_parts) < 2:
        return None
    venue_str = at_parts[-1]
    # Strip date suffix "(DD Mon YY)" and any " with ..." after it
    venue_str = re.sub(r"\s*\(\d{2}\s+\w+\s+\d{2}\).*$", "", venue_str)
    return venue_str.strip() or None


def parse_songkick_attendance(description: str) -> str | None:
    """Parse attendance status from Songkick DESCRIPTION field.

    Returns:
        "going", "interested", or None if status cannot be determined.
    """
    if "You're going." in description:
        return "going"
    if "You're tracking this event." in description:
        return "interested"
    return None
```

**Step 4: Run tests**

```bash
uv run pytest tests/test_songkick_parser.py -v
```

**Step 5: Lint & type check**

```bash
uv run ruff check src/resonance/concerts/ && uv run mypy src/resonance/concerts/
```

**Step 6: Commit**

```bash
git add src/resonance/concerts/__init__.py src/resonance/concerts/parser.py tests/test_songkick_parser.py
git commit -m "feat: add Songkick event title parser with artist extraction"
```

---

### Task 4: iCal Feed Parser

Parse iCal (VCALENDAR) data into structured event data, with Songkick-specific extensions.

**Files:**
- Modify: `pyproject.toml` (add `icalendar` dependency)
- Create: `src/resonance/concerts/ical.py`
- Create: `tests/test_ical_parser.py`

**Step 1: Add `icalendar` dependency**

```bash
uv add icalendar
```

**Step 2: Write failing tests**

Create `tests/test_ical_parser.py` with sample iCal data. Test:
- Parsing a minimal VCALENDAR with one VEVENT
- Extracting DTSTART → date
- Extracting LOCATION → venue name
- Extracting UID → external_id
- Extracting URL → external_url
- Songkick feed: artist candidates from SUMMARY
- Songkick feed: attendance from DESCRIPTION
- Generic feed: raw SUMMARY, no artist extraction
- Empty calendar → empty list
- Multiple events in one calendar

Use inline iCal strings as test fixtures:

```python
SAMPLE_SONGKICK_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Songkick//Events//EN
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260511
SUMMARY:Puscifer at Golden Gate Theatre (11 May 26)
LOCATION:Golden Gate Theatre\\, San Francisco\\, CA\\, US
UID:songkick-event-12345@songkick.com
URL:https://www.songkick.com/concerts/12345
DESCRIPTION:You're going.\\n\\nMore details: https://...
END:VEVENT
END:VCALENDAR
"""
```

**Step 3: Implement iCal parser**

Create `src/resonance/concerts/ical.py` with:
- `ParsedEvent` Pydantic model (title, event_date, venue_name, venue_location, external_id, external_url, artist_candidates, attendance_status)
- `parse_ical_feed(ical_text, feed_type)` → returns `list[ParsedEvent]`
- For Songkick feeds: call `parser_module.parse_songkick_summary()` and `parse_songkick_attendance()`
- For generic feeds: store raw SUMMARY only

`ParsedEvent` fields:
```python
class VenueData(pydantic.BaseModel):
    name: str
    city: str | None = None
    state: str | None = None
    country: str | None = None

class ParsedEvent(pydantic.BaseModel):
    title: str
    event_date: datetime.date
    venue: VenueData | None = None
    external_id: str
    external_url: str | None = None
    artist_candidates: list[parser_module.ArtistCandidate] = []
    attendance_status: str | None = None
```

Songkick LOCATION format: `"Venue Name, City, State, Country"` (backslash-escaped commas in iCal).

**Step 4: Run tests**

```bash
uv run pytest tests/test_ical_parser.py -v
```

**Step 5: Lint & type check**

```bash
uv run ruff check src/resonance/concerts/ical.py && uv run mypy src/resonance/concerts/ical.py
```

**Step 6: Commit**

```bash
git add pyproject.toml uv.lock src/resonance/concerts/ical.py tests/test_ical_parser.py
git commit -m "feat: add iCal feed parser with Songkick-specific extensions"
```

---

### Task 5: Concert Upsert Helpers

Database upsert functions for venues, events, candidates, attendance, and artist matching.

**Files:**
- Create: `src/resonance/concerts/sync.py`
- Create: `tests/test_concert_sync.py`

**Step 1: Write failing tests**

These tests need a database session. Check how existing sync runner tests work — look at `tests/test_sync_runner.py` for fixture patterns. If tests use a real async session with SQLite, follow that pattern. If they mock, follow that pattern.

Key upsert behaviors to test:
- `upsert_venue`: creates venue on first call, returns existing on second call with same (name, city, state, country)
- `upsert_event`: creates event on first call, updates on second call with same (source_service, external_id)
- `upsert_candidate`: creates candidate, matches against existing Artist by case-insensitive name
- `upsert_attendance`: creates attendance record, updates status on re-sync
- `match_candidates_to_artists`: finds existing Artist by exact name match, sets matched_artist_id and confidence

**Step 2: Implement upsert helpers**

In `src/resonance/concerts/sync.py`:

```python
"""Upsert helpers for concert data — venues, events, candidates, attendance."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import structlog

import resonance.concerts.ical as ical_module
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
    """Find venue by (name, city, state, country) or create."""
    ...


async def upsert_event(
    session: AsyncSession,
    parsed: ical_module.ParsedEvent,
    source_service: types_module.ServiceType,
    venue: concert_models.Venue | None,
) -> tuple[concert_models.Event, bool]:
    """Find event by (source_service, external_id) or create. Returns (event, created)."""
    ...


async def upsert_candidates(
    session: AsyncSession,
    event: concert_models.Event,
    candidates: list[ical_module.ArtistCandidate],  # from parser
) -> int:
    """Create EventArtistCandidate rows for parsed artist names. Returns count created."""
    ...


async def upsert_attendance(
    session: AsyncSession,
    user_id: uuid.UUID,
    event: concert_models.Event,
    status: str,
    source_service: types_module.ServiceType,
) -> None:
    """Create or update UserEventAttendance."""
    ...


async def match_candidates_to_artists(
    session: AsyncSession,
    event: concert_models.Event,
) -> int:
    """Match pending candidates to existing Artists by case-insensitive name.
    Returns count matched."""
    ...
```

**Step 3: Run tests**

```bash
uv run pytest tests/test_concert_sync.py -v
```

**Step 4: Lint & type check**

```bash
uv run ruff check src/resonance/concerts/sync.py && uv run mypy src/resonance/concerts/sync.py
```

**Step 5: Commit**

```bash
git add src/resonance/concerts/sync.py tests/test_concert_sync.py
git commit -m "feat: add concert data upsert helpers (venue, event, candidates, attendance)"
```

---

### Task 6: Calendar Sync Worker Task

The arq task function that orchestrates a full feed sync: fetch → parse → upsert → match.

**Files:**
- Create: `src/resonance/concerts/worker.py`
- Modify: `src/resonance/worker.py` (register new task)
- Create: `tests/test_concert_worker.py`

**Step 1: Write failing tests**

Test `sync_calendar_feed` task function with mocked HTTP responses:
- Fetches the feed URL
- Parses events from iCal response
- Creates venues and events
- Creates artist candidates (Songkick feeds only)
- Creates attendance records (Songkick attendance feeds only)
- Updates `last_synced_at` on the feed
- Creates a Task record tracking the sync

**Step 2: Implement the worker task**

Create `src/resonance/concerts/worker.py`:

```python
"""Calendar feed sync task for the arq worker."""

from __future__ import annotations

import datetime
import typing
from typing import Any

import httpx
import structlog

import resonance.concerts.ical as ical_module
import resonance.concerts.sync as concert_sync
import resonance.models.concert as concert_models
import resonance.types as types_module

if typing.TYPE_CHECKING:
    import sqlalchemy as sa
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

# Map feed_type to source_service
_FEED_TYPE_TO_SERVICE: dict[types_module.FeedType, types_module.ServiceType] = {
    types_module.FeedType.SONGKICK_ATTENDANCE: types_module.ServiceType.SONGKICK,
    types_module.FeedType.SONGKICK_TRACKED_ARTIST: types_module.ServiceType.SONGKICK,
    types_module.FeedType.ICAL_GENERIC: types_module.ServiceType.ICAL,
}


async def sync_calendar_feed(ctx: dict[str, Any], feed_id: str) -> None:
    """Fetch and sync an iCal calendar feed.

    1. Load the UserCalendarFeed by ID
    2. HTTP GET the feed URL
    3. Parse iCal → list of ParsedEvent
    4. For each event: upsert venue, upsert event, upsert candidates, upsert attendance
    5. Match candidates to existing artists
    6. Update last_synced_at
    """
    ...
```

**Step 3: Register in worker.py**

In `src/resonance/worker.py`:
- Import `resonance.concerts.worker as concert_worker`
- Add `sync_calendar_feed` to the `WorkerSettings.functions` list
- Wrap with heartbeat like other functions

**Step 4: Run tests**

```bash
uv run pytest tests/test_concert_worker.py -v
```

**Step 5: Lint & type check**

```bash
uv run ruff check src/resonance/concerts/worker.py src/resonance/worker.py && uv run mypy src/resonance/concerts/ src/resonance/worker.py
```

**Step 6: Commit**

```bash
git add src/resonance/concerts/worker.py src/resonance/worker.py tests/test_concert_worker.py
git commit -m "feat: add calendar feed sync worker task"
```

---

### Task 7: Calendar Feed API Endpoints

API endpoints for managing calendar feeds and triggering syncs.

**Files:**
- Create: `src/resonance/api/v1/calendar_feeds.py`
- Modify: `src/resonance/api/v1/__init__.py` (register router)
- Create: `tests/test_api_calendar_feeds.py`

**Step 1: Write failing tests**

Test endpoints:
- `POST /api/v1/calendar-feeds/songkick` — accepts `{"username": "..."}`, creates 2 feeds
- `POST /api/v1/calendar-feeds/ical` — accepts `{"url": "...", "label": "..."}`
- `GET /api/v1/calendar-feeds` — lists user's feeds
- `DELETE /api/v1/calendar-feeds/{id}` — deletes a feed
- `POST /api/v1/calendar-feeds/{id}/sync` — triggers sync, returns task ID

Follow patterns from existing API tests (e.g., `tests/test_api_admin.py`, `tests/test_api_sync.py`).

**Step 2: Implement endpoints**

```python
"""Calendar feed management API endpoints."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import fastapi
import pydantic
import sqlalchemy as sa
import structlog

import resonance.models.concert as concert_models
import resonance.models.task as task_models
import resonance.types as types_module

if TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()
router = fastapi.APIRouter(prefix="/calendar-feeds", tags=["calendar-feeds"])


class SongkickFeedRequest(pydantic.BaseModel):
    username: str


class GenericFeedRequest(pydantic.BaseModel):
    url: str
    label: str | None = None


@router.post("/songkick")
async def add_songkick_feeds(body: SongkickFeedRequest, ...) -> ...:
    """Create two Songkick feeds (attendance + tracked artist) from username."""
    # Generate URLs:
    # f"https://www.songkick.com/users/{username}/calendars.ics?filter=attendance"
    # f"https://www.songkick.com/users/{username}/calendars.ics?filter=tracked_artist"
    ...


@router.post("/ical")
async def add_generic_feed(body: GenericFeedRequest, ...) -> ...:
    """Add a generic iCal feed URL."""
    ...


@router.get("")
async def list_feeds(...) -> ...:
    """List current user's calendar feeds."""
    ...


@router.delete("/{feed_id}")
async def delete_feed(feed_id: uuid.UUID, ...) -> ...:
    """Delete a calendar feed."""
    ...


@router.post("/{feed_id}/sync")
async def trigger_feed_sync(feed_id: uuid.UUID, ...) -> ...:
    """Trigger a sync for a specific feed. Returns task ID."""
    # Create a Task with task_type=CALENDAR_SYNC, enqueue sync_calendar_feed
    ...
```

**Step 3: Register router**

In `src/resonance/api/v1/__init__.py`, import and include the calendar feeds router.

**Step 4: Run tests**

```bash
uv run pytest tests/test_api_calendar_feeds.py -v
```

**Step 5: Lint & type check**

```bash
uv run ruff check src/resonance/api/v1/calendar_feeds.py && uv run mypy src/resonance/api/v1/
```

**Step 6: Commit**

```bash
git add src/resonance/api/v1/calendar_feeds.py src/resonance/api/v1/__init__.py tests/test_api_calendar_feeds.py
git commit -m "feat: add calendar feed management API endpoints"
```

---

### Task 8: Alembic Migration

Create database migration for all new tables.

**Files:**
- Create: `alembic/versions/<hash>_add_concert_tables.py`

**Step 1: Generate migration**

The migration must be written manually (no local DB for autogenerate). Create tables:
- `venues`
- `events`
- `event_artist_candidates`
- `event_artists`
- `user_event_attendance`
- `user_calendar_feeds`

Also add CHECK constraints for enum columns (matching existing pattern with `native_enum=False`).

Refer to existing migrations in `alembic/versions/` for the project's style. Key patterns:
- Use `op.create_table()` with explicit column definitions
- Add indexes for common query patterns (events by date, by venue_id)
- Add unique constraints as defined in the models

**Step 2: Review migration**

Verify the migration:
- Creates all 6 tables
- All FK relationships are correct
- All unique constraints match the model definitions
- Enum columns have appropriate CHECK constraints
- Indexes exist for performance-critical queries

**Step 3: Commit**

```bash
git add alembic/versions/
git commit -m "feat: add migration for concert tables (venues, events, candidates, attendance, feeds)"
```

---

### Task 9: Full Integration Test

End-to-end test that exercises the full pipeline: add feed → sync → verify data.

**Files:**
- Create: `tests/test_concert_integration.py`

**Step 1: Write integration test**

Test the full pipeline with mocked HTTP:
1. Create a UserCalendarFeed (Songkick attendance type)
2. Mock the HTTP response with sample iCal data containing 2-3 events
3. Run `sync_calendar_feed`
4. Verify:
   - Venues created
   - Events created with correct dates and titles
   - Artist candidates created with correct names and positions
   - Attendance records created with correct status
   - `last_synced_at` updated on the feed
5. Run sync again with same data → verify no duplicates (idempotent)

**Step 2: Run all tests**

```bash
uv run pytest -v
```

**Step 3: Full quality check**

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src/
```

**Step 4: Commit**

```bash
git add tests/test_concert_integration.py
git commit -m "test: add end-to-end concert integration test"
```

---

## Task Dependency Graph

```
Task 1 (types/enums)
  └→ Task 2 (models)
       ├→ Task 3 (parser) — no model dependency, but logically next
       │    └→ Task 4 (iCal parser) — depends on parser
       ├→ Task 5 (upsert helpers) — depends on models + iCal parser
       │    └→ Task 6 (worker task) — depends on upserts
       │         └→ Task 7 (API) — depends on worker
       └→ Task 8 (migration) — depends on models being final
Task 9 (integration) — depends on all above
```

Tasks 3-4 (parser) and Task 8 (migration) can run in parallel with Task 5 once Task 2 is done.
