# Concert Archives CSV Import — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add Concert Archives as a data source via CSV file upload, creating events/venues/candidates from exported concert history.

**Architecture:** New `CONCERT_ARCHIVES` service type with `file_upload` auth. CSV parsed into `ParsedEvent` objects reusing the existing upsert pipeline. Upload endpoint accepts multipart file, enqueues an arq background task that processes the CSV and reports results via the sync task system. Composite event matching via synthetic `external_id` generated from `(date, venue, city)`.

**Tech Stack:** Python 3.14, FastAPI (UploadFile), SQLAlchemy 2.0 async, arq, HTMX, Pydantic, csv stdlib module.

**Design doc:** `docs/plans/2026-05-19-concert-archives-csv-import-design.md`

---

### Task 1: Add CONCERT_ARCHIVES to ServiceType enum

**Files:**
- Modify: `src/resonance/types.py:6-18` (ServiceType enum)

**Step 1: Add the enum value**

In `src/resonance/types.py`, add `CONCERT_ARCHIVES` to `ServiceType`:

```python
class ServiceType(enum.StrEnum):
    SPOTIFY = "spotify"
    LASTFM = "lastfm"
    LISTENBRAINZ = "listenbrainz"
    SONGKICK = "songkick"
    BANDSINTOWN = "bandsintown"
    BANDCAMP = "bandcamp"
    SOUNDCLOUD = "soundcloud"
    ICAL = "ical"
    CONCERT_ARCHIVES = "concert_archives"  # <-- new
    MANUAL = "manual"
    TEST = "test"
```

**Step 2: Add CONCERT_ARCHIVES_IMPORT to TaskType**

In `src/resonance/types.py`, add to `TaskType`:

```python
CONCERT_ARCHIVES_IMPORT = "concert_archives_import"
```

**Step 3: Run existing tests to make sure nothing breaks**

Run: `uv run pytest tests/ -x -q`
Expected: All pass (no code depends on exhaustive enum checks)

**Step 4: Commit**

```bash
git add src/resonance/types.py
git commit -m "feat: add CONCERT_ARCHIVES service type and import task type"
```

---

### Task 2: Alembic migration for CHECK constraints

**Files:**
- Create: `alembic/versions/a4v5w6x7y8z9_add_concert_archives_service_type.py`

**Step 1: Create the migration**

Follow the exact pattern from `v9q0r1s2t3u4_add_manual_service_type.py`. The migration updates CHECK constraints on all tables that store `ServiceType` values, adding `'CONCERT_ARCHIVES'` to the allowed list. Also add `'CONCERT_ARCHIVES_IMPORT'` to the task_type CHECK constraint on the `tasks` table.

The `_TABLES_AND_COLUMNS` list, `_FIND_CONSTRAINTS_SQL`, and `_replace_constraints` helper are identical to the MANUAL migration. Only the `_OLD_VALUES` / `_NEW_VALUES` strings change.

Use revision ID `a4v5w6x7y8z9` and set `down_revision` to `z3u4v5w6x7y8` (the current head).

**Step 2: Commit**

```bash
git add alembic/versions/a4v5w6x7y8z9_add_concert_archives_service_type.py
git commit -m "feat: migration to add concert_archives to service type constraints"
```

---

### Task 3: CSV parser module with tests

**Files:**
- Create: `src/resonance/concerts/concert_archives.py`
- Create: `tests/test_concert_archives_parser.py`

**Step 1: Write tests for `parse_export_date`**

```python
# tests/test_concert_archives_parser.py
import datetime

import resonance.concerts.concert_archives as ca_module


class TestParseExportDate:
    def test_standard_filename(self) -> None:
        result = ca_module.parse_export_date(
            "mike.dougherty - Concert Archives Export - 05-19-2026.csv"
        )
        assert result == datetime.date(2026, 5, 19)

    def test_no_date_in_filename(self) -> None:
        assert ca_module.parse_export_date("concerts.csv") is None

    def test_different_date(self) -> None:
        result = ca_module.parse_export_date(
            "user - Concert Archives Export - 12-25-2025.csv"
        )
        assert result == datetime.date(2025, 12, 25)
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_concert_archives_parser.py -v`
Expected: FAIL (module not found)

**Step 3: Implement `parse_export_date`**

```python
# src/resonance/concerts/concert_archives.py
"""Concert Archives CSV import — parsing and data extraction."""

from __future__ import annotations

import datetime
import re


_EXPORT_DATE_PATTERN = re.compile(r"(\d{2})-(\d{2})-(\d{4})")


def parse_export_date(filename: str) -> datetime.date | None:
    """Extract the export date from a Concert Archives filename.

    Looks for MM-DD-YYYY pattern in the filename.
    """
    match = _EXPORT_DATE_PATTERN.search(filename)
    if match is None:
        return None
    month, day, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
    try:
        return datetime.date(year, month, day)
    except ValueError:
        return None
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_concert_archives_parser.py::TestParseExportDate -v`
Expected: PASS

**Step 5: Write tests for `parse_username`**

```python
class TestParseUsername:
    def test_from_filename(self) -> None:
        result = ca_module.parse_username(
            "mike.dougherty - Concert Archives Export - 05-19-2026.csv",
            urls=[],
        )
        assert result == "mike.dougherty"

    def test_from_url_fallback(self) -> None:
        result = ca_module.parse_username(
            "concerts.csv",
            urls=["https://www.concertarchives.org/mike-dougherty/concerts/foo"],
        )
        assert result == "mike-dougherty"

    def test_no_source(self) -> None:
        assert ca_module.parse_username("concerts.csv", urls=[]) is None
```

**Step 6: Run to verify failure, then implement `parse_username`**

```python
_USERNAME_FROM_FILENAME = re.compile(r"^(.+?)\s*-\s*Concert Archives Export")
_USERNAME_FROM_URL = re.compile(
    r"concertarchives\.org/([^/]+)/concerts/"
)


def parse_username(
    filename: str, urls: list[str]
) -> str | None:
    """Extract Concert Archives username from filename or URL."""
    match = _USERNAME_FROM_FILENAME.match(filename)
    if match is not None:
        return match.group(1).strip()
    for url in urls:
        match = _USERNAME_FROM_URL.search(url)
        if match is not None:
            return match.group(1)
    return None
```

**Step 7: Run to verify pass**

Run: `uv run pytest tests/test_concert_archives_parser.py::TestParseUsername -v`
Expected: PASS

**Step 8: Write tests for `parse_location`**

```python
import resonance.concerts.ical as ical_module


class TestParseLocation:
    def test_us_location(self) -> None:
        result = ca_module.parse_location("San Francisco, California, United States")
        assert result == ical_module.VenueData(
            name="", city="San Francisco", state="California", country="United States"
        )

    def test_international_location(self) -> None:
        result = ca_module.parse_location("Toronto, Ontario, Canada")
        assert result == ical_module.VenueData(
            name="", city="Toronto", state="Ontario", country="Canada"
        )

    def test_empty(self) -> None:
        assert ca_module.parse_location("") is None
```

**Step 9: Implement `parse_location`**

```python
import resonance.concerts.ical as ical_module


def parse_location(location: str) -> ical_module.VenueData | None:
    """Parse Concert Archives location string into VenueData.

    Format: "City, State/Province, Country"
    """
    if not location.strip():
        return None
    parts = [p.strip() for p in location.split(",")]
    if len(parts) >= 3:
        return ical_module.VenueData(
            name="", city=parts[0], state=parts[1], country=parts[2]
        )
    if len(parts) == 2:
        return ical_module.VenueData(
            name="", city=parts[0], state=None, country=parts[1]
        )
    return None
```

**Step 10: Run to verify pass**

Run: `uv run pytest tests/test_concert_archives_parser.py::TestParseLocation -v`

**Step 11: Write tests for `generate_external_id`**

This function generates a synthetic deterministic ID from `(date, venue, city)` for idempotent matching.

```python
class TestGenerateExternalId:
    def test_basic(self) -> None:
        result = ca_module.generate_external_id(
            datetime.date(2026, 9, 26), "SF Masonic Auditorium", "San Francisco"
        )
        assert result == "2026-09-26_sf-masonic-auditorium_san-francisco"

    def test_normalizes_whitespace_and_case(self) -> None:
        result = ca_module.generate_external_id(
            datetime.date(2026, 9, 26), "  SF Masonic  ", "  San Francisco  "
        )
        assert result == "2026-09-26_sf-masonic_san-francisco"

    def test_no_venue(self) -> None:
        result = ca_module.generate_external_id(
            datetime.date(2026, 9, 26), None, None
        )
        assert result == "2026-09-26__"
```

**Step 12: Implement `generate_external_id`**

```python
def generate_external_id(
    event_date: datetime.date,
    venue_name: str | None,
    city: str | None,
) -> str:
    """Generate a deterministic external ID for composite event matching."""
    def _normalize(s: str | None) -> str:
        if not s:
            return ""
        return re.sub(r"\s+", "-", s.strip().lower())

    return f"{event_date.isoformat()}_{_normalize(venue_name)}_{_normalize(city)}"
```

**Step 13: Run to verify pass**

Run: `uv run pytest tests/test_concert_archives_parser.py::TestGenerateExternalId -v`

**Step 14: Write tests for `parse_artists`**

```python
import resonance.concerts.parser as parser_module


class TestParseArtists:
    def test_single_artist(self) -> None:
        result = ca_module.parse_artists("Beck", "")
        assert len(result) == 1
        assert result[0] == parser_module.ArtistCandidate(
            name="Beck", position=0, confidence=90
        )

    def test_multiple_artists(self) -> None:
        result = ca_module.parse_artists("The Sword / Red Fang", "")
        assert len(result) == 2
        assert result[0].name == "The Sword"
        assert result[1].name == "Red Fang"
        assert result[0].position == 0
        assert result[1].position == 1

    def test_bands_seen_and_not_seen_combined(self) -> None:
        result = ca_module.parse_artists("Slipknot / KISS", "Muse / Evanescence")
        assert len(result) == 4
        assert [a.name for a in result] == [
            "Slipknot", "KISS", "Muse", "Evanescence"
        ]
        assert [a.position for a in result] == [0, 1, 2, 3]

    def test_empty_bands(self) -> None:
        result = ca_module.parse_artists("", "")
        assert result == []

    def test_all_confidence_90(self) -> None:
        result = ca_module.parse_artists("A / B / C", "D")
        assert all(a.confidence == 90 for a in result)

    def test_w_slash_in_name(self) -> None:
        result = ca_module.parse_artists(
            "", "Lea Bertucci w/ Norbert Rodenkirchen"
        )
        assert len(result) == 1
        assert result[0].name == "Lea Bertucci w/ Norbert Rodenkirchen"
```

**Step 15: Implement `parse_artists`**

```python
import resonance.concerts.parser as parser_module


def parse_artists(
    bands_seen: str, bands_not_seen: str
) -> list[parser_module.ArtistCandidate]:
    """Parse slash-separated artist names into candidates."""
    combined = []
    for field in (bands_seen, bands_not_seen):
        if field.strip():
            combined.extend(name.strip() for name in field.split(" / "))
    return [
        parser_module.ArtistCandidate(name=name, position=pos, confidence=90)
        for pos, name in enumerate(combined)
        if name
    ]
```

**Step 16: Run to verify pass**

Run: `uv run pytest tests/test_concert_archives_parser.py::TestParseArtists -v`

**Step 17: Write tests for `parse_csv`**

This is the main function that parses the full CSV into `ParsedEvent` objects.

```python
class TestParseCsv:
    def test_basic_event(self) -> None:
        csv_content = (
            "Start Date,End Date,Status,Concert Name,Bands Seen,"
            "Bands Not Seen,Venue,Location,URL\n"
            '09/26/2026,,Upcoming,,Beck,"",SF Masonic Auditorium,'
            '"San Francisco, California, United States",'
            "https://www.concertarchives.org/mike/concerts/beck-abc123\n"
        )
        result = ca_module.parse_csv(csv_content)
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_date == datetime.date(2026, 9, 26)
        assert event.venue is not None
        assert event.venue.name == "SF Masonic Auditorium"
        assert event.venue.city == "San Francisco"
        assert len(event.artist_candidates) == 1
        assert event.artist_candidates[0].name == "Beck"
        assert event.attendance_status == "going"

    def test_cancelled_event_no_attendance(self) -> None:
        csv_content = (
            "Start Date,End Date,Status,Concert Name,Bands Seen,"
            "Bands Not Seen,Venue,Location,URL\n"
            '03/15/2020,,Cancelled,,Some Band,"",Venue,'
            '"City, State, Country",https://example.com\n'
        )
        result = ca_module.parse_csv(csv_content)
        assert len(result.events) == 1
        assert result.events[0].attendance_status is None

    def test_missing_date_uses_sentinel(self) -> None:
        csv_content = (
            "Start Date,End Date,Status,Concert Name,Bands Seen,"
            "Bands Not Seen,Venue,Location,URL\n"
            ',,Past,,Band,"",Venue,"City, State, Country",https://ex.com\n'
        )
        result = ca_module.parse_csv(csv_content)
        assert len(result.events) == 1
        assert result.events[0].event_date == datetime.date(1970, 1, 1)
        assert len(result.warnings) == 1

    def test_invalid_headers_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Missing required columns"):
            ca_module.parse_csv("Col1,Col2,Col3\na,b,c\n")

    def test_concert_name_as_title(self) -> None:
        csv_content = (
            "Start Date,End Date,Status,Concert Name,Bands Seen,"
            "Bands Not Seen,Venue,Location,URL\n"
            '08/30/2005,,Past,Gigantour,Megadeth / Dream Theater,"",Venue,'
            '"City, State, Country",https://ex.com\n'
        )
        result = ca_module.parse_csv(csv_content)
        assert result.events[0].title == "Gigantour"

    def test_no_concert_name_synthesizes_title(self) -> None:
        csv_content = (
            "Start Date,End Date,Status,Concert Name,Bands Seen,"
            "Bands Not Seen,Venue,Location,URL\n"
            '09/26/2026,,Upcoming,,Beck,"",SF Masonic,'
            '"San Francisco, CA, US",https://ex.com\n'
        )
        result = ca_module.parse_csv(csv_content)
        assert result.events[0].title == "Beck at SF Masonic"
```

**Step 18: Implement `parse_csv`**

```python
import csv
import io
import dataclasses

import pydantic


class ParseResult(pydantic.BaseModel):
    """Result of parsing a Concert Archives CSV."""
    events: list[ical_module.ParsedEvent]
    warnings: list[str]


_REQUIRED_COLUMNS = {
    "Start Date", "End Date", "Status", "Concert Name",
    "Bands Seen", "Bands Not Seen", "Venue", "Location", "URL",
}
_SENTINEL_DATE = datetime.date(1970, 1, 1)


def parse_csv(content: str) -> ParseResult:
    """Parse a Concert Archives CSV export into ParsedEvent objects."""
    reader = csv.DictReader(io.StringIO(content))
    if reader.fieldnames is None:
        msg = "Empty CSV file"
        raise ValueError(msg)

    missing = _REQUIRED_COLUMNS - set(reader.fieldnames)
    if missing:
        msg = f"Missing required columns: {', '.join(sorted(missing))}"
        raise ValueError(msg)

    events: list[ical_module.ParsedEvent] = []
    warnings: list[str] = []

    for row_num, row in enumerate(reader, start=2):
        event_date = _parse_date(row["Start Date"])
        if event_date is None:
            event_date = _SENTINEL_DATE
            warnings.append(f"Row {row_num}: missing start date, using 1970-01-01")

        venue_name = row["Venue"].strip()
        location = parse_location(row["Location"])
        if location is not None and venue_name:
            venue_data = dataclasses.replace(
                ical_module.VenueData(
                    name=venue_name,
                    city=location.city,
                    state=location.state,
                    country=location.country,
                ),
            )
        elif venue_name:
            venue_data = ical_module.VenueData(name=venue_name)
        else:
            venue_data = None

        # NOTE: VenueData is a pydantic BaseModel, not a dataclass.
        # Construct it directly instead of using dataclasses.replace.
        # Fix this during implementation — check VenueData's actual type.

        candidates = parse_artists(row["Bands Seen"], row["Bands Not Seen"])

        concert_name = row["Concert Name"].strip()
        if concert_name:
            title = concert_name
        elif candidates:
            title = f"{candidates[0].name} at {venue_name}" if venue_name else candidates[0].name
        else:
            title = venue_name or f"Concert on {event_date.isoformat()}"

        city = location.city if location else None
        external_id = generate_external_id(event_date, venue_name or None, city)

        status = row["Status"].strip()
        attendance = "going" if status in ("Past", "Upcoming") else None

        events.append(ical_module.ParsedEvent(
            title=title,
            event_date=event_date,
            venue=venue_data,
            external_id=external_id,
            external_url=row["URL"].strip() or None,
            artist_candidates=candidates,
            attendance_status=attendance,
        ))

    return ParseResult(events=events, warnings=warnings)


def _parse_date(date_str: str) -> datetime.date | None:
    """Parse MM/DD/YYYY date string."""
    date_str = date_str.strip()
    if not date_str:
        return None
    try:
        return datetime.datetime.strptime(date_str, "%m/%d/%Y").date()
    except ValueError:
        return None
```

**Step 19: Run all parser tests**

Run: `uv run pytest tests/test_concert_archives_parser.py -v`
Expected: All PASS

**Step 20: Run linting and type checking**

Run: `uv run ruff check src/resonance/concerts/concert_archives.py tests/test_concert_archives_parser.py && uv run mypy src/resonance/concerts/concert_archives.py`
Expected: Clean

**Step 21: Commit**

```bash
git add src/resonance/concerts/concert_archives.py tests/test_concert_archives_parser.py
git commit -m "feat: add Concert Archives CSV parser with tests"
```

---

### Task 4: ConcertArchivesConnector class

**Files:**
- Create: `src/resonance/connectors/concert_archives.py`

**Step 1: Create the connector**

Follow the Songkick/iCal lightweight pattern:

```python
"""Concert Archives connector — CSV file upload sync."""

from __future__ import annotations

import resonance.connectors.base as base_module
import resonance.types as types_module


class ConcertArchivesConnector:
    """Minimal connector for Concert Archives CSV import connections."""

    service_type = types_module.ServiceType.CONCERT_ARCHIVES

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        return base_module.ConnectionConfig(
            auth_type="file_upload",
            sync_function="sync_concert_archives",
            sync_style="full",
        )
```

**Step 2: Register in app.py and worker.py**

In `src/resonance/app.py`, add to the connector registration block:

```python
import resonance.connectors.concert_archives as concert_archives_module
# ...
connector_registry.register(concert_archives_module.ConcertArchivesConnector())
```

Same in `src/resonance/worker.py` in the connector registration block.

**Step 3: Run tests**

Run: `uv run pytest tests/ -x -q`
Expected: All pass

**Step 4: Commit**

```bash
git add src/resonance/connectors/concert_archives.py src/resonance/app.py src/resonance/worker.py
git commit -m "feat: add ConcertArchivesConnector with file_upload auth type"
```

---

### Task 5: Background task — sync_concert_archives

**Files:**
- Modify: `src/resonance/concerts/worker.py` (add new task function)
- Modify: `src/resonance/worker.py` (register in WorkerSettings + _TASK_DISPATCH)
- Create: `tests/test_concert_archives_worker.py`

**Step 1: Write a basic test for the task**

Test that the task parses CSV, upserts events, and completes the sync task. Mock the database session following the pattern in `tests/test_concert_worker.py`.

Key behaviors to test:
- CSV is parsed and events are upserted
- Task status transitions: PENDING → RUNNING → COMPLETED
- Result dict contains counts (events_created, events_updated, etc.)
- Cancelled events get no attendance record
- Warnings from parsing are included in the result

**Step 2: Run to verify failure**

Run: `uv run pytest tests/test_concert_archives_worker.py -v`
Expected: FAIL

**Step 3: Implement the task**

Add `sync_concert_archives` to `src/resonance/concerts/worker.py`:

```python
async def sync_concert_archives(
    ctx: dict[str, Any], task_id: str, csv_content: str
) -> None:
    """Parse and import a Concert Archives CSV export.

    Args:
        ctx: arq worker context dict (contains session_factory).
        task_id: UUID string of the Task tracking this import.
        csv_content: The full CSV file content as a string.
    """
    session_factory = ctx["session_factory"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("concert_archives_task_not_found")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            # Load connection for user_id and last_synced_at update
            connection = await _load_connection(
                session, str(task.service_connection_id)
            )
            if connection is None:
                await lifecycle_module.fail_task(
                    session, task, "ServiceConnection not found"
                )
                await session.commit()
                return

            log = log.bind(user_id=str(connection.user_id))

            # Parse CSV
            parse_result = concert_archives_module.parse_csv(csv_content)
            log.info(
                "concert_archives_parsed",
                event_count=len(parse_result.events),
                warning_count=len(parse_result.warnings),
            )

            # Process events using existing upsert pipeline
            events_created = 0
            events_updated = 0
            candidates_created = 0
            candidates_matched = 0
            source_service = types_module.ServiceType.CONCERT_ARCHIVES

            for parsed in parse_result.events:
                venue = None
                if parsed.venue is not None:
                    venue = await concert_sync.upsert_venue(session, parsed.venue)

                event, created = await concert_sync.upsert_event(
                    session, parsed, source_service, venue
                )
                if created:
                    events_created += 1
                else:
                    events_updated += 1

                if parsed.artist_candidates:
                    new = await concert_sync.upsert_candidates(
                        session, event, parsed.artist_candidates
                    )
                    candidates_created += new

                if parsed.attendance_status is not None:
                    await concert_sync.upsert_attendance(
                        session, connection.user_id, event,
                        parsed.attendance_status, source_service,
                    )

                matched = await concert_sync.match_candidates_to_artists(
                    session, event
                )
                candidates_matched += matched

            connection.last_synced_at = datetime.datetime.now(datetime.UTC)

            result: dict[str, object] = {
                "events_created": events_created,
                "events_updated": events_updated,
                "candidates_created": candidates_created,
                "candidates_matched": candidates_matched,
                "total_events": len(parse_result.events),
                "warnings": parse_result.warnings,
            }

            await lifecycle_module.complete_task(session, task, result)
            await session.commit()
            log.info("concert_archives_sync_completed", **result)

        except Exception:
            log.exception("concert_archives_sync_failed")
            if task is not None:
                import traceback
                await lifecycle_module.fail_task(
                    session, task, traceback.format_exc()
                )
                await session.commit()
```

**Step 4: Register in _TASK_DISPATCH and WorkerSettings**

In `src/resonance/worker.py`, add to `_TASK_DISPATCH`:

```python
types_module.TaskType.CONCERT_ARCHIVES_IMPORT: (
    "sync_concert_archives",
    lambda t: (str(t.id),),  # csv_content passed separately at enqueue time
),
```

In `WorkerSettings.functions`, add:

```python
arq.func(
    heartbeat_module.with_heartbeat(concert_worker.sync_concert_archives),
    timeout=3600,
),
```

**Step 5: Run tests**

Run: `uv run pytest tests/test_concert_archives_worker.py -v`
Expected: PASS

**Step 6: Run full test suite**

Run: `uv run pytest tests/ -x -q`
Expected: All pass

**Step 7: Commit**

```bash
git add src/resonance/concerts/worker.py src/resonance/worker.py tests/test_concert_archives_worker.py
git commit -m "feat: add sync_concert_archives arq task"
```

---

### Task 6: Upload API endpoint

**Files:**
- Create: `src/resonance/api/v1/concert_archives.py`
- Modify: `src/resonance/api/v1/__init__.py` (register router)
- Create: `tests/test_concert_archives_api.py`

**Step 1: Write tests for the upload endpoint**

Key behaviors to test:
- Unauthenticated returns 401
- Valid CSV creates connection (if needed) and returns task_id
- Stale export rejected with 409
- Invalid CSV (wrong headers) returns 422
- File too large returns 413
- Concurrent import rejected with 409

**Step 2: Run to verify failure**

Run: `uv run pytest tests/test_concert_archives_api.py -v`
Expected: FAIL

**Step 3: Implement the endpoint**

```python
# src/resonance/api/v1/concert_archives.py
"""Concert Archives CSV upload endpoint."""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.api.v1.deps as deps_module
import resonance.concerts.concert_archives as ca_module
import resonance.models as models
import resonance.types as types_module

router = fastapi.APIRouter(
    prefix="/connections/concert-archives",
    tags=["concert-archives"],
)

_MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB


@router.post("/upload")
async def upload_csv(
    request: fastapi.Request,
    file: fastapi.UploadFile,
    export_date: str | None = fastapi.Form(default=None),
    user_id: uuid.UUID = fastapi.Depends(deps_module.get_current_user_id),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> dict[str, str]:
    """Upload a Concert Archives CSV export."""
    # Read and validate file size
    content = await file.read()
    if len(content) > _MAX_FILE_SIZE:
        raise fastapi.HTTPException(status_code=413, detail="File too large (max 5MB)")

    csv_text = content.decode("utf-8")

    # Validate CSV headers before proceeding
    try:
        parse_result = ca_module.parse_csv(csv_text)
    except ValueError as exc:
        raise fastapi.HTTPException(status_code=422, detail=str(exc)) from exc

    # Parse export date
    resolved_date: datetime.date
    if export_date:
        try:
            resolved_date = datetime.date.fromisoformat(export_date)
        except ValueError as exc:
            raise fastapi.HTTPException(
                status_code=422, detail=f"Invalid export_date: {exc}"
            ) from exc
    else:
        detected = ca_module.parse_export_date(file.filename or "")
        resolved_date = detected or datetime.date.today()

    # Find or create Concert Archives connection
    conn_stmt = sa.select(models.ServiceConnection).where(
        models.ServiceConnection.user_id == user_id,
        models.ServiceConnection.service_type == types_module.ServiceType.CONCERT_ARCHIVES,
    )
    conn_result = await db.execute(conn_stmt)
    connection = conn_result.scalar_one_or_none()

    if connection is not None:
        # Stale export check
        last_export = connection.sync_watermark.get("last_export_date")
        if last_export and resolved_date.isoformat() < last_export:
            raise fastapi.HTTPException(
                status_code=409,
                detail=f"A newer export ({last_export}) was already imported",
            )
    else:
        # Extract username
        urls = [e.external_url for e in parse_result.events if e.external_url]
        username = ca_module.parse_username(file.filename or "", urls)

        connection = models.ServiceConnection(
            id=uuid.uuid4(),
            user_id=user_id,
            service_type=types_module.ServiceType.CONCERT_ARCHIVES,
            external_user_id=username,
        )
        db.add(connection)

    # Check for concurrent import
    running_stmt = sa.select(models.Task).where(
        models.Task.user_id == user_id,
        models.Task.service_connection_id == connection.id,
        models.Task.task_type == types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
        models.Task.status.in_([
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.RUNNING,
        ]),
    )
    running_result = await db.execute(running_stmt)
    if running_result.scalar_one_or_none() is not None:
        raise fastapi.HTTPException(
            status_code=409, detail="An import is already running"
        )

    # Update export date watermark
    connection.sync_watermark = {
        **connection.sync_watermark,
        "last_export_date": resolved_date.isoformat(),
    }

    # Create task
    task = models.Task(
        user_id=user_id,
        service_connection_id=connection.id,
        task_type=types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
        status=types_module.SyncStatus.PENDING,
    )
    db.add(task)
    await db.commit()

    # Enqueue arq job directly (bypasses _TASK_DISPATCH since we pass csv_content)
    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "sync_concert_archives",
        str(task.id),
        csv_text,
        _job_id=f"sync_concert_archives:{task.id}",
    )

    return {"status": "started", "task_id": str(task.id)}
```

**Step 4: Register the router**

In `src/resonance/api/v1/__init__.py`, add:

```python
import resonance.api.v1.concert_archives as concert_archives_module
router.include_router(concert_archives_module.router)
```

**Step 5: Run tests**

Run: `uv run pytest tests/test_concert_archives_api.py -v`
Expected: PASS

**Step 6: Run full test suite**

Run: `uv run pytest tests/ -x -q`
Expected: All pass

**Step 7: Commit**

```bash
git add src/resonance/api/v1/concert_archives.py src/resonance/api/v1/__init__.py tests/test_concert_archives_api.py
git commit -m "feat: add Concert Archives CSV upload endpoint"
```

---

### Task 7: UI — connection tile and upload form

**Files:**
- Create: `src/resonance/templates/partials/concert_archives_connect.html`
- Modify: `src/resonance/templates/account.html` (add tile + include)
- Create: `src/resonance/ui/partials/concert_archives.py` (HTMX partial routes)
- Modify: `src/resonance/app.py` or UI router (register partial routes)

**Step 1: Create the upload form partial**

Follow the Songkick pattern with states: button → form → success → error.

```html
{% if state == "button" %}
<button
    hx-get="/partials/concert-archives-upload"
    hx-target="#concert-archives-connect"
    hx-swap="innerHTML"
    class="outline"
>Connect Concert Archives</button>

{% elif state == "form" %}
<form
    hx-post="/api/v1/connections/concert-archives/upload"
    hx-target="#concert-archives-connect"
    hx-swap="innerHTML"
    hx-encoding="multipart/form-data"
>
    <label>
        CSV Export File
        <input type="file" name="file" accept=".csv" required>
    </label>
    <fieldset>
        <label>
            <input type="checkbox" id="ca-use-today" checked
                   onchange="document.getElementById('ca-export-date').disabled = this.checked">
            Use today's date
        </label>
        <input type="date" name="export_date" id="ca-export-date" disabled>
    </fieldset>
    <div class="grid">
        <button type="submit">Upload</button>
        <button
            hx-get="/partials/concert-archives-connect"
            hx-target="#concert-archives-connect"
            hx-swap="innerHTML"
            class="secondary outline"
            type="button"
        >Cancel</button>
    </div>
</form>

{% elif state == "success" %}
<p><ins>Import started — {{ event_count }} events queued for processing.</ins></p>

{% elif state == "error" %}
<p><mark>{{ error_message | default("Upload failed.") }}</mark></p>
<button
    hx-get="/partials/concert-archives-upload"
    hx-target="#concert-archives-connect"
    hx-swap="innerHTML"
    class="outline"
>Try Again</button>
{% endif %}
```

**Step 2: Add to account.html**

In the "Connect Another Service" section, add after the Songkick include:

```html
<div id="concert-archives-connect">
    {% include "partials/concert_archives_connect.html" %}
</div>
```

**Step 3: Create partial routes**

Create route handlers for the HTMX partials (button state, upload form state). Follow the pattern from the Songkick partials. The upload form partial serves the form state; the actual upload is handled by the API endpoint in Task 6.

**Step 4: Test manually in browser**

Run: `uv run uvicorn resonance.app:create_app --factory --reload`

1. Navigate to Account page
2. Verify "Connect Concert Archives" button appears
3. Click it — upload form should appear
4. Upload the test CSV from `data/`
5. Verify import starts and events appear in the events list

**Step 5: Commit**

```bash
git add src/resonance/templates/partials/concert_archives_connect.html \
        src/resonance/templates/account.html \
        src/resonance/ui/partials/concert_archives.py
git commit -m "feat: add Concert Archives UI — upload form on connections page"
```

---

### Task 8: CLI command

**Files:**
- Modify: `src/resonance/cli.py`

**Step 1: Add `feed-add concert_archives` support**

Extend the existing `feed-add` CLI command to support Concert Archives. When the user runs:

```bash
resonance-api feed-add concert_archives --file path/to/export.csv
```

It reads the file locally and POSTs to `/api/v1/connections/concert-archives/upload` as multipart form data.

Add `--export-date YYYY-MM-DD` flag for date override.
Add `--wait` flag to poll the task until complete and print the summary.

**Step 2: Test via CLI**

```bash
uv run resonance-api feed-add concert_archives \
    --file "data/mike.dougherty - Concert Archives Export - 05-19-2026.csv" \
    --wait
```

Expected: Import starts, waits for completion, prints summary.

**Step 3: Commit**

```bash
git add src/resonance/cli.py
git commit -m "feat: add Concert Archives CSV import to CLI"
```

---

### Task 9: Security review

**Files:** All files created/modified in Tasks 1-8

**Step 1: Review the upload endpoint for security issues**

Checklist:
- [ ] File size enforced before full read (413 on oversize)
- [ ] CSV headers validated before processing
- [ ] Auth required on endpoint
- [ ] Concurrent import prevented
- [ ] No path traversal possible (content is a string, no file paths)
- [ ] CSV parsed via stdlib `csv` module (no eval, no formula execution)
- [ ] Band names rendered in templates are auto-escaped by Jinja2
- [ ] URL field stored as-is (not fetched, no SSRF)
- [ ] UTF-8 decode errors handled gracefully
- [ ] No arbitrary file writes
- [ ] Export date validated (no injection via date string)

**Step 2: Test edge cases**

- Upload a non-CSV file (e.g., a binary)
- Upload a CSV with extremely long field values
- Upload with malicious content in band names (`<script>alert(1)</script>`)
- Upload while not authenticated
- Upload with invalid UTF-8 bytes
- Concurrent upload from two browser tabs

**Step 3: Fix any issues found**

**Step 4: Commit fixes**

```bash
git commit -m "fix: address security review findings for CSV upload"
```

---

### Task 10: Final integration testing and cleanup

**Step 1: Run full test suite**

Run: `uv run pytest tests/ -v`
Expected: All pass

**Step 2: Run linting and type checking**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: Clean

**Step 3: Test end-to-end with real data**

1. Start the app locally with a database
2. Upload the real Concert Archives CSV via the UI
3. Verify events appear in the events list
4. Check artist candidates are created and auto-matched
5. Verify venues are created with correct city/state/country
6. Re-upload the same file — verify idempotent (no duplicates)
7. Disconnect Concert Archives and verify data persists
8. Reconnect and re-upload — verify connection recreated

**Step 4: Create final commit**

```bash
git commit -m "test: add integration tests for Concert Archives import"
```
