# Concert Archives CSV Import — Design

**Date:** 2026-05-19
**Status:** Approved

## Summary

Add Concert Archives as a data source via CSV file upload. Concert Archives
(concertarchives.org) is a community-driven platform for documenting live music
history. They offer a data export feature that produces a CSV of a user's
concert attendance history. Resonance imports this CSV to populate events,
venues, artist candidates, and attendance records.

## CSV Format

Concert Archives exports contain these columns:

| Column | Example | Notes |
|--------|---------|-------|
| Start Date | 09/26/2026 | MM/DD/YYYY format |
| End Date | 10/09/2022 | Empty for single-day events |
| Status | Past, Upcoming, Cancelled | |
| Concert Name | Ozzfest 2001 | May be empty for single-headliner shows |
| Bands Seen | Slipknot / KISS / Danzig | Slash-separated (` / `) |
| Bands Not Seen | Muse / Evanescence | Same format; artists user didn't watch |
| Venue | The Regency Ballroom | |
| Location | San Francisco, California, United States | Comma-separated |
| URL | https://www.concertarchives.org/... | Per-user permalink |

## Data Model Changes

### New service type

`CONCERT_ARCHIVES` added to the `ServiceType` enum.

### New auth type

`file_upload` added to the connection config auth types. The "connect" flow
opens a file upload dialog instead of OAuth or username input.

### Connection fields

- `external_user_id`: Concert Archives username, parsed from the CSV filename
  (e.g., `mike.dougherty` from `mike.dougherty - Concert Archives Export - ...`)
  or extracted from URL paths as fallback
- `service_links.last_export_date`: Tracks the most recent export date to reject
  stale uploads

## CSV Parsing

### Module

`src/resonance/concerts/concert_archives.py`

### Artist parsing

- Concatenate `Bands Seen` and `Bands Not Seen` into a single artist list
- Split on ` / ` (space-slash-space) — this delimiter does not conflict with
  band names containing `/` or `w/`
- All artists get confidence 90 (user-curated data, matching Songkick
  "unambiguous" level) which triggers auto-matching against the artist catalog
- Positions assigned sequentially starting at 0 (headliner first, matching
  Songkick convention)
- No distinction between "seen" and "not seen" — both are equally relevant for
  playlist generation. Concert Archives is the source of truth for attendance
  tracking; Resonance doesn't duplicate that concern

### Venue and location parsing

- `Venue` column → venue name
- `Location` column → parse comma-separated `City, State, Country`
- US locations have a state code (2 uppercase chars); international locations
  have city + country only

### Event date handling

- `Start Date` parsed as MM/DD/YYYY → `event_date`
- `End Date` stored as metadata for multi-day events (festivals)
- Missing start date: use sentinel `1970-01-01`, flag in import warnings. Do not
  skip the row — best-effort import, never silently drop data.

### Concert name

- Use `Concert Name` as event title when present
- When empty (single-headliner shows), synthesize from headliner + venue

### Status mapping

- **Past / Upcoming**: Create `UserEventAttendance` with status `GOING`
- **Cancelled**: Import event, venue, and candidates but create no attendance
  record. The event data is preserved for historical reference.

### Export date detection

`parse_export_date(filename: str) -> date | None`

- Regex for `MM-DD-YYYY` pattern in the filename
- Returns `None` if no date detected

## Event Matching

Concert Archives URLs are not reliable stable identifiers — some have UUIDs,
some are title-based slugs that could change if the event is edited. Events can
also be updated after creation (titles added, artists corrected).

### Composite matching key

`(source_service="concert_archives", event_date, venue_name_normalized, city_normalized)`

- Normalization: lowercase, strip whitespace
- On match: update title, artists, URL
- No match: create new event

### Full re-import model

Every upload is a complete snapshot of the user's Concert Archives history. The
import iterates over the entire file and upserts everything — creating new
events, updating changed ones. Users may add historical concerts at any time and
re-export.

### Stale export rejection

Per-import check: if the connection's `last_export_date` is newer than the
uploaded file's export date, reject the entire upload with a 409. Prevents an
older export from reverting changes from a newer one.

### Cross-source dedup

Matching Concert Archives events against Songkick events is explicitly out of
scope. This is a separate feature that benefits from a proper event dedup system
(similar to existing artist/track dedup). See follow-up issues.

## Upload Flow

### API endpoint

`POST /api/v1/connections/concert-archives/upload`

- Accepts `multipart/form-data`: CSV file + `export_date` field
- Requires authenticated session
- If no Concert Archives connection exists, creates one
- If connection exists and export date is stale, rejects with 409
- Creates a `SyncTask` and enqueues the import job to arq
- Returns task ID for progress polling

### Background task

`sync_concert_archives_csv` arq task:

1. Parse CSV content → `list[ParsedEvent]`
2. For each row: upsert venue, upsert event (composite match), upsert
   candidates, upsert attendance
3. Run `match_candidates_to_artists` per event
4. Update connection's `last_export_date`
5. Complete task with summary stats

CSV content is passed as a string to the arq task (serialized to Redis). The 5MB
file size limit bounds the worst case. If larger imports are needed in the
future, consider object storage.

### Import summary

Returned in task result, displayed in UI:

- Events created: N
- Events updated: N
- Artists matched: N / M candidates
- Warnings: missing dates, unparseable rows, etc.

## UI — Connections Page

### Connect flow

1. Concert Archives appears as a service tile on the connections page
2. Click "Connect" → panel opens with:
   - File upload input (`.csv` only)
   - Export date (auto-detected from filename, editable)
   - "Use today's date" checkbox (auto-checked and locked if no date detected)
   - Upload button
3. On success: connection created, sync task starts, dashboard shows progress

### Re-sync flow

After connecting, Concert Archives appears on the dashboard with last import
date and event count. The "Sync" button opens the same upload panel. Same action
whether first import or tenth.

### Disconnect

Matches current Spotify/Songkick behavior:

- Hard-deletes the `ServiceConnection` record
- Imported events, venues, candidates, and attendance persist (no FK cascade)
- Re-connecting and uploading creates a new connection and upserts normally

## CLI Access

`resonance-api sync concert_archives --file path/to/export.csv`

- POSTs to the same upload endpoint
- Parses export date from filename
- `--export-date YYYY-MM-DD` flag to override
- `--wait` to poll until complete and print summary

## Security Mitigations

File upload endpoints are high-risk surface area:

- **File size limit:** 5MB max, enforced before reading the full body (413 on
  oversize)
- **Content validation:** First line must contain expected CSV column headers
- **Auth required:** Authenticated session required — no anonymous uploads
- **Concurrent import limit:** One Concert Archives sync task per user at a time
- **No file execution:** CSV parsed via Python's `csv` module as plain text
- **Post-implementation security review** before merging — specifically checking
  for path traversal, memory exhaustion, CSV injection into downstream HTML
  (XSS via band names in Jinja2 templates), and SSRF from URL fields

No row count or field length limits — the 5MB file size cap provides sufficient
bounding. Legitimate data includes bands with extremely long names and festivals
with 100+ artists per day.

## Out of Scope (Follow-up Issues)

1. **Cross-source event dedup** — matching Concert Archives events to Songkick
   events by date/venue/artists
2. **Source-aware data visibility** — hiding data from disconnected services
3. **Task isolation** — preventing long imports from blocking other sync jobs
4. **Long-value UI truncation** — truncate+expand for long values in list views
