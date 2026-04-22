# Concert Integration Design — iCal Feeds & Event Data Model

**Date**: 2026-04-20
**Status**: Accepted

## Goal

Import concert/live event data from iCal calendar feeds, model events with
venues and artist associations, and link concert artists to resonance's
existing Artist entities. This builds the data foundation for future concert-
driven playlist generation without implementing playlist features yet.

## Data Sources

### Songkick iCal Feeds

Songkick provides unauthenticated iCal feeds per user. No API key required —
just the username. Two feed types:

- **Attendance** (`filter=attendance`): Events the user marked "going" or
  "tracking." Attendance status is embedded in the DESCRIPTION field
  ("You're going." vs "You're tracking this event.").
- **Tracked artist** (`filter=tracked_artist`): All upcoming shows by artists
  the user follows on Songkick. No attendance status. Broader coverage.

URL pattern: `songkick.com/users/{username}/calendars.ics?filter={type}`

### Generic iCal

Any user-provided iCal URL. No attendance parsing, no artist name extraction
(raw SUMMARY only). Enables venue calendars, Bandsintown calendar exports, or
other sources.

### Deferred Sources

- **Setlist.fm API** — post-show setlist data for "relive the show" playlists
- **ConcertArchives.org** — concert attendance history
- **Foopee** — Bay Area concert listings (scraping only)
- **Bandsintown API** — artist-only API, not viable for consumer use

## Data Model

### Event

A live music event at a specific venue on a specific date.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID PK | |
| title | String | Raw SUMMARY from iCal |
| event_date | Date | From DTSTART |
| venue_id | FK → Venue | Nullable (some iCal events lack location) |
| source_service | ServiceType enum | SONGKICK, ICAL, etc. |
| external_id | String | Songkick UID, iCal UID |
| external_url | String (nullable) | Link to source page |
| service_links | JSON | Cross-service linking (same pattern as Artist/Track) |

- Unique constraint: `(source_service, external_id)`
- TimestampMixin

### Venue

A physical location where events happen.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID PK | |
| name | String | Venue name |
| address | String (nullable) | Street address |
| city | String (nullable) | |
| state | String (nullable) | |
| postal_code | String (nullable) | |
| country | String (nullable) | 2-letter code |
| service_links | JSON | |

- Unique constraint: `(name, city, state, country)`
- TimestampMixin

### EventArtistCandidate

Staged artist-to-event associations pending user review. Candidates are
extracted by parsing the event title but are NOT committed as real links until
accepted. This prevents data poisoning from parsing errors.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID PK | |
| event_id | FK → Event | |
| raw_name | String | Name as extracted from iCal SUMMARY |
| matched_artist_id | FK → Artist (nullable) | If a name match was found |
| position | Integer | 0=headliner, 1+=support (order from SUMMARY) |
| confidence_score | Integer (0-100) | Parser's confidence in the extraction |
| status | Enum | pending, accepted, rejected |

- Unique constraint: `(event_id, raw_name)`
- TimestampMixin
- `rejected` means hidden from default view, not permanently dismissed.
  A "show all" toggle reveals rejected candidates for re-acceptance.

### EventArtist

Confirmed artist-to-event links. Created when a candidate is accepted.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID PK | |
| event_id | FK → Event | |
| artist_id | FK → Artist | |
| position | Integer | Headliner vs support ordering |
| raw_name | String | Preserved from candidate for reference |

- Unique constraint: `(event_id, artist_id)`

### UserEventAttendance

Per-user attendance status. Songkick-specific for now — generic iCal feeds
do not populate this.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID PK | |
| user_id | FK → User | |
| event_id | FK → Event | |
| status | Enum | going, interested, none |
| source_service | ServiceType | Which service provided the status |

- Unique constraint: `(user_id, event_id)`
- TimestampMixin

### UserCalendarFeed

Configured iCal feed URLs per user.

| Column | Type | Notes |
|--------|------|-------|
| id | UUID PK | |
| user_id | FK → User | |
| feed_type | Enum | songkick_attendance, songkick_tracked_artist, ical_generic |
| url | String | Full iCal URL |
| label | String (nullable) | User-provided display name |
| last_synced_at | DateTime (nullable) | |
| enabled | Boolean | Default true |

- Unique constraint: `(user_id, url)`
- TimestampMixin

## iCal Feed Architecture

### Songkick Convenience Layer

User provides their Songkick username. Resonance generates two
`UserCalendarFeed` rows automatically:

- `filter=attendance` → `feed_type = songkick_attendance`
- `filter=tracked_artist` → `feed_type = songkick_tracked_artist`

The Songkick layer extends the generic iCal parser with:
- Attendance status parsing from DESCRIPTION field
- Artist name extraction from SUMMARY (Songkick uses a consistent format)

### Generic iCal

User provides any iCal URL directly. Stored with `feed_type = ical_generic`.
No attendance parsing. No artist name extraction — raw SUMMARY stored as
event title only. Users can manually link artists later.

### Sync Flow

1. Fetch iCal URL (HTTP GET, no auth)
2. Parse VCALENDAR → list of VEVENTs
3. For each VEVENT:
   a. Parse LOCATION → upsert Venue (deduplicate by name+city+state+country)
   b. Upsert Event (deduplicate by source_service+external_id)
   c. For Songkick feeds: extract artist name candidates from SUMMARY
   d. For Songkick feeds: parse attendance from DESCRIPTION → upsert
      UserEventAttendance
4. Run candidate artist matching (case-insensitive name match against
   existing Artists)
5. For unmatched candidates: create new Artist with service_links indicating
   source
6. Update `last_synced_at` on the feed

Sync runs as a worker task using the existing Task infrastructure.

## Artist Name Parsing (Songkick Only)

Songkick SUMMARY follows a consistent pattern:

```
Puscifer and Dave Hill at Golden Gate Theatre (11 May 26)
Lagwagon, Strung Out, and Swingin' Utters at The Fillmore (16 May 26) with Western Addiction
Sleepbomb at Bottom of the Hill (04 Jun 26) with Hazzard's Cure and Ominess
```

### Strategy

1. Split on ` at ` — left = artists, right = venue+date
2. Strip date suffix `(DD Mon YY)` from venue
3. Split artist string on `, ` and ` with ` — artists before `with` are
   headliners, after are support
4. Handle ` and ` within comma-separated lists (Oxford comma pattern)
5. Assign position: headliners 0, 1, 2..., support acts continue sequence

### Confidence Scoring

- **90**: Unambiguous parse — no conflicting delimiters, clean split
- **30**: Ambiguous — multiple ` at ` occurrences, ` and ` without commas,
  or other signals that the parse may be wrong

Thresholds will evolve as we encounter more edge cases.

### Known Limitations

- Artist names containing ` at ` (e.g., "Panic! at the Disco") will
  mis-split. Low confidence score flags these for review.
- Artist names containing `, ` or ` and ` may over-split.
- Generic iCal feeds skip parsing entirely — no assumptions about format.
- Fuzzy matching is out of scope. Corrections via #42 entity merge/split.

### Data Integrity Approach

Parsing errors must not corrupt the Artist table. The candidate staging model
ensures:

- Raw iCal data is preserved verbatim on the Event
- Parsed names are stored as candidates, not committed links
- Users review and accept/reject candidates before they become EventArtist
  records
- Rejected candidates are soft-hidden, not deleted — always recoverable
- New Artist entities created from candidates are tagged with their source
  service for easy identification

## ServiceType & ConnectorCapability

- `ServiceType.SONGKICK` — already exists in the enum
- `ServiceType.ICAL` — new, for generic calendar feeds
- `ConnectorCapability.EVENTS` — already exists, unused. Songkick connector
  declares this.

## What's Deferred

- Playlist generation from concert data
- Fuzzy/ML artist matching (improve confidence scoring)
- Setlist.fm, ConcertArchives.org, Foopee integrations
- Full #42 entity merge/split UI (this design enables it)
- Concert history / "shows I attended" views
- Venue dedup across sources
- Location-based discovery (requires paid APIs)

## Related

- [#42](https://github.com/megadoomer-io/resonance/issues/42) — User-managed
  entity merging. Concert artist corrections will use this system.
- [#6](https://github.com/megadoomer-io/resonance/issues/6) — Themes
  (unrelated but open)
