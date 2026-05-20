# Candidate-Based Entity Resolution (#82, #73)

## Context

Resonance imports concert data from multiple sources (Songkick, Concert Archives). The same venue, event, or artist can appear from different sources with different formatting. The initial approach (destructive merge) deleted duplicate records permanently — no audit trail, no undo, no way to fix incorrect merges like the Exuvia case (two distinct artists with similar names merged into one).

This design replaces destructive dedup with a candidate-based resolution system: each source produces candidate records, resolution links them to a canonical entity, and raw source data is never destroyed. Unlinking is the undo mechanism. This extends the existing EventArtistCandidate pattern to venues and events.

## Data Model

### New Tables

**`venue_candidates`** — raw venue data from a source, never deleted

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | PK |
| source_service | ServiceType | |
| external_id | String(512) | |
| name | String(512) | as-provided by source |
| city | String(256) | nullable |
| state | String(256) | nullable |
| country | String(256) | nullable |
| address | String(512) | nullable |
| postal_code | String(32) | nullable |
| resolved_venue_id | FK → venues | nullable |
| status | CandidateStatus | PENDING / AUTO_ACCEPTED / ACCEPTED / REJECTED |
| confidence_score | Integer | 0–100 |
| created_at, updated_at | Timestamp | via TimestampMixin |

Unique constraint: `(source_service, external_id)`

**`event_candidates`** — raw event data from a source, never deleted

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | PK |
| source_service | ServiceType | |
| external_id | String(512) | |
| external_url | String(1024) | nullable |
| title | String(1024) | as-provided |
| event_date | Date | |
| venue_candidate_id | FK → venue_candidates | nullable |
| attendance_status | String | nullable, raw status from source |
| resolved_event_id | FK → events | nullable |
| status | CandidateStatus | PENDING / AUTO_ACCEPTED / ACCEPTED / REJECTED |
| confidence_score | Integer | 0–100 |
| created_at, updated_at | Timestamp | via TimestampMixin |

Unique constraint: `(source_service, external_id)`

**`entity_exclusions`** — records that two entities are known to be distinct

| Column | Type | Notes |
|--------|------|-------|
| id | UUID | PK |
| entity_type | String | "artist" / "venue" / "event" |
| entity_a_id | UUID | smaller UUID (canonical ordering) |
| entity_b_id | UUID | larger UUID |
| created_at | Timestamp | |

Unique constraint: `(entity_type, entity_a_id, entity_b_id)`

### CandidateStatus Enum Update

Extend the existing `CandidateStatus` enum (currently PENDING / ACCEPTED / REJECTED) with AUTO_ACCEPTED. This distinguishes human-verified from auto-matched resolutions.

Migration: `ALTER TYPE candidatestatus ADD VALUE 'AUTO_ACCEPTED'` — since native_enum=False (varchar), this is just a CHECK constraint update.

### Existing Tables — Changes

- **Venue**: add `candidates` relationship (back_populates). No column changes.
- **Event**: add `candidates` relationship (back_populates). No column changes. Keep existing `source_service` and `external_id` columns for backward compatibility during migration; these become redundant once all access goes through candidates.
- **EventArtistCandidate**: gains exclusion-awareness during auto-matching. No schema change — the exclusion check is query-side logic.

## Resolution States

```
PENDING ──── auto-match finds entity ────► AUTO_ACCEPTED
  │                                            │
  │                                            │ human confirms
  │                                            ▼
  │──── human picks entity ──────────────► ACCEPTED
  │
  │──── human says "not this entity" ───► REJECTED
  │         (resolved_id stays populated     │
  │          to remember what was rejected)   │
  │                                           │
  ◄───────── returns to PENDING ──────────────┘
             (can match to different entity)
```

REJECTED is entity-specific. Rejecting candidate X from entity Y means "X is not Y." The candidate returns to PENDING and can be matched to entity Z.

## Import Flow

Today: parser → `upsert_venue()` → `upsert_event()` → `upsert_candidates()` → `match_candidates_to_artists()`

New flow:

1. **Create/update VenueCandidate** — upsert on `(source_service, external_id)`. Raw source data stored as-is.
2. **Auto-resolve VenueCandidate** — normalize fields via `normalize_name()`, query existing Venues for matches, check EntityExclusion. High confidence → AUTO_ACCEPTED + link. No match → create new Venue from candidate data, AUTO_ACCEPT.
3. **Create/update EventCandidate** — upsert on `(source_service, external_id)`. Links to VenueCandidate.
4. **Auto-resolve EventCandidate** — match by `(event_date, resolved_venue_id)` across sources, check EntityExclusion. High confidence → AUTO_ACCEPTED + link. No match → create new Event, AUTO_ACCEPT.
5. **Create EventArtistCandidates** — same as today, against the resolved Event.
6. **Auto-match artists** — same as today, with `normalize_name()` and exclusion checks.

### Key functions (in `src/resonance/concerts/sync.py`):

- `upsert_venue_candidate(session, venue_data, source_service) -> VenueCandidate` — replaces `upsert_venue()`
- `resolve_venue_candidate(session, candidate) -> Venue` — auto-match or create
- `upsert_event_candidate(session, parsed, source_service, venue_candidate) -> EventCandidate` — replaces `upsert_event()`
- `resolve_event_candidate(session, candidate) -> Event` — auto-match or create
- `upsert_attendance()` — unchanged, operates on resolved Event
- `upsert_candidates()` — unchanged, operates on resolved Event
- `match_candidates_to_artists()` — add exclusion check

## Splitting and Unlinking

**Unlink a candidate**: set `resolved_entity_id` to null, status to PENDING. Candidate returns to review queue. Entity unchanged.

**Split an entity** (the Exuvia case):
1. Select which candidates belong to a new entity
2. Create new entity from selected candidates' data
3. Re-point selected candidates to new entity
4. Create EntityExclusion between original and new entity
5. Original entity keeps remaining candidates, unchanged

**Undo a merge**: a merge is "two candidates resolved to the same entity." Undo = split.

**Undo a split**: re-resolve the candidate to the original entity, remove the EntityExclusion. If the wrongly-created entity is now orphaned, auto-delete it (see below).

## Orphan Cleanup

After any unlink/split operation, check if the affected entity is orphaned:

- **Truly orphaned** (no candidates AND no connections): auto-delete.
  - Artist: no Tracks, no EventArtists, no UserArtistRelations
  - Venue: no Events
  - Event: no EventArtists, no EventArtistCandidates, no UserEventAttendance
- **Source-orphaned but data-connected**: surface in admin UI for review. Do not auto-delete.

## Destructive Dedup Removal

The destructive merge functions added in the previous commit (`merge_venues`, `merge_events`, `find_and_merge_duplicate_venues`, `find_and_merge_duplicate_concerts`) will be replaced by candidate-aware resolution. The admin UI buttons will trigger "suggest merges" (find potential duplicates and present them for review) rather than "merge now."

The existing `find_and_merge_duplicate_artists` and `find_and_merge_duplicate_tracks` remain for now but are candidates for the same treatment in a future pass.

## Migration Strategy

### Phase 1: Schema + Backfill

**Migration 1**: Create `venue_candidates`, `event_candidates`, `entity_exclusions` tables. Add AUTO_ACCEPTED to CandidateStatus CHECK constraint.

**Migration 2**: Backfill candidates from existing data:
- For each existing Venue → create a VenueCandidate with the venue's data, status=AUTO_ACCEPTED, resolved_venue_id=venue.id. Use the venue's first event's source_service for the candidate's source_service. Generate external_id from `generate_external_id()` or use the venue's id as fallback.
- For each existing Event → create an EventCandidate with the event's data, status=AUTO_ACCEPTED, resolved_event_id=event.id, source_service and external_id from the event itself, venue_candidate_id from the venue's backfilled candidate.

After this phase: system works exactly as before. Every entity has at least one candidate as provenance.

### Phase 2: Import Path

Update `src/resonance/concerts/sync.py`:
- Replace `upsert_venue()` with `upsert_venue_candidate()` + `resolve_venue_candidate()`
- Replace `upsert_event()` with `upsert_event_candidate()` + `resolve_event_candidate()`
- Update `match_candidates_to_artists()` with exclusion checks

Update `src/resonance/concerts/worker.py`:
- Songkick worker calls new candidate functions
- Concert Archives worker calls new candidate functions

After this phase: new imports go through the candidate layer. Existing data unchanged.

### Phase 3: Admin UI

- Venue/event candidate review page (pending candidates needing resolution)
- Split entity workflow
- Unlink candidate action
- Orphaned entity view
- "Suggest merges" view (finds potential duplicates via normalized matching, presents for review)

### Phase 4: Remove Destructive Dedup

- Remove `merge_venues`, `merge_events`, `find_and_merge_duplicate_venues`, `find_and_merge_duplicate_concerts` from `dedup.py`
- Remove or replace admin dedup buttons with "suggest merges" buttons
- Remove destructive dedup worker dispatch

## Files Changed

| File | Change |
|------|--------|
| `src/resonance/models/concert.py` | Add VenueCandidate, EventCandidate, EntityExclusion models; add relationships to Venue, Event |
| `src/resonance/types.py` | Add AUTO_ACCEPTED to CandidateStatus |
| `src/resonance/concerts/sync.py` | Replace upsert_venue/upsert_event with candidate-based functions; add exclusion checks to artist matching |
| `src/resonance/concerts/worker.py` | Update both import workers to use candidate functions |
| `src/resonance/dedup.py` | Remove venue/event destructive merge functions (Phase 4) |
| `src/resonance/ui/routes.py` | Add candidate review, split, unlink endpoints; update dedup buttons |
| `src/resonance/templates/admin.html` | Add candidate review UI, split workflow, orphan view |
| `src/resonance/normalize.py` | Already done — used by auto-resolution |
| `alembic/versions/` | Migration for new tables + backfill |
| `tests/` | New test files for candidate resolution, splitting, exclusions |

## Verification

1. `uv run pytest` — all tests pass at each phase boundary
2. `uv run ruff check . && uv run ruff format --check .` — lint clean
3. `uv run mypy src/` — type check clean
4. After Phase 1: verify backfill — every Venue and Event has exactly one candidate, all AUTO_ACCEPTED
5. After Phase 2: import a Concert Archives CSV, verify VenueCandidates and EventCandidates are created and auto-resolved
6. After Phase 3: test split workflow with a known duplicate (or create a test case), verify EntityExclusion prevents re-merge
7. After Phase 4: verify dedup buttons show suggestions instead of executing merges
