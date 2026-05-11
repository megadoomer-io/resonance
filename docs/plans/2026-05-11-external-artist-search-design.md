# External Artist Search & Import

## Summary

Enhance artist search across the application to search external services (MusicBrainz, Spotify, etc.) and import artists into the local database. Provides two tiers of search: inline auto-search when local results are empty, and a full modal for deliberate search with URL paste and multi-service support.

## Motivation

Concert event lineups reference artists that may not exist in the local database yet. Currently the only way to add them is through sync pipelines. Users need a way to search external catalogs and import artists on demand, with enough metadata to disambiguate (e.g., "Genesis" the UK prog band vs. the US hip-hop producer).

## UX Flow

### Tier 1 — Inline (existing dropdown, enhanced)

The existing artist search partial is enhanced with external search fallback:

1. User types in artist search input (works in any context — event page, artists page, etc.)
2. After 300ms debounce, local ILIKE search fires (existing behavior)
3. Local results render with **rich detail**: name, disambiguation, type, area, years
4. If **zero local results** and **3+ characters typed**:
   - Show spinner: "Searching externally..."
   - Start 2-second idle timer
   - After 2 seconds of no further input, fire `GET /api/v1/artists/search-external?q=<query>&services=musicbrainz`
   - External results appear below a divider, styled with external icon
   - Each result has an "Import" button
5. On Import: `POST /api/v1/artists/import` creates the artist locally, result row updates to a local artist row with standard actions (e.g., add to event)
6. "Search external..." link always present at bottom → opens modal

### Tier 2 — Modal (deliberate path)

Full-featured search dialog for power-user needs:

1. Opens with search input pre-filled from dropdown text
2. Explicit search button + Enter key (no auto-fire on keystroke)
3. Service checkboxes: MusicBrainz always available and checked by default; other services (Spotify, etc.) shown only if user has a connected account
4. URL detection: if input matches a service URL pattern, skip search and fetch specific artist with confirmation step
5. Rich results table: name, disambiguation, type, area, years, already-imported badge
6. Import button per result → POST, modal closes, parent context refreshed

### Result Row Component (shared)

Both local and external results use the same row layout:

- **Name** (bold) + **disambiguation** (lighter text)
- **Type** · **Area** · **Years** (e.g., "Group · United Kingdom · 1996–present")
- Local results: service badges
- External results: source badge + Import button
- Already-imported external results: shown as local (no import button)

### Lazy Enrichment

Existing artists with an MBID but missing metadata get enriched on first view:

- Template renders artist row immediately with available data
- `hx-get="/partials/artist-enrich/<artist_id>"` fires on load with `hx-trigger="load"`
- Endpoint checks: has MBID + null disambiguation + no recent enrichment request (stale after 3 minutes)
- If eligible: sets `service_links["musicbrainz"]["enrichment_requested_at"]`, fetches from MusicBrainz, updates DB, returns enriched row HTML
- If already enriched or no MBID: returns same row unchanged (no API call)

## Data Model Changes

### New columns on `Artist`

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `disambiguation` | VARCHAR(512) | Yes | MusicBrainz disambiguation string |
| `artist_type` | VARCHAR(64) | Yes | "Group", "Person", "Orchestra", "Choir", "Character", "Other" |
| `area` | VARCHAR(256) | Yes | Country/region from MusicBrainz |
| `begin_year` | INTEGER | Yes | Formation/birth year |
| `end_year` | INTEGER | Yes | Dissolution/death year (null if active) |

Existing `origin` field is kept as free-text, separate from the structured `area` field.

### service_links restructure

MBID storage migrated from flat `service_links["listenbrainz"]` (string) to nested `service_links["musicbrainz"]["id"]`. All services follow the same nested pattern:

```json
{
  "musicbrainz": {
    "id": "cc197bad-dc9c-440d-a5b5-d52ba2e14234",
    "enrichment_requested_at": "2026-05-11T17:30:00Z"
  },
  "spotify": {
    "id": "4gzpq5DPGxSnKTe4SA8HAU"
  },
  "listenbrainz": {}
}
```

## API Endpoints

### `GET /api/v1/artists/search-external`

Query params:
- `q` — artist name search (mutually exclusive with `url`)
- `url` — service URL to parse and look up specific artist
- `services` — comma-separated list of services to search (default: `musicbrainz`)

Response:
```json
[
  {
    "mbid": "cc197bad-dc9c-440d-a5b5-d52ba2e14234",
    "name": "Coldplay",
    "disambiguation": "British rock band",
    "artist_type": "Group",
    "area": "United Kingdom",
    "begin_year": 1996,
    "end_year": null,
    "source": "musicbrainz",
    "already_imported": false,
    "local_artist_id": null
  }
]
```

When `already_imported` is true, `local_artist_id` contains the existing UUID.

### `POST /api/v1/artists/import`

Body:
```json
{
  "mbid": "cc197bad-dc9c-440d-a5b5-d52ba2e14234",
  "name": "Coldplay",
  "disambiguation": "British rock band",
  "artist_type": "Group",
  "area": "United Kingdom",
  "begin_year": 1996,
  "end_year": null,
  "service_ids": {
    "spotify": "4gzpq5DPGxSnKTe4SA8HAU"
  }
}
```

Returns the created or existing `Artist`. If an artist with the given MBID already exists, returns it without creating a duplicate.

## Backend Implementation

### URL parsing — per-connector

Each connector implements a class method `parse_url(url: str) -> str | None` that returns the extracted identifier if the URL matches the service's patterns, or `None`. The `search-external` endpoint iterates registered connectors to find the matching service.

Supported patterns:
- `musicbrainz.org/artist/<uuid>` → MBID
- `listenbrainz.org/artist/<uuid>` → MBID (same identifier)
- `open.spotify.com/artist/<id>` → Spotify ID
- `last.fm/music/<name>` → URL-decoded artist name

### MusicBrainz search

Extract artist search from the existing ListenBrainz connector into a reusable function. Queries `musicbrainz.org/ws/2/artist/?query=<name>&fmt=json`. Public API, no auth required. Respects 1 req/sec rate limit via existing pacing/budget system.

### Spotify search

Add `artist_search(query: str)` method to Spotify connector. Calls `/v1/search?type=artist&q=<query>`. Requires user's OAuth token. User-initiated searches use `high_priority=True` to stay responsive even during background syncs.

### Import logic

1. Check if MBID already exists in any artist's `service_links["musicbrainz"]["id"]` → return existing
2. Create `Artist` with name, metadata fields, and `service_links["musicbrainz"]["id"]`
3. If additional service IDs provided (e.g., Spotify), populate `service_links["spotify"]["id"]`

## Migration Plan

### Migration 1 — Schema: Add artist metadata columns

Add `disambiguation`, `artist_type`, `area`, `begin_year`, `end_year` to `artists` table. All nullable, no defaults.

### Migration 2 — Data: Restructure MBID in service_links

For each artist where `service_links["listenbrainz"]` is a string MBID:
- Set `service_links["musicbrainz"] = {"id": <mbid>}`
- Leave `service_links["listenbrainz"]` unchanged

Deploy with the code change that reads from the new location. Application code reads from `service_links["musicbrainz"]["id"]` as primary source with fallback to `service_links["listenbrainz"]` during transition.

## Out of Scope

- Genre tags (separate design session planned)
- Bulk artist import
- Automatic lineup matching from external search (existing entity matching handles this)
- Backfill enrichment for all existing artists (lazy enrichment covers this organically)
