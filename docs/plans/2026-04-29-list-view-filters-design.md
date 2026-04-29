# List View Search & Filtering Design

## Overview

Add search, filtering, and preset filter sets to all five list views (events, artists, tracks, listening history, playlists). The goal is to make every list view a useful work surface for finding and exploring entities.

## UI Layout

Each list view gets a consistent three-layer filter bar:

1. **Preset buttons + quick search** (always visible) — preset pills on the left, search box on the right. Presets are one-click shortcuts that pre-populate field filters. Only one preset active at a time; clicking the active preset clears all filters.

2. **"Filters" toggle** (collapsed by default) — expands to reveal per-column filter inputs. Uses a `<details>` element for expand/collapse. When a preset activates column filters, the panel expands automatically.

3. **Column filters** (inside the expandable panel) — small inputs in the table header row for precise per-field filtering. Text inputs, multi-select checkbox dropdowns, and date range pickers depending on the field type.

### Multi-Select Dropdowns

Fields with enumerable values (attendance status, source service, etc.) use a `<details><summary>` element styled as a dropdown, with checkboxes inside. No JS library dependency. Selected values combine with OR within the field and AND across fields.

### Filter Composition

- Quick search: broad OR match across all entity fields (including related entities)
- Column filters: AND across fields, OR within multi-select fields
- Presets: populate column filters transparently (user can see and tweak what the preset set)
- All three layers compose — quick search AND column filters AND preset-populated values

## Per-View Specifications

### Events

**Presets:**
- **Upcoming** (default active on page load) — `event_date >= today`
- **Going** — events with user attendance status "going"
- **Needs Review** — events with at least one pending candidate

**Quick search** matches: event title, venue name, confirmed artist names, candidate raw names.

**Column filters:**
- Title (text)
- Date (date range — from/to)
- Venue (text)
- Artists (text — matches confirmed + candidates)
- Attendance (multi-select checkboxes — going, interested, none)
- Candidates (multi-select checkboxes — has pending, no pending)

**Default sort:** event date ascending when "Upcoming" is active, descending otherwise.

### Artists

**Presets:**
- **Has Events** — artists linked to at least one concert event
- **No Tracks** — artists matched to events but with no library tracks

**Quick search** matches: artist name, origin, service link keys/values.

**Column filters:**
- Name (text)
- Origin (text)
- Services (multi-select checkboxes — Spotify, ListenBrainz, Last.fm, MusicBrainz, etc.)
- Tracks (dropdown — has tracks / no tracks / all)
- Events (dropdown — has events / no events / all)

**Default sort:** alphabetical by name.

### Tracks

**Presets:**
- **Recently Played** — tracks with listening events in the last 30 days
- **No Service Links** — tracks with no external service associations

**Quick search** matches: track title, artist name, service link keys/values.

**Column filters:**
- Title (text)
- Artist (text)
- Services (multi-select checkboxes — same set as artists)
- Duration (range — min/max, or has/no duration)

**Default sort:** alphabetical by title.

### Listening History

**Presets:**
- **Spotify** — listens from Spotify only
- **ListenBrainz** — listens from ListenBrainz only
- **Last.fm** — listens from Last.fm only

**Quick search** matches: track title, artist name.

**Column filters:**
- Track (text)
- Artist (text)
- Source (multi-select checkboxes — Spotify, ListenBrainz, Last.fm)
- Date (date range — from/to)

**Default sort:** listened_at descending.

### Playlists

**Presets:** None.

**Quick search** matches: playlist name, generator profile name.

**Column filters:**
- Name (text)
- Created (date range — from/to)
- Track count (range — min/max)

**Default sort:** created_at descending.

## HTMX Interaction Pattern

### Filter Triggers

- Text inputs: `hx-trigger="input changed delay:300ms"` (debounced)
- Checkbox/dropdown changes: `hx-trigger="change"` (immediate)
- All filter changes include `page=1` to reset pagination
- Target: list container div (replaces table + pagination)

### Presets

- Clicking a preset populates filter inputs via inline script, then triggers the HTMX request
- If column filters are collapsed and a preset uses them, the panel expands
- Active preset gets a visual indicator (filled vs outline pill)
- Clicking the active preset clears all filters

### URL State

- Filter params pushed to URL via `hx-push-url="true"` for bookmarking and browser back
- Direct navigation with filter params pre-populates the inputs
- Server routes handle both fresh page loads and HTMX partial requests with the same param parsing

## Server-Side Filter Framework

A shared filter engine builds SQLAlchemy query clauses from a per-view field registry. Each view declares its filterable fields and their types; the engine handles query param parsing and clause construction.

### Filter Field Types

```python
class TextField:
    """ILIKE match with wildcard escaping."""
    name: str
    column: sa.Column
    join: relationship | None  # optional join for related entity fields

class MultiSelectField:
    """OR across selected values within the field."""
    name: str
    column: sa.Column
    options: list[str]

class DateRangeField:
    """From/to date filtering."""
    name: str
    column: sa.Column

class NumericRangeField:
    """Min/max numeric filtering."""
    name: str
    column: sa.Column

class ExistsField:
    """Boolean EXISTS subquery (has related records or not)."""
    name: str
    related_model: type
    condition: sa.ColumnElement
```

### Per-View Registration

```python
EVENT_FILTERS = [
    TextField("title", Event.title),
    TextField("venue", Venue.name, join=Event.venue),
    TextField("artist", Artist.name, join=...),
    DateRangeField("date", Event.event_date),
    MultiSelectField("attendance", UserEventAttendance.status, options=[...]),
    ExistsField("has_pending", EventArtistCandidate, condition=...),
]
```

### Query Building

```python
def apply_filters(
    query: sa.Select,
    filters: list[FilterField],
    params: QueryParams,
) -> sa.Select:
    """Apply filter clauses from query params to the base query."""
    ...
```

### Query Param Convention

- Text: `?title=foo&artist=bar` (ILIKE)
- Multi-select: `?service=spotify&service=listenbrainz` (repeated param)
- Date range: `?date_from=2026-05-01&date_to=2026-06-01`
- Numeric range: `?duration_min=60&duration_max=300`
- Boolean: `?has_tracks=true`
- Quick search: `?q=term` (OR across all text-searchable fields)

### Quick Search

The `q` parameter triggers a broad OR match across all `TextField` entries in the view's filter registry, plus any configured related-entity fields. This runs in addition to (AND with) any active column filters.

Related entity matching uses `EXISTS` subqueries to avoid cartesian product joins that would inflate result counts.
