# Playlist UI Design

## Overview

Add visual playlist viewing to the Resonance website — a list page and detail page following existing UI patterns (Pico CSS, HTMX, Jinja2 partials).

## List Page

- **Route**: `GET /playlists`
- **Nav**: "Playlists" link added after "Dashboard" in base navigation
- **Table columns**: Name (links to detail), Tracks (count), Source (generator type badge), Created (localized timestamp)
- **Query**: Playlists ordered by `created_at DESC`, paginated at 50. Joins `GenerationRecord` → `GeneratorProfile` for generator type.
- **HTMX**: Same pagination pattern as artists/tracks — `hx-get` targeting `#playlist-list`

## Detail Page

- **Route**: `GET /playlists/{playlist_id}`
- **Header**: Playlist name, description (if present), track count, creation date
- **Generation metadata** (prominent, inline): Profile name, generator type, freshness actual vs target, sources breakdown. Omitted if no GenerationRecord exists.
- **Track table** (paginated): Position, title, artist, score, source. HTMX pagination via partial swap.
- **Navigation**: "Back to Playlists" link at top

## Files

**Create:**
- `templates/playlist.html` — list page
- `templates/playlist_detail.html` — detail page
- `templates/partials/playlist_list.html` — list table partial
- `templates/partials/playlist_detail_tracks.html` — track table partial

**Modify:**
- `templates/base.html` — add nav link
- `ui/routes.py` — add route handlers

## Decisions

- No links from track/artist names to their list pages (no detail pages or filtering exist yet)
- Generation metadata prominent by default (may collapse later once workflow is familiar)
- Track table paginated (playlist length is uncapped)
- No new models, migrations, or API changes needed
