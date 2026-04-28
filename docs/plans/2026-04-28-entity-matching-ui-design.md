# Entity Matching UI Design

## Overview

Add entity detail pages (artists, tracks, events), side-by-side comparison/merge views, and artist candidate matching — enabling users to review, correct, and manage entity relationships that affect playlist generation quality.

## Entity Detail Pages

Three new detail pages following the playlist detail page pattern.

### Artist Detail (`GET /artists/{artist_id}`)
- Header: Name, origin (new field), service links as external links
- Library section: Tracks by this artist (paginated table)
- Events section: Concerts linked via EventArtist
- Candidates section: Pending EventArtistCandidates matched to this artist, with accept/reject buttons
- Potential duplicates section: Artists with similar names (case-insensitive), each linking to comparison view

### Track Detail (`GET /tracks/{track_id}`)
- Header: Title, artist name (links to artist detail), duration, service links
- Listening history: Recent listens for this track (paginated)
- Potential duplicates: Tracks with same title + same artist, linking to comparison view

### Event Detail (`GET /events/{event_id}`)
- Header: Title, date, venue, external link
- Confirmed artists: EventArtist entries (link to artist detail)
- Pending candidates: EventArtistCandidates with raw name, matched artist (if any), confidence score, accept/reject buttons
- Add artist: Search input to find local artists and create a new candidate

All list pages (artists, tracks, events) get clickable names linking to detail pages.

## Comparison & Merge Flow

### Comparison Page (`GET /artists/{id}/compare/{other_id}`, `/tracks/{id}/compare/{other_id}`)

Side-by-side layout showing both entities. For artists: name, origin, service links, track count, event count, created date.

The system pre-selects a canonical using the existing `_pick_canonical` logic from dedup.py (MBID holder wins, then more service links, then oldest). Labeled "Keep" vs "Merge into". A "Swap" link lets the user override.

### Preview Step

Below the comparison, a "Preview Merge" button loads (via HTMX) a summary of what will change: tracks repointed, events repointed, listening events affected, service links merged.

### Confirm

After preview, a "Confirm Merge" button executes the merge using existing `merge_artists()` / `merge_tracks()` functions. Redirects to the surviving entity's detail page.

## Candidate Accept/Reject Flow

### On Event Detail Pages
Each pending candidate shows: raw name, matched artist link (or "No match"), confidence score, accept/reject buttons.
- Accept: Creates EventArtist, sets candidate status to ACCEPTED
- Reject: Sets candidate status to REJECTED

### On Artist Detail Pages
Pending candidates where `matched_artist_id` points to this artist show in a "Pending matches" section. Same accept/reject actions.

### Add Artist to Event
Search input on event detail page — type a name, HTMX search shows matching local artists. Clicking a result creates an EventArtistCandidate with `status=PENDING` and `matched_artist_id` set. User reviews the candidate normally (accept if correct, reject and try again if wrong).

## Data Changes

- Add `origin` field (nullable String) to Artist model
- New Alembic migration for the field

## New API Endpoints

- `GET /api/v1/artists/search?q=...` — search artists by name
- `POST /api/v1/events/{event_id}/candidates/{candidate_id}/accept`
- `POST /api/v1/events/{event_id}/candidates/{candidate_id}/reject`
- `POST /api/v1/events/{event_id}/candidates` — create candidate from local artist search
- `POST /api/v1/artists/{id}/merge/{other_id}` — preview merge
- `POST /api/v1/artists/{id}/merge/{other_id}/confirm` — execute merge
- `POST /api/v1/tracks/{id}/merge/{other_id}/confirm` — execute merge

## New Templates

- `artist_detail.html`, `track_detail.html`, `event_detail.html`
- `artist_compare.html`, `track_compare.html`
- Partials for each section (track list, candidate list, merge preview, search results)

## Modified Files

- List page partials — make names clickable links to detail pages
- `ui/routes.py` — new route handlers
- `models/music.py` — add `origin` field

## Not in V1

- External service search for new artists (Spotify, MusicBrainz, Last.fm lookups)
- Bulk artist add for festivals
- Listening event dedup UI (automated system handles this)

## Decisions

- `origin` stored on Artist model (lightweight, avoids on-demand API calls); on-demand external metadata fetch considered for future
- Merge requires preview step before confirmation (shows exactly what changes)
- Manual artist add to events creates candidates (not direct EventArtist), preserving single review gate
- Local artist search only in v1; external service discovery as follow-up
