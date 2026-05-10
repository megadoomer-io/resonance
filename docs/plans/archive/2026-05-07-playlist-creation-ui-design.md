# Playlist Creation UI Design

## Context

The playlist generation pipeline (profiles, scoring, track selection) is fully built and accessible via CLI and API, but has no web UI. Users must use the CLI to create generator profiles and trigger generation. This design adds a web UI for the complete playlist creation flow.

## User Flows

### Flow A — From Playlists page

1. User clicks "New Playlist" on `/playlists`
2. `/playlists/new` renders a form: type selector, event picker, parameter sliders, name
3. Submit creates a profile (hidden from user) + triggers generation
4. Redirects to `/playlists/generating/{task_id}` — polls task status
5. On completion, auto-redirects to `/playlists/{playlist_id}`

### Flow B — From Event detail page

1. User clicks "Generate Playlist" (with lightning bolt) on `/events/{event_id}`
2. Navigates to `/playlists/new?event_id={id}&type=concert_prep` — same form with type and event pre-selected (not locked)
3. Same steps 3-5 as Flow A

## Form Components

**Type selector** — radio buttons reading from `GENERATOR_TYPE_CONFIG`. Selecting a type shows/hides relevant input fields. New types appear automatically.

**Event picker** (for concert_prep) — dropdown of upcoming events with confirmed artists, sorted by date. Format: "Artist @ Venue — Date". Pre-selected via query param.

**Parameter sliders** — HTML range inputs per featured parameter. Labels from `ParameterDefinition` metadata (display_name, labels, default_value). Also max_tracks (default 30) and optional freshness_target.

**Name** — auto-generated from type + event (e.g., "Concert Prep: Crobot @ The Fillmore"), editable. Profile gets the same name silently.

## Generating Page

After form submit, redirects to `/playlists/generating/{task_id}`:
- Shows playlist name and progress indicator
- HTMX polls `GET /admin/tasks/{task_id}` every 3 seconds
- On completion, auto-redirects to `/playlists/{playlist_id}`
- On failure, shows error with "Try Again" link

## Profile Cleanup

Profiles are hidden implementation scaffolding — users think in "playlists."

**Post-delete hook**: when a playlist is deleted, check if its profile has remaining playlists. If not, delete the profile in the same transaction.

**Periodic bulk job**: safety-net sweep for orphan profiles (no associated playlists). Runs as a `BULK_JOB` operation.

## New Files

| File | Purpose |
|------|---------|
| `templates/playlists_new.html` | New Playlist form |
| `templates/playlists_generating.html` | Task status with auto-redirect |

## Modified Files

| File | Change |
|------|--------|
| `ui/routes.py` | Add `GET/POST /playlists/new`, `GET /playlists/generating/{task_id}` |
| `templates/partials/playlist_list.html` | Add "New Playlist" button |
| `templates/event_detail.html` | Add "Generate Playlist" button |
| `api/v1/playlists.py` | Post-delete profile cleanup |

## Future Work

- Scheduled periodic syncs per user (no manual sync required)
- Periodic orphan profile cleanup bulk job
- Additional generator types beyond concert_prep
