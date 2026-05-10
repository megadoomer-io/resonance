# Playlist Export to Spotify — Design

## Context

Resonance generates playlists locally from aggregated music data, but they stay inside the app. Users can't listen to them on Spotify without manually recreating them. This design adds the ability to export resonance playlists to a user's connected Spotify account(s).

## Key Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Interaction model | Export with update | One-shot export that remembers the link. Re-export updates the same Spotify playlist instead of creating duplicates. Sync is always user-initiated. |
| Unmatched tracks | Search Spotify at export time | Tracks from ListenBrainz/MusicBrainz may lack Spotify IDs. Search by `track:Title artist:Artist` and persist matches to `service_links` for future reuse. |
| Playlist visibility | Always private | User can change visibility in Spotify. Avoids exposing listening habits by default. |
| Link storage | `service_links` on Playlist model | Matches the Track/Artist pattern. Per-connection keying supports multiple Spotify accounts. |
| OAuth scope upgrade | Force re-authorization | Single active user. Add `playlist-modify-private` scope and invalidate existing tokens. Future users authorize with correct scopes automatically. |
| Export feedback | Status page with task polling | Consistent with playlist generation flow. Background worker task with HTMX polling status page. |
| Multiple accounts | One task per connection | Independent success/failure. Clean progress tracking per account. |
| Reverse sync (Spotify → Resonance) | Future work | Different problem domain (reverse track matching, conflict resolution, score/position handling). Data structure accommodates it via `_origin` field. |

## Data Model Changes

### Playlist `service_links`

Add `service_links` (JSON, nullable) to the `Playlist` model:

```json
{
  "spotify": {
    "<connection_uuid>": {
      "playlist_id": "6rqhFgbbKwnb9MLmUQDhG6",
      "exported_at": "2026-05-10T22:30:00Z"
    }
  }
}
```

Per-connection keying supports multiple Spotify accounts. Each entry tracks the external playlist ID and when it was last exported.

For future playlist import support, an `_origin` key (not tied to a user connection) tracks where a playlist came from:

```json
{
  "spotify": {
    "_origin": {
      "playlist_id": "abc123",
      "owner": "some_spotify_user",
      "imported_at": "2026-05-10T..."
    },
    "<connection_uuid>": {
      "playlist_id": "xyz789",
      "exported_at": "2026-05-10T..."
    }
  }
}
```

### New Task Type

Add `PLAYLIST_EXPORT` to the `TaskType` enum. Task params:

```json
{
  "playlist_id": "<uuid>",
  "connection_id": "<uuid>"
}
```

### Alembic Migration

Single migration adding:
- `service_links` column to `playlists` table
- `PLAYLIST_EXPORT` value to `TaskType` enum CHECK constraint

### OAuth Scope

Add `playlist-modify-private` to `SpotifyConnector` scope list. Invalidate existing Spotify tokens to force re-authorization.

## Spotify Connector Additions

Add `ConnectorCapability.PLAYLIST_WRITE` to the Spotify connector's capability set.

New methods:

| Method | Spotify Endpoint | Purpose |
|--------|-----------------|---------|
| `create_playlist(token, name, description)` | `POST /me/playlists` | Create private playlist, return Spotify playlist ID |
| `add_tracks_to_playlist(token, playlist_id, uris)` | `POST /playlists/{id}/items` | Add tracks (up to 100 URIs per request, batch if needed) |
| `replace_playlist_tracks(token, playlist_id, uris)` | `PUT /playlists/{id}/items` | Replace all tracks for re-export |
| `search_track(token, query)` | `GET /search?type=track` | Search by `track:Title artist:Artist`, return top result ID or None |

All methods use the existing `_request()` infrastructure (rate limit handling, retry logic). Search calls use conservative delay between requests to protect rate budget.

## API Endpoint

### `POST /api/v1/playlists/{playlist_id}/export`

Enqueues export task(s).

Request body:
```json
{
  "connection_ids": ["<uuid>", ...]
}
```

- Validates playlist exists and belongs to the user
- Validates each connection is a Spotify connection owned by the user
- Enqueues one `PLAYLIST_EXPORT` task per connection
- Returns `202 Accepted` with task IDs:
  ```json
  {"tasks": [{"task_id": "...", "connection_id": "..."}]}
  ```
- If `connection_ids` is omitted or empty, exports to all Spotify connections the user has

## Export Worker Task

Handler: `handle_playlist_export(task, session)`, dispatched via `_TASK_DISPATCH`.

Flow:

1. **Load** playlist with all tracks (eager-load track `service_links`)
2. **Load** the `ServiceConnection` for the target Spotify account; refresh token if needed
3. **Track matching** — For each track missing a Spotify ID in `service_links`:
   - Search Spotify: `track:{title} artist:{artist_name}`
   - If found, persist Spotify ID back to track's `service_links`
   - If not found, mark as skipped
4. **Playlist creation/update**:
   - No existing export for this connection → `create_playlist()` + `add_tracks_to_playlist()`
   - Existing export → `replace_playlist_tracks()` on the existing Spotify playlist
5. **Update playlist** — Write Spotify playlist ID and `exported_at` timestamp to `playlist.service_links`
6. **Complete task** — Store result summary:
   ```json
   {
     "exported": 28,
     "skipped": 2,
     "skipped_tracks": ["Track A", "Track B"],
     "spotify_playlist_id": "..."
   }
   ```

## UI

### Playlist List Page (`/playlists`)

Each playlist card gets an export button (Spotify icon). Indicators:
- Never exported → export button
- In sync → subtle "synced" badge
- Out of sync → amber indicator (playlist changed since last export)

Clicking exports to all connected Spotify accounts.

### Playlist Detail Page (`/playlists/{playlist_id}`)

Export section below playlist metadata showing each connected Spotify account:

| State | Display | Action |
|-------|---------|--------|
| No Spotify connections | "Connect Spotify to export" | Link to connections page |
| Not exported | — | "Export to Spotify" button |
| In sync | "Synced to Spotify · 3 days ago" | "Update" button + open in Spotify link |
| Out of sync | "Playlist changed since last export" (amber) | "Update on Spotify" button (prominent) + open in Spotify link |

**Sync state detection**: Compare `playlist.updated_at` against `service_links[connection].exported_at`. If the playlist was updated after the last export, it's out of sync. No Spotify API calls needed.

### Export Status Page (`/playlists/exporting/{playlist_id}`)

Same pattern as playlist generation status page:

1. User clicks export → `POST /api/v1/playlists/{id}/export`
2. Redirect to `/playlists/exporting/{playlist_id}?tasks=task1,task2`
3. HTMX polls task status per-task
4. Shows per-account progress: "Matching tracks... Exporting to Spotify (mike@...)..."
5. On completion: summary ("Exported 28/30 tracks — 2 not found on Spotify") + open in Spotify link + back to playlist link
6. On failure: error message + "Try Again"

## Future Work

- **Reverse sync** (Spotify → Resonance) — Import track additions/removals from linked Spotify playlists back into resonance. Data structure supports this via `_origin` field.
- **Other services** — `service_links` and `PLAYLIST_WRITE` capability extend naturally to other services that support playlist creation.
- **Auto-sync** — Option to automatically export when a playlist is regenerated, instead of requiring manual export.
