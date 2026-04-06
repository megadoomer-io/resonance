# Incremental Sync Design

Addresses [#13](https://github.com/megadoomer-io/resonance/issues/13).

## Overview

Replace full re-syncs with watermark-based incremental syncs. Each service connection stores per-data-type watermarks that tell the next sync where to resume. This reduces API calls, avoids rate limit pressure, and makes syncs faster after the initial full fetch.

## Design Philosophy

- **Sync forward, don't detect removals.** Unfollows, unsaves, and deleted listens are not tracked. Users can force a full refresh by clearing the watermark (implemented separately in #25).
- **Each service does what's natural for its API.** ListenBrainz has server-side time filtering. Spotify uses client-side stop-early detection. No forced uniformity.
- **Watermarks are a first-class concept on the connection**, not scattered across task history.

## Data Model

### ServiceConnection.sync_watermark

New JSON column on `ServiceConnection`:

```python
sync_watermark: Mapped[dict[str, dict[str, object]]] = mapped_column(
    JSON, server_default="{}", default=dict
)
```

Keyed by data type, with service-specific checkpoint values per key.

**ListenBrainz:**

```json
{
  "listens": {"last_listened_at": 1712345678}
}
```

**Spotify:**

```json
{
  "recently_played": {"last_played_at": "2026-04-05T12:00:00Z"},
  "saved_tracks": {"last_saved_at": "2026-04-05T12:00:00Z"},
  "followed_artists": {"after_cursor": "spotify_artist_id"}
}
```

**Clearing:** Setting a key to `null` or removing it triggers a full fetch for that data type on next sync. The API for clearing/overriding watermarks is deferred to #25.

**Migration:** One Alembic migration adding the column with `server_default='{}'`.

## Strategy Changes

### plan() — Reading Watermarks

Each strategy's `plan()` reads watermarks directly from `connection.sync_watermark` and passes them into child task `params`. This replaces the current ListenBrainz `_get_watermark()` helper that queries task history.

- **ListenBrainz:** Reads `sync_watermark.get("listens")` → sets `min_ts` param. If absent, full sync.
- **Spotify:** Reads each key independently. `recently_played` gets `after` timestamp, `saved_tracks` gets `last_saved_at` for stop-early detection, `followed_artists` gets `after_cursor` for cursor resume.

`plan()` stays simple — it reads watermarks and creates child tasks. No extra API calls during planning (except ListenBrainz `get_listen_count()` which is already there).

### execute() — Using Watermarks

Watermark data arrives via `task.params` (same as today). Each data type uses it differently:

**ListenBrainz `listens`:** Already uses `min_ts` from params — no change needed.

**Spotify `recently_played`:** Pass stored timestamp as `after` parameter to the API. Stop when no more results.

**Spotify `saved_tracks`:** Paginate newest-first. The first page response includes a `total` count. Compare against existing records in DB for this connection:
- If total matches → fast-finish, no further requests needed.
- If total doesn't match → set `progress_total`, process results, continue paginating with stop-early logic: if all items on a page are duplicates (already exist in DB), stop.

**Spotify `followed_artists`:** Pass stored `after_cursor` to resume pagination. If empty response, nothing new. Always does a full fetch since the list is typically small (1-3 API calls).

### Watermark Write Timing

When a child task completes successfully, the worker updates both the task status and the connection's watermark in the same DB transaction:

```python
# In sync_range after successful execute()
task.status = COMPLETED
task.result = result_dict
connection.sync_watermark[data_type_key] = new_watermark_value
await session.commit()  # atomic
```

This means partial syncs (2 of 3 Spotify types complete before a failure) still save progress for the types that succeeded.

**Watermark values written per data type:**

| Data Type | Watermark Value | Source |
|-----------|----------------|--------|
| `listens` (ListenBrainz) | Timestamp of newest listen processed | First item on first page |
| `recently_played` (Spotify) | `played_at` of newest play | First item on first page |
| `saved_tracks` (Spotify) | `added_at` of newest saved track | First item on first page |
| `followed_artists` (Spotify) | Last cursor used | Final `after` cursor from pagination |

The watermark captures the *newest* item seen, not the oldest. On next sync, we ask "give me everything newer than this."

## Progress Tracking

Progress total is populated when the data is available:

- **ListenBrainz:** `get_listen_count()` during `plan()` (existing behavior).
- **Spotify:** First page response during `execute()` includes `total`. If total matches existing count → fast-finish. Otherwise set `progress_total` at that point and continue.

For incremental syncs that stop early, progress may jump (e.g., 15% → complete). This is acceptable — the user sees the sync caught up quickly.

## Changes by File

### Schema
- `models/user.py` — Add `sync_watermark` JSON column to `ServiceConnection`
- New Alembic migration — Add column with `server_default='{}'`

### Sync Strategies
- `sync/listenbrainz.py` — `plan()` reads from `connection.sync_watermark["listens"]` instead of querying task history. Remove `_get_watermark()`. Execute unchanged (already returns watermark in result dict).
- `sync/spotify.py` — `plan()` reads per-data-type watermarks. `execute()` implements stop-early for `saved_tracks`, cursor resume for `followed_artists`, `after` timestamp for `recently_played`. Returns watermark values in result dict.

### Worker
- `worker.py` — After successful task completion in `sync_range`, write watermark from the task's result dict back to `connection.sync_watermark`.

### Connectors
- `connectors/spotify.py` — Ensure `recently_played` accepts an `after` timestamp parameter. Ensure `followed_artists` accepts an `after` cursor. Ensure `saved_tracks` response exposes `total`.
- `connectors/listenbrainz.py` — No changes expected.

### Upsert Functions
- `sync/runner.py` — Ensure track upsert returns whether the item was created vs already existed, so `saved_tracks` execute can detect all-duplicates on a page.

## Not in Scope

- UI for clearing/overriding watermarks (#25)
- Unfollow/unsave detection
- Sync performance batching (#16)
- New service connectors
