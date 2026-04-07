# Incremental Watermark Updates During Sync

## Problem

The sync watermark on `ServiceConnection` is only written at the end of a successful `sync_range` execution. If the worker crashes mid-sync (OOM, pod eviction, unhandled exception), all progress is lost and the entire sync range must be re-processed from scratch.

For large backfills (e.g., 123k ListenBrainz listens), this means potentially hours of wasted work. The upsert logic prevents data corruption, but re-processing is unnecessary overhead.

## Design

### Two-Ended Watermark Structure

Expand the watermark from a single high-water mark to a range:

```json
{
  "listens": {
    "newest_synced_at": 1700000000,
    "oldest_synced_at": 1650000000
  }
}
```

- `newest_synced_at` — the most recent listen timestamp in the synced range (set from the first page)
- `oldest_synced_at` — the oldest listen timestamp processed so far (advances as pages are committed)

This structure is pagination-direction-agnostic. A forward-paginating connector would fix `oldest_synced_at` early and advance `newest_synced_at`.

### Per-Page Watermark Update

After each page is committed in the strategy's `execute()` method, update `connection.sync_watermark` in the same transaction:

```python
# After upserting artists, tracks, events for this page...
max_ts = listens[-1].listened_at
task.progress_current = items_created

updated_watermarks = dict(connection.sync_watermark)
updated_watermarks["listens"] = {
    "newest_synced_at": last_listened_at,
    "oldest_synced_at": max_ts,
}
connection.sync_watermark = updated_watermarks

await session.commit()
```

The `execute()` signature gains a `connection` parameter:

```python
async def execute(session, task, connector, connection) -> dict
```

The worker already loads the `ServiceConnection` in `sync_range` and passes it through.

### Crash Recovery in `plan_sync`

When `plan_sync` reads a two-ended watermark, it detects whether the previous sync was interrupted:

- **Complete sync** (`oldest_synced_at` reached `min_ts` or end of history): plan one task from `newest_synced_at` upward for new listens since last sync.
- **Interrupted sync** (`oldest_synced_at` > expected floor): plan two tasks:
  1. New listens since last sync: `now` down to `newest_synced_at`
  2. Remaining backfill: `oldest_synced_at` down to original start

### Spotify Applicability

Same pattern applies to Spotify strategies. The `connection` parameter is threaded through, and watermarks are updated per-page. Spotify data types use their own checkpoint fields alongside timestamps where applicable:

- `saved_tracks`: `{"newest_synced_at": ..., "last_offset": 100}`
- `recently_played`: `{"newest_synced_at": ..., "oldest_synced_at": ...}`
- `followed_artists`: `{"after_cursor": "..."}`

### Backward Compatibility

Legacy watermarks (`{"listens": {"last_listened_at": ...}}`) are treated as equivalent to `{"newest_synced_at": <value>}` with no `oldest_synced_at`, meaning the range below was fully synced. No data migration required.

## Changes

| File | Change |
|------|--------|
| `sync/base.py` | Update `SyncStrategy.execute()` signature to include `connection` |
| `sync/listenbrainz.py` | Add `connection` param, update watermark after each page commit |
| `sync/spotify.py` | Add `connection` param, update watermark after each page commit |
| `worker.py` (`sync_range`) | Pass `connection` to `strategy.execute()` |
| `worker.py` (`plan_sync`) | Handle two-ended watermark: detect interrupted sync, plan split tasks |

No database migrations, no new models, no API changes.

## Error Handling

- **Crash during page commit**: transaction rolls back atomically, watermark stays at previous page's value. At most one page is re-processed.
- **Orphaned task re-enqueue**: no changes needed. Task params and connection watermark are both current; the more-recent checkpoint wins.
- **Concurrent sync tasks**: safe because each task writes to a different `data_type_key` within the watermark dict.
- **Page limit reached**: watermark reflects progress through `oldest_synced_at`; next `plan_sync` picks up naturally.
