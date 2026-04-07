# Batch Sync Performance Optimization

Addresses [#16](https://github.com/megadoomer-io/resonance/issues/16).

## Overview

The current sync processes each listen individually with 3-4 DB queries per item. For 123K listens this takes ~100 minutes. Three optimizations reduce this to ~10-15 minutes: larger page sizes, batched flushes, and bulk dedup queries.

## 1. Larger Page Sizes

ListenBrainz's `get_listens` API supports up to 1000 items per page. Change from `count=100` to `count=1000` in the execute method. This reduces HTTP round-trips from ~1,230 to ~123 for a full 123K listen sync.

Spotify stays at 50 (API maximum). Each strategy controls its own page size — no shared configuration needed.

The `MAX_PAGES` constant (5000) is unchanged. At 1000 per page, that's 5M listens before hitting the limit.

## 2. Flush Batching

Restructure the per-item loop into three passes per page:

**Current (per-item):**
```
for each listen:
    upsert_artist → flush
    upsert_track → flush
    upsert_event
commit
```
= 2000 flushes + 1 commit per 1000-item page

**New (per-page):**
```
for each listen: upsert_artist
flush once

for each listen: upsert_track
flush once

for each listen: upsert_event
commit
```
= 2 flushes + 1 commit per 1000-item page

The ordering preserves foreign key dependencies: artists must exist before tracks (FK), tracks must exist before events (FK). Each flush sends pending INSERTs/UPDATEs within the same transaction, making IDs available for the next pass.

Applied to both ListenBrainz and Spotify sync strategies for consistency.

## 3. Bulk Dedup Queries

Before processing each page, pre-fetch all existing artists and tracks in two bulk queries:

```python
# Collect external IDs from the page
artist_ids = {listen.track.artist_external_id for listen in listens}
track_ids = {listen.track.external_id for listen in listens}

# Bulk fetch by service_links (one query each)
artist_map = await bulk_fetch_artists(session, service_key, artist_ids)
track_map = await bulk_fetch_tracks(session, service_key, track_ids)
```

The per-item upsert functions gain an optional cache parameter. When provided, they check the cache before querying the DB. Items not found in the cache fall through to the existing per-item lookup logic (MBID cross-reference, name matching) — this handles edge cases while the bulk path covers ~90% of items.

## 4. Adaptive Page Size

ListenBrainz has a multi-year gap in listening history. When paginating backward through time with `count=1000`, the server may take a long time scanning empty time ranges to fill the response — long enough to cause `RemoteProtocolError` (server disconnect/timeout). With `count=100`, the server responds faster because it has less to accumulate.

**Solution:** Start at `count=1000` and adaptively reduce on failure. Since pagination uses `max_ts` (a cursor, not an offset), changing page size mid-stream is safe — no data is skipped.

```
Start: count = 1000
On success → grow back toward 1000 (double, capped at 1000)
On RemoteProtocolError → halve count (min 100), retry same max_ts
```

Example flow through a gap:
```
count=1000, max_ts=T  → timeout
count=500,  max_ts=T  → timeout
count=250,  max_ts=T  → success (got 250 items)
count=500,  max_ts=T' → success (got 500 items)
count=1000, max_ts=T" → success (back to full speed)
```

**Where it lives:** In `ListenBrainzSyncStrategy.execute()`. The strategy wraps the `get_listens` call in a try/except for transient errors. On failure, it halves `count` and retries the same `max_ts`. On success, it doubles `count` for the next page (capped at 1000). The connector's built-in transient retry still handles other transient errors normally.

**Important:** The adaptive retry must catch the error *before* it reaches the connector's transient retry loop. Otherwise the connector exhausts 5 retries at the same (too-large) page size. The strategy should call `get_listens` with a shorter timeout and handle the retry with reduced page size itself, or the connector's transient retry should be made page-size-aware.

The simplest approach: catch `RemoteProtocolError` and `ReadTimeout` in the strategy's page loop, reduce `count`, and retry. The connector's transient retry handles other transient errors (connection refused, etc.) that aren't page-size-related.

## Changes by File

### sync/runner.py
- Add `bulk_fetch_artists(session, service_key, external_ids) -> dict[str, Artist]`
- Add `bulk_fetch_tracks(session, service_key, external_ids) -> dict[str, Track]`
- Upsert functions gain optional `artist_cache` / `track_cache` params

### sync/listenbrainz.py
- Change `count=100` to `count=1000` (initial)
- Restructure loop into three passes with bulk pre-fetch
- Adaptive page size: halve on timeout, double on success

### sync/spotify.py
- Apply same flush batching and bulk pre-fetch to `_sync_saved_tracks` and `_sync_recently_played`
- Page sizes stay at 50

### Tests
- `test_sync_runner.py` — bulk fetch helpers, cache passthrough
- `test_sync_listenbrainz.py` — updated for page size and batched pattern
- `test_sync_spotify_strategy.py` — updated for batched flush pattern

## Expected Impact

| Metric | Before | After |
|--------|--------|-------|
| HTTP round-trips (123K listens) | ~1,230 | ~123 |
| DB flushes per page | ~2,000 | 2 |
| DB queries per page | ~3,000 | ~5 |
| Estimated full sync time | ~100 min | ~10-15 min |
