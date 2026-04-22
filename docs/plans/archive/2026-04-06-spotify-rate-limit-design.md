# Spotify Rate Limit Redesign

Addresses [#20](https://github.com/megadoomer-io/resonance/issues/20) (remaining items after incremental sync).

## Overview

Spotify's Development Mode rate limits are undisclosed but aggressive — tripping them results in 4-14+ hour lockouts affecting all endpoints including OAuth. The current 0.2s pacing (5 req/s) is far too aggressive. This design adds conservative pacing and a request budget safety net.

## Design Philosophy

- **Fixed pacing as primary control.** 5s between requests is simple, predictable, and keeps us well under the limit.
- **Rolling window budget as safety net.** Counts requests per 30s window with a ceiling of 10. Almost never fires under normal 5s pacing (max 6 req/30s), but catches edge cases (retries, concurrent syncs, auth refreshes).
- **Observable for iterative tuning.** Per-request budget logging at info level so we can see consumption patterns and adjust the sweet spot over time.

## Changes

### RateLimitBudget (ratelimit.py)

Add optional `window_seconds` and `window_ceiling` parameters:

```python
RateLimitBudget(
    default_interval=5.0,
    window_seconds=30,
    window_ceiling=10,
)
```

When `window_ceiling` is `None` (default), window tracking is disabled — zero overhead for services that provide real rate limit headers (ListenBrainz).

**New state:**
- `_request_timestamps: list[float]` — monotonic timestamps of recent requests, pruned to window on each check

**New methods:**
- `record_request()` — append current timestamp, prune entries older than `window_seconds`
- `check_window_budget() -> float` — return seconds to wait (0 if under ceiling, time-until-oldest-ages-out if at ceiling)

**New property:**
- `window_used -> int` — current count of requests in the window (for logging)

### BaseConnector._request() (base.py)

After the existing `paced_interval` sleep, add budget window check:

1. Call `check_window_budget()` — if > 0, log a warning and sleep
2. After request completes (success or 429), call `record_request()`
3. Log budget status at info level when window tracking is enabled

### Spotify Connector (spotify.py)

Change constructor:
```python
self._budget = ratelimit_module.RateLimitBudget(
    default_interval=5.0,
    window_seconds=30,
    window_ceiling=10,
)
```

### Logging

**Per-request (info level, demote to debug after tuning):**
```json
{"event": "request_budget_status", "window_used": 3, "window_ceiling": 10, "window_seconds": 30}
```

**Budget throttle (warning level):**
```json
{"event": "request_budget_throttled", "window_used": 10, "window_ceiling": 10, "wait_seconds": 4.2}
```

### Not Changed

- ListenBrainz connector — keeps `default_interval=0.2`, no window config
- Page sizes — kept at 50 (reducing would increase request count)
- No schema or migration changes
