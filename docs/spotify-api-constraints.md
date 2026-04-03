# Spotify Web API Constraints (February 2026)

Reference for Spotify's Development Mode restrictions that affect Resonance.
Based on the [February 2026 announcement](https://developer.spotify.com/blog/2026-02-06-update-on-developer-access-and-platform-security),
[endpoint changes](https://developer.spotify.com/documentation/web-api/references/changes/february-2026),
and [migration guide](https://developer.spotify.com/documentation/web-api/tutorials/february-2026-migration-guide).

## Development Mode Limits

Resonance runs as a Development Mode app. Extended Quota Mode requires a
commercial use case and approval process that doesn't apply to personal/hobbyist projects.

| Constraint | Value |
|---|---|
| Client IDs per developer | 1 |
| Authorized users per app | 5 |
| Account requirement | Spotify Premium |
| Rate limit window | Rolling 30 seconds |
| Rate limit threshold | Low (exact number undisclosed) |

## Rate Limiting Behavior

- Rate limits are calculated per app (Client ID) across all users, not per user.
- Exceeding the limit returns HTTP 429 with a `Retry-After` header (seconds).
- In practice, Spotify returns extremely long `Retry-After` values (4-14+ hours)
  once the limit is tripped — not proportional to the overage.
- There is no documented way to query current rate limit usage or remaining budget
  in dev mode (no `X-RateLimit-Remaining` headers like ListenBrainz provides).

## Available Endpoints (used by Resonance)

These endpoints survived the February 2026 changes and are available in dev mode:

| Endpoint | Use in Resonance |
|---|---|
| `GET /me` | User profile during OAuth callback |
| `GET /me/following` | Sync followed artists |
| `GET /me/tracks` | Sync saved tracks (library) |
| `GET /me/player/recently-played` | Sync recent listening history |
| `POST /authorize` | OAuth authorization flow |
| `POST /api/token` | Token exchange and refresh |

## Removed Endpoints (previously available)

Batch endpoints that could have reduced request count are gone:

- `GET /tracks` (batch) — must use `GET /tracks/{id}` individually
- `GET /artists` (batch) — must use `GET /artists/{id}` individually
- `GET /albums` (batch) — must use `GET /albums/{id}` individually
- `GET /artists/{id}/top-tracks` — removed entirely
- `GET /browse/new-releases` — removed entirely

## Field Changes

Some response fields were removed from all endpoints:

- **Track**: `available_markets`, `linked_from`, `popularity` removed
- **Artist**: `followers`, `popularity` removed
- **Album**: `album_group`, `available_markets`, `label`, `popularity` removed
- **User** (`GET /me`): `country`, `email`, `explicit_content`, `followers`, `product` removed

`external_ids` on tracks and albums was initially removed but reverted in March 2026.

## Implications for Sync Strategy

1. **Request budget is tiny** — every API call counts. Full library syncs
   that paginate through hundreds of pages will exhaust the budget fast.
2. **No batch endpoints** — can't fetch multiple items per request for
   enrichment. Each metadata lookup is one request.
3. **Aggressive punishment** — once rate limited, the app is locked out for
   hours, affecting all endpoints including OAuth.
4. **Incremental sync is essential** — must track watermarks and only fetch
   new/changed data on subsequent syncs.
5. **Request pacing must be conservative** — spread requests over time rather
   than bursting, even within the 30-second window.
