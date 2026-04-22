# Last.fm Connector Design

## Overview

Add Last.fm as a third service connector for Resonance. Syncs scrobble history and loved tracks to round out the historical listening profile alongside Spotify and ListenBrainz.

## Auth Flow

Last.fm uses a non-standard web auth scheme (not OAuth2):

1. Redirect user to `https://www.last.fm/api/auth/?api_key=KEY&cb=CALLBACK_URL`
2. User authorizes on Last.fm
3. Last.fm redirects to `/api/v1/auth/lastfm/callback?token=TOKEN`
4. Connector calls `auth.getSession` (signed API call with md5 signature) → returns permanent session key + username
5. Session key stored as `encrypted_access_token` (never expires, no refresh token)
6. `get_current_user` calls `user.getInfo` → returns username and display name

### Auth Callback Changes

- Accept both `code` and `token` as optional query params in the callback
- Pass whichever is present to `connector.exchange_code()`
- Add `exchange_code` and `get_current_user` as abstract methods on `BaseConnector` (removes `type: ignore` comments)
- Future improvement: `get_callback_params(request)` method per connector for cleaner delegation

### API Signing

Every Last.fm API call requires an `api_sig` parameter:
```
api_sig = md5(sorted_param_keys_and_values_concatenated + shared_secret)
```

The connector needs `api_key` and `shared_secret` for all requests.

## Data Sync

### Phase 1 (this implementation)

- **Scrobble history** (`user.getRecentTracks`) — Full paginated listening history with timestamps. Maps to `ListeningEvent`. API returns 200 items per page.
- **Loved tracks** (`user.getLovedTracks`) — Explicitly loved tracks. Maps to `UserTrackRelation` with `LIKE` type. Paginated.

### Future phases

- **Top artists/tracks** (`user.getTopArtists`, `user.getTopTracks`) — Aggregated stats by period
- **Artist tags** (`artist.getTopTags`) — Genre/style tags, valuable for playlist generation

### Dedup Strategy

Fuzzy timestamp matching for listening events: when upserting, check if the same `(user_id, track_id)` has an existing event within 60 seconds. This handles clock skew between services (e.g., Spotify scrobbles to both Last.fm and ListenBrainz with slightly different timestamps).

### Entity Resolution

Same pattern as ListenBrainz:
- Last.fm API returns `mbid` fields on artists and tracks (MusicBrainz IDs)
- Match by MBID first (authoritative), then exact name
- Store MBIDs in `service_links` to enrich cross-service matching

## Sync Strategy

`LastFmSyncStrategy` implementing `SyncStrategy` ABC:

- `concurrency = "sequential"` (respect rate limits)
- `plan()` creates two child tasks:
  1. `data_type: "recent_tracks"` — scrobble history
  2. `data_type: "loved_tracks"` — loved tracks
- `execute()` dispatches based on `data_type`, paginates through API, upserts data
- Incremental sync via watermarks (same pattern as Spotify/LB):
  - `recent_tracks`: `last_scrobbled_at` timestamp
  - `loved_tracks`: `last_loved_at` timestamp

## Config

```python
# Last.fm API
lastfm_api_key: str = ""
lastfm_shared_secret: str = ""
```

Callback URL: `{BASE_URL}/api/v1/auth/lastfm/callback`

## Rate Limiting

Last.fm allows 5 requests per second per API key. The existing `RateLimitBudget` with `default_interval=0.2` handles this. Last.fm doesn't return rate limit headers, so the budget uses the default interval (designed for this case).

## Kubernetes

- Add `LASTFM_API_KEY` and `LASTFM_SHARED_SECRET` to the `resonance-app-secrets` SealedSecret
- No redirect URI env var needed (constructed from BASE_URL)

## Not in Scope

- Artist tags sync
- Top artists/tracks aggregation
- Play count tracking
- `get_callback_params()` refactor (future improvement)
