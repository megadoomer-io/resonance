# Last.fm Connector Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add Last.fm as a third service connector with scrobble history and loved tracks sync, including fuzzy timestamp dedup and MBID entity resolution.

**Architecture:** Last.fm connector handles non-standard web auth (token-based, not OAuth2 code exchange) with API signature signing. LastFmSyncStrategy implements the SyncStrategy ABC with two sequential child tasks (recent_tracks, loved_tracks). Auth callback accepts both `code` and `token` query params to support Last.fm's auth flow alongside OAuth2 services. Abstract methods added to BaseConnector to remove type: ignore comments.

**Tech Stack:** Python 3.14, FastAPI, httpx, hashlib (md5 signing), arq

---

### Task 1: Config + BaseConnector Abstract Methods

**Files:**
- Modify: `src/resonance/config.py`
- Modify: `src/resonance/connectors/base.py`
- Modify: `src/resonance/api/v1/auth.py` (remove type: ignore comments)
- Test: `tests/test_config.py`

**Step 1: Add Last.fm config fields**

Add to `src/resonance/config.py` after the MusicBrainz settings:

```python
# Last.fm API
lastfm_api_key: str = ""
lastfm_shared_secret: str = ""
```

No redirect path property needed — Last.fm callback URL is constructed directly in the connector.

**Step 2: Add abstract methods to BaseConnector**

In `src/resonance/connectors/base.py`, add abstract methods:

```python
@abc.abstractmethod
def get_auth_url(self, state: str) -> str:
    """Build the authorization URL for this service."""
    ...

@abc.abstractmethod
async def exchange_code(self, code: str) -> TokenResponse:
    """Exchange an auth code/token for access tokens."""
    ...

@abc.abstractmethod
async def get_current_user(self, access_token: str) -> dict[str, str]:
    """Get the current user's profile. Returns {id, display_name}."""
    ...
```

IMPORTANT: Check if `BaseConnector` currently has `has_capability` as its only method. The TestConnector doesn't implement auth methods — it will need stub implementations that raise `NotImplementedError` since it uses instant connect.

Update `TestConnector` in `src/resonance/connectors/test.py` to implement the abstract methods:
```python
def get_auth_url(self, state: str) -> str:
    raise NotImplementedError("Test connector uses instant connect")

async def exchange_code(self, code: str) -> base_module.TokenResponse:
    raise NotImplementedError("Test connector uses instant connect")

async def get_current_user(self, access_token: str) -> dict[str, str]:
    raise NotImplementedError("Test connector uses instant connect")
```

**Step 3: Remove type: ignore comments from auth callback**

In `src/resonance/api/v1/auth.py`, remove the `type: ignore[attr-defined]` comments on calls to `get_auth_url`, `exchange_code`, and `get_current_user` since they're now abstract methods on the ABC.

**Step 4: Update auth callback to accept `token` query param**

The callback currently requires `code: str`. Change it to accept both:

```python
@router.get("/{service}/callback")
async def auth_callback(
    service: str,
    request: fastapi.Request,
    session: ...,
    db: ...,
    code: str | None = None,
    token: str | None = None,
    state: str = "",
) -> ...:
    # Use whichever is provided
    auth_code = code or token
    if auth_code is None:
        raise fastapi.HTTPException(status_code=400, detail="Missing code or token")

    # ... rest of callback uses auth_code instead of code
    tokens = await connector.exchange_code(code=auth_code)
```

**Step 5: Add config test**

```python
def test_settings_has_lastfm_credentials() -> None:
    settings = config_module.Settings()
    assert hasattr(settings, "lastfm_api_key")
    assert hasattr(settings, "lastfm_shared_secret")
```

**Step 6: Run tests, lint, commit**

```bash
git add src/resonance/config.py src/resonance/connectors/base.py src/resonance/connectors/test.py src/resonance/api/v1/auth.py tests/
git commit -m "feat: add Last.fm config, BaseConnector abstract methods, and flexible auth callback (#39)"
```

---

### Task 2: Last.fm Connector

**Files:**
- Create: `src/resonance/connectors/lastfm.py`
- Test: `tests/test_lastfm_connector.py`

**Step 1: Create Last.fm connector**

The connector needs:
- API signature helper (`_sign_params`)
- `get_auth_url(state)` — builds Last.fm auth redirect URL
- `exchange_code(token)` — calls `auth.getSession` with signed params
- `get_current_user(session_key)` — calls `user.getInfo`
- `get_recent_tracks(session_key, username, page, limit, from_ts)` — paginated scrobbles
- `get_loved_tracks(session_key, username, page, limit)` — paginated loved tracks

```python
"""Last.fm connector with web auth and scrobble/loved tracks data."""

import hashlib
import urllib.parse
from typing import Any

import httpx
import pydantic
import structlog

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module

logger = structlog.get_logger()

LASTFM_AUTH_URL = "https://www.last.fm/api/auth/"
LASTFM_API_BASE = "https://ws.audioscrobbler.com/2.0/"


class LastFmConnector(base_module.BaseConnector):
    """Connector for the Last.fm API."""

    service_type = types_module.ServiceType.LASTFM
    capabilities = frozenset({
        base_module.ConnectorCapability.AUTHENTICATION,
        base_module.ConnectorCapability.LISTENING_HISTORY,
        base_module.ConnectorCapability.TRACK_RATINGS,
    })

    def __init__(self, settings: config_module.Settings) -> None:
        self._api_key = settings.lastfm_api_key
        self._shared_secret = settings.lastfm_shared_secret
        self._callback_url = f"{settings.base_url}/api/v1/auth/lastfm/callback"
        self._http_client: httpx.AsyncClient | None = None
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.2)

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
        return self._http_client

    def _sign_params(self, params: dict[str, str]) -> str:
        """Generate Last.fm API signature (md5 of sorted params + secret)."""
        sorted_params = "".join(
            f"{k}{v}" for k, v in sorted(params.items()) if k != "format"
        )
        return hashlib.md5(
            (sorted_params + self._shared_secret).encode()
        ).hexdigest()

    async def _api_call(
        self,
        method: str,
        params: dict[str, str] | None = None,
        signed: bool = False,
    ) -> dict[str, Any]:
        """Make a Last.fm API call."""
        call_params: dict[str, str] = {
            "method": method,
            "api_key": self._api_key,
            "format": "json",
            **(params or {}),
        }
        if signed:
            call_params["api_sig"] = self._sign_params(call_params)

        response = await self._request(
            "GET", LASTFM_API_BASE, params=call_params
        )
        data: dict[str, Any] = response.json()
        if "error" in data:
            raise httpx.HTTPStatusError(
                f"Last.fm API error {data['error']}: {data.get('message', '')}",
                request=response.request,
                response=response,
            )
        return data

    def get_auth_url(self, state: str) -> str:
        """Build Last.fm authorization URL."""
        params = urllib.parse.urlencode({
            "api_key": self._api_key,
            "cb": self._callback_url,
        })
        return f"{LASTFM_AUTH_URL}?{params}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        """Exchange a Last.fm auth token for a session key."""
        logger.info("Exchanging Last.fm token for session key")
        data = await self._api_call(
            "auth.getSession",
            params={"token": code},
            signed=True,
        )
        session_info = data["session"]
        logger.info("Got Last.fm session for user: %s", session_info["name"])
        return base_module.TokenResponse(
            access_token=session_info["key"],
            # Last.fm session keys never expire
            refresh_token=None,
            expires_in=None,
            scope=None,
        )

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        """Get the Last.fm user profile."""
        logger.info("Fetching Last.fm user profile")
        data = await self._api_call(
            "user.getInfo",
            params={"sk": access_token},
            signed=True,
        )
        user = data["user"]
        return {"id": user["name"], "display_name": user["realname"] or user["name"]}

    async def get_recent_tracks(
        self,
        username: str,
        *,
        page: int = 1,
        limit: int = 200,
        from_ts: int | None = None,
    ) -> dict[str, Any]:
        """Get paginated scrobble history."""
        params: dict[str, str] = {
            "user": username,
            "page": str(page),
            "limit": str(limit),
            "extended": "0",
        }
        if from_ts is not None:
            params["from"] = str(from_ts)
        return await self._api_call("user.getRecentTracks", params=params)

    async def get_loved_tracks(
        self,
        username: str,
        *,
        page: int = 1,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Get paginated loved tracks."""
        params: dict[str, str] = {
            "user": username,
            "page": str(page),
            "limit": str(limit),
        }
        return await self._api_call("user.getLovedTracks", params=params)
```

**Step 2: Write tests**

Test service_type, capabilities, `_sign_params` determinism, `get_auth_url` format, and mock API calls for `exchange_code`, `get_current_user`, `get_recent_tracks`, `get_loved_tracks`.

**Step 3: Run tests, lint, commit**

```bash
git add src/resonance/connectors/lastfm.py tests/test_lastfm_connector.py
git commit -m "feat: add Last.fm connector with web auth and scrobble API (#39)"
```

---

### Task 3: Last.fm Sync Strategy

**Files:**
- Create: `src/resonance/sync/lastfm.py`
- Modify: `src/resonance/worker.py` (register strategy + connector)
- Modify: `src/resonance/app.py` (register connector)
- Test: `tests/test_sync_lastfm_strategy.py`

**Step 1: Create LastFmSyncStrategy**

Following the Spotify strategy pattern:

```python
"""Last.fm sync strategy — scrobble history and loved tracks."""

class LastFmSyncStrategy(sync_base.SyncStrategy):
    concurrency = "sequential"

    def __init__(self, token_encryption_key: str) -> None:
        self._token_encryption_key = token_encryption_key

    async def plan(self, session, connection, connector):
        watermarks = connection.sync_watermark or {}
        descriptors = []

        # Task 1: Recent tracks (scrobble history)
        recent_wm = watermarks.get("recent_tracks", {})
        last_scrobbled = recent_wm.get("last_scrobbled_at")
        descriptors.append(SyncTaskDescriptor(
            task_type=SyncTaskType.TIME_RANGE,
            params={
                "data_type": "recent_tracks",
                "from_ts": last_scrobbled,
            },
            description="Scrobble history" + (" (incremental)" if last_scrobbled else " (full)"),
        ))

        # Task 2: Loved tracks
        loved_wm = watermarks.get("loved_tracks", {})
        descriptors.append(SyncTaskDescriptor(
            task_type=SyncTaskType.TIME_RANGE,
            params={
                "data_type": "loved_tracks",
            },
            description="Loved tracks",
        ))

        return descriptors

    async def execute(self, session, task, connector, connection):
        data_type = task.params.get("data_type")
        # Decrypt session key
        session_key = crypto_module.decrypt_token(
            connection.encrypted_access_token, self._token_encryption_key
        )
        username = connection.external_user_id

        if data_type == "recent_tracks":
            return await self._sync_recent_tracks(
                session, task, connector, connection, session_key, username
            )
        elif data_type == "loved_tracks":
            return await self._sync_loved_tracks(
                session, task, connector, connection, session_key, username
            )
        return {"items_created": 0, "items_updated": 0}
```

For `_sync_recent_tracks`:
- Paginate through `user.getRecentTracks` (Last.fm uses page numbers, not cursor)
- Parse each track: extract artist name, track name, mbid, timestamp
- Use upsert helpers from runner.py
- Fuzzy timestamp dedup: check ±60 seconds for same user+track
- Update watermark per page
- Handle `@attr.nowplaying` flag (skip currently playing tracks — no timestamp)

For `_sync_loved_tracks`:
- Paginate through `user.getLovedTracks`
- Create `UserTrackRelation` with `LIKE` type
- Always full-fetch (loved tracks don't have a reliable watermark)

**Step 2: Register in worker.py and app.py**

Worker:
```python
import resonance.connectors.lastfm as lastfm_module
import resonance.sync.lastfm as lastfm_sync

# In startup():
connector_registry.register(lastfm_module.LastFmConnector(settings=settings))
wctx["strategies"][types_module.ServiceType.LASTFM] = lastfm_sync.LastFmSyncStrategy(
    token_encryption_key=settings.token_encryption_key
)
```

App:
```python
import resonance.connectors.lastfm as lastfm_module
connector_registry.register(lastfm_module.LastFmConnector(settings=settings))
```

**Step 3: Run tests, lint, commit**

```bash
git add src/resonance/sync/lastfm.py src/resonance/worker.py src/resonance/app.py tests/
git commit -m "feat: add Last.fm sync strategy with scrobble history and loved tracks (#39)"
```

---

### Task 4: Fuzzy Timestamp Dedup

**Files:**
- Modify: `src/resonance/sync/runner.py`
- Test: `tests/test_sync_runner.py`

**Step 1: Update `_upsert_listening_event` for fuzzy dedup**

Currently checks exact `(user_id, track_id, listened_at)` match. Add a ±60 second window:

```python
async def _upsert_listening_event(session, user_id, track_data, played_at, *, dedup_window_seconds=60):
    # ... existing track lookup ...

    listened_dt = datetime.datetime.fromisoformat(played_at)

    # Fuzzy dedup: check within ±window seconds
    window = datetime.timedelta(seconds=dedup_window_seconds)
    check_stmt = sa.select(models_module.ListeningEvent).where(
        models_module.ListeningEvent.user_id == user_id,
        models_module.ListeningEvent.track_id == track.id,
        models_module.ListeningEvent.listened_at >= listened_dt - window,
        models_module.ListeningEvent.listened_at <= listened_dt + window,
    ).limit(1)
    # ... rest unchanged
```

**Step 2: Add tests for fuzzy dedup**

**Step 3: Run tests, lint, commit**

```bash
git add src/resonance/sync/runner.py tests/test_sync_runner.py
git commit -m "feat: add fuzzy timestamp dedup (±60s) for listening events (#39)"
```

---

### Task 5: UI + Connect Button

**Files:**
- Modify: `src/resonance/templates/login.html`
- Modify: `src/resonance/templates/account.html`

**Step 1: Add Last.fm connect buttons**

In `login.html`, add alongside Spotify and ListenBrainz:
```html
<a href="/api/v1/auth/lastfm" role="button" class="outline">Connect with Last.fm</a>
```

In `account.html`, add in the "Connect Another Service" section:
```html
<a href="/api/v1/auth/lastfm" role="button" class="outline">Connect Last.fm</a>
```

**Step 2: Commit**

```bash
git add src/resonance/templates/login.html src/resonance/templates/account.html
git commit -m "feat: add Last.fm connect buttons to login and account pages (#39)"
```

---

### Task 6: Kubernetes Secrets

**Files:**
- Modify: megadoomer-config sealed secrets and helm-values

**Step 1: Re-seal app-secrets with Last.fm credentials**

Get the Last.fm API key and shared secret from the user, re-seal the `resonance-app-secrets` SealedSecret with the new values added.

**Step 2: Commit to megadoomer-config**

```bash
cd ~/src/github.com/megadoomer-io/megadoomer-config
git add applications/resonance/resonance/do/sealed-secret-app-secrets.yaml
git commit -m "feat(resonance): add Last.fm API credentials"
```

---

## Summary

After all 6 tasks:

| Component | Status |
|-----------|--------|
| Config | `lastfm_api_key`, `lastfm_shared_secret` |
| BaseConnector | Abstract methods for `get_auth_url`, `exchange_code`, `get_current_user` |
| Auth callback | Accepts `code` or `token` query params, delegates to connector |
| LastFmConnector | Web auth + API signing + scrobble/loved API methods |
| LastFmSyncStrategy | Sequential: recent_tracks + loved_tracks with watermarks |
| Fuzzy dedup | ±60s window on listening event timestamp matching |
| UI | Connect buttons on login + account pages |
| K8s secrets | Last.fm credentials sealed and deployed |

**Future work (not in scope):**
- Artist tags sync (`artist.getTopTags`)
- Top artists/tracks aggregation
- Play count tracking
- `get_callback_params()` per-connector refactor
