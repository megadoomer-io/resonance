# ListenBrainz Connector + Rate Limit Budget Manager

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add ListenBrainz as a second service connector (auth via MusicBrainz OAuth2, data via ListenBrainz API) and introduce a shared rate limit budget manager that paces API requests across all connectors.

**Architecture:** Rate limit budget manager tracks remaining requests and distributes them evenly across the reset window, with priority lanes for interactive vs background requests. ListenBrainz connector authenticates via MusicBrainz OAuth2 (standard code flow), reads listening history from the public ListenBrainz API using the authenticated username. Sync runner is generalized to support ListenBrainz alongside Spotify. Entity resolution matches artists/tracks by MBID first, then exact name.

**Tech Stack:** Python 3.14, httpx, MusicBrainz OAuth2, ListenBrainz API

---

### Task 1: Rate Limit Budget Manager

**Files:**
- Create: `src/resonance/connectors/ratelimit.py`
- Test: `tests/test_ratelimit.py`

**Step 1: Write the failing tests**

Create `tests/test_ratelimit.py`:

```python
import asyncio
import time

import pytest

import resonance.connectors.ratelimit as ratelimit_module


class TestRateLimitBudget:
    def test_initial_state_allows_requests(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        assert budget.can_proceed()

    def test_update_from_headers(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=10, reset_in=30.0)
        assert budget.remaining == 10
        assert budget.reset_in == 30.0

    def test_paced_interval_spreads_requests(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=10, reset_in=30.0)
        interval = budget.paced_interval()
        assert 2.5 <= interval <= 3.5  # ~30/10 = 3.0 seconds

    def test_paced_interval_with_no_remaining(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=0, reset_in=5.0)
        interval = budget.paced_interval()
        assert interval >= 5.0  # must wait until reset

    def test_paced_interval_uses_default_when_no_data(self) -> None:
        budget = ratelimit_module.RateLimitBudget(default_interval=1.0)
        interval = budget.paced_interval()
        assert interval == 1.0

    def test_can_proceed_false_when_exhausted(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=0, reset_in=10.0)
        assert not budget.can_proceed()

    def test_high_priority_bypasses_pacing(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=5, reset_in=100.0)
        # High priority should return 0 interval (go immediately)
        interval = budget.paced_interval(high_priority=True)
        assert interval == 0.0

    def test_high_priority_blocked_when_exhausted(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        budget.update(remaining=0, reset_in=10.0)
        # Even high priority must wait when remaining is 0
        interval = budget.paced_interval(high_priority=True)
        assert interval >= 10.0

    def test_update_from_response_headers(self) -> None:
        budget = ratelimit_module.RateLimitBudget()
        headers = {
            "X-RateLimit-Remaining": "15",
            "X-RateLimit-Reset-In": "45",
        }
        budget.update_from_headers(headers)
        assert budget.remaining == 15
        assert budget.reset_in == 45.0

    def test_update_from_spotify_headers(self) -> None:
        """Spotify uses Retry-After instead of X-RateLimit headers."""
        budget = ratelimit_module.RateLimitBudget()
        headers = {"Retry-After": "5"}
        budget.update_from_headers(headers)
        assert budget.remaining == 0
        assert budget.reset_in == 5.0

    def test_update_from_empty_headers(self) -> None:
        """No rate limit headers — budget should remain unchanged."""
        budget = ratelimit_module.RateLimitBudget(default_interval=0.5)
        budget.update_from_headers({})
        interval = budget.paced_interval()
        assert interval == 0.5
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_ratelimit.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

Create `src/resonance/connectors/ratelimit.py`:

```python
"""Rate limit budget manager for API connectors.

Tracks remaining request budget from API response headers and paces
requests to spread them evenly across the rate limit window. Supports
priority lanes: high-priority requests (auth, user actions) bypass
pacing but still respect hard limits; normal-priority requests
(background sync) are paced.

Works with services that provide rate limit headers (ListenBrainz
X-RateLimit-* headers, Spotify Retry-After) and degrades gracefully
to a configurable default interval when no headers are available.
"""

from __future__ import annotations

import time
from typing import Any


class RateLimitBudget:
    """Track and pace API requests based on rate limit budget."""

    def __init__(self, default_interval: float = 0.2) -> None:
        self._default_interval = default_interval
        self._remaining: int | None = None
        self._reset_in: float | None = None
        self._last_update: float = 0.0

    @property
    def remaining(self) -> int | None:
        """Number of requests remaining in current window."""
        return self._remaining

    @property
    def reset_in(self) -> float | None:
        """Seconds until rate limit window resets."""
        if self._reset_in is None:
            return None
        elapsed = time.monotonic() - self._last_update
        return max(0.0, self._reset_in - elapsed)

    def update(self, remaining: int, reset_in: float) -> None:
        """Update budget from known values."""
        self._remaining = remaining
        self._reset_in = reset_in
        self._last_update = time.monotonic()

    def update_from_headers(self, headers: dict[str, Any]) -> None:
        """Update budget from HTTP response headers.

        Supports two styles:
        - ListenBrainz: X-RateLimit-Remaining, X-RateLimit-Reset-In
        - Spotify: Retry-After (implies remaining=0)
        """
        remaining_str = headers.get("X-RateLimit-Remaining")
        reset_in_str = headers.get("X-RateLimit-Reset-In")

        if remaining_str is not None and reset_in_str is not None:
            self.update(
                remaining=int(remaining_str),
                reset_in=float(reset_in_str),
            )
            return

        retry_after_str = headers.get("Retry-After")
        if retry_after_str is not None:
            self.update(remaining=0, reset_in=float(retry_after_str))

    def can_proceed(self) -> bool:
        """Check if a request can be made without waiting."""
        if self._remaining is None:
            return True
        if self._remaining > 0:
            return True
        reset_in = self.reset_in
        return reset_in is not None and reset_in <= 0

    def paced_interval(self, high_priority: bool = False) -> float:
        """Calculate seconds to wait before next request.

        Args:
            high_priority: If True, bypass pacing (go immediately)
                unless budget is fully exhausted.
        """
        if self._remaining is None:
            return 0.0 if high_priority else self._default_interval

        reset_in = self.reset_in or 0.0

        if self._remaining <= 0:
            return reset_in

        if high_priority:
            return 0.0

        return reset_in / self._remaining
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_ratelimit.py -v`
Expected: PASS

**Step 5: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 6: Commit**

```bash
git add src/resonance/connectors/ratelimit.py tests/test_ratelimit.py
git commit -m "feat: add rate limit budget manager with priority lanes"
```

---

### Task 2: Retrofit Spotify Connector with Budget Manager

**Files:**
- Modify: `src/resonance/connectors/spotify.py`
- Modify: `tests/test_spotify_connector.py`

**Step 1: Replace `_request` retry logic with budget manager**

Update `SpotifyConnector.__init__` to create a `RateLimitBudget`:

```python
def __init__(self, settings: config_module.Settings) -> None:
    self._client_id = settings.spotify_client_id
    self._client_secret = settings.spotify_client_secret
    self._redirect_uri = settings.spotify_redirect_uri
    self._http_client: httpx.AsyncClient | None = None
    self._budget = ratelimit_module.RateLimitBudget(default_interval=0.2)
```

Replace `_request` method to use the budget:

```python
async def _request(
    self,
    method: str,
    url: str,
    *,
    high_priority: bool = False,
    **kwargs: Any,
) -> httpx.Response:
    """Make an HTTP request with rate limit budget management."""
    for attempt in range(_MAX_RETRIES + 1):
        # Pace the request
        interval = self._budget.paced_interval(high_priority=high_priority)
        if interval > _MAX_RETRY_DELAY:
            logger.error(
                "Rate limit wait %.0fs exceeds max %ds — failing",
                interval,
                _MAX_RETRY_DELAY,
            )
            raise httpx.HTTPStatusError(
                "Rate limit exceeded",
                request=httpx.Request(method, url),
                response=httpx.Response(429),
            )
        if interval > 0:
            logger.debug("Pacing: waiting %.1fs before %s %s", interval, method, url)
            await asyncio.sleep(interval)

        response = await self.http_client.request(method, url, **kwargs)

        # Update budget from response headers
        self._budget.update_from_headers(dict(response.headers))

        if response.status_code != 429:
            response.raise_for_status()
            return response

        logger.warning(
            "429 on %s %s, attempt %d/%d",
            method, url, attempt + 1, _MAX_RETRIES + 1,
        )

    response.raise_for_status()
    return response  # unreachable
```

Mark auth-related calls as high priority:

```python
async def exchange_code(self, code: str) -> base_module.TokenResponse:
    response = await self._request("POST", SPOTIFY_TOKEN_URL, high_priority=True, data={...})

async def get_current_user(self, access_token: str) -> dict[str, str]:
    response = await self._request("GET", f"{SPOTIFY_API_BASE}/me", high_priority=True, headers={...})
```

Data-fetching methods (get_followed_artists, get_saved_tracks, get_recently_played) remain normal priority (default).

**Step 2: Update tests**

The mock transport tests should still pass since the budget manager adds sleep(0) for high priority and a small default interval otherwise. For tests, the budget will have no data so it uses `default_interval=0.2`. Override the budget's default in test fixtures to avoid sleeps:

```python
@pytest.fixture
def connector(settings):
    c = spotify_module.SpotifyConnector(settings=settings)
    c._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    return c
```

**Step 3: Run tests, lint, type check**

Run: `uv run pytest -q && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 4: Commit**

```bash
git add src/resonance/connectors/spotify.py tests/test_spotify_connector.py
git commit -m "refactor: retrofit Spotify connector with rate limit budget manager"
```

---

### Task 3: Generalize Data Models for Multi-Service Support

**Files:**
- Modify: `src/resonance/connectors/base.py`
- Modify: `tests/test_connectors.py`

The current base models are named `SpotifyArtistData` and `SpotifyTrackData` but they're generic enough for any service. Rename them to `ArtistData` and `TrackData`.

**Step 1: Rename models in base.py**

```python
class ArtistData(pydantic.BaseModel):
    """Artist data returned from a connector."""
    external_id: str
    name: str
    service: types_module.ServiceType

class TrackData(pydantic.BaseModel):
    """Track data returned from a connector."""
    external_id: str
    title: str
    artist_external_id: str
    artist_name: str
    service: types_module.ServiceType
```

Keep backward-compatible aliases:

```python
# Backward compatibility
SpotifyArtistData = ArtistData
SpotifyTrackData = TrackData
```

**Step 2: Update imports across codebase**

Search for all uses of `SpotifyArtistData` and `SpotifyTrackData` and replace with `ArtistData` / `TrackData`. The aliases ensure nothing breaks during the transition. Key files to update:
- `src/resonance/connectors/spotify.py`
- `src/resonance/sync/runner.py`
- `tests/test_spotify_connector.py`
- `tests/test_sync_runner.py`
- `tests/test_connectors.py`

**Step 3: Run all tests**

Run: `uv run pytest -q`
Expected: All pass.

**Step 4: Remove backward-compatible aliases once all references updated**

Remove `SpotifyArtistData = ArtistData` and `SpotifyTrackData = TrackData` from base.py.

**Step 5: Run lint, type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 6: Commit**

```bash
git add src/resonance/connectors/base.py src/resonance/connectors/spotify.py src/resonance/sync/runner.py tests/
git commit -m "refactor: rename SpotifyArtistData/SpotifyTrackData to ArtistData/TrackData"
```

---

### Task 4: Config + MusicBrainz OAuth Credentials

**Files:**
- Modify: `src/resonance/config.py`
- Modify: `tests/test_config.py`

**Step 1: Add MusicBrainz OAuth settings**

Add to `Settings`:

```python
# MusicBrainz OAuth (for ListenBrainz auth)
musicbrainz_client_id: str = ""
musicbrainz_client_secret: str = ""
musicbrainz_redirect_uri: str = "http://localhost:8000/api/v1/auth/listenbrainz/callback"
```

**Step 2: Add test**

```python
def test_settings_has_musicbrainz_credentials() -> None:
    settings = config_module.Settings()
    assert hasattr(settings, "musicbrainz_client_id")
    assert hasattr(settings, "musicbrainz_client_secret")
    assert hasattr(settings, "musicbrainz_redirect_uri")
```

**Step 3: Run tests, lint, commit**

```bash
git add src/resonance/config.py tests/test_config.py
git commit -m "feat: add MusicBrainz OAuth config for ListenBrainz connector"
```

---

### Task 5: ListenBrainz Connector

**Files:**
- Create: `src/resonance/connectors/listenbrainz.py`
- Test: `tests/test_listenbrainz_connector.py`

**Step 1: Write the failing tests**

```python
import httpx
import pytest

import resonance.connectors.base as base_module
import resonance.connectors.listenbrainz as lb_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.config as config_module


@pytest.fixture
def settings() -> config_module.Settings:
    return config_module.Settings(
        musicbrainz_client_id="test-client-id",
        musicbrainz_client_secret="test-client-secret",
        musicbrainz_redirect_uri="http://localhost:8000/api/v1/auth/listenbrainz/callback",
    )


@pytest.fixture
def connector(settings: config_module.Settings) -> lb_module.ListenBrainzConnector:
    c = lb_module.ListenBrainzConnector(settings=settings)
    c._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    return c


class TestProperties:
    def test_service_type(self, connector: lb_module.ListenBrainzConnector) -> None:
        from resonance.types import ServiceType
        assert connector.service_type == ServiceType.LISTENBRAINZ

    def test_capabilities(self, connector: lb_module.ListenBrainzConnector) -> None:
        caps = connector.capabilities
        assert base_module.ConnectorCapability.AUTHENTICATION in caps
        assert base_module.ConnectorCapability.LISTENING_HISTORY in caps


class TestOAuth:
    def test_get_auth_url_musicbrainz(
        self, connector: lb_module.ListenBrainzConnector
    ) -> None:
        url = connector.get_auth_url(state="random-state")
        assert "musicbrainz.org/oauth2/authorize" in url
        assert "test-client-id" in url
        assert "random-state" in url

    async def test_exchange_code(
        self, connector: lb_module.ListenBrainzConnector
    ) -> None:
        mock_response = httpx.Response(
            200,
            json={
                "access_token": "mb-access-123",
                "refresh_token": "mb-refresh-456",
                "expires_in": 3600,
                "token_type": "Bearer",
            },
        )
        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return mock_response

        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        )
        result = await connector.exchange_code("auth-code")
        assert result.access_token == "mb-access-123"

    async def test_get_current_user(
        self, connector: lb_module.ListenBrainzConnector
    ) -> None:
        mock_response = httpx.Response(
            200,
            json={
                "sub": "musicbrainz-user-id",
                "metabrainz_user_id": 12345,
                "musicbrainz_id": "testuser",
            },
        )
        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return mock_response

        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        )
        result = await connector.get_current_user("mb-access-token")
        assert result["id"] == "testuser"
        assert result["display_name"] == "testuser"


class TestListeningHistory:
    async def test_get_listens(
        self, connector: lb_module.ListenBrainzConnector
    ) -> None:
        mock_response = httpx.Response(
            200,
            json={
                "payload": {
                    "count": 2,
                    "listens": [
                        {
                            "listened_at": 1711900000,
                            "track_metadata": {
                                "track_name": "Alison",
                                "artist_name": "Slowdive",
                                "additional_info": {
                                    "recording_mbid": "rec-mbid-1",
                                    "artist_mbids": ["art-mbid-1"],
                                },
                            },
                        },
                        {
                            "listened_at": 1711899000,
                            "track_metadata": {
                                "track_name": "Blue Skied An' Clear",
                                "artist_name": "Slowdive",
                                "additional_info": {
                                    "recording_mbid": "rec-mbid-2",
                                    "artist_mbids": ["art-mbid-1"],
                                },
                            },
                        },
                    ],
                }
            },
        )
        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return mock_response

        connector._http_client = httpx.AsyncClient(
            transport=httpx.MockTransport(mock_handler)
        )
        events = await connector.get_listens("testuser")
        assert len(events) == 2
        assert events[0].track.title == "Alison"
        assert events[0].track.artist_name == "Slowdive"
        assert events[0].listened_at == 1711900000
```

**Step 2: Write the connector**

Create `src/resonance/connectors/listenbrainz.py`:

```python
"""ListenBrainz connector with MusicBrainz OAuth and listening history."""

import asyncio
import logging
import urllib.parse
from typing import Any

import httpx
import pydantic

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module

logger = logging.getLogger(__name__)

MUSICBRAINZ_AUTH_URL = "https://musicbrainz.org/oauth2/authorize"
MUSICBRAINZ_TOKEN_URL = "https://musicbrainz.org/oauth2/token"
MUSICBRAINZ_USERINFO_URL = "https://musicbrainz.org/oauth2/userinfo"
LISTENBRAINZ_API_BASE = "https://api.listenbrainz.org/1"

_MAX_RETRIES = 3
_MAX_RETRY_DELAY = 30


class ListenBrainzListenItem(pydantic.BaseModel):
    """A listen event from ListenBrainz."""

    track: base_module.TrackData
    listened_at: int  # UNIX timestamp


class ListenBrainzConnector(base_module.BaseConnector):
    """Connector for ListenBrainz via MusicBrainz OAuth."""

    service_type = types_module.ServiceType.LISTENBRAINZ
    capabilities = frozenset(
        {
            base_module.ConnectorCapability.AUTHENTICATION,
            base_module.ConnectorCapability.LISTENING_HISTORY,
        }
    )

    def __init__(self, settings: config_module.Settings) -> None:
        self._client_id = settings.musicbrainz_client_id
        self._client_secret = settings.musicbrainz_client_secret
        self._redirect_uri = settings.musicbrainz_redirect_uri
        self._http_client: httpx.AsyncClient | None = None
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.2)

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient()
        return self._http_client

    async def _request(
        self,
        method: str,
        url: str,
        *,
        high_priority: bool = False,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make an HTTP request with rate limit budget management."""
        for attempt in range(_MAX_RETRIES + 1):
            interval = self._budget.paced_interval(high_priority=high_priority)
            if interval > _MAX_RETRY_DELAY:
                logger.error(
                    "Rate limit wait %.0fs exceeds max %ds — failing",
                    interval, _MAX_RETRY_DELAY,
                )
                raise httpx.HTTPStatusError(
                    "Rate limit exceeded",
                    request=httpx.Request(method, url),
                    response=httpx.Response(429),
                )
            if interval > 0:
                await asyncio.sleep(interval)

            response = await self.http_client.request(method, url, **kwargs)
            self._budget.update_from_headers(dict(response.headers))

            if response.status_code != 429:
                response.raise_for_status()
                return response

            logger.warning(
                "429 on %s %s, attempt %d/%d",
                method, url, attempt + 1, _MAX_RETRIES + 1,
            )

        response.raise_for_status()
        return response  # unreachable

    def get_auth_url(self, state: str) -> str:
        """Build MusicBrainz OAuth authorization URL."""
        params = urllib.parse.urlencode(
            {
                "response_type": "code",
                "client_id": self._client_id,
                "redirect_uri": self._redirect_uri,
                "scope": "profile",
                "state": state,
                "access_type": "offline",
            }
        )
        return f"{MUSICBRAINZ_AUTH_URL}?{params}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        """Exchange auth code for MusicBrainz OAuth tokens."""
        response = await self._request(
            "POST",
            MUSICBRAINZ_TOKEN_URL,
            high_priority=True,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._redirect_uri,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        data = response.json()
        return base_module.TokenResponse(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            expires_in=data.get("expires_in"),
            scope=data.get("scope"),
        )

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        """Get the MusicBrainz user profile (username = LB username)."""
        response = await self._request(
            "GET",
            MUSICBRAINZ_USERINFO_URL,
            high_priority=True,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        data = response.json()
        username = data["musicbrainz_id"]
        return {"id": username, "display_name": username}

    async def get_listens(
        self,
        username: str,
        *,
        max_ts: int | None = None,
        min_ts: int | None = None,
        count: int = 100,
    ) -> list[ListenBrainzListenItem]:
        """Fetch listening history for a user.

        Args:
            username: ListenBrainz/MusicBrainz username.
            max_ts: Return listens before this UNIX timestamp.
            min_ts: Return listens after this UNIX timestamp.
            count: Number of listens per page (max 100).
        """
        params: dict[str, str | int] = {"count": count}
        if max_ts is not None:
            params["max_ts"] = max_ts
        if min_ts is not None:
            params["min_ts"] = min_ts

        response = await self._request(
            "GET",
            f"{LISTENBRAINZ_API_BASE}/user/{username}/listens",
            params=params,
        )
        data = response.json()

        items: list[ListenBrainzListenItem] = []
        for listen in data["payload"]["listens"]:
            meta = listen["track_metadata"]
            additional = meta.get("additional_info", {})
            items.append(
                ListenBrainzListenItem(
                    track=base_module.TrackData(
                        external_id=additional.get("recording_mbid", ""),
                        title=meta["track_name"],
                        artist_external_id=(
                            additional.get("artist_mbids", [""])[0]
                            if additional.get("artist_mbids")
                            else ""
                        ),
                        artist_name=meta["artist_name"],
                        service=types_module.ServiceType.LISTENBRAINZ,
                    ),
                    listened_at=listen["listened_at"],
                )
            )

        return items
```

**Step 3: Run tests, lint, type check, commit**

```bash
git add src/resonance/connectors/listenbrainz.py tests/test_listenbrainz_connector.py
git commit -m "feat: add ListenBrainz connector with MusicBrainz OAuth and listen history"
```

---

### Task 6: Register ListenBrainz Connector + UI Updates

**Files:**
- Modify: `src/resonance/app.py`
- Modify: `src/resonance/templates/login.html`
- Modify: `src/resonance/templates/account.html`

**Step 1: Register connector in app factory**

Add to `create_app()`:

```python
import resonance.connectors.listenbrainz as listenbrainz_module

connector_registry.register(
    listenbrainz_module.ListenBrainzConnector(settings=settings)
)
```

**Step 2: Add ListenBrainz login button**

Update `login.html` to show both options:

```html
<a href="/api/v1/auth/spotify" role="button">Connect with Spotify</a>
<a href="/api/v1/auth/listenbrainz" role="button" class="outline">Connect with ListenBrainz</a>
```

**Step 3: Add ListenBrainz connect button to account page**

Update `account.html` "Connect Another Service" section:

```html
<h2>Connect Another Service</h2>
<p>
    <a href="/api/v1/auth/spotify" role="button">Connect Spotify</a>
    <a href="/api/v1/auth/listenbrainz" role="button" class="outline">Connect ListenBrainz</a>
</p>
```

**Step 4: Run tests, lint, commit**

```bash
git add src/resonance/app.py src/resonance/templates/login.html src/resonance/templates/account.html
git commit -m "feat: register ListenBrainz connector and add UI connect buttons"
```

---

### Task 7: ListenBrainz Sync Runner

**Files:**
- Modify: `src/resonance/sync/runner.py`
- Modify: `tests/test_sync_runner.py`

The sync runner currently only handles Spotify's data format. Generalize it to support ListenBrainz.

**Step 1: Add ListenBrainz sync path**

The `SyncableConnector` Protocol needs updating. Rather than a single Protocol, check the connector's service_type and call the appropriate methods. ListenBrainz sync:
1. Gets the username from `ServiceConnection.external_user_id`
2. Calls `connector.get_listens(username, max_ts=...)` in a loop, paginating backward
3. For each listen, upserts Artist (matching by MBID or name) and Track, then creates ListeningEvent

**Step 2: Update entity resolution for MBID matching**

Modify `_upsert_artist` to check for MBID match across services:

```python
async def _upsert_artist(session, artist_data):
    # First: check service_links for this service's ID
    # (existing behavior)

    # Second: if artist has an MBID, check if any existing artist
    # has the same MBID in their service_links
    if artist_data.service == ServiceType.LISTENBRAINZ and artist_data.external_id:
        # Check for MBID match in any service's links
        stmt = sa.select(Artist).where(
            Artist.service_links["listenbrainz"].as_string() == artist_data.external_id
        )
        # ... also check if a Spotify artist has this MBID stored

    # Third: fall back to exact name match
    stmt = sa.select(Artist).where(Artist.name == artist_data.name)
```

The full implementation should:
1. Look up by service-specific ID in service_links (exact match, existing behavior)
2. If not found and we have an MBID, look up by MBID in service_links
3. If not found, look up by exact artist name
4. If found by name or MBID, merge the new service_links into the existing record
5. If not found at all, create new

Same pattern for `_upsert_track`.

**Step 3: Add sync function for ListenBrainz**

```python
async def _sync_listenbrainz(
    job: models_module.SyncJob,
    connector: listenbrainz_module.ListenBrainzConnector,
    session: AsyncSession,
    username: str,
) -> tuple[int, int]:
    """Sync listening history from ListenBrainz."""
    items_created = 0
    items_updated = 0
    max_ts: int | None = None

    while True:
        listens = await connector.get_listens(
            username, max_ts=max_ts, count=100
        )
        if not listens:
            break

        for listen in listens:
            await _upsert_artist_from_track(session, listen.track)
            created = await _upsert_track(session, listen.track)
            if created:
                items_created += 1
            else:
                items_updated += 1

            played_at = datetime.datetime.fromtimestamp(
                listen.listened_at, tz=datetime.UTC
            ).isoformat()
            await _upsert_listening_event(
                session, job.user_id, listen.track, played_at,
            )

        max_ts = listens[-1].listened_at
        await session.commit()  # commit per page to avoid huge transactions

    return items_created, items_updated
```

Update `run_sync` to dispatch based on connector type:

```python
async def run_sync(job, connector, session, access_token):
    # ... set status to RUNNING ...

    if isinstance(connector, listenbrainz_module.ListenBrainzConnector):
        # For LB, access_token is not used for API calls —
        # we use the username (stored as external_user_id)
        # The caller should pass the username as access_token
        # or we refactor the signature
        items_created, items_updated = await _sync_listenbrainz(
            job, connector, session, access_token
        )
    else:
        # Existing Spotify sync path
        ...
```

Note: The `access_token` parameter is overloaded for ListenBrainz (it's actually the username). A cleaner approach is to pass a dict of connector-specific params, but for now this works.

Actually, better approach: look up the ServiceConnection in the sync runner to get the external_user_id:

```python
connection_stmt = sa.select(models_module.ServiceConnection).where(
    models_module.ServiceConnection.id == job.service_connection_id
)
connection = (await session.execute(connection_stmt)).scalar_one()
username = connection.external_user_id
```

This way the sync trigger doesn't need to change its signature.

**Step 4: Run tests, lint, type check, commit**

```bash
git add src/resonance/sync/runner.py tests/test_sync_runner.py
git commit -m "feat: add ListenBrainz sync with MBID-based entity resolution"
```

---

### Task 8: Kubernetes Secrets for MusicBrainz OAuth

**Files:**
- Modify: `megadoomer-config` sealed secrets

**Step 1: Seal MusicBrainz credentials**

Create a sealed secret adding `MUSICBRAINZ_CLIENT_ID` and `MUSICBRAINZ_CLIENT_SECRET` to the `resonance-app-secrets` SealedSecret.

```bash
kubectl --context=megadoomer-do create secret generic resonance-app-secrets \
  --namespace=resonance \
  --dry-run=client -o yaml \
  --from-literal=SESSION_SECRET_KEY='...' \
  --from-literal=TOKEN_ENCRYPTION_KEY='...' \
  --from-literal=SPOTIFY_CLIENT_ID='...' \
  --from-literal=SPOTIFY_CLIENT_SECRET='...' \
  --from-literal=MUSICBRAINZ_CLIENT_ID='gcVueg0z3KNOvFFGG0oWAPyIzs0bEK__' \
  --from-literal=MUSICBRAINZ_CLIENT_SECRET='zZIPjWVzrBEoHR9OgXs2mfld-gz2SPXn' | \
  kubeseal --controller-name=sealed-secrets-controller \
           --controller-namespace=sealed-secrets \
           --context=megadoomer-do --format=yaml
```

**Step 2: Add redirect URI to helm-values.yaml**

Add to the main container env:

```yaml
MUSICBRAINZ_REDIRECT_URI: "https://resonance.megadoomer.io/api/v1/auth/listenbrainz/callback"
```

**Step 3: Commit to megadoomer-config**

```bash
cd ~/src/github.com/megadoomer-io/megadoomer-config
git add applications/resonance/resonance/do/
git commit -m "feat(resonance): add MusicBrainz OAuth credentials for ListenBrainz"
```

---

## Summary

After all 8 tasks:

| Component | Status |
|-----------|--------|
| Rate limit budget manager | Shared utility with priority lanes, header parsing for LB + Spotify |
| Spotify connector | Retrofitted with budget manager, auth calls high-priority |
| Data models | ArtistData/TrackData renamed from Spotify-specific names |
| ListenBrainz connector | MusicBrainz OAuth + ListenBrainz listen history API |
| Sync runner | Generalized for LB, MBID-based entity resolution, per-page commits |
| UI | Login + account pages show ListenBrainz connect option |
| K8s secrets | MusicBrainz OAuth credentials sealed and deployed |

**Not in scope (future):**
- ListenBrainz feedback (love/hate)
- MusicBrainz metadata connector
- Token refresh automation
- Cross-service entity resolution beyond MBID + exact name match
