"""ListenBrainz connector with MusicBrainz OAuth and listen history."""

import asyncio
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

_MAX_RETRIES = 3
_MAX_RETRY_DELAY = 30  # seconds — don't wait longer than this per retry

MUSICBRAINZ_AUTH_URL = "https://musicbrainz.org/oauth2/authorize"
MUSICBRAINZ_TOKEN_URL = "https://musicbrainz.org/oauth2/token"
MUSICBRAINZ_USERINFO_URL = "https://musicbrainz.org/oauth2/userinfo"
LISTENBRAINZ_API_BASE = "https://api.listenbrainz.org/1"


class ListenBrainzListenItem(pydantic.BaseModel):
    """A track with its listened_at timestamp from ListenBrainz."""

    track: base_module.TrackData
    listened_at: int


class ListenBrainzConnector(base_module.BaseConnector):
    """Connector for the ListenBrainz API with MusicBrainz OAuth."""

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
        """Lazily create and return the HTTP client."""
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
        """Make an HTTP request with budget-aware pacing and retry on 429.

        Args:
            method: HTTP method (GET, POST, etc.).
            url: Request URL.
            high_priority: If True, skip pacing when budget is available.
            **kwargs: Additional arguments passed to httpx request.

        Returns:
            The HTTP response.

        Raises:
            httpx.HTTPStatusError: On non-429 HTTP errors or when the rate
                limit wait exceeds the maximum retry delay.
        """
        for attempt in range(_MAX_RETRIES + 1):
            interval = self._budget.paced_interval(high_priority=high_priority)
            if interval > _MAX_RETRY_DELAY:
                logger.error(
                    "Rate limit wait %.0fs exceeds max %ds",
                    interval,
                    _MAX_RETRY_DELAY,
                )
                raise httpx.HTTPStatusError(
                    "Rate limit exceeded",
                    request=httpx.Request(method, url),
                    response=httpx.Response(429),
                )
            if interval > 0:
                logger.debug(
                    "Pacing: waiting %.1fs before %s %s", interval, method, url
                )
                await asyncio.sleep(interval)

            response = await self.http_client.request(method, url, **kwargs)
            self._budget.update_from_headers(dict(response.headers))

            if response.status_code != 429:
                response.raise_for_status()
                return response

            logger.warning(
                "429 on %s %s, attempt %d/%d",
                method,
                url,
                attempt + 1,
                _MAX_RETRIES + 1,
            )

        response.raise_for_status()
        return response  # unreachable, raise_for_status throws

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
        """Exchange an authorization code for access and refresh tokens."""
        logger.info("Exchanging MusicBrainz OAuth code for tokens")
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
        logger.info("MusicBrainz token exchange successful")
        return base_module.TokenResponse.model_validate(response.json())

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        """Get the current user's MusicBrainz profile."""
        logger.info("Fetching MusicBrainz user profile")
        response = await self._request(
            "GET",
            MUSICBRAINZ_USERINFO_URL,
            high_priority=True,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        data: dict[str, str] = response.json()
        # MusicBrainz userinfo uses OpenID Connect 'sub' field for username
        username = data["sub"]
        logger.info("Got MusicBrainz user: %s", username)
        return {"id": username, "display_name": username}

    async def get_listen_count(self, username: str) -> int:
        """Get the total number of listens for a user."""
        response = await self._request(
            "GET",
            f"{LISTENBRAINZ_API_BASE}/user/{username}/listen-count",
        )
        data = response.json()
        count: int = data["payload"]["count"]
        return count

    async def get_listens(
        self,
        username: str,
        *,
        max_ts: int | None = None,
        min_ts: int | None = None,
        count: int = 100,
    ) -> list[ListenBrainzListenItem]:
        """Fetch listening history for a ListenBrainz user.

        Args:
            username: ListenBrainz username.
            max_ts: Only return listens with listened_at less than this value.
            min_ts: Only return listens with listened_at greater than this value.
            count: Maximum number of listens to return.

        Returns:
            List of listen items with track data and timestamps.
        """
        logger.info("Fetching listens for user %s", username)
        params: dict[str, int] = {"count": count}
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
            metadata = listen["track_metadata"]
            additional_info: dict[str, Any] = metadata.get("additional_info", {})

            recording_mbid = additional_info.get("recording_mbid", "")
            artist_mbids: list[str] = additional_info.get("artist_mbids", [])
            first_artist_mbid = artist_mbids[0] if artist_mbids else ""

            items.append(
                ListenBrainzListenItem(
                    track=base_module.TrackData(
                        external_id=recording_mbid,
                        title=metadata["track_name"],
                        artist_external_id=first_artist_mbid,
                        artist_name=metadata["artist_name"],
                        service=types_module.ServiceType.LISTENBRAINZ,
                    ),
                    listened_at=listen["listened_at"],
                )
            )

        logger.info("Fetched %d listens for user %s", len(items), username)
        return items
