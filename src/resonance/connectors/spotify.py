"""Spotify connector with OAuth and data fetching."""

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

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"

_SCOPES = (
    "user-read-recently-played "
    "user-follow-read "
    "user-library-read "
    "user-read-email "
    "user-read-private"
)


class PlayedTrackItem(pydantic.BaseModel):
    """A track with its played_at timestamp from recently played."""

    track: base_module.TrackData
    played_at: str


class SpotifyConnector(base_module.BaseConnector):
    """Connector for the Spotify Web API."""

    service_type = types_module.ServiceType.SPOTIFY
    capabilities = frozenset(
        {
            base_module.ConnectorCapability.AUTHENTICATION,
            base_module.ConnectorCapability.LISTENING_HISTORY,
            base_module.ConnectorCapability.FOLLOWS,
            base_module.ConnectorCapability.TRACK_RATINGS,
        }
    )

    def __init__(self, settings: config_module.Settings) -> None:
        self._client_id = settings.spotify_client_id
        self._client_secret = settings.spotify_client_secret
        self._redirect_uri = settings.spotify_redirect_uri
        self._http_client: httpx.AsyncClient | None = None
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.2)

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazily create and return the HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=30.0)
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
        """Build Spotify OAuth authorization URL."""
        params = urllib.parse.urlencode(
            {
                "client_id": self._client_id,
                "response_type": "code",
                "redirect_uri": self._redirect_uri,
                "scope": _SCOPES,
                "state": state,
            }
        )
        return f"{SPOTIFY_AUTH_URL}?{params}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        """Exchange an authorization code for access and refresh tokens."""
        logger.info("Exchanging OAuth code for tokens")
        response = await self._request(
            "POST",
            SPOTIFY_TOKEN_URL,
            high_priority=True,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._redirect_uri,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        logger.info("Token exchange successful")
        return base_module.TokenResponse.model_validate(response.json())

    async def refresh_access_token(
        self, refresh_token: str
    ) -> base_module.TokenResponse:
        """Refresh an expired access token."""
        logger.info("Refreshing access token")
        response = await self._request(
            "POST",
            SPOTIFY_TOKEN_URL,
            high_priority=True,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        logger.info("Token refresh successful")
        return base_module.TokenResponse.model_validate(response.json())

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        """Get the current user's profile."""
        logger.info("Fetching current user profile")
        response = await self._request(
            "GET",
            f"{SPOTIFY_API_BASE}/me",
            high_priority=True,
            headers={"Authorization": f"Bearer {access_token}"},
        )
        data: dict[str, str] = response.json()
        logger.info("Got user profile: %s", data.get("id", "unknown"))
        return {"id": data["id"], "display_name": data["display_name"]}

    async def get_followed_artists(
        self, access_token: str
    ) -> list[base_module.ArtistData]:
        """Paginate through followed artists."""
        logger.info("Fetching followed artists")
        artists: list[base_module.ArtistData] = []
        after: str | None = None
        headers = {"Authorization": f"Bearer {access_token}"}

        while True:
            params: dict[str, str | int] = {"type": "artist", "limit": 50}
            if after is not None:
                params["after"] = after

            response = await self._request(
                "GET",
                f"{SPOTIFY_API_BASE}/me/following",
                headers=headers,
                params=params,
            )
            data = response.json()

            for item in data["artists"]["items"]:
                artists.append(
                    base_module.ArtistData(
                        external_id=item["id"],
                        name=item["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    )
                )

            cursor_after = data["artists"]["cursors"]["after"]
            if cursor_after is None:
                break
            after = cursor_after

        logger.info("Fetched %d followed artists", len(artists))
        return artists

    async def get_saved_tracks(self, access_token: str) -> list[base_module.TrackData]:
        """Paginate through saved (liked) tracks."""
        logger.info("Fetching saved tracks")
        tracks: list[base_module.TrackData] = []
        url: str | None = f"{SPOTIFY_API_BASE}/me/tracks"
        headers = {"Authorization": f"Bearer {access_token}"}

        while url is not None:
            response = await self._request(
                "GET",
                url,
                headers=headers,
                params={"limit": 50},
            )
            data = response.json()

            for item in data["items"]:
                track = item["track"]
                primary_artist = track["artists"][0]
                tracks.append(
                    base_module.TrackData(
                        external_id=track["id"],
                        title=track["name"],
                        artist_external_id=primary_artist["id"],
                        artist_name=primary_artist["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    )
                )

            url = data.get("next")

        logger.info("Fetched %d saved tracks", len(tracks))
        return tracks

    async def get_recently_played(self, access_token: str) -> list[PlayedTrackItem]:
        """Get recently played tracks."""
        logger.info("Fetching recently played tracks")
        response = await self._request(
            "GET",
            f"{SPOTIFY_API_BASE}/me/player/recently-played",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"limit": 50},
        )
        data = response.json()

        items: list[PlayedTrackItem] = []
        for item in data["items"]:
            track = item["track"]
            primary_artist = track["artists"][0]
            items.append(
                PlayedTrackItem(
                    track=base_module.TrackData(
                        external_id=track["id"],
                        title=track["name"],
                        artist_external_id=primary_artist["id"],
                        artist_name=primary_artist["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    ),
                    played_at=item["played_at"],
                )
            )

        logger.info("Fetched %d recently played tracks", len(items))
        return items
