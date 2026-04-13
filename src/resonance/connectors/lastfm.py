"""Last.fm connector with web auth and scrobble API."""

import hashlib
import urllib.parse
from typing import Any

import httpx
import structlog

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module

logger = structlog.get_logger()

LASTFM_AUTH_URL = "https://www.last.fm/api/auth/"
LASTFM_API_BASE = "https://ws.audioscrobbler.com/2.0/"


class LastFmApiError(Exception):
    """Raised when the Last.fm API returns an error response."""

    def __init__(self, code: int, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"Last.fm API error {code}: {message}")


class LastFmConnector(base_module.BaseConnector):
    """Connector for the Last.fm API with web authentication."""

    service_type = types_module.ServiceType.LASTFM
    capabilities = frozenset(
        {
            base_module.ConnectorCapability.AUTHENTICATION,
            base_module.ConnectorCapability.LISTENING_HISTORY,
            base_module.ConnectorCapability.TRACK_RATINGS,
        }
    )

    def __init__(self, settings: config_module.Settings) -> None:
        self._api_key = settings.lastfm_api_key
        self._shared_secret = settings.lastfm_shared_secret
        self._callback_url = f"{settings.base_url}/api/v1/auth/lastfm/callback"
        self._http_client = None
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.2)

    def _sign_params(self, params: dict[str, str]) -> str:
        """Generate md5 API signature for Last.fm requests.

        Sorts all params alphabetically by key, concatenates key+value pairs,
        appends the shared secret, and returns the md5 hex digest.
        The ``format`` param is excluded from the signature per Last.fm spec.

        Args:
            params: Request parameters to sign.

        Returns:
            Hex-encoded md5 signature string.
        """
        filtered = {k: v for k, v in params.items() if k != "format"}
        raw = "".join(f"{k}{v}" for k, v in sorted(filtered.items()))
        raw += self._shared_secret
        return hashlib.md5(raw.encode()).hexdigest()

    async def _api_call(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        signed: bool = False,
    ) -> dict[str, Any]:
        """Make a GET request to the Last.fm API.

        Args:
            method: Last.fm API method name (e.g. ``user.getInfo``).
            params: Additional query parameters.
            signed: If True, compute and include ``api_sig``.

        Returns:
            Parsed JSON response as a dictionary.

        Raises:
            LastFmApiError: If the response contains an ``error`` key.
        """
        request_params: dict[str, Any] = {
            "method": method,
            "api_key": self._api_key,
            "format": "json",
        }
        if params:
            request_params.update(params)

        if signed:
            # Build string params for signing (format is excluded internally)
            sign_params = {k: str(v) for k, v in request_params.items()}
            request_params["api_sig"] = self._sign_params(sign_params)

        # Retry on server errors (5xx) — Last.fm occasionally returns 500
        import asyncio as _asyncio

        last_exc: Exception | None = None
        for attempt in range(4):
            try:
                response = await self._request(
                    "GET", LASTFM_API_BASE, params=request_params
                )
                break
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code >= 500 and attempt < 3:
                    wait = 2**attempt  # 1, 2, 4 seconds
                    logger.warning(
                        "lastfm_server_error",
                        status=exc.response.status_code,
                        attempt=attempt + 1,
                        retry_in=wait,
                    )
                    await _asyncio.sleep(wait)
                    last_exc = exc
                    continue
                raise
        else:
            if last_exc:
                raise last_exc

        data: dict[str, Any] = response.json()

        if "error" in data:
            raise LastFmApiError(
                code=int(data["error"]),
                message=str(data.get("message", "Unknown error")),
            )

        return data

    def get_auth_url(self, state: str) -> str:
        """Build the Last.fm web authentication URL.

        Note: Last.fm does not support a state parameter.
        The ``state`` argument is accepted for interface compatibility
        but is not included in the URL.

        Args:
            state: Ignored (Last.fm auth does not support state).

        Returns:
            Authorization URL string.
        """
        params = urllib.parse.urlencode(
            {
                "api_key": self._api_key,
                "cb": self._callback_url,
            }
        )
        return f"{LASTFM_AUTH_URL}?{params}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        """Exchange an auth token for a permanent session key.

        Last.fm uses a ``token`` (received via callback) exchanged through
        the ``auth.getSession`` signed API call. Session keys are permanent
        and do not expire.

        Args:
            code: The auth token from the Last.fm callback.

        Returns:
            TokenResponse with session key as access_token.
        """
        logger.info("Exchanging Last.fm auth token for session key")
        data = await self._api_call(
            "auth.getSession",
            params={"token": code},
            signed=True,
        )
        session_key: str = data["session"]["key"]
        logger.info("Last.fm session key obtained")
        return base_module.TokenResponse(access_token=session_key)

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        """Get the current user's Last.fm profile.

        Args:
            access_token: Last.fm session key.

        Returns:
            Dict with ``id`` (username) and ``display_name`` (realname or username).
        """
        logger.info("Fetching Last.fm user profile")
        data = await self._api_call(
            "user.getInfo",
            params={"sk": access_token},
            signed=True,
        )
        user = data["user"]
        username: str = user["name"]
        realname: str = user.get("realname", "")
        display_name = realname if realname else username
        logger.info("Got Last.fm user: %s", username)
        return {"id": username, "display_name": display_name}

    async def get_recent_tracks(
        self,
        username: str,
        page: int = 1,
        limit: int = 200,
        from_ts: int | None = None,
    ) -> dict[str, Any]:
        """Fetch recent tracks for a Last.fm user.

        Returns the raw API response; the sync strategy will parse it.

        Args:
            username: Last.fm username.
            page: Page number (1-indexed).
            limit: Maximum tracks per page.
            from_ts: Only return tracks after this Unix timestamp.

        Returns:
            Raw Last.fm API response dict.
        """
        params: dict[str, Any] = {
            "user": username,
            "page": page,
            "limit": limit,
        }
        if from_ts is not None:
            params["from"] = from_ts
        return await self._api_call("user.getRecentTracks", params=params)

    async def get_loved_tracks(
        self,
        username: str,
        page: int = 1,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Fetch loved tracks for a Last.fm user.

        Returns the raw API response; the sync strategy will parse it.

        Args:
            username: Last.fm username.
            page: Page number (1-indexed).
            limit: Maximum tracks per page.

        Returns:
            Raw Last.fm API response dict.
        """
        params: dict[str, Any] = {
            "user": username,
            "page": page,
            "limit": limit,
        }
        return await self._api_call("user.getLovedTracks", params=params)
