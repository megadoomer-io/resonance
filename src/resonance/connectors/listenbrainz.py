"""ListenBrainz connector with MusicBrainz OAuth and listen history."""

import urllib.parse
from typing import Any

import pydantic
import structlog

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module

logger = structlog.get_logger()

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
        self._http_client = None
        self._budget = ratelimit_module.RateLimitBudget(default_interval=0.2)

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
        max_retries: int | None = None,
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
            max_retries=max_retries,
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
                        duration_ms=additional_info.get("duration_ms"),
                    ),
                    listened_at=listen["listened_at"],
                )
            )

        logger.info("Fetched %d listens for user %s", len(items), username)
        return items
