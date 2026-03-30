"""Spotify connector with OAuth and data fetching."""

import urllib.parse

import httpx
import pydantic

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.types as types_module

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

    track: base_module.SpotifyTrackData
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

    @property
    def http_client(self) -> httpx.AsyncClient:
        """Lazily create and return the HTTP client."""
        if self._http_client is None:
            self._http_client = httpx.AsyncClient()
        return self._http_client

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
        response = await self.http_client.post(
            SPOTIFY_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._redirect_uri,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        response.raise_for_status()
        return base_module.TokenResponse.model_validate(response.json())

    async def refresh_access_token(
        self, refresh_token: str
    ) -> base_module.TokenResponse:
        """Refresh an expired access token."""
        response = await self.http_client.post(
            SPOTIFY_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        response.raise_for_status()
        return base_module.TokenResponse.model_validate(response.json())

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        """Get the current user's profile."""
        response = await self.http_client.get(
            f"{SPOTIFY_API_BASE}/me",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        data: dict[str, str] = response.json()
        return {"id": data["id"], "display_name": data["display_name"]}

    async def get_followed_artists(
        self, access_token: str
    ) -> list[base_module.SpotifyArtistData]:
        """Paginate through followed artists."""
        artists: list[base_module.SpotifyArtistData] = []
        after: str | None = None
        headers = {"Authorization": f"Bearer {access_token}"}

        while True:
            params: dict[str, str | int] = {"type": "artist", "limit": 50}
            if after is not None:
                params["after"] = after

            response = await self.http_client.get(
                f"{SPOTIFY_API_BASE}/me/following",
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            data = response.json()

            for item in data["artists"]["items"]:
                artists.append(
                    base_module.SpotifyArtistData(
                        external_id=item["id"],
                        name=item["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    )
                )

            cursor_after = data["artists"]["cursors"]["after"]
            if cursor_after is None:
                break
            after = cursor_after

        return artists

    async def get_saved_tracks(
        self, access_token: str
    ) -> list[base_module.SpotifyTrackData]:
        """Paginate through saved (liked) tracks."""
        tracks: list[base_module.SpotifyTrackData] = []
        url: str | None = f"{SPOTIFY_API_BASE}/me/tracks"
        headers = {"Authorization": f"Bearer {access_token}"}

        while url is not None:
            response = await self.http_client.get(
                url,
                headers=headers,
                params={"limit": 50},
            )
            response.raise_for_status()
            data = response.json()

            for item in data["items"]:
                track = item["track"]
                primary_artist = track["artists"][0]
                tracks.append(
                    base_module.SpotifyTrackData(
                        external_id=track["id"],
                        title=track["name"],
                        artist_external_id=primary_artist["id"],
                        artist_name=primary_artist["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    )
                )

            url = data.get("next")

        return tracks

    async def get_recently_played(self, access_token: str) -> list[PlayedTrackItem]:
        """Get recently played tracks."""
        response = await self.http_client.get(
            f"{SPOTIFY_API_BASE}/me/player/recently-played",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"limit": 50},
        )
        response.raise_for_status()
        data = response.json()

        items: list[PlayedTrackItem] = []
        for item in data["items"]:
            track = item["track"]
            primary_artist = track["artists"][0]
            items.append(
                PlayedTrackItem(
                    track=base_module.SpotifyTrackData(
                        external_id=track["id"],
                        title=track["name"],
                        artist_external_id=primary_artist["id"],
                        artist_name=primary_artist["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    ),
                    played_at=item["played_at"],
                )
            )

        return items
