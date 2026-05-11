"""Spotify connector with OAuth and data fetching."""

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

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"

_SCOPES = (
    "user-read-recently-played "
    "user-follow-read "
    "user-library-read "
    "user-read-email "
    "user-read-private "
    "playlist-modify-private"
)


class PlayedTrackItem(pydantic.BaseModel):
    """A track with its played_at timestamp from recently played."""

    track: base_module.TrackData
    played_at: str


class SavedTrackItem(pydantic.BaseModel):
    """A saved track with its added_at timestamp."""

    track: base_module.TrackData
    added_at: str


class SavedTrackPage(pydantic.BaseModel):
    """A page of saved tracks with total count."""

    items: list[SavedTrackItem]
    total: int
    next_url: str | None


class SpotifyConnector(base_module.BaseConnector):
    """Connector for the Spotify Web API."""

    service_type = types_module.ServiceType.SPOTIFY
    capabilities = frozenset(
        {
            base_module.ConnectorCapability.AUTHENTICATION,
            base_module.ConnectorCapability.LISTENING_HISTORY,
            base_module.ConnectorCapability.FOLLOWS,
            base_module.ConnectorCapability.TRACK_RATINGS,
            base_module.ConnectorCapability.PLAYLIST_WRITE,
        }
    )

    @staticmethod
    def parse_url(url: str) -> str | None:
        """Extract a Spotify artist ID from an open.spotify.com URL.

        Args:
            url: An absolute URL to inspect.

        Returns:
            The Spotify artist ID if the URL is a recognized artist page,
            or ``None`` otherwise.
        """
        parsed = urllib.parse.urlparse(url)
        if parsed.hostname != "open.spotify.com":
            return None
        parts = parsed.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "artist":
            return parts[1]
        return None

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        """Return the connection configuration for Spotify."""
        return base_module.ConnectionConfig(
            auth_type="oauth",
            sync_function="plan_sync",
            sync_style="incremental",
        )

    def __init__(self, settings: config_module.Settings) -> None:
        self._client_id = settings.spotify_client_id
        self._client_secret = settings.spotify_client_secret
        self._redirect_uri = settings.spotify_redirect_uri
        self._http_client = None
        self._budget = ratelimit_module.RateLimitBudget(
            default_interval=5.0,
            window_seconds=30,
            window_ceiling=10,
        )

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
        self, access_token: str, *, after: str | None = None
    ) -> list[base_module.ArtistData]:
        """Paginate through followed artists."""
        logger.info("Fetching followed artists")
        artists: list[base_module.ArtistData] = []
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

    async def get_saved_tracks_page(
        self,
        access_token: str,
        *,
        url: str | None = None,
        limit: int = 50,
    ) -> SavedTrackPage:
        """Fetch one page of saved tracks."""
        target_url = url or f"{SPOTIFY_API_BASE}/me/tracks"
        headers = {"Authorization": f"Bearer {access_token}"}
        params: dict[str, str | int] | None = (
            None if url is not None else {"limit": limit}
        )
        response = await self._request(
            "GET",
            target_url,
            headers=headers,
            params=params,
        )
        data = response.json()

        items: list[SavedTrackItem] = []
        for item in data["items"]:
            track = item["track"]
            primary_artist = track["artists"][0]
            items.append(
                SavedTrackItem(
                    track=base_module.TrackData(
                        external_id=track["id"],
                        title=track["name"],
                        artist_external_id=primary_artist["id"],
                        artist_name=primary_artist["name"],
                        service=types_module.ServiceType.SPOTIFY,
                        duration_ms=track.get("duration_ms"),
                    ),
                    added_at=item["added_at"],
                )
            )

        return SavedTrackPage(
            items=items,
            total=data["total"],
            next_url=data.get("next"),
        )

    async def get_recently_played(
        self, access_token: str, *, after: str | None = None
    ) -> list[PlayedTrackItem]:
        """Get recently played tracks, optionally only after a timestamp."""
        logger.info("Fetching recently played tracks")
        params: dict[str, str | int] = {"limit": 50}
        if after is not None:
            params["after"] = after
        response = await self._request(
            "GET",
            f"{SPOTIFY_API_BASE}/me/player/recently-played",
            headers={"Authorization": f"Bearer {access_token}"},
            params=params,
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
                        duration_ms=track.get("duration_ms"),
                    ),
                    played_at=item["played_at"],
                )
            )

        logger.info("Fetched %d recently played tracks", len(items))
        return items

    async def create_playlist(
        self,
        access_token: str,
        name: str,
        description: str = "",
    ) -> str:
        """Create a playlist on the user's Spotify account.

        Spotify dev mode ignores ``public=False``, so playlists are always
        public.  We send ``public=True`` to match the actual behavior.
        """
        response = await self._request(
            "POST",
            f"{SPOTIFY_API_BASE}/me/playlists",
            headers={"Authorization": f"Bearer {access_token}"},
            json={
                "name": name,
                "description": description,
                "public": True,
            },
        )
        data: dict[str, str] = response.json()
        logger.info("spotify_playlist_created", playlist_id=data["id"])
        return data["id"]

    async def add_tracks_to_playlist(
        self,
        access_token: str,
        playlist_id: str,
        uris: list[str],
    ) -> None:
        """Add tracks to a Spotify playlist, batching if over 100."""
        headers = {"Authorization": f"Bearer {access_token}"}
        for i in range(0, len(uris), 100):
            batch = uris[i : i + 100]
            await self._request(
                "POST",
                f"{SPOTIFY_API_BASE}/playlists/{playlist_id}/items",
                headers=headers,
                json={"uris": batch},
            )
        logger.info("spotify_tracks_added", playlist_id=playlist_id, count=len(uris))

    async def replace_playlist_tracks(
        self,
        access_token: str,
        playlist_id: str,
        uris: list[str],
    ) -> None:
        """Replace all tracks in a Spotify playlist."""
        await self._request(
            "PUT",
            f"{SPOTIFY_API_BASE}/playlists/{playlist_id}/items",
            headers={"Authorization": f"Bearer {access_token}"},
            json={"uris": uris},
        )
        logger.info("spotify_tracks_replaced", playlist_id=playlist_id, count=len(uris))

    async def search_track(
        self,
        access_token: str,
        title: str,
        artist_name: str,
    ) -> str | None:
        """Search Spotify for a track by title and artist. Returns track ID or None."""
        query = f"track:{title} artist:{artist_name}"
        response = await self._request(
            "GET",
            f"{SPOTIFY_API_BASE}/search",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": query, "type": "track", "limit": 1},
        )
        data = response.json()
        items = data.get("tracks", {}).get("items", [])
        if not items:
            return None
        result: str = items[0]["id"]
        return result

    async def get_artist_by_id(
        self,
        access_token: str,
        artist_id: str,
    ) -> dict[str, Any] | None:
        """Fetch a single artist from Spotify by ID."""
        try:
            response = await self._request(
                "GET",
                f"{SPOTIFY_API_BASE}/artists/{artist_id}",
                headers={"Authorization": f"Bearer {access_token}"},
                high_priority=True,
            )
        except httpx.HTTPStatusError:
            return None
        data: dict[str, Any] = response.json()
        return {"spotify_id": data["id"], "name": data["name"]}

    async def search_artists(
        self,
        access_token: str,
        query: str,
        *,
        limit: int = 10,
    ) -> list[dict[str, Any]]:
        """Search Spotify for artists by name.

        Args:
            access_token: OAuth access token.
            query: Artist name search query.
            limit: Maximum number of results to return.

        Returns:
            List of dicts with ``spotify_id`` and ``name`` keys.
        """
        response = await self._request(
            "GET",
            f"{SPOTIFY_API_BASE}/search",
            headers={"Authorization": f"Bearer {access_token}"},
            params={"q": query, "type": "artist", "limit": limit},
            high_priority=True,
        )
        data = response.json()
        results: list[dict[str, Any]] = []
        for artist in data.get("artists", {}).get("items", []):
            results.append(
                {
                    "spotify_id": artist["id"],
                    "name": artist["name"],
                }
            )
        return results
