"""ListenBrainz connector with MusicBrainz OAuth and listen history."""

import urllib.parse
from typing import Any

import httpx
import pydantic
import structlog

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.services.artist_utils as artist_utils
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
    display_name = "ListenBrainz"
    icon = "headphones"
    color = "var(--color-listenbrainz)"
    capabilities = frozenset(
        {
            base_module.ConnectorCapability.AUTHN,
            base_module.ConnectorCapability.LISTENING_HISTORY,
            base_module.ConnectorCapability.TRACK_DISCOVERY,
        }
    )

    _MUSICBRAINZ_API = "https://musicbrainz.org/ws/2"
    _RECOGNIZED_HOSTS = frozenset({"musicbrainz.org", "listenbrainz.org"})

    @staticmethod
    def parse_url(url: str) -> str | None:
        """Extract a MusicBrainz artist UUID from a service URL.

        Recognizes musicbrainz.org and listenbrainz.org artist pages.

        Args:
            url: An absolute URL to inspect.

        Returns:
            The artist UUID if the URL is a recognized artist page,
            or ``None`` otherwise.
        """
        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname or ""
        if host.startswith("www."):
            host = host[4:]
        if host not in ListenBrainzConnector._RECOGNIZED_HOSTS:
            return None
        parts = parsed.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "artist":
            return parts[1]
        return None

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        """Return the connection configuration for ListenBrainz."""
        return base_module.ConnectionConfig(
            auth_type="oauth",
            sync_function="plan_sync",
            sync_style="incremental",
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

            # duration_ms is preferred; fall back to duration (seconds)
            duration_ms = additional_info.get("duration_ms")
            if duration_ms is None:
                raw_duration = additional_info.get("duration")
                if raw_duration and int(raw_duration) > 0:
                    duration_ms = int(raw_duration) * 1000

            items.append(
                ListenBrainzListenItem(
                    track=base_module.TrackData(
                        external_id=recording_mbid,
                        title=metadata["track_name"],
                        artist_external_id=first_artist_mbid,
                        artist_name=metadata["artist_name"],
                        service=types_module.ServiceType.LISTENBRAINZ,
                        duration_ms=duration_ms,
                    ),
                    listened_at=listen["listened_at"],
                )
            )

        logger.info("Fetched %d listens for user %s", len(items), username)
        return items

    async def search_artists(
        self, query: str, *, limit: int = 10
    ) -> list[dict[str, Any]]:
        """Search MusicBrainz for artists by name.

        Args:
            query: Artist name or search query.
            limit: Maximum number of results to return.

        Returns:
            List of parsed artist dicts with keys: mbid, name,
            disambiguation, artist_type, area, begin_year, end_year.
        """
        response = await self._request(
            "GET",
            f"{self._MUSICBRAINZ_API}/artist/",
            params={"query": query, "fmt": "json", "limit": limit},
        )
        results = [self._parse_mb_artist(a) for a in response.json().get("artists", [])]
        logger.info(
            "musicbrainz_artist_search",
            query=query,
            result_count=len(results),
        )
        return results

    async def get_artist_by_mbid(self, mbid: str) -> dict[str, Any] | None:
        """Fetch a single artist from MusicBrainz by MBID.

        Args:
            mbid: MusicBrainz artist identifier.

        Returns:
            Parsed artist dict, or None if the MBID is not found.
        """
        try:
            response = await self._request(
                "GET",
                f"{self._MUSICBRAINZ_API}/artist/{mbid}",
                params={"fmt": "json"},
            )
        except httpx.HTTPStatusError:
            logger.warning("musicbrainz_artist_lookup_failed", mbid=mbid)
            return None
        result = self._parse_mb_artist(response.json())
        logger.info(
            "musicbrainz_artist_lookup",
            mbid=mbid,
            name=result.get("name"),
        )
        return result

    @staticmethod
    def _parse_mb_artist(data: dict[str, Any]) -> dict[str, Any]:
        """Parse a MusicBrainz artist response into a structured dict.

        Args:
            data: Raw artist dict from the MusicBrainz API.

        Returns:
            Dict with keys: mbid, name, disambiguation, artist_type,
            area, begin_year, end_year.
        """
        life_span = data.get("life-span", {})
        begin = life_span.get("begin", "")
        end = life_span.get("end", "")
        return {
            "mbid": data["id"],
            "name": data["name"],
            "disambiguation": data.get("disambiguation", ""),
            "artist_type": data.get("type", ""),
            "area": data.get("area", {}).get("name", ""),
            "begin_year": int(begin[:4]) if begin and len(begin) >= 4 else None,
            "end_year": int(end[:4]) if end and len(end) >= 4 else None,
        }

    async def discover_tracks(
        self,
        artist_name: str,
        service_links: dict[str, str] | None,
        limit: int = 20,
    ) -> list[base_module.DiscoveredTrack]:
        """Discover tracks for an artist via MusicBrainz recordings.

        Args:
            artist_name: Name of the artist to discover tracks for.
            service_links: Optional mapping of service names to external IDs.
                If a "listenbrainz" key is present, its value is used as the
                MusicBrainz artist ID directly.
            limit: Maximum number of recordings to return.

        Returns:
            List of discovered tracks with popularity scores.
        """
        mbid = artist_utils.get_mbid(service_links)

        if not mbid:
            # Search MusicBrainz by name using reusable method
            results = await self.search_artists(artist_name, limit=1)
            if not results:
                return []
            mbid = results[0]["mbid"]

        # Fetch recordings for artist
        rec_resp = await self._request(
            "GET",
            f"{self._MUSICBRAINZ_API}/recording/",
            params={
                "artist": mbid,
                "fmt": "json",
                "limit": limit,
            },
        )
        recordings: list[dict[str, Any]] = rec_resp.json().get("recordings", [])

        return [
            base_module.DiscoveredTrack(
                external_id=rec["id"],
                title=rec["title"],
                artist_name=artist_name,
                artist_external_id=mbid,
                service=types_module.ServiceType.LISTENBRAINZ,
                duration_ms=rec.get("length"),
                popularity_score=max(0, 100 - i * 5),
            )
            for i, rec in enumerate(recordings)
        ]
