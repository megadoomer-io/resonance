"""Tests for the Spotify connector."""

import urllib.parse

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.connectors.spotify as spotify_module
import resonance.types as types_module


def _make_settings() -> config_module.Settings:
    """Create test settings with Spotify credentials."""
    return config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        spotify_redirect_path="/callback",
    )


class TestSpotifyConnectorProperties:
    """Tests for SpotifyConnector service_type and capabilities."""

    def test_service_type(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        assert connector.service_type == types_module.ServiceType.SPOTIFY

    def test_capabilities(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        expected = frozenset(
            {
                base_module.ConnectorCapability.AUTHENTICATION,
                base_module.ConnectorCapability.LISTENING_HISTORY,
                base_module.ConnectorCapability.FOLLOWS,
                base_module.ConnectorCapability.TRACK_RATINGS,
            }
        )
        assert connector.capabilities == expected

    def test_is_base_connector(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        assert isinstance(connector, base_module.BaseConnector)


class TestGetAuthUrl:
    """Tests for get_auth_url."""

    def test_contains_client_id(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        url = connector.get_auth_url(state="test-state")
        assert "client_id=test-client-id" in url

    def test_contains_state(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        url = connector.get_auth_url(state="my-state")
        assert "state=my-state" in url

    def test_contains_response_type_code(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert "response_type=code" in url

    def test_contains_scopes(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        scopes = params["scope"][0].split(" ")
        assert "user-read-recently-played" in scopes
        assert "user-follow-read" in scopes
        assert "user-library-read" in scopes
        assert "user-read-email" in scopes
        assert "user-read-private" in scopes

    def test_starts_with_spotify_auth_url(self) -> None:
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert url.startswith(spotify_module.SPOTIFY_AUTH_URL)


class TestExchangeCode:
    """Tests for exchange_code."""

    @pytest.mark.anyio()
    async def test_returns_token_response(self) -> None:
        token_data = {
            "access_token": "access-abc",
            "refresh_token": "refresh-xyz",
            "expires_in": 3600,
            "scope": "user-read-email",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/api/token"
            return httpx.Response(200, json=token_data)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = spotify_module.SpotifyConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.exchange_code(code="auth-code-123")

        assert isinstance(result, base_module.TokenResponse)
        assert result.access_token == "access-abc"
        assert result.refresh_token == "refresh-xyz"
        assert result.expires_in == 3600


class TestRefreshAccessToken:
    """Tests for refresh_access_token."""

    @pytest.mark.anyio()
    async def test_returns_token_response(self) -> None:
        token_data = {
            "access_token": "new-access",
            "refresh_token": "new-refresh",
            "expires_in": 3600,
            "scope": "user-read-email",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            body = request.content.decode()
            assert "grant_type=refresh_token" in body
            assert "refresh_token=old-refresh" in body
            return httpx.Response(200, json=token_data)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = spotify_module.SpotifyConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.refresh_access_token(refresh_token="old-refresh")

        assert isinstance(result, base_module.TokenResponse)
        assert result.access_token == "new-access"


class TestGetCurrentUser:
    """Tests for get_current_user."""

    @pytest.mark.anyio()
    async def test_returns_user_dict(self) -> None:
        user_data = {
            "id": "user123",
            "display_name": "Test User",
            "email": "test@example.com",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/me"
            assert request.headers["Authorization"] == "Bearer my-token"
            return httpx.Response(200, json=user_data)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = spotify_module.SpotifyConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_current_user(access_token="my-token")

        assert result == {"id": "user123", "display_name": "Test User"}


class TestGetFollowedArtists:
    """Tests for get_followed_artists."""

    @pytest.mark.anyio()
    async def test_parses_spotify_response(self) -> None:
        api_response = {
            "artists": {
                "items": [
                    {"id": "art1", "name": "Artist One"},
                    {"id": "art2", "name": "Artist Two"},
                ],
                "cursors": {"after": None},
                "total": 2,
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert "type=artist" in str(request.url)
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = spotify_module.SpotifyConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_followed_artists(access_token="token")

        assert len(result) == 2
        assert isinstance(result[0], base_module.ArtistData)
        assert result[0].external_id == "art1"
        assert result[0].name == "Artist One"
        assert result[0].service == types_module.ServiceType.SPOTIFY
        assert result[1].external_id == "art2"
        assert result[1].name == "Artist Two"

    @pytest.mark.anyio()
    async def test_paginates_through_results(self) -> None:
        page1 = {
            "artists": {
                "items": [{"id": "art1", "name": "Artist One"}],
                "cursors": {"after": "cursor-abc"},
                "total": 2,
            }
        }
        page2 = {
            "artists": {
                "items": [{"id": "art2", "name": "Artist Two"}],
                "cursors": {"after": None},
                "total": 2,
            }
        }
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(200, json=page1)
            return httpx.Response(200, json=page2)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = spotify_module.SpotifyConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_followed_artists(access_token="token")

        assert len(result) == 2
        assert call_count == 2


class TestGetSavedTracks:
    """Tests for get_saved_tracks."""

    @pytest.mark.anyio()
    async def test_parses_spotify_response(self) -> None:
        api_response = {
            "items": [
                {
                    "track": {
                        "id": "track1",
                        "name": "Song One",
                        "artists": [{"id": "art1", "name": "Artist One"}],
                    }
                },
                {
                    "track": {
                        "id": "track2",
                        "name": "Song Two",
                        "artists": [
                            {"id": "art2", "name": "Artist Two"},
                            {"id": "art3", "name": "Feat Artist"},
                        ],
                    }
                },
            ],
            "next": None,
            "total": 2,
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = spotify_module.SpotifyConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_saved_tracks(access_token="token")

        assert len(result) == 2
        assert isinstance(result[0], base_module.TrackData)
        assert result[0].external_id == "track1"
        assert result[0].title == "Song One"
        assert result[0].artist_external_id == "art1"
        assert result[0].artist_name == "Artist One"
        # Uses first artist (primary) even when multiple are present
        assert result[1].artist_external_id == "art2"
        assert result[1].artist_name == "Artist Two"


class TestGetRecentlyPlayed:
    """Tests for get_recently_played."""

    @pytest.mark.anyio()
    async def test_parses_response_with_played_at(self) -> None:
        api_response = {
            "items": [
                {
                    "track": {
                        "id": "track1",
                        "name": "Song One",
                        "artists": [{"id": "art1", "name": "Artist One"}],
                    },
                    "played_at": "2024-01-15T10:30:00.000Z",
                },
                {
                    "track": {
                        "id": "track2",
                        "name": "Song Two",
                        "artists": [{"id": "art2", "name": "Artist Two"}],
                    },
                    "played_at": "2024-01-15T09:15:00.000Z",
                },
            ],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert "limit=50" in str(request.url)
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = spotify_module.SpotifyConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_recently_played(access_token="token")

        assert len(result) == 2
        assert isinstance(result[0], spotify_module.PlayedTrackItem)
        assert result[0].track.external_id == "track1"
        assert result[0].track.title == "Song One"
        assert result[0].played_at == "2024-01-15T10:30:00.000Z"
        assert result[1].track.external_id == "track2"
        assert result[1].played_at == "2024-01-15T09:15:00.000Z"


class TestPlayedTrackItem:
    """Tests for PlayedTrackItem model."""

    def test_creation(self) -> None:
        track = base_module.TrackData(
            external_id="t1",
            title="Song",
            artist_external_id="a1",
            artist_name="Artist",
            service=types_module.ServiceType.SPOTIFY,
        )
        item = spotify_module.PlayedTrackItem(
            track=track, played_at="2024-01-15T10:30:00.000Z"
        )
        assert item.track.external_id == "t1"
        assert item.played_at == "2024-01-15T10:30:00.000Z"
