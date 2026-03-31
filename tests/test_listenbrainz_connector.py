"""Tests for the ListenBrainz connector."""

import urllib.parse

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module


def _make_settings() -> config_module.Settings:
    """Create test settings with MusicBrainz credentials."""
    return config_module.Settings(
        musicbrainz_client_id="test-mb-client-id",
        musicbrainz_client_secret="test-mb-client-secret",
        musicbrainz_redirect_path="/callback",
    )


class TestListenBrainzConnectorProperties:
    """Tests for ListenBrainzConnector service_type and capabilities."""

    def test_service_type(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        assert connector.service_type == types_module.ServiceType.LISTENBRAINZ

    def test_capabilities(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        expected = frozenset(
            {
                base_module.ConnectorCapability.AUTHENTICATION,
                base_module.ConnectorCapability.LISTENING_HISTORY,
            }
        )
        assert connector.capabilities == expected

    def test_is_base_connector(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        assert isinstance(connector, base_module.BaseConnector)


class TestGetAuthUrlMusicBrainz:
    """Tests for get_auth_url with MusicBrainz OAuth."""

    def test_contains_musicbrainz_domain(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="test-state")
        assert "musicbrainz.org" in url

    def test_contains_client_id(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="test-state")
        assert "client_id=test-mb-client-id" in url

    def test_contains_state(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="my-state")
        assert "state=my-state" in url

    def test_contains_response_type_code(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert "response_type=code" in url

    def test_contains_profile_scope(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert params["scope"] == ["profile"]

    def test_contains_offline_access_type(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert "access_type=offline" in url

    def test_starts_with_musicbrainz_auth_url(self) -> None:
        connector = listenbrainz_module.ListenBrainzConnector(settings=_make_settings())
        url = connector.get_auth_url(state="s")
        assert url.startswith(listenbrainz_module.MUSICBRAINZ_AUTH_URL)


class TestExchangeCode:
    """Tests for exchange_code."""

    @pytest.mark.anyio()
    async def test_returns_token_response(self) -> None:
        token_data = {
            "access_token": "access-abc",
            "refresh_token": "refresh-xyz",
            "expires_in": 3600,
            "scope": "profile",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/oauth2/token"
            body = request.content.decode()
            assert "grant_type=authorization_code" in body
            assert "code=auth-code-123" in body
            return httpx.Response(200, json=token_data)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.exchange_code(code="auth-code-123")

        assert isinstance(result, base_module.TokenResponse)
        assert result.access_token == "access-abc"
        assert result.refresh_token == "refresh-xyz"
        assert result.expires_in == 3600


class TestGetCurrentUser:
    """Tests for get_current_user."""

    @pytest.mark.anyio()
    async def test_returns_user_dict(self) -> None:
        userinfo_data = {
            "sub": "user-uuid-123",
            "musicbrainz_id": "cooluser42",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/oauth2/userinfo"
            assert request.headers["Authorization"] == "Bearer my-token"
            return httpx.Response(200, json=userinfo_data)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_current_user(access_token="my-token")

        assert result == {"id": "cooluser42", "display_name": "cooluser42"}


class TestGetListens:
    """Tests for get_listens."""

    @pytest.mark.anyio()
    async def test_parses_listens_response(self) -> None:
        api_response = {
            "payload": {
                "listens": [
                    {
                        "listened_at": 1700000000,
                        "track_metadata": {
                            "track_name": "Song One",
                            "artist_name": "Artist One",
                            "additional_info": {
                                "recording_mbid": "rec-mbid-1",
                                "artist_mbids": ["art-mbid-1"],
                            },
                        },
                    },
                    {
                        "listened_at": 1700001000,
                        "track_metadata": {
                            "track_name": "Song Two",
                            "artist_name": "Artist Two",
                            "additional_info": {
                                "recording_mbid": "rec-mbid-2",
                                "artist_mbids": ["art-mbid-2a", "art-mbid-2b"],
                            },
                        },
                    },
                ]
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            assert "/user/cooluser42/listens" in str(request.url)
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_listens(username="cooluser42")

        assert len(result) == 2
        assert isinstance(result[0], listenbrainz_module.ListenBrainzListenItem)

        assert result[0].listened_at == 1700000000
        assert result[0].track.title == "Song One"
        assert result[0].track.artist_name == "Artist One"
        assert result[0].track.external_id == "rec-mbid-1"
        assert result[0].track.artist_external_id == "art-mbid-1"
        assert result[0].track.service == types_module.ServiceType.LISTENBRAINZ

        assert result[1].listened_at == 1700001000
        assert result[1].track.title == "Song Two"
        assert result[1].track.artist_name == "Artist Two"
        assert result[1].track.external_id == "rec-mbid-2"
        # Uses first artist MBID
        assert result[1].track.artist_external_id == "art-mbid-2a"

    @pytest.mark.anyio()
    async def test_handles_missing_mbids(self) -> None:
        """Listens without recording_mbid or artist_mbids use empty strings."""
        api_response = {
            "payload": {
                "listens": [
                    {
                        "listened_at": 1700002000,
                        "track_metadata": {
                            "track_name": "Unknown Track",
                            "artist_name": "Unknown Artist",
                            "additional_info": {},
                        },
                    },
                ]
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_listens(username="user1")

        assert len(result) == 1
        assert result[0].track.external_id == ""
        assert result[0].track.artist_external_id == ""

    @pytest.mark.anyio()
    async def test_passes_query_params(self) -> None:
        """Verify max_ts, min_ts, and count are forwarded as query params."""
        api_response = {"payload": {"listens": []}}

        def handler(request: httpx.Request) -> httpx.Response:
            url_str = str(request.url)
            assert "max_ts=1700000000" in url_str
            assert "min_ts=1699000000" in url_str
            assert "count=50" in url_str
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        settings = _make_settings()
        connector = listenbrainz_module.ListenBrainzConnector(settings=settings)
        connector._http_client = httpx.AsyncClient(transport=transport)
        connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)

        result = await connector.get_listens(
            username="user1", max_ts=1700000000, min_ts=1699000000, count=50
        )

        assert result == []
