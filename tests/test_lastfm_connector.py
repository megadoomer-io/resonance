"""Tests for the Last.fm connector."""

import hashlib
import urllib.parse

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.lastfm as lastfm_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.types as types_module


def _make_settings() -> config_module.Settings:
    """Create test settings with Last.fm credentials."""
    return config_module.Settings(
        lastfm_api_key="test-api-key",
        lastfm_shared_secret="test-shared-secret",
        base_url="http://localhost:8000",
    )


def _make_connector(
    handler: httpx.MockTransport | None = None,
) -> lastfm_module.LastFmConnector:
    """Create a connector with optional mock transport and zero-delay budget."""
    settings = _make_settings()
    connector = lastfm_module.LastFmConnector(settings=settings)
    connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    if handler is not None:
        connector._http_client = httpx.AsyncClient(transport=handler)
    return connector


class TestLastFmConnectorProperties:
    """Tests for LastFmConnector service_type and capabilities."""

    def test_service_type(self) -> None:
        connector = _make_connector()
        assert connector.service_type == types_module.ServiceType.LASTFM

    def test_capabilities(self) -> None:
        connector = _make_connector()
        expected = frozenset(
            {
                base_module.ConnectorCapability.AUTHENTICATION,
                base_module.ConnectorCapability.LISTENING_HISTORY,
                base_module.ConnectorCapability.TRACK_RATINGS,
            }
        )
        assert connector.capabilities == expected

    def test_is_base_connector(self) -> None:
        connector = _make_connector()
        assert isinstance(connector, base_module.BaseConnector)


class TestSignParams:
    """Tests for _sign_params signature generation."""

    def test_sign_params_deterministic(self) -> None:
        """Same params produce the same signature."""
        connector = _make_connector()
        params = {"method": "auth.getSession", "api_key": "key123", "token": "tok456"}
        sig1 = connector._sign_params(params)
        sig2 = connector._sign_params(params)
        assert sig1 == sig2

    def test_sign_params_excludes_format(self) -> None:
        """The 'format' param must not be included in signature calculation."""
        connector = _make_connector()
        params_without_format = {"method": "auth.getSession", "api_key": "key123"}
        params_with_format = {
            "method": "auth.getSession",
            "api_key": "key123",
            "format": "json",
        }
        sig_without = connector._sign_params(params_without_format)
        sig_with = connector._sign_params(params_with_format)
        assert sig_without == sig_with

    def test_sign_params_correct_value(self) -> None:
        """Verify the signature matches manual md5 calculation."""
        connector = _make_connector()
        params = {"api_key": "abc", "method": "user.getInfo"}
        # Sorted: api_key=abc, method=user.getInfo
        # Concatenation: api_keyabcmethoduser.getInfotest-shared-secret
        raw = "api_keyabcmethoduser.getInfo" + "test-shared-secret"
        expected = hashlib.md5(raw.encode()).hexdigest()
        assert connector._sign_params(params) == expected


class TestGetAuthUrl:
    """Tests for get_auth_url."""

    def test_contains_api_key(self) -> None:
        connector = _make_connector()
        url = connector.get_auth_url(state="test-state")
        assert "api_key=test-api-key" in url

    def test_contains_callback_url(self) -> None:
        connector = _make_connector()
        url = connector.get_auth_url(state="test-state")
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        assert "cb" in params
        assert params["cb"] == ["http://localhost:8000/api/v1/auth/lastfm/callback"]

    def test_starts_with_lastfm_auth_url(self) -> None:
        connector = _make_connector()
        url = connector.get_auth_url(state="test-state")
        assert url.startswith(lastfm_module.LASTFM_AUTH_URL)


class TestExchangeCode:
    """Tests for exchange_code."""

    @pytest.mark.anyio()
    async def test_returns_token_response(self) -> None:
        session_data = {
            "session": {
                "name": "testuser",
                "key": "session-key-abc123",
                "subscriber": 0,
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            assert "method=auth.getSession" in url
            assert "token=my-auth-token" in url
            assert "api_sig=" in url
            return httpx.Response(200, json=session_data)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.exchange_code(code="my-auth-token")

        assert isinstance(result, base_module.TokenResponse)
        assert result.access_token == "session-key-abc123"
        assert result.refresh_token is None
        assert result.expires_in is None


class TestGetCurrentUser:
    """Tests for get_current_user."""

    @pytest.mark.anyio()
    async def test_returns_user_dict(self) -> None:
        user_data = {
            "user": {
                "name": "testuser",
                "realname": "Test User",
                "playcount": "12345",
                "url": "https://www.last.fm/user/testuser",
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            assert "method=user.getInfo" in url
            assert "sk=my-session-key" in url
            assert "api_sig=" in url
            return httpx.Response(200, json=user_data)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.get_current_user(access_token="my-session-key")

        assert result == {"id": "testuser", "display_name": "Test User"}

    @pytest.mark.anyio()
    async def test_fallback_display_name_to_username(self) -> None:
        """When realname is empty, display_name falls back to username."""
        user_data = {
            "user": {
                "name": "testuser",
                "realname": "",
                "playcount": "12345",
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=user_data)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.get_current_user(access_token="my-session-key")

        assert result == {"id": "testuser", "display_name": "testuser"}


class TestGetRecentTracks:
    """Tests for get_recent_tracks."""

    @pytest.mark.anyio()
    async def test_returns_raw_response(self) -> None:
        api_response = {
            "recenttracks": {
                "track": [
                    {
                        "name": "Song One",
                        "artist": {"#text": "Artist One", "mbid": "a1"},
                        "date": {"uts": "1700000000"},
                    }
                ],
                "@attr": {"page": "1", "totalPages": "1", "total": "1"},
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            assert "method=user.getRecentTracks" in url
            assert "user=testuser" in url
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.get_recent_tracks(username="testuser")

        assert "recenttracks" in result
        assert len(result["recenttracks"]["track"]) == 1

    @pytest.mark.anyio()
    async def test_passes_from_ts(self) -> None:
        api_response = {"recenttracks": {"track": [], "@attr": {"total": "0"}}}

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            assert "from=1699000000" in url
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        await connector.get_recent_tracks(username="testuser", from_ts=1699000000)


class TestGetLovedTracks:
    """Tests for get_loved_tracks."""

    @pytest.mark.anyio()
    async def test_returns_raw_response(self) -> None:
        api_response = {
            "lovedtracks": {
                "track": [
                    {
                        "name": "Loved Song",
                        "artist": {"name": "Loved Artist", "mbid": "a1"},
                        "date": {"uts": "1700000000"},
                    }
                ],
                "@attr": {"page": "1", "totalPages": "1", "total": "1"},
            }
        }

        def handler(request: httpx.Request) -> httpx.Response:
            url = str(request.url)
            assert "method=user.getLovedTracks" in url
            assert "user=testuser" in url
            return httpx.Response(200, json=api_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        result = await connector.get_loved_tracks(username="testuser")

        assert "lovedtracks" in result
        assert len(result["lovedtracks"]["track"]) == 1


class TestApiCallErrorHandling:
    """Tests for _api_call error handling."""

    @pytest.mark.anyio()
    async def test_raises_on_api_error_response(self) -> None:
        """Last.fm returns errors as 200 with an 'error' key in JSON."""
        error_response = {
            "error": 10,
            "message": "Invalid API key",
        }

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=error_response)

        transport = httpx.MockTransport(handler)
        connector = _make_connector(transport)

        with pytest.raises(lastfm_module.LastFmApiError, match="Invalid API key"):
            await connector._api_call("user.getInfo")
