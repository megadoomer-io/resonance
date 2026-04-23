"""Tests for the connector framework."""

from unittest.mock import AsyncMock

import httpx
import pytest

import resonance.connectors.base as base_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.connectors.registry as registry_module
import resonance.types as types_module


class FakeConnector(base_module.BaseConnector):
    """Concrete test implementation of BaseConnector."""

    service_type = types_module.ServiceType.SPOTIFY
    capabilities = frozenset({base_module.ConnectorCapability.FOLLOWS})

    def __init__(self) -> None:
        self._http_client = None
        self._budget = ratelimit_module.RateLimitBudget()

    @staticmethod
    def connection_config() -> base_module.ConnectionConfig:
        return base_module.ConnectionConfig(
            auth_type="oauth",
            sync_function="plan_sync",
            sync_style="incremental",
        )

    def get_auth_url(self, state: str) -> str:
        return f"https://fake.example.com/auth?state={state}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        return base_module.TokenResponse(access_token="fake-token")

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        return {"id": "fake-user", "display_name": "Fake User"}


class TestConnectorCapability:
    """Tests for ConnectorCapability enum."""

    def test_listening_history_value(self) -> None:
        assert base_module.ConnectorCapability.LISTENING_HISTORY == "listening_history"

    def test_authentication_value(self) -> None:
        assert base_module.ConnectorCapability.AUTHENTICATION == "authentication"

    def test_all_capabilities_are_strings(self) -> None:
        for cap in base_module.ConnectorCapability:
            assert isinstance(cap, str)


class TestBaseConnector:
    """Tests for BaseConnector ABC."""

    def test_has_capability_true(self) -> None:
        connector = FakeConnector()
        assert connector.has_capability(base_module.ConnectorCapability.FOLLOWS) is True

    def test_has_capability_false(self) -> None:
        connector = FakeConnector()
        assert (
            connector.has_capability(base_module.ConnectorCapability.LISTENING_HISTORY)
            is False
        )


class TestTokenResponse:
    """Tests for TokenResponse model."""

    def test_minimal(self) -> None:
        token = base_module.TokenResponse(access_token="abc123")
        assert token.access_token == "abc123"
        assert token.refresh_token is None
        assert token.expires_in is None
        assert token.scope is None

    def test_full(self) -> None:
        token = base_module.TokenResponse(
            access_token="abc",
            refresh_token="def",
            expires_in=3600,
            scope="read",
        )
        assert token.refresh_token == "def"
        assert token.expires_in == 3600
        assert token.scope == "read"


class TestArtistData:
    """Tests for ArtistData model."""

    def test_creation(self) -> None:
        artist = base_module.ArtistData(
            external_id="abc",
            name="Test Artist",
            service=types_module.ServiceType.SPOTIFY,
        )
        assert artist.external_id == "abc"
        assert artist.name == "Test Artist"
        assert artist.service == types_module.ServiceType.SPOTIFY


class TestTrackData:
    """Tests for TrackData model."""

    def test_creation(self) -> None:
        track = base_module.TrackData(
            external_id="track1",
            title="Test Track",
            artist_external_id="artist1",
            artist_name="Test Artist",
            service=types_module.ServiceType.SPOTIFY,
        )
        assert track.external_id == "track1"
        assert track.title == "Test Track"
        assert track.artist_external_id == "artist1"
        assert track.artist_name == "Test Artist"


class TestConnectorRegistry:
    """Tests for ConnectorRegistry."""

    def test_register_and_retrieve(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        result = registry.get(types_module.ServiceType.SPOTIFY)
        assert result is connector

    def test_get_unknown_service_returns_none(self) -> None:
        registry = registry_module.ConnectorRegistry()
        result = registry.get(types_module.ServiceType.LASTFM)
        assert result is None

    def test_get_by_capability(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        results = registry.get_by_capability(base_module.ConnectorCapability.FOLLOWS)
        assert len(results) == 1
        assert results[0] is connector

    def test_get_by_capability_no_matches(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        results = registry.get_by_capability(
            base_module.ConnectorCapability.LISTENING_HISTORY
        )
        assert results == []

    def test_all(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = FakeConnector()
        registry.register(connector)
        assert registry.all() == [connector]

    def test_all_empty(self) -> None:
        registry = registry_module.ConnectorRegistry()
        assert registry.all() == []


class TestRequestRateLimitCap:
    """Tests for _request rate limit handling."""

    @pytest.mark.asyncio
    async def test_raises_when_retry_after_exceeds_cap(self) -> None:
        """A 429 with Retry-After above _MAX_RATE_LIMIT_WAIT raises."""
        connector = FakeConnector()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.request.return_value = httpx.Response(
            429, headers={"Retry-After": "9999"}
        )
        connector._http_client = mock_client

        with pytest.raises(base_module.RateLimitExceededError) as exc_info:
            await connector._request("GET", "https://api.example.com/test")

        assert exc_info.value.retry_after == 9999.0
        assert exc_info.value.max_wait == 120.0

    @pytest.mark.asyncio
    async def test_retries_when_retry_after_within_cap(self) -> None:
        """A 429 with Retry-After within cap retries and succeeds."""
        connector = FakeConnector()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        request = httpx.Request("GET", "https://api.example.com/test")
        mock_client.request.side_effect = [
            httpx.Response(429, headers={"Retry-After": "0"}, request=request),
            httpx.Response(200, json={"ok": True}, request=request),
        ]
        connector._http_client = mock_client

        response = await connector._request("GET", "https://api.example.com/test")
        assert response.status_code == 200
        assert mock_client.request.call_count == 2
