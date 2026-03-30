"""Tests for the connector framework."""

import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.types as types_module


class FakeConnector(base_module.BaseConnector):
    """Concrete test implementation of BaseConnector."""

    service_type = types_module.ServiceType.SPOTIFY
    capabilities = frozenset({base_module.ConnectorCapability.FOLLOWS})


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


class TestSpotifyArtistData:
    """Tests for SpotifyArtistData model."""

    def test_creation(self) -> None:
        artist = base_module.SpotifyArtistData(
            external_id="abc",
            name="Test Artist",
            service=types_module.ServiceType.SPOTIFY,
        )
        assert artist.external_id == "abc"
        assert artist.name == "Test Artist"
        assert artist.service == types_module.ServiceType.SPOTIFY


class TestSpotifyTrackData:
    """Tests for SpotifyTrackData model."""

    def test_creation(self) -> None:
        track = base_module.SpotifyTrackData(
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
