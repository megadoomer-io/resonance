"""Tests for ConnectionConfig and connector-declared configuration."""

import resonance.connectors.base as base_module
import resonance.connectors.ical as ical_module
import resonance.connectors.registry as registry_module
import resonance.connectors.songkick as songkick_module
import resonance.types as types_module


class TestConnectionConfig:
    """Tests for the ConnectionConfig dataclass."""

    def test_config_has_required_fields(self) -> None:
        """Construct a ConnectionConfig and verify all fields are accessible."""
        config = base_module.ConnectionConfig(
            auth_type="oauth",
            sync_function="plan_sync",
            sync_style="incremental",
        )
        assert config.auth_type == "oauth"
        assert config.sync_function == "plan_sync"
        assert config.sync_style == "incremental"
        assert config.derive_urls is None

    def test_config_with_derive_urls(self) -> None:
        """ConnectionConfig accepts an optional derive_urls callable."""

        def my_deriver(username: str) -> list[str]:
            return [f"https://example.com/{username}"]

        config = base_module.ConnectionConfig(
            auth_type="username",
            sync_function="sync_calendar_feed",
            sync_style="full",
            derive_urls=my_deriver,
        )
        assert config.derive_urls is my_deriver
        assert config.derive_urls("alice") == ["https://example.com/alice"]

    def test_config_is_frozen(self) -> None:
        """ConnectionConfig should be immutable."""
        import dataclasses

        config = base_module.ConnectionConfig(
            auth_type="oauth",
            sync_function="plan_sync",
            sync_style="incremental",
        )
        with __import__("pytest").raises(dataclasses.FrozenInstanceError):
            config.auth_type = "url"  # type: ignore[misc]


class TestSongkickConfig:
    """Tests for the Songkick connector configuration."""

    def test_songkick_auth_type(self) -> None:
        config = songkick_module.SongkickConnector.connection_config()
        assert config.auth_type == "username"

    def test_songkick_sync_function(self) -> None:
        config = songkick_module.SongkickConnector.connection_config()
        assert config.sync_function == "sync_calendar_feed"

    def test_songkick_sync_style(self) -> None:
        config = songkick_module.SongkickConnector.connection_config()
        assert config.sync_style == "full"

    def test_songkick_derive_urls_produces_two_urls(self) -> None:
        config = songkick_module.SongkickConnector.connection_config()
        assert config.derive_urls is not None
        urls = config.derive_urls("testuser")
        assert len(urls) == 2

    def test_songkick_derive_urls_content(self) -> None:
        config = songkick_module.SongkickConnector.connection_config()
        assert config.derive_urls is not None
        urls = config.derive_urls("alice")
        assert (
            "https://www.songkick.com/users/alice/calendars.ics?filter=attendance"
            in urls
        )
        assert (
            "https://www.songkick.com/users/alice/calendars.ics?filter=tracked_artist"
            in urls
        )

    def test_songkick_service_type(self) -> None:
        assert (
            songkick_module.SongkickConnector.service_type
            == types_module.ServiceType.SONGKICK
        )


class TestICalConfig:
    """Tests for the iCal connector configuration."""

    def test_ical_auth_type(self) -> None:
        config = ical_module.ICalConnector.connection_config()
        assert config.auth_type == "url"

    def test_ical_sync_function(self) -> None:
        config = ical_module.ICalConnector.connection_config()
        assert config.sync_function == "sync_calendar_feed"

    def test_ical_sync_style(self) -> None:
        config = ical_module.ICalConnector.connection_config()
        assert config.sync_style == "full"

    def test_ical_no_derive_urls(self) -> None:
        config = ical_module.ICalConnector.connection_config()
        assert config.derive_urls is None

    def test_ical_service_type(self) -> None:
        assert ical_module.ICalConnector.service_type == types_module.ServiceType.ICAL


class TestSpotifyConfig:
    """Tests for the Spotify connector's ConnectionConfig."""

    def test_spotify_auth_type(self) -> None:
        """Spotify uses OAuth authentication."""
        # SpotifyConnector.connection_config() is a static method, no instance needed
        from resonance.connectors.spotify import SpotifyConnector

        config = SpotifyConnector.connection_config()
        assert config.auth_type == "oauth"

    def test_spotify_sync_function(self) -> None:
        from resonance.connectors.spotify import SpotifyConnector

        config = SpotifyConnector.connection_config()
        assert config.sync_function == "plan_sync"

    def test_spotify_sync_style(self) -> None:
        from resonance.connectors.spotify import SpotifyConnector

        config = SpotifyConnector.connection_config()
        assert config.sync_style == "incremental"


class TestRegistryGetConfig:
    """Tests for ConnectorRegistry.get_config()."""

    def test_get_config_for_registered_connector(self) -> None:
        registry = registry_module.ConnectorRegistry()
        connector = songkick_module.SongkickConnector()
        registry.register(connector)
        config = registry.get_config(types_module.ServiceType.SONGKICK)
        assert config is not None
        assert config.auth_type == "username"

    def test_get_config_returns_none_for_unknown(self) -> None:
        registry = registry_module.ConnectorRegistry()
        config = registry.get_config(types_module.ServiceType.BANDCAMP)
        assert config is None


class TestTrackDiscoveryCapability:
    """Tests for the TRACK_DISCOVERY capability and DiscoveredTrack model."""

    def test_track_discovery_exists(self) -> None:
        assert base_module.ConnectorCapability.TRACK_DISCOVERY == "track_discovery"

    def test_discovered_track_fields(self) -> None:
        track = base_module.DiscoveredTrack(
            external_id="abc123",
            title="Test Song",
            artist_name="Test Artist",
            artist_external_id="artist123",
            service=types_module.ServiceType.LISTENBRAINZ,
            popularity_score=75,
        )
        assert track.title == "Test Song"
        assert track.popularity_score == 75
        assert track.duration_ms is None

    def test_discovered_track_defaults(self) -> None:
        """Verify default values for optional fields."""
        track = base_module.DiscoveredTrack(
            external_id="xyz789",
            title="Another Song",
            artist_name="Another Artist",
            artist_external_id="artist789",
            service=types_module.ServiceType.SPOTIFY,
        )
        assert track.popularity_score == 0
        assert track.duration_ms is None

    def test_discovered_track_with_duration(self) -> None:
        """DiscoveredTrack accepts an explicit duration_ms."""
        track = base_module.DiscoveredTrack(
            external_id="abc123",
            title="Test Song",
            artist_name="Test Artist",
            artist_external_id="artist123",
            service=types_module.ServiceType.LISTENBRAINZ,
            duration_ms=240000,
        )
        assert track.duration_ms == 240000
