"""Tests for connector display metadata and dynamic login page."""

from __future__ import annotations

import resonance.connectors.concert_archives as concert_archives_module
import resonance.connectors.ical as ical_module
import resonance.connectors.registry as registry_module
import resonance.connectors.songkick as songkick_module
import resonance.connectors.test as test_module
import resonance.types as types_module
import resonance.ui.common as common


class TestConnectorDisplayMetadata:
    def test_spotify_display_name(self) -> None:
        from resonance.connectors import spotify as spotify_module

        assert spotify_module.SpotifyConnector.display_name == "Spotify"

    def test_lastfm_display_name(self) -> None:
        from resonance.connectors import lastfm as lastfm_module

        assert lastfm_module.LastFmConnector.display_name == "Last.fm"

    def test_listenbrainz_display_name(self) -> None:
        from resonance.connectors import listenbrainz as lb_module

        assert lb_module.ListenBrainzConnector.display_name == "ListenBrainz"

    def test_songkick_display_name(self) -> None:
        assert songkick_module.SongkickConnector.display_name == "Songkick"

    def test_ical_display_name(self) -> None:
        assert ical_module.ICalConnector.display_name == "iCal"

    def test_concert_archives_display_name(self) -> None:
        c = concert_archives_module.ConcertArchivesConnector
        assert c.display_name == "Concert Archives"

    def test_test_connector_display_name(self) -> None:
        assert test_module.TestConnector.display_name == "Test"

    def test_spotify_icon(self) -> None:
        from resonance.connectors import spotify as spotify_module

        assert spotify_module.SpotifyConnector.icon == "music"

    def test_spotify_color(self) -> None:
        from resonance.connectors import spotify as spotify_module

        assert spotify_module.SpotifyConnector.color == "var(--color-spotify)"

    def test_songkick_color_empty(self) -> None:
        assert songkick_module.SongkickConnector.color == ""


class TestRegistryDisplayMethods:
    def _make_registry(self) -> registry_module.ConnectorRegistry:
        registry = registry_module.ConnectorRegistry()
        registry.register(songkick_module.SongkickConnector())
        registry.register(ical_module.ICalConnector())
        registry.register(test_module.TestConnector())
        return registry

    def test_display_name_from_registry(self) -> None:
        registry = self._make_registry()
        assert registry.display_name(types_module.ServiceType.SONGKICK) == "Songkick"

    def test_icon_from_registry(self) -> None:
        registry = self._make_registry()
        assert registry.icon(types_module.ServiceType.SONGKICK) == "ticket"

    def test_color_from_registry(self) -> None:
        registry = self._make_registry()
        assert registry.color(types_module.ServiceType.SONGKICK) == ""

    def test_unregistered_service_fallback_name(self) -> None:
        registry = self._make_registry()
        name = registry.display_name(types_module.ServiceType.BANDCAMP)
        assert name == "Bandcamp"

    def test_unregistered_service_fallback_icon(self) -> None:
        registry = self._make_registry()
        assert registry.icon(types_module.ServiceType.BANDCAMP) == "link"


class TestTemplateFiltersWithRegistry:
    def test_service_name_filter_uses_registry(self) -> None:
        registry = registry_module.ConnectorRegistry()
        registry.register(songkick_module.SongkickConnector())
        common.set_connector_registry(registry)

        assert common._service_name("songkick") == "Songkick"

    def test_service_name_filter_fallback(self) -> None:
        common.set_connector_registry(registry_module.ConnectorRegistry())
        assert common._service_name("bandcamp") == "Bandcamp"

    def test_service_icon_filter_uses_registry(self) -> None:
        registry = registry_module.ConnectorRegistry()
        registry.register(songkick_module.SongkickConnector())
        common.set_connector_registry(registry)

        assert common._service_icon("songkick") == "ticket"

    def test_service_color_filter_uses_registry(self) -> None:
        from resonance.connectors import spotify as spotify_module

        settings = __import__("resonance.config", fromlist=["Settings"]).Settings(
            spotify_client_id="x", spotify_client_secret="x"
        )
        registry = registry_module.ConnectorRegistry()
        registry.register(spotify_module.SpotifyConnector(settings=settings))
        common.set_connector_registry(registry)

        assert common._service_color("spotify") == "var(--color-spotify)"
