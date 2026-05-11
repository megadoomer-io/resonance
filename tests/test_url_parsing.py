"""Tests for connector URL parsing — each connector recognizes its own service URLs."""

import resonance.connectors.lastfm as lastfm_module
import resonance.connectors.listenbrainz as lb_module
import resonance.connectors.spotify as spotify_module


class TestListenBrainzParseUrl:
    """parse_url recognizes MusicBrainz and ListenBrainz artist URLs."""

    def test_musicbrainz_artist_url(self) -> None:
        url = "https://musicbrainz.org/artist/cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        assert (
            lb_module.ListenBrainzConnector.parse_url(url)
            == "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        )

    def test_listenbrainz_artist_url(self) -> None:
        url = "https://listenbrainz.org/artist/cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        assert (
            lb_module.ListenBrainzConnector.parse_url(url)
            == "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        )

    def test_musicbrainz_with_trailing_slash(self) -> None:
        url = "https://musicbrainz.org/artist/cc197bad-dc9c-440d-a5b5-d52ba2e14234/"
        assert (
            lb_module.ListenBrainzConnector.parse_url(url)
            == "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        )

    def test_non_matching_url(self) -> None:
        assert lb_module.ListenBrainzConnector.parse_url("https://example.com") is None

    def test_musicbrainz_non_artist_url(self) -> None:
        assert (
            lb_module.ListenBrainzConnector.parse_url(
                "https://musicbrainz.org/release/abc"
            )
            is None
        )

    def test_listenbrainz_non_artist_url(self) -> None:
        assert (
            lb_module.ListenBrainzConnector.parse_url(
                "https://listenbrainz.org/user/someone"
            )
            is None
        )

    def test_musicbrainz_with_query_params(self) -> None:
        url = "https://musicbrainz.org/artist/cc197bad-dc9c-440d-a5b5-d52ba2e14234?ref=search"
        assert (
            lb_module.ListenBrainzConnector.parse_url(url)
            == "cc197bad-dc9c-440d-a5b5-d52ba2e14234"
        )


class TestSpotifyParseUrl:
    """SpotifyConnector.parse_url recognizes open.spotify.com artist URLs."""

    def test_spotify_artist_url(self) -> None:
        url = "https://open.spotify.com/artist/4gzpq5DPGxSnKTe4SA8HAU"
        assert (
            spotify_module.SpotifyConnector.parse_url(url) == "4gzpq5DPGxSnKTe4SA8HAU"
        )

    def test_spotify_artist_url_with_query(self) -> None:
        url = "https://open.spotify.com/artist/4gzpq5DPGxSnKTe4SA8HAU?si=abc123"
        assert (
            spotify_module.SpotifyConnector.parse_url(url) == "4gzpq5DPGxSnKTe4SA8HAU"
        )

    def test_spotify_artist_url_with_trailing_slash(self) -> None:
        url = "https://open.spotify.com/artist/4gzpq5DPGxSnKTe4SA8HAU/"
        assert (
            spotify_module.SpotifyConnector.parse_url(url) == "4gzpq5DPGxSnKTe4SA8HAU"
        )

    def test_non_matching_url(self) -> None:
        assert spotify_module.SpotifyConnector.parse_url("https://example.com") is None

    def test_spotify_non_artist_url(self) -> None:
        assert (
            spotify_module.SpotifyConnector.parse_url(
                "https://open.spotify.com/track/abc"
            )
            is None
        )

    def test_spotify_album_url(self) -> None:
        assert (
            spotify_module.SpotifyConnector.parse_url(
                "https://open.spotify.com/album/abc"
            )
            is None
        )


class TestLastFmParseUrl:
    """LastFmConnector.parse_url recognizes last.fm/music/<artist> URLs."""

    def test_lastfm_artist_url(self) -> None:
        url = "https://www.last.fm/music/My+Morning+Jacket"
        assert lastfm_module.LastFmConnector.parse_url(url) == "My Morning Jacket"

    def test_lastfm_artist_url_without_www(self) -> None:
        url = "https://last.fm/music/Radiohead"
        assert lastfm_module.LastFmConnector.parse_url(url) == "Radiohead"

    def test_lastfm_artist_url_with_percent_encoding(self) -> None:
        url = "https://www.last.fm/music/My%20Morning%20Jacket"
        assert lastfm_module.LastFmConnector.parse_url(url) == "My Morning Jacket"

    def test_lastfm_artist_url_with_trailing_slash(self) -> None:
        url = "https://www.last.fm/music/Radiohead/"
        assert lastfm_module.LastFmConnector.parse_url(url) == "Radiohead"

    def test_lastfm_non_music_url(self) -> None:
        assert (
            lastfm_module.LastFmConnector.parse_url("https://www.last.fm/user/someone")
            is None
        )

    def test_non_matching_url(self) -> None:
        assert lastfm_module.LastFmConnector.parse_url("https://example.com") is None

    def test_lastfm_artist_subpage_ignored(self) -> None:
        """Sub-paths under /music/<artist>/ are not artist URLs."""
        url = "https://www.last.fm/music/Radiohead/+albums"
        assert lastfm_module.LastFmConnector.parse_url(url) is None

    def test_lastfm_artist_with_special_chars(self) -> None:
        url = "https://www.last.fm/music/Guns+N%27+Roses"
        assert lastfm_module.LastFmConnector.parse_url(url) == "Guns N' Roses"
