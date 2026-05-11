"""Tests for artist_utils MBID helper functions."""

import resonance.services.artist_utils as artist_utils


class TestGetMbid:
    def test_reads_from_musicbrainz_nested(self) -> None:
        links = {"musicbrainz": {"id": "abc-123"}}
        assert artist_utils.get_mbid(links) == "abc-123"

    def test_falls_back_to_listenbrainz_flat(self) -> None:
        links = {"listenbrainz": "abc-123"}
        assert artist_utils.get_mbid(links) == "abc-123"

    def test_prefers_musicbrainz_over_listenbrainz(self) -> None:
        links = {"musicbrainz": {"id": "new-id"}, "listenbrainz": "old-id"}
        assert artist_utils.get_mbid(links) == "new-id"

    def test_returns_none_for_empty(self) -> None:
        assert artist_utils.get_mbid(None) is None
        assert artist_utils.get_mbid({}) is None

    def test_returns_none_for_empty_musicbrainz_dict(self) -> None:
        links: dict[str, dict[str, str]] = {"musicbrainz": {}}
        assert artist_utils.get_mbid(links) is None

    def test_returns_none_for_musicbrainz_dict_with_empty_id(self) -> None:
        links = {"musicbrainz": {"id": ""}}
        assert artist_utils.get_mbid(links) is None

    def test_ignores_non_mbid_keys(self) -> None:
        links = {"spotify": "some-id", "lastfm": "other-id"}
        assert artist_utils.get_mbid(links) is None


class TestHasMbid:
    def test_true_for_musicbrainz_nested(self) -> None:
        assert artist_utils.has_mbid({"musicbrainz": {"id": "abc"}}) is True

    def test_true_for_listenbrainz_flat(self) -> None:
        assert artist_utils.has_mbid({"listenbrainz": "abc"}) is True

    def test_false_for_empty(self) -> None:
        assert artist_utils.has_mbid(None) is False
        assert artist_utils.has_mbid({}) is False

    def test_false_for_empty_musicbrainz_dict(self) -> None:
        assert artist_utils.has_mbid({"musicbrainz": {}}) is False

    def test_false_for_non_mbid_keys(self) -> None:
        assert artist_utils.has_mbid({"spotify": "some-id"}) is False
