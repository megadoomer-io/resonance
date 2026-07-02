"""Tests for the ListenBrainz artist-tags client (#136 genre model)."""

from __future__ import annotations

import pytest

import resonance.config as config_module
import resonance.services.artist_tags as artist_tags_module


def _settings() -> config_module.Settings:
    return config_module.Settings(mbid_mapper_batch_size=2)


def _client() -> artist_tags_module.ArtistTagsClient:
    return artist_tags_module.ArtistTagsClient(_settings())


# A trimmed real-shape response (verified live against the LB artist endpoint).
_RADIOHEAD = "a74b1b7f-71a5-4011-9441-d0b5e4122711"
_PAYLOAD = [
    {
        "artist_mbid": _RADIOHEAD,
        "name": "Radiohead",
        "tag": {
            "artist": [
                {"artist_mbid": _RADIOHEAD, "count": 2, "tag": "alternative"},
                {
                    "artist_mbid": _RADIOHEAD,
                    "count": 18,
                    "genre_mbid": "0e3fc579-2d24-4f20-9dae-736e1ec78798",
                    "tag": "rock",
                },
            ]
        },
    }
]


class TestParse:
    def test_keys_by_artist_mbid_and_extracts_fields(self) -> None:
        out = artist_tags_module.ArtistTagsClient._parse(_PAYLOAD)
        assert set(out) == {_RADIOHEAD}
        tags = {t.tag: t for t in out[_RADIOHEAD]}
        assert tags["rock"].count == 18
        assert tags["rock"].genre_mbid == "0e3fc579-2d24-4f20-9dae-736e1ec78798"
        # Folksonomy tag: no genre_mbid.
        assert tags["alternative"].genre_mbid is None

    def test_missing_tag_block_yields_empty_list(self) -> None:
        out = artist_tags_module.ArtistTagsClient._parse(
            [{"artist_mbid": _RADIOHEAD, "name": "Radiohead"}]
        )
        assert out == {_RADIOHEAD: []}

    def test_entry_without_mbid_is_skipped(self) -> None:
        out = artist_tags_module.ArtistTagsClient._parse(
            [{"name": "no mbid", "tag": {"artist": [{"tag": "x", "count": 1}]}}]
        )
        assert out == {}

    def test_falls_back_to_mbid_field(self) -> None:
        out = artist_tags_module.ArtistTagsClient._parse(
            [{"mbid": _RADIOHEAD, "tag": {"artist": []}}]
        )
        assert out == {_RADIOHEAD: []}

    def test_tag_without_name_is_dropped(self) -> None:
        out = artist_tags_module.ArtistTagsClient._parse(
            [{"artist_mbid": _RADIOHEAD, "tag": {"artist": [{"count": 3}]}}]
        )
        assert out == {_RADIOHEAD: []}

    def test_non_list_payload_raises_transient(self) -> None:
        with pytest.raises(artist_tags_module.ArtistTagsUnavailableError):
            artist_tags_module.ArtistTagsClient._parse({"error": "nope"})

    def test_duplicate_tag_deduped_last_wins(self) -> None:
        # Guards the (artist_id, tag) unique constraint against a duplicate tag
        # in the source response.
        out = artist_tags_module.ArtistTagsClient._parse(
            [
                {
                    "artist_mbid": _RADIOHEAD,
                    "tag": {
                        "artist": [
                            {"tag": "rock", "count": 1},
                            {"tag": "rock", "count": 9, "genre_mbid": "g"},
                        ]
                    },
                }
            ]
        )
        tags = out[_RADIOHEAD]
        assert len(tags) == 1
        assert tags[0].count == 9  # last wins
        assert tags[0].genre_mbid == "g"

    def test_overlong_tag_truncated_to_column_width(self) -> None:
        long_tag = "x" * 400
        out = artist_tags_module.ArtistTagsClient._parse(
            [{"artist_mbid": _RADIOHEAD, "tag": {"artist": [{"tag": long_tag}]}}]
        )
        assert len(out[_RADIOHEAD][0].tag) == 256


class TestFetchTagsChunking:
    @pytest.mark.anyio()
    async def test_splits_into_batches_and_merges(self) -> None:
        client = _client()  # batch size 2
        seen_chunks: list[list[str]] = []

        async def fake_chunk(chunk: list[str]) -> dict[str, list[object]]:
            seen_chunks.append(chunk)
            return {m: [] for m in chunk}

        client._fetch_chunk = fake_chunk  # type: ignore[method-assign]
        out = await client.fetch_tags(["a", "b", "c"])

        assert seen_chunks == [["a", "b"], ["c"]]
        assert set(out) == {"a", "b", "c"}

    @pytest.mark.anyio()
    async def test_empty_input_returns_empty(self) -> None:
        assert await _client().fetch_tags([]) == {}
