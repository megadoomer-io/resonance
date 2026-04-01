"""Tests for the sync runner upsert functions."""

from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.connectors.base as base_module
import resonance.sync.runner as runner_module
import resonance.types as types_module


def _make_artist_data(
    external_id: str = "art1",
    name: str = "Artist One",
    service: types_module.ServiceType = types_module.ServiceType.SPOTIFY,
) -> base_module.ArtistData:
    return base_module.ArtistData(
        external_id=external_id,
        name=name,
        service=service,
    )


def _make_track_data(
    external_id: str = "track1",
    title: str = "Song One",
    artist_external_id: str = "art1",
    artist_name: str = "Artist One",
    service: types_module.ServiceType = types_module.ServiceType.SPOTIFY,
) -> base_module.TrackData:
    return base_module.TrackData(
        external_id=external_id,
        title=title,
        artist_external_id=artist_external_id,
        artist_name=artist_name,
        service=service,
    )


class TestMBIDArtistMatching:
    """Tests for MBID-based cross-service entity resolution."""

    @pytest.mark.anyio()
    async def test_mbid_artist_matching_merges_service_links(self) -> None:
        """Artist with MBID matches existing record and merges service_links."""
        session = AsyncMock()
        existing_artist = MagicMock()
        existing_artist.name = "Existing Artist"
        existing_artist.service_links = {"musicbrainz": "mbid-123"}

        # 1. service_links["listenbrainz"] -> None (step 1)
        # 2. service_links["musicbrainz"] -> existing (step 2, skips "listenbrainz")
        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        match_result = MagicMock()
        match_result.scalar_one_or_none.return_value = existing_artist

        session.execute.side_effect = [no_result, match_result]
        session.add = MagicMock()

        artist_data = _make_artist_data(
            external_id="mbid-123",
            name="Existing Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        created = await runner_module._upsert_artist(session, artist_data)

        assert created is False
        assert existing_artist.service_links["listenbrainz"] == "mbid-123"

    @pytest.mark.anyio()
    async def test_mbid_artist_matching_falls_back_to_name(self) -> None:
        """Artist with MBID falls back to name match when no MBID match."""
        session = AsyncMock()
        existing_artist = MagicMock()
        existing_artist.name = "Same Name"
        existing_artist.service_links = {"spotify": "sp-123"}

        # Queries:
        # 1. service_links["listenbrainz"] -> None (step 1)
        # 2. service_links["musicbrainz"] -> None (step 2, skips LB key)
        # 3. name match -> existing (step 3)
        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        match_result = MagicMock()
        match_result.scalar_one_or_none.return_value = existing_artist

        session.execute.side_effect = [
            no_result,
            no_result,
            match_result,
        ]
        session.add = MagicMock()

        artist_data = _make_artist_data(
            external_id="mbid-456",
            name="Same Name",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        created = await runner_module._upsert_artist(session, artist_data)

        assert created is False
        assert existing_artist.service_links["listenbrainz"] == "mbid-456"

    @pytest.mark.anyio()
    async def test_empty_external_id_skips_service_links_lookup(self) -> None:
        """Artist with empty external_id skips service_links and uses name."""
        session = AsyncMock()
        existing_artist = MagicMock()
        existing_artist.name = "Name Only"
        existing_artist.service_links = {}

        # Only name match query (skips service_links and MBID checks)
        match_result = MagicMock()
        match_result.scalar_one_or_none.return_value = existing_artist

        session.execute.side_effect = [match_result]
        session.add = MagicMock()

        artist_data = _make_artist_data(
            external_id="",
            name="Name Only",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        created = await runner_module._upsert_artist(session, artist_data)

        assert created is False
        # Should NOT have added empty string to service_links
        assert "listenbrainz" not in existing_artist.service_links

    @pytest.mark.anyio()
    async def test_creates_new_artist_when_no_match(self) -> None:
        """Artist with no existing match is created."""
        session = AsyncMock()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None

        # All lookups return None: service_links, MBID checks, name
        session.execute.side_effect = [
            no_result,
            no_result,
            no_result,
            no_result,
        ]
        session.add = MagicMock()

        artist_data = _make_artist_data(
            external_id="new-mbid",
            name="Brand New Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        created = await runner_module._upsert_artist(session, artist_data)

        assert created is True
        session.add.assert_called_once()
