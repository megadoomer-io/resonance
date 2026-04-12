"""Tests for the sync runner upsert functions."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy.dialects.postgresql as pg_dialect

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
    async def test_empty_external_id_still_records_service_presence(self) -> None:
        """Artist with empty external_id still records the service key."""
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
        # Service key is recorded even without an external ID
        assert "listenbrainz" in existing_artist.service_links
        assert existing_artist.service_links["listenbrainz"] == ""

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

    @pytest.mark.anyio()
    async def test_creates_new_artist_without_external_id_records_service(self) -> None:
        """New artist with empty external_id still records service presence."""
        session = AsyncMock()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None

        # Only name match query (skips service_links and MBID checks)
        session.execute.side_effect = [no_result]
        session.add = MagicMock()

        artist_data = _make_artist_data(
            external_id="",
            name="No MBID Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        created = await runner_module._upsert_artist(session, artist_data)

        assert created is True
        added_artist = session.add.call_args[0][0]
        assert "listenbrainz" in added_artist.service_links
        assert added_artist.service_links["listenbrainz"] == ""


class TestListeningEventUpsert:
    """Tests for _upsert_listening_event using INSERT ... ON CONFLICT DO NOTHING."""

    @pytest.mark.anyio()
    async def test_uses_on_conflict_do_nothing_insert(self) -> None:
        """Listening event insert uses PostgreSQL ON CONFLICT DO NOTHING."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        existing_track = MagicMock()
        existing_track.id = uuid.uuid4()
        existing_track.duration_ms = None

        # Track lookup by title (empty external_id skips service_links)
        title_result = MagicMock()
        title_result.scalar_one_or_none.return_value = existing_track
        # Fuzzy dedup check -> no match
        dedup_result = MagicMock()
        dedup_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [title_result, dedup_result, AsyncMock()]

        track_data = _make_track_data(
            external_id="",
            title="Test Song",
            artist_external_id="",
            artist_name="Some Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        await runner_module._upsert_listening_event(
            session, user_id, track_data, "2025-01-15T12:00:00+00:00"
        )

        # The third execute call should be an INSERT ... ON CONFLICT DO NOTHING
        insert_call = session.execute.call_args_list[2]
        stmt = insert_call[0][0]
        assert isinstance(stmt, pg_dialect.Insert)

    @pytest.mark.anyio()
    async def test_creates_event_for_track_without_external_id(self) -> None:
        """Track with empty external_id is found by title and event is inserted."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        existing_track = MagicMock()
        existing_track.id = uuid.uuid4()
        existing_track.duration_ms = None

        # Track lookup by title
        title_result = MagicMock()
        title_result.scalar_one_or_none.return_value = existing_track
        # Fuzzy dedup check -> no match
        dedup_result = MagicMock()
        dedup_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [title_result, dedup_result, AsyncMock()]

        track_data = _make_track_data(
            external_id="",
            title="No MBID Song",
            artist_external_id="",
            artist_name="Some Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        await runner_module._upsert_listening_event(
            session, user_id, track_data, "2025-01-15T12:00:00+00:00"
        )

        # Track lookup + dedup check + INSERT = 3
        assert session.execute.call_count == 3

    @pytest.mark.anyio()
    async def test_returns_when_no_track_found_by_title(self) -> None:
        """No event created when track has no external_id and title match fails."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [no_result]

        track_data = _make_track_data(
            external_id="",
            title="Unknown Song",
            artist_external_id="",
            artist_name="Unknown Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        await runner_module._upsert_listening_event(
            session, user_id, track_data, "2025-01-15T12:00:00+00:00"
        )

        # Only the track lookup, no insert
        assert session.execute.call_count == 1

    @pytest.mark.anyio()
    async def test_dedup_within_60s_window_skips_insert(self) -> None:
        """Event within 60s of an existing event is deduplicated (no insert)."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        existing_track = MagicMock()
        existing_track.id = uuid.uuid4()
        existing_track.duration_ms = None

        existing_event = MagicMock()

        # 1. Track lookup by title -> found
        title_result = MagicMock()
        title_result.scalar_one_or_none.return_value = existing_track
        # 2. Fuzzy dedup check -> existing event found within window
        dedup_result = MagicMock()
        dedup_result.scalar_one_or_none.return_value = existing_event

        session.execute.side_effect = [title_result, dedup_result]

        track_data = _make_track_data(
            external_id="",
            title="Test Song",
            artist_external_id="",
            artist_name="Some Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        await runner_module._upsert_listening_event(
            session, user_id, track_data, "2025-01-15T12:00:30+00:00"
        )

        # Only track lookup + dedup check, NO insert
        assert session.execute.call_count == 2

    @pytest.mark.anyio()
    async def test_dedup_beyond_60s_window_inserts(self) -> None:
        """Event more than 60s from any existing event is NOT deduplicated."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        existing_track = MagicMock()
        existing_track.id = uuid.uuid4()
        existing_track.duration_ms = None

        # 1. Track lookup by title -> found
        title_result = MagicMock()
        title_result.scalar_one_or_none.return_value = existing_track
        # 2. Fuzzy dedup check -> no match (events are >60s apart)
        dedup_result = MagicMock()
        dedup_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [title_result, dedup_result, AsyncMock()]

        track_data = _make_track_data(
            external_id="",
            title="Test Song",
            artist_external_id="",
            artist_name="Some Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        await runner_module._upsert_listening_event(
            session, user_id, track_data, "2025-01-15T12:05:00+00:00"
        )

        # Track lookup + dedup check + insert = 3
        assert session.execute.call_count == 3

    @pytest.mark.anyio()
    async def test_exact_timestamp_match_still_deduplicates(self) -> None:
        """Exact same timestamp is caught by the fuzzy window (regression)."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        existing_track = MagicMock()
        existing_track.id = uuid.uuid4()
        existing_track.duration_ms = None

        existing_event = MagicMock()

        # 1. Track lookup by service_links -> found
        svc_result = MagicMock()
        svc_result.scalar_one_or_none.return_value = existing_track
        # 2. Fuzzy dedup check -> exact match found
        dedup_result = MagicMock()
        dedup_result.scalar_one_or_none.return_value = existing_event

        session.execute.side_effect = [svc_result, dedup_result]

        track_data = _make_track_data(
            external_id="track-id",
            title="Test Song",
            artist_external_id="art1",
            artist_name="Artist One",
            service=types_module.ServiceType.SPOTIFY,
        )

        await runner_module._upsert_listening_event(
            session, user_id, track_data, "2025-01-15T12:00:00+00:00"
        )

        # Track lookup + dedup check, NO insert
        assert session.execute.call_count == 2

    @pytest.mark.anyio()
    async def test_falls_back_to_title_when_service_links_miss(self) -> None:
        """Track with external_id falls back to title when service_links miss."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        existing_track = MagicMock()
        existing_track.id = uuid.uuid4()
        existing_track.duration_ms = None

        # 1. service_links lookup -> None
        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        # 2. title match -> existing track
        title_result = MagicMock()
        title_result.scalar_one_or_none.return_value = existing_track
        # 3. Fuzzy dedup check -> no match
        dedup_result = MagicMock()
        dedup_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [
            no_result,
            title_result,
            dedup_result,
            AsyncMock(),
        ]

        track_data = _make_track_data(
            external_id="some-id",
            title="Known Song",
            artist_external_id="art1",
            artist_name="Artist One",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        await runner_module._upsert_listening_event(
            session, user_id, track_data, "2025-01-15T12:00:00+00:00"
        )

        # Track lookups (2) + dedup check (1) + insert (1) = 4
        assert session.execute.call_count == 4


class TestBulkFetchArtists:
    """Tests for bulk_fetch_artists."""

    @pytest.mark.anyio()
    async def test_returns_empty_dict_for_empty_ids(self) -> None:
        """Returns empty dict when no IDs are provided."""
        session = AsyncMock()
        result = await runner_module.bulk_fetch_artists(session, "listenbrainz", set())
        assert result == {}
        session.execute.assert_not_called()

    @pytest.mark.anyio()
    async def test_filters_out_empty_ids(self) -> None:
        """Returns empty dict when all IDs are empty strings."""
        session = AsyncMock()
        result = await runner_module.bulk_fetch_artists(session, "listenbrainz", {""})
        assert result == {}
        session.execute.assert_not_called()

    @pytest.mark.anyio()
    async def test_returns_mapping_for_found_artists(self) -> None:
        """Returns external_id -> Artist mapping for found records."""
        session = AsyncMock()
        artist1 = MagicMock()
        artist1.service_links = {"listenbrainz": "mbid-1"}
        artist2 = MagicMock()
        artist2.service_links = {"listenbrainz": "mbid-2"}

        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [artist1, artist2]
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock
        session.execute.return_value = result_mock

        result = await runner_module.bulk_fetch_artists(
            session, "listenbrainz", {"mbid-1", "mbid-2"}
        )

        assert result == {"mbid-1": artist1, "mbid-2": artist2}
        session.execute.assert_called_once()


class TestBulkFetchTracks:
    """Tests for bulk_fetch_tracks."""

    @pytest.mark.anyio()
    async def test_returns_empty_dict_for_empty_ids(self) -> None:
        """Returns empty dict when no IDs are provided."""
        session = AsyncMock()
        result = await runner_module.bulk_fetch_tracks(session, "listenbrainz", set())
        assert result == {}
        session.execute.assert_not_called()

    @pytest.mark.anyio()
    async def test_returns_mapping_for_found_tracks(self) -> None:
        """Returns external_id -> Track mapping for found records."""
        session = AsyncMock()
        track1 = MagicMock()
        track1.service_links = {"spotify": "sp-1"}
        track2 = MagicMock()
        track2.service_links = {"spotify": "sp-2"}

        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [track1, track2]
        result_mock = MagicMock()
        result_mock.scalars.return_value = scalars_mock
        session.execute.return_value = result_mock

        result = await runner_module.bulk_fetch_tracks(
            session, "spotify", {"sp-1", "sp-2"}
        )

        assert result == {"sp-1": track1, "sp-2": track2}
        session.execute.assert_called_once()


class TestArtistCachePassthrough:
    """Tests for artist_cache parameter on _upsert_artist."""

    @pytest.mark.anyio()
    async def test_cache_hit_skips_db_query(self) -> None:
        """When artist is in cache, no DB query is made."""
        session = AsyncMock()
        cached_artist = MagicMock()
        cached_artist.name = "Old Name"
        artist_cache = {"art1": cached_artist}

        artist_data = _make_artist_data(
            external_id="art1",
            name="New Name",
            service=types_module.ServiceType.SPOTIFY,
        )

        created = await runner_module._upsert_artist(
            session, artist_data, artist_cache=artist_cache
        )

        assert created is False
        assert cached_artist.name == "New Name"
        session.execute.assert_not_called()

    @pytest.mark.anyio()
    async def test_cache_miss_falls_through_to_db(self) -> None:
        """When artist is not in cache, falls through to DB query."""
        session = AsyncMock()
        existing_artist = MagicMock()
        existing_artist.name = "Existing"
        existing_artist.service_links = {"spotify": "art2"}

        match_result = MagicMock()
        match_result.scalar_one_or_none.return_value = existing_artist
        session.execute.side_effect = [match_result]

        artist_cache: dict[str, object] = {"other-id": MagicMock()}

        artist_data = _make_artist_data(
            external_id="art2",
            name="Updated Name",
            service=types_module.ServiceType.SPOTIFY,
        )

        created = await runner_module._upsert_artist(
            session,
            artist_data,
            artist_cache=artist_cache,  # type: ignore[arg-type]
        )

        assert created is False
        session.execute.assert_called_once()


class TestTrackCachePassthrough:
    """Tests for track_cache parameter on _upsert_track."""

    @pytest.mark.anyio()
    async def test_cache_hit_skips_db_query(self) -> None:
        """When track is in cache, no DB query is made."""
        session = AsyncMock()
        cached_track = MagicMock()
        track_cache = {"track1": cached_track}

        track_data = _make_track_data(
            external_id="track1",
            title="Song One",
            service=types_module.ServiceType.SPOTIFY,
        )

        created = await runner_module._upsert_track(
            session, track_data, track_cache=track_cache
        )

        assert created is False
        session.execute.assert_not_called()

    @pytest.mark.anyio()
    async def test_cache_miss_falls_through_to_db(self) -> None:
        """When track is not in cache, falls through to DB query."""
        session = AsyncMock()
        existing_track = MagicMock()
        existing_track.service_links = {"spotify": "track2"}

        match_result = MagicMock()
        match_result.scalar_one_or_none.return_value = existing_track
        session.execute.side_effect = [match_result]

        track_cache: dict[str, object] = {"other-id": MagicMock()}

        track_data = _make_track_data(
            external_id="track2",
            title="Song Two",
            service=types_module.ServiceType.SPOTIFY,
        )

        created = await runner_module._upsert_track(
            session,
            track_data,
            track_cache=track_cache,  # type: ignore[arg-type]
        )

        assert created is False
        session.execute.assert_called_once()
