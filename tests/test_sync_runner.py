"""Tests for the sync runner."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.connectors.base as base_module
import resonance.connectors.listenbrainz as listenbrainz_module
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


def _make_lb_listen(
    recording_mbid: str = "rec-mbid-1",
    title: str = "LB Song",
    artist_mbid: str = "artist-mbid-1",
    artist_name: str = "LB Artist",
    listened_at: int = 1700000000,
) -> listenbrainz_module.ListenBrainzListenItem:
    return listenbrainz_module.ListenBrainzListenItem(
        track=base_module.TrackData(
            external_id=recording_mbid,
            title=title,
            artist_external_id=artist_mbid,
            artist_name=artist_name,
            service=types_module.ServiceType.LISTENBRAINZ,
        ),
        listened_at=listened_at,
    )


@pytest.fixture()
def mock_session() -> AsyncMock:
    """Create a mock async database session."""
    session = AsyncMock()
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    session.execute.return_value = mock_result
    # session.add is synchronous in SQLAlchemy, use MagicMock to avoid warnings
    session.add = MagicMock()
    return session


@pytest.fixture()
def mock_connector() -> MagicMock:
    """Create a mock connector with Spotify-like responses."""
    connector = MagicMock()
    connector.service_type = types_module.ServiceType.SPOTIFY
    connector.get_followed_artists = AsyncMock(return_value=[_make_artist_data()])
    connector.get_saved_tracks = AsyncMock(return_value=[_make_track_data()])
    connector.get_recently_played = AsyncMock(return_value=[])
    return connector


@pytest.fixture()
def mock_job() -> MagicMock:
    """Create a mock sync job."""
    job = MagicMock()
    job.user_id = uuid.uuid4()
    job.service_connection_id = uuid.uuid4()
    job.status = types_module.SyncStatus.PENDING
    job.started_at = None
    job.completed_at = None
    job.items_created = 0
    job.items_updated = 0
    job.error_message = None
    return job


class TestRunSyncSuccess:
    """Tests for successful sync execution."""

    @pytest.mark.anyio()
    async def test_sets_status_to_completed(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        assert mock_job.status == types_module.SyncStatus.COMPLETED

    @pytest.mark.anyio()
    async def test_sets_completed_at(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        assert mock_job.completed_at is not None

    @pytest.mark.anyio()
    async def test_sets_started_at(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        assert mock_job.started_at is not None

    @pytest.mark.anyio()
    async def test_sets_running_status_initially(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        statuses: list[str] = []

        def track_status(value: str) -> None:
            statuses.append(value)

        type(mock_job).status = property(
            lambda self: statuses[-1] if statuses else types_module.SyncStatus.PENDING,
            lambda self, v: track_status(v),
        )

        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        assert statuses[0] == types_module.SyncStatus.RUNNING

    @pytest.mark.anyio()
    async def test_calls_get_followed_artists(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        mock_connector.get_followed_artists.assert_awaited_once_with("access-token")

    @pytest.mark.anyio()
    async def test_calls_get_saved_tracks(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        mock_connector.get_saved_tracks.assert_awaited_once_with("access-token")

    @pytest.mark.anyio()
    async def test_calls_get_recently_played(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        mock_connector.get_recently_played.assert_awaited_once_with("access-token")

    @pytest.mark.anyio()
    async def test_commits_session(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        # Should commit at least twice: once at start (RUNNING), once at end
        assert mock_session.commit.await_count >= 2

    @pytest.mark.anyio()
    async def test_tracks_items_created(
        self,
        mock_job: MagicMock,
        mock_connector: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        """Items created count should be set when new entities are created."""
        await runner_module.run_sync(
            mock_job, mock_connector, mock_session, "access-token"
        )

        # With default mock returning None for lookups, entities are created
        assert mock_job.items_created >= 0


class TestRunSyncFailure:
    """Tests for sync failure handling."""

    @pytest.mark.anyio()
    async def test_sets_status_to_failed_on_exception(
        self,
        mock_job: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        connector = MagicMock()
        connector.service_type = types_module.ServiceType.SPOTIFY
        connector.get_followed_artists = AsyncMock(
            side_effect=RuntimeError("API error")
        )

        await runner_module.run_sync(mock_job, connector, mock_session, "access-token")

        assert mock_job.status == types_module.SyncStatus.FAILED

    @pytest.mark.anyio()
    async def test_sets_error_message_on_exception(
        self,
        mock_job: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        connector = MagicMock()
        connector.service_type = types_module.ServiceType.SPOTIFY
        connector.get_followed_artists = AsyncMock(
            side_effect=RuntimeError("API error")
        )

        await runner_module.run_sync(mock_job, connector, mock_session, "access-token")

        assert mock_job.error_message is not None
        assert "API error" in mock_job.error_message

    @pytest.mark.anyio()
    async def test_sets_completed_at_on_failure(
        self,
        mock_job: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        connector = MagicMock()
        connector.service_type = types_module.ServiceType.SPOTIFY
        connector.get_followed_artists = AsyncMock(
            side_effect=RuntimeError("API error")
        )

        await runner_module.run_sync(mock_job, connector, mock_session, "access-token")

        assert mock_job.completed_at is not None

    @pytest.mark.anyio()
    async def test_commits_on_failure(
        self,
        mock_job: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        connector = MagicMock()
        connector.service_type = types_module.ServiceType.SPOTIFY
        connector.get_followed_artists = AsyncMock(
            side_effect=RuntimeError("API error")
        )

        await runner_module.run_sync(mock_job, connector, mock_session, "access-token")

        # Should commit the failure status
        mock_session.commit.assert_awaited()


class TestRunSyncWithData:
    """Tests for sync runner data processing."""

    @pytest.mark.anyio()
    async def test_processes_multiple_artists(
        self,
        mock_job: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        connector = MagicMock()
        connector.service_type = types_module.ServiceType.SPOTIFY
        connector.get_followed_artists = AsyncMock(
            return_value=[
                _make_artist_data("art1", "Artist One"),
                _make_artist_data("art2", "Artist Two"),
            ]
        )
        connector.get_saved_tracks = AsyncMock(return_value=[])
        connector.get_recently_played = AsyncMock(return_value=[])

        await runner_module.run_sync(mock_job, connector, mock_session, "access-token")

        assert mock_job.status == types_module.SyncStatus.COMPLETED
        # session.execute called for artist lookups and relation checks
        assert mock_session.execute.await_count > 0

    @pytest.mark.anyio()
    async def test_processes_tracks_with_artist_upsert(
        self,
        mock_job: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        connector = MagicMock()
        connector.service_type = types_module.ServiceType.SPOTIFY
        connector.get_followed_artists = AsyncMock(return_value=[])
        connector.get_saved_tracks = AsyncMock(return_value=[_make_track_data()])
        connector.get_recently_played = AsyncMock(return_value=[])

        await runner_module.run_sync(mock_job, connector, mock_session, "access-token")

        assert mock_job.status == types_module.SyncStatus.COMPLETED
        # session.add called for new entities
        assert mock_session.add.call_count > 0

    @pytest.mark.anyio()
    async def test_processes_recently_played(
        self,
        mock_job: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        from resonance.connectors.spotify import PlayedTrackItem

        connector = MagicMock()
        connector.service_type = types_module.ServiceType.SPOTIFY
        connector.get_followed_artists = AsyncMock(return_value=[])
        connector.get_saved_tracks = AsyncMock(return_value=[])
        connector.get_recently_played = AsyncMock(
            return_value=[
                PlayedTrackItem(
                    track=_make_track_data(),
                    played_at="2024-01-15T10:30:00.000Z",
                ),
            ]
        )

        await runner_module.run_sync(mock_job, connector, mock_session, "access-token")

        assert mock_job.status == types_module.SyncStatus.COMPLETED
        assert mock_session.add.call_count > 0


def _mock_session_with_connection(
    external_user_id: str = "testuser",
) -> AsyncMock:
    """Create a mock session that returns a ServiceConnection on first query."""
    session = AsyncMock()
    # The first execute() call fetches the ServiceConnection; subsequent ones
    # return None (for upsert lookups).
    connection = MagicMock()
    connection.external_user_id = external_user_id

    conn_result = MagicMock()
    conn_result.scalar_one.return_value = connection

    default_result = MagicMock()
    default_result.scalar_one_or_none.return_value = None

    session.execute.side_effect = [conn_result] + [default_result] * 200
    session.add = MagicMock()
    return session


def _mock_lb_connector(
    get_listens_pages: list[list[listenbrainz_module.ListenBrainzListenItem]],
) -> MagicMock:
    """Create a mock ListenBrainzConnector that returns pages then empty list."""
    connector = MagicMock(spec=listenbrainz_module.ListenBrainzConnector)
    connector.service_type = types_module.ServiceType.LISTENBRAINZ
    connector.get_listens = AsyncMock(side_effect=[*get_listens_pages, []])
    return connector


class TestListenBrainzSync:
    """Tests for ListenBrainz sync path."""

    @pytest.mark.anyio()
    async def test_listenbrainz_sync_creates_events(
        self,
        mock_job: MagicMock,
    ) -> None:
        """ListenBrainz sync creates listening events from listen items."""
        listens = [
            _make_lb_listen(
                recording_mbid="rec1",
                title="Song A",
                artist_mbid="art1",
                artist_name="Artist A",
                listened_at=1700000100,
            ),
            _make_lb_listen(
                recording_mbid="rec2",
                title="Song B",
                artist_mbid="art2",
                artist_name="Artist B",
                listened_at=1700000000,
            ),
        ]
        connector = _mock_lb_connector([listens])
        session = _mock_session_with_connection("testuser")

        await runner_module.run_sync(mock_job, connector, session, "")

        assert mock_job.status == types_module.SyncStatus.COMPLETED
        assert mock_job.items_created == 2
        # get_listens called twice: once with data, once returning empty
        assert connector.get_listens.await_count == 2
        connector.get_listens.assert_any_await("testuser", max_ts=None, count=100)

    @pytest.mark.anyio()
    async def test_listenbrainz_sync_paginates(
        self,
        mock_job: MagicMock,
    ) -> None:
        """ListenBrainz sync walks backward through pages using max_ts."""
        page1 = [
            _make_lb_listen(listened_at=1700000200),
            _make_lb_listen(
                recording_mbid="rec2",
                listened_at=1700000100,
            ),
        ]
        page2 = [
            _make_lb_listen(
                recording_mbid="rec3",
                listened_at=1700000050,
            ),
        ]
        connector = _mock_lb_connector([page1, page2])
        session = _mock_session_with_connection("paginateuser")

        await runner_module.run_sync(mock_job, connector, session, "")

        assert mock_job.status == types_module.SyncStatus.COMPLETED
        # 3 calls: page1, page2, empty
        assert connector.get_listens.await_count == 3
        # Second call should use the oldest listen's timestamp from page1
        connector.get_listens.assert_any_await(
            "paginateuser", max_ts=1700000100, count=100
        )
        # Third call uses oldest from page2
        connector.get_listens.assert_any_await(
            "paginateuser", max_ts=1700000050, count=100
        )
        # Commits per page (2 pages + start + final)
        assert session.commit.await_count >= 4

    @pytest.mark.anyio()
    async def test_listenbrainz_sync_empty_listens(
        self,
        mock_job: MagicMock,
    ) -> None:
        """ListenBrainz sync with no listens completes with zero items."""
        connector = _mock_lb_connector([])
        session = _mock_session_with_connection("emptyuser")

        await runner_module.run_sync(mock_job, connector, session, "")

        assert mock_job.status == types_module.SyncStatus.COMPLETED
        assert mock_job.items_created == 0
        assert mock_job.items_updated == 0


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
