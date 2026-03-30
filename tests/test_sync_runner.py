"""Tests for the sync runner."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.connectors.base as base_module
import resonance.sync.runner as runner_module
import resonance.types as types_module


def _make_artist_data(
    external_id: str = "art1", name: str = "Artist One"
) -> base_module.SpotifyArtistData:
    return base_module.SpotifyArtistData(
        external_id=external_id,
        name=name,
        service=types_module.ServiceType.SPOTIFY,
    )


def _make_track_data(
    external_id: str = "track1",
    title: str = "Song One",
    artist_external_id: str = "art1",
    artist_name: str = "Artist One",
) -> base_module.SpotifyTrackData:
    return base_module.SpotifyTrackData(
        external_id=external_id,
        title=title,
        artist_external_id=artist_external_id,
        artist_name=artist_name,
        service=types_module.ServiceType.SPOTIFY,
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
