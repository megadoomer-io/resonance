"""Tests for the Spotify sync strategy."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.connectors.base as connector_base
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
import resonance.sync.base as sync_base
import resonance.sync.spotify as sync_spotify_module
import resonance.types as types_module

_TEST_ENCRYPTION_KEY = "pVnp8Zq06TgKRtq3xvI5zouN1E84mlTsX1V9UhMwJuI="


def _make_connection(
    access_token: str = "test-token",
    sync_watermark: dict[str, dict[str, object]] | None = None,
) -> MagicMock:
    """Create a mock ServiceConnection with an encrypted access token."""
    conn = MagicMock()
    conn.id = uuid.uuid4()
    conn.encrypted_access_token = crypto_module.encrypt_token(
        access_token, _TEST_ENCRYPTION_KEY
    )
    conn.service_type = types_module.ServiceType.SPOTIFY
    conn.sync_watermark = sync_watermark or {}
    return conn


def _make_task(
    params: dict[str, object] | None = None,
) -> MagicMock:
    """Create a mock Task."""
    task = MagicMock()
    task.id = uuid.uuid4()
    task.user_id = uuid.uuid4()
    task.service_connection_id = uuid.uuid4()
    task.params = params or {}
    return task


class TestSpotifyPlan:
    """Tests for SpotifySyncStrategy.plan()."""

    @pytest.mark.asyncio
    async def test_returns_three_descriptors(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 3

    @pytest.mark.asyncio
    async def test_descriptor_data_types(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        data_types = [d.params["data_type"] for d in descriptors]
        assert data_types == ["followed_artists", "saved_tracks", "recently_played"]

    @pytest.mark.asyncio
    async def test_descriptors_have_descriptions(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert desc.description != ""

    @pytest.mark.asyncio
    async def test_descriptors_do_not_contain_access_token(self) -> None:
        """Access tokens must not be persisted in task params."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection(access_token="my-secret-token")

        descriptors = await strategy.plan(session, connection, connector)

        for desc in descriptors:
            assert "access_token" not in desc.params

    @pytest.mark.asyncio
    async def test_passes_watermarks_to_params(self) -> None:
        """Watermarks from connection are passed into descriptor params."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "recently_played": {"last_played_at": "2026-04-05T12:00:00Z"},
                "saved_tracks": {"last_saved_at": "2026-04-05T12:00:00Z"},
                "followed_artists": {"after_cursor": "abc123"},
            }
        )
        descriptors = await strategy.plan(session, connection, connector)
        by_type = {d.params["data_type"]: d for d in descriptors}
        assert (
            by_type["recently_played"].params["last_played_at"]
            == "2026-04-05T12:00:00Z"
        )
        assert by_type["saved_tracks"].params["last_saved_at"] == "2026-04-05T12:00:00Z"
        assert by_type["followed_artists"].params["after_cursor"] == "abc123"

    @pytest.mark.asyncio
    async def test_no_watermarks_passes_none(self) -> None:
        """Without watermarks, params contain None for watermark fields."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection()
        descriptors = await strategy.plan(session, connection, connector)
        by_type = {d.params["data_type"]: d for d in descriptors}
        assert by_type["recently_played"].params.get("last_played_at") is None
        assert by_type["saved_tracks"].params.get("last_saved_at") is None
        assert by_type["followed_artists"].params.get("after_cursor") is None

    @pytest.mark.asyncio
    async def test_incremental_description(self) -> None:
        """Descriptions mention 'new' for incremental sync."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        connector = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "recently_played": {"last_played_at": "2026-04-05T12:00:00Z"}
            }
        )
        descriptors = await strategy.plan(session, connection, connector)
        by_type = {d.params["data_type"]: d for d in descriptors}
        assert "new" in by_type["recently_played"].description.lower()
        # followed_artists and saved_tracks have no watermark,
        # so keep original description
        assert (
            by_type["followed_artists"].description == "Fetching your followed artists"
        )
        assert by_type["saved_tracks"].description == "Fetching your saved tracks"

    def test_concurrency_is_sequential(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        assert strategy.concurrency == "sequential"


class TestSpotifyExecute:
    """Tests for SpotifySyncStrategy.execute()."""

    @pytest.mark.asyncio
    async def test_followed_artists_dispatches_to_connector(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "followed_artists"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_followed_artists",
                new_callable=AsyncMock,
                return_value=(2, 1, {}),
            ) as mock_sync,
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        mock_sync.assert_awaited_once()
        assert result["items_created"] == 2
        assert result["items_updated"] == 1
        assert result["watermark"] == {}

    @pytest.mark.asyncio
    async def test_unknown_data_type_returns_zero_counts(self) -> None:
        """Unknown data_type logs a warning and returns zero counts."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "nonexistent_type"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with patch.object(
            strategy,
            "_get_access_token",
            new_callable=AsyncMock,
            return_value="tok",
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        assert result["items_created"] == 0
        assert result["items_updated"] == 0
        assert result["watermark"] == {}

    @pytest.mark.asyncio
    async def test_rate_limit_raises_defer_request(self) -> None:
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "followed_artists"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_followed_artists",
                new_callable=AsyncMock,
                side_effect=connector_base.RateLimitExceededError(
                    retry_after=300.0, max_wait=120.0
                ),
            ),
            pytest.raises(sync_base.DeferRequest) as exc_info,
        ):
            connection = _make_connection()
            await strategy.execute(session, task, connector, connection)

        assert exc_info.value.retry_after == 300.0
        assert exc_info.value.resume_params["data_type"] == "followed_artists"


class TestSpotifyWatermarkOutput:
    """Tests for watermark values in execute() results."""

    @pytest.mark.asyncio
    async def test_followed_artists_empty_watermark(self) -> None:
        """followed_artists always full-fetches and returns empty watermark."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "followed_artists"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_followed_artists",
                new_callable=AsyncMock,
                return_value=(3, 0, {}),
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        assert result["watermark"] == {}

    @pytest.mark.asyncio
    async def test_saved_tracks_watermark(self) -> None:
        """saved_tracks returns last_saved_at in watermark."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_saved_tracks",
                new_callable=AsyncMock,
                return_value=(10, 5, {"last_saved_at": "2026-04-06T12:00:00Z"}),
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        assert result["watermark"] == {"last_saved_at": "2026-04-06T12:00:00Z"}

    @pytest.mark.asyncio
    async def test_recently_played_watermark(self) -> None:
        """recently_played returns last_played_at in watermark."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "recently_played"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_recently_played",
                new_callable=AsyncMock,
                return_value=(7, {"last_played_at": "2026-04-06T10:00:00Z"}),
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        assert result["watermark"] == {"last_played_at": "2026-04-06T10:00:00Z"}

    @pytest.mark.asyncio
    async def test_empty_watermark_when_no_data(self) -> None:
        """When helper returns empty watermark, result contains empty dict."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "followed_artists"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_followed_artists",
                new_callable=AsyncMock,
                return_value=(0, 0, {}),
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        assert result["watermark"] == {}


class TestSavedTracksStopEarly:
    """Tests for saved_tracks stop-early and fast-finish behavior."""

    @pytest.mark.asyncio
    async def test_stop_early_when_all_duplicates(self) -> None:
        """Pagination stops when all items on a page are duplicates."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})

        # Create two pages: first has some new, second is all duplicates
        track1 = connector_base.TrackData(
            external_id="t1",
            title="Track 1",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )
        track2 = connector_base.TrackData(
            external_id="t2",
            title="Track 2",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )
        page1 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track1, added_at="2026-04-06T12:00:00Z"
                ),
            ],
            total=2,
            next_url="https://api.spotify.com/v1/me/tracks?offset=1",
        )
        page2 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track2, added_at="2026-04-05T12:00:00Z"
                ),
            ],
            total=2,
            next_url=None,
        )

        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connector.get_saved_tracks_page = AsyncMock(side_effect=[page1, page2])

        # Mock existing count to not trigger fast-finish (different from total)
        count_result = MagicMock()
        count_result.scalar_one.return_value = 0
        session.execute = AsyncMock(return_value=count_result)
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "resonance.sync.runner.bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner.bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner._upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch(
                "resonance.sync.runner._upsert_track",
                new_callable=AsyncMock,
                # Page 1: track is new (True), Page 2: track is duplicate (False)
                side_effect=[True, False],
            ),
            patch(
                "resonance.sync.runner._upsert_user_track_relation",
                new_callable=AsyncMock,
            ),
        ):
            created, updated, watermark = await sync_spotify_module._sync_saved_tracks(
                session, task, connector, "tok"
            )

        assert created == 1
        assert updated == 1
        assert watermark == {"last_saved_at": "2026-04-06T12:00:00Z"}
        # Should have fetched 2 pages (stopped after page 2 because all duplicates)
        assert connector.get_saved_tracks_page.await_count == 2

    @pytest.mark.asyncio
    async def test_fast_finish_when_total_matches_existing(self) -> None:
        """Fast-finish skips processing when total matches existing count."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})

        track1 = connector_base.TrackData(
            external_id="t1",
            title="Track 1",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )
        page1 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track1, added_at="2026-04-06T12:00:00Z"
                ),
            ],
            total=5,
            next_url="https://api.spotify.com/v1/me/tracks?offset=1",
        )

        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connector.get_saved_tracks_page = AsyncMock(return_value=page1)

        # Existing count matches total -> fast-finish
        count_result = MagicMock()
        count_result.scalar_one.return_value = 5
        session.execute = AsyncMock(return_value=count_result)

        created, updated, watermark = await sync_spotify_module._sync_saved_tracks(
            session, task, connector, "tok"
        )

        assert created == 0
        assert updated == 0
        assert watermark == {"last_saved_at": "2026-04-06T12:00:00Z"}
        # Only fetched one page (fast-finish)
        assert connector.get_saved_tracks_page.await_count == 1
        # Progress should be set to total
        assert task.progress_total == 5
        assert task.progress_current == 5

    @pytest.mark.asyncio
    async def test_processes_all_pages_when_no_duplicates(self) -> None:
        """All pages are processed when tracks are new."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})

        track1 = connector_base.TrackData(
            external_id="t1",
            title="Track 1",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )
        track2 = connector_base.TrackData(
            external_id="t2",
            title="Track 2",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )

        page1 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track1, added_at="2026-04-06T12:00:00Z"
                ),
            ],
            total=2,
            next_url="https://api.spotify.com/v1/me/tracks?offset=1",
        )
        page2 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track2, added_at="2026-04-05T12:00:00Z"
                ),
            ],
            total=2,
            next_url=None,
        )

        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connector.get_saved_tracks_page = AsyncMock(side_effect=[page1, page2])

        count_result = MagicMock()
        count_result.scalar_one.return_value = 0
        session.execute = AsyncMock(return_value=count_result)
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "resonance.sync.runner.bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner.bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner._upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch(
                "resonance.sync.runner._upsert_track",
                new_callable=AsyncMock,
                return_value=True,  # All tracks are new
            ),
            patch(
                "resonance.sync.runner._upsert_user_track_relation",
                new_callable=AsyncMock,
            ),
        ):
            created, updated, _watermark = await sync_spotify_module._sync_saved_tracks(
                session, task, connector, "tok"
            )

        assert created == 2
        assert updated == 0
        # Both pages fetched
        assert connector.get_saved_tracks_page.await_count == 2

    @pytest.mark.asyncio
    async def test_empty_page_stops_iteration(self) -> None:
        """An empty page stops the pagination loop."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})

        page1 = spotify_module.SavedTrackPage(
            items=[],
            total=0,
            next_url=None,
        )

        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connector.get_saved_tracks_page = AsyncMock(return_value=page1)

        count_result = MagicMock()
        count_result.scalar_one.return_value = 5  # Different from total=0
        session.execute = AsyncMock(return_value=count_result)

        created, updated, watermark = await sync_spotify_module._sync_saved_tracks(
            session, task, connector, "tok"
        )

        assert created == 0
        assert updated == 0
        assert watermark == {}


class TestSavedTracksPerPageWatermark:
    """Tests for per-page watermark updates in _sync_saved_tracks."""

    @pytest.mark.asyncio
    async def test_watermark_updated_on_connection_after_each_page(self) -> None:
        """connection.sync_watermark is updated after each page commit."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})
        connection = _make_connection()

        track1 = connector_base.TrackData(
            external_id="t1",
            title="Track 1",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )
        track2 = connector_base.TrackData(
            external_id="t2",
            title="Track 2",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )

        page1 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track1, added_at="2026-04-06T12:00:00Z"
                ),
            ],
            total=2,
            next_url="https://api.spotify.com/v1/me/tracks?offset=1",
        )
        page2 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track2, added_at="2026-04-05T12:00:00Z"
                ),
            ],
            total=2,
            next_url=None,
        )

        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connector.get_saved_tracks_page = AsyncMock(side_effect=[page1, page2])

        count_result = MagicMock()
        count_result.scalar_one.return_value = 0
        session.execute = AsyncMock(return_value=count_result)
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "resonance.sync.runner.bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner.bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner._upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch(
                "resonance.sync.runner._upsert_track",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "resonance.sync.runner._upsert_user_track_relation",
                new_callable=AsyncMock,
            ),
        ):
            (
                _created,
                _updated,
                _watermark,
            ) = await sync_spotify_module._sync_saved_tracks(
                session,
                task,
                connector,
                "tok",
                connection=connection,
                data_type="saved_tracks",
            )

        # Watermark should have been set on the connection
        assert connection.sync_watermark == {
            "saved_tracks": {"last_saved_at": "2026-04-06T12:00:00Z"},
        }

    @pytest.mark.asyncio
    async def test_watermark_preserves_existing_watermarks(self) -> None:
        """Per-page update preserves watermarks for other data types."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})
        connection = _make_connection(
            sync_watermark={
                "recently_played": {"last_played_at": "2026-04-01T00:00:00Z"},
            }
        )

        track1 = connector_base.TrackData(
            external_id="t1",
            title="Track 1",
            artist_external_id="a1",
            artist_name="Artist 1",
            service=types_module.ServiceType.SPOTIFY,
        )
        page1 = spotify_module.SavedTrackPage(
            items=[
                spotify_module.SavedTrackItem(
                    track=track1, added_at="2026-04-06T12:00:00Z"
                ),
            ],
            total=1,
            next_url=None,
        )

        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connector.get_saved_tracks_page = AsyncMock(return_value=page1)

        count_result = MagicMock()
        count_result.scalar_one.return_value = 0
        session.execute = AsyncMock(return_value=count_result)
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)

        with (
            patch(
                "resonance.sync.runner.bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner.bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch(
                "resonance.sync.runner._upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch(
                "resonance.sync.runner._upsert_track",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch(
                "resonance.sync.runner._upsert_user_track_relation",
                new_callable=AsyncMock,
            ),
        ):
            await sync_spotify_module._sync_saved_tracks(
                session,
                task,
                connector,
                "tok",
                connection=connection,
                data_type="saved_tracks",
            )

        # Both the existing watermark and new one should be present
        assert connection.sync_watermark == {
            "recently_played": {"last_played_at": "2026-04-01T00:00:00Z"},
            "saved_tracks": {"last_saved_at": "2026-04-06T12:00:00Z"},
        }

    @pytest.mark.asyncio
    async def test_no_watermark_update_without_connection(self) -> None:
        """When connection is not passed, no watermark update on connection."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})

        page1 = spotify_module.SavedTrackPage(
            items=[],
            total=0,
            next_url=None,
        )

        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connector.get_saved_tracks_page = AsyncMock(return_value=page1)

        count_result = MagicMock()
        count_result.scalar_one.return_value = 5
        session.execute = AsyncMock(return_value=count_result)

        # Should work without connection (backward compatible)
        created, updated, _watermark = await sync_spotify_module._sync_saved_tracks(
            session, task, connector, "tok"
        )

        assert created == 0
        assert updated == 0

    @pytest.mark.asyncio
    async def test_execute_passes_connection_to_sync_saved_tracks(self) -> None:
        """execute() passes connection to _sync_saved_tracks."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        connection = _make_connection()

        with (
            patch.object(
                strategy,
                "_get_access_token",
                new_callable=AsyncMock,
                return_value="tok",
            ),
            patch.object(
                sync_spotify_module,
                "_sync_saved_tracks",
                new_callable=AsyncMock,
                return_value=(10, 5, {"last_saved_at": "2026-04-06T12:00:00Z"}),
            ) as mock_sync,
        ):
            await strategy.execute(session, task, connector, connection)

        # Verify connection and data_type were passed
        mock_sync.assert_awaited_once_with(
            session,
            task,
            connector,
            "tok",
            connection=connection,
            data_type="saved_tracks",
        )


class TestCastConnector:
    """Tests for _cast_connector helper."""

    def test_returns_spotify_connector(self) -> None:
        connector = MagicMock(spec=spotify_module.SpotifyConnector)
        result = sync_spotify_module._cast_connector(connector)
        assert result is connector

    def test_raises_type_error_for_wrong_type(self) -> None:
        connector = MagicMock(spec=connector_base.BaseConnector)
        with pytest.raises(TypeError, match="Expected SpotifyConnector"):
            sync_spotify_module._cast_connector(connector)
