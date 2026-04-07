"""Tests for the ListenBrainz sync strategy."""

from __future__ import annotations

import datetime
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import resonance.connectors.base as connector_base
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.models.task as task_module
import resonance.sync.base as sync_base
import resonance.sync.listenbrainz as lb_sync_module
import resonance.types as types_module

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_connection(
    *,
    connection_id: uuid.UUID | None = None,
    external_user_id: str = "testuser",
    sync_watermark: dict[str, dict[str, object]] | None = None,
) -> MagicMock:
    """Create a mock ServiceConnection."""
    conn = MagicMock()
    conn.id = connection_id or uuid.uuid4()
    conn.external_user_id = external_user_id
    conn.service_type = types_module.ServiceType.LISTENBRAINZ
    conn.sync_watermark = sync_watermark or {}
    return conn


def _make_task(
    *,
    task_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    params: dict[str, object] | None = None,
) -> task_module.SyncTask:
    """Create a SyncTask instance for testing."""
    return task_module.SyncTask(
        id=task_id or uuid.uuid4(),
        user_id=user_id or uuid.uuid4(),
        service_connection_id=uuid.uuid4(),
        task_type=types_module.SyncTaskType.TIME_RANGE,
        status=types_module.SyncStatus.RUNNING,
        params=params or {"username": "testuser"},
    )


def _make_listen(
    listened_at: int,
    track_title: str = "Test Song",
    artist_name: str = "Test Artist",
) -> listenbrainz_module.ListenBrainzListenItem:
    """Create a ListenBrainzListenItem for testing."""
    return listenbrainz_module.ListenBrainzListenItem(
        track=connector_base.TrackData(
            external_id=f"mbid-{listened_at}",
            title=track_title,
            artist_external_id=f"artist-mbid-{listened_at}",
            artist_name=artist_name,
            service=types_module.ServiceType.LISTENBRAINZ,
        ),
        listened_at=listened_at,
    )


def _make_lb_connector() -> MagicMock:
    """Create a mock ListenBrainzConnector."""
    connector = MagicMock(spec=listenbrainz_module.ListenBrainzConnector)
    connector.get_listen_count = AsyncMock(return_value=500)
    connector.get_listens = AsyncMock(return_value=[])
    return connector


# ---------------------------------------------------------------------------
# concurrency attribute
# ---------------------------------------------------------------------------


class TestConcurrency:
    """Tests for the concurrency class attribute."""

    def test_concurrency_is_parallel(self) -> None:
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        assert strategy.concurrency == "parallel"


# ---------------------------------------------------------------------------
# plan() tests
# ---------------------------------------------------------------------------


class TestPlan:
    """Tests for ListenBrainzSyncStrategy.plan()."""

    @pytest.mark.asyncio
    async def test_full_sync_no_watermark(self) -> None:
        """Returns a single descriptor with min_ts=None for full sync."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection()
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        desc = descriptors[0]
        assert desc.task_type == types_module.SyncTaskType.TIME_RANGE
        assert desc.params["username"] == connection.external_user_id
        assert desc.params["min_ts"] is None
        assert desc.progress_total == 500

    @pytest.mark.asyncio
    async def test_incremental_sync_with_watermark(self) -> None:
        """Uses watermark for incremental sync (min_ts set)."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={"listens": {"last_listened_at": 1700000000}},
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        desc = descriptors[0]
        assert desc.params["min_ts"] == 1700000000

    @pytest.mark.asyncio
    async def test_incremental_description_says_since(self) -> None:
        """Description mentions 'since' for incremental sync."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        watermark_ts = 1700000000
        connection = _make_connection(
            sync_watermark={"listens": {"last_listened_at": watermark_ts}},
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        desc = descriptors[0]
        assert "since" in desc.description.lower()
        # Should include the date
        expected_date = (
            datetime.datetime.fromtimestamp(watermark_ts, tz=datetime.UTC)
            .date()
            .isoformat()
        )
        assert expected_date in desc.description

    @pytest.mark.asyncio
    async def test_full_sync_description(self) -> None:
        """Description for full sync says 'Syncing listening history'."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection()
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert descriptors[0].description == "Syncing listening history"

    @pytest.mark.asyncio
    async def test_listen_count_failure_sets_progress_total_none(self) -> None:
        """progress_total is None when get_listen_count raises."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection()
        connector = _make_lb_connector()
        connector.get_listen_count = AsyncMock(side_effect=httpx.HTTPError("API error"))

        descriptors = await strategy.plan(session, connection, connector)

        assert descriptors[0].progress_total is None


# ---------------------------------------------------------------------------
# plan() backward-compatible watermark tests
# ---------------------------------------------------------------------------


class TestPlanBackwardCompatibility:
    """Tests for two-ended watermark reading with backward compatibility."""

    @pytest.mark.asyncio
    async def test_legacy_last_listened_at_treated_as_newest_synced_at(self) -> None:
        """Legacy watermark with only last_listened_at plans one task."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={"listens": {"last_listened_at": 1700000000}},
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        desc = descriptors[0]
        assert desc.params["min_ts"] == 1700000000
        assert "since" in desc.description.lower()

    @pytest.mark.asyncio
    async def test_new_watermark_with_both_fields_plans_two_tasks(self) -> None:
        """Watermark with newest_synced_at and oldest_synced_at plans two tasks."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "listens": {
                    "newest_synced_at": 1700000000,
                    "oldest_synced_at": 1650000000,
                }
            },
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 2

    @pytest.mark.asyncio
    async def test_new_watermark_with_only_newest_plans_one_task(self) -> None:
        """Watermark with only newest_synced_at (no oldest) plans one task."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "listens": {
                    "newest_synced_at": 1700000000,
                }
            },
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        assert len(descriptors) == 1
        desc = descriptors[0]
        assert desc.params["min_ts"] == 1700000000

    @pytest.mark.asyncio
    async def test_interrupted_sync_first_task_has_min_ts(self) -> None:
        """First task (new listens) has min_ts=newest_synced_at."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "listens": {
                    "newest_synced_at": 1700000000,
                    "oldest_synced_at": 1680000000,
                }
            },
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        first = descriptors[0]
        assert first.params["min_ts"] == 1700000000
        assert "since" in first.description.lower()

    @pytest.mark.asyncio
    async def test_interrupted_sync_second_task_has_max_ts(self) -> None:
        """Second task (backfill) has max_ts=oldest_synced_at and min_ts=None."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        connection = _make_connection(
            sync_watermark={
                "listens": {
                    "newest_synced_at": 1700000000,
                    "oldest_synced_at": 1680000000,
                }
            },
        )
        connector = _make_lb_connector()

        descriptors = await strategy.plan(session, connection, connector)

        second = descriptors[1]
        assert second.params["max_ts"] == 1680000000
        assert second.params["min_ts"] is None
        assert "backfill" in second.description.lower()


# ---------------------------------------------------------------------------
# execute() tests
# ---------------------------------------------------------------------------


class TestExecute:
    """Tests for ListenBrainzSyncStrategy.execute()."""

    @pytest.mark.asyncio
    async def test_upserts_listens(self) -> None:
        """Calls connector and upserts for each listen."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100, "Song A", "Artist A")
        listen2 = _make_listen(1700000050, "Song B", "Artist B")

        # First call returns 2 listens, second call returns empty (stop)
        connector.get_listens = AsyncMock(side_effect=[[listen1, listen2], []])

        with (
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ) as mock_upsert_artist,
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ) as mock_upsert_track,
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ) as mock_upsert_event,
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        assert result["items_created"] == 2
        assert result["last_listened_at"] == 1700000100
        assert mock_upsert_artist.call_count == 2
        assert mock_upsert_track.call_count == 2
        assert mock_upsert_event.call_count == 2
        session.commit.assert_called()

    @pytest.mark.asyncio
    async def test_page_limit_stops_sync_with_note(self) -> None:
        """Sync stops after hitting the page limit and includes a note."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        # Return a non-empty page every time (would loop forever without limit)
        listen = _make_listen(1700000100, "Song A", "Artist A")
        connector.get_listens = AsyncMock(return_value=[listen])

        with (
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module,
                "MAX_PAGES",
                3,
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        # Should have fetched exactly MAX_PAGES pages
        assert connector.get_listens.call_count == 3
        assert result["items_created"] == 3
        assert result.get("page_limit_reached") is True

    @pytest.mark.asyncio
    async def test_shutdown_request_raises_with_resume_params(self) -> None:
        """ShutdownRequest raised immediately when shutdown_requested is set."""
        sync_base.shutdown_requested.set()
        try:
            strategy = lb_sync_module.ListenBrainzSyncStrategy()
            session = AsyncMock()
            task = _make_task(
                params={
                    "username": "testuser",
                    "max_ts": 99999,
                    "items_so_far": 42,
                    "pages_fetched": 3,
                    "last_listened_at": 1700000100,
                },
            )
            connector = _make_lb_connector()

            connection = _make_connection()
            with pytest.raises(sync_base.ShutdownRequest) as exc_info:
                await strategy.execute(session, task, connector, connection)

            assert exc_info.value.resume_params["max_ts"] == 99999
            assert exc_info.value.resume_params["items_so_far"] == 42
            assert exc_info.value.resume_params["pages_fetched"] == 3
            assert exc_info.value.resume_params["last_listened_at"] == 1700000100
            # Shutdown check fires before the API call
            connector.get_listens.assert_not_called()
        finally:
            sync_base.shutdown_requested.clear()

    @pytest.mark.asyncio
    async def test_rate_limit_raises_defer_request(self) -> None:
        """RateLimitExceededError raises DeferRequest with resume params."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        connector.get_listens = AsyncMock(
            side_effect=connector_base.RateLimitExceededError(
                retry_after=300.0, max_wait=120.0
            )
        )

        connection = _make_connection()
        with pytest.raises(sync_base.DeferRequest) as exc_info:
            await strategy.execute(session, task, connector, connection)

        assert exc_info.value.retry_after == 300.0
        assert "max_ts" in exc_info.value.resume_params
        assert "items_so_far" in exc_info.value.resume_params

    @pytest.mark.asyncio
    async def test_deferral_resume_preserves_watermark(self) -> None:
        """Full deferral cycle: execute -> DeferRequest -> resume with watermark.

        Verifies that last_listened_at survives the deferral cycle: the first
        execute captures it from page 1, the DeferRequest includes it in
        resume_params, and a resumed execute uses the preserved value rather
        than re-deriving it from a later page.
        """
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)

        # Phase 1: First execute fetches page 1, then hits rate limit on page 2
        task_phase1 = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100, "Song A", "Artist A")
        listen2 = _make_listen(1700000050, "Song B", "Artist B")

        connector.get_listens = AsyncMock(
            side_effect=[
                [listen1, listen2],
                connector_base.RateLimitExceededError(
                    retry_after=300.0, max_wait=120.0
                ),
            ]
        )

        with (
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
            pytest.raises(sync_base.DeferRequest) as exc_info,
        ):
            connection = _make_connection()
            await strategy.execute(session, task_phase1, connector, connection)

        defer = exc_info.value
        assert defer.resume_params["last_listened_at"] == 1700000100
        assert defer.resume_params["items_so_far"] == 2
        assert defer.resume_params["max_ts"] == 1700000050

        # Phase 2: Simulate the worker merging resume_params into task.params,
        # then re-executing. The resumed execute should preserve last_listened_at
        # from phase 1 even though page 2's listens are older.
        merged_params = {**task_phase1.params, **defer.resume_params}
        task_phase2 = _make_task(params=merged_params)

        listen3 = _make_listen(1700000020, "Song C", "Artist C")

        connector2 = _make_lb_connector()
        connector2.get_listens = AsyncMock(side_effect=[[listen3], []])

        with (
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            result = await strategy.execute(
                session, task_phase2, connector2, connection
            )

        # last_listened_at should be from phase 1 (1700000100), not page 2 (1700000020)
        assert result["last_listened_at"] == 1700000100
        # items_created should include both phases: 2 from phase 1 + 1 from phase 2
        assert result["items_created"] == 3

    @pytest.mark.asyncio
    async def test_result_includes_watermark_dict(self) -> None:
        """Result includes a 'watermark' dict for the worker to write back."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100, "Song A", "Artist A")
        connector.get_listens = AsyncMock(side_effect=[[listen1], []])

        with (
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module, "_upsert_track", new_callable=AsyncMock
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        assert result["watermark"] == {
            "newest_synced_at": 1700000100,
            "oldest_synced_at": 1700000100,
        }

    @pytest.mark.asyncio
    async def test_result_no_watermark_when_no_listens(self) -> None:
        """Result has no 'watermark' key when no listens were processed."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()
        connector.get_listens = AsyncMock(return_value=[])

        connection = _make_connection()
        result = await strategy.execute(session, task, connector, connection)

        assert "watermark" not in result


# ---------------------------------------------------------------------------
# Incremental watermark tests
# ---------------------------------------------------------------------------


class TestIncrementalWatermark:
    """Tests for per-page watermark updates during execute()."""

    @pytest.mark.asyncio
    async def test_watermark_updated_after_each_page(self) -> None:
        """connection.sync_watermark is updated after each page commit."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()
        connection = _make_connection()

        # Page 1: listens at 1700000200 (newest) and 1700000100 (oldest)
        page1 = [
            _make_listen(1700000200, "Song A", "Artist A"),
            _make_listen(1700000100, "Song B", "Artist B"),
        ]
        # Page 2: listens at 1700000050 and 1700000010
        page2 = [
            _make_listen(1700000050, "Song C", "Artist C"),
            _make_listen(1700000010, "Song D", "Artist D"),
        ]

        connector.get_listens = AsyncMock(side_effect=[page1, page2, []])

        # Track watermark values after each commit
        watermarks_after_commit: list[dict[str, object]] = []

        async def capture_watermark() -> None:
            watermarks_after_commit.append(
                dict(connection.sync_watermark.get("listens", {}))
            )

        session.commit = AsyncMock(side_effect=capture_watermark)

        with (
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            await strategy.execute(session, task, connector, connection)

        # After page 1: newest=1700000200, oldest=1700000100
        assert watermarks_after_commit[0] == {
            "newest_synced_at": 1700000200,
            "oldest_synced_at": 1700000100,
        }
        # After page 2: newest still 1700000200, oldest advances to 1700000010
        assert watermarks_after_commit[1] == {
            "newest_synced_at": 1700000200,
            "oldest_synced_at": 1700000010,
        }

    @pytest.mark.asyncio
    async def test_result_watermark_uses_two_ended_structure(self) -> None:
        """Final result has watermark with newest/oldest_synced_at."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()
        connection = _make_connection()

        page1 = [
            _make_listen(1700000200, "Song A", "Artist A"),
            _make_listen(1700000100, "Song B", "Artist B"),
        ]
        page2 = [
            _make_listen(1700000050, "Song C", "Artist C"),
        ]

        connector.get_listens = AsyncMock(side_effect=[page1, page2, []])

        with (
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            result = await strategy.execute(session, task, connector, connection)

        assert result["watermark"] == {
            "newest_synced_at": 1700000200,
            "oldest_synced_at": 1700000050,
        }
        # last_listened_at should still be present for backward compat
        assert result["last_listened_at"] == 1700000200


# ---------------------------------------------------------------------------
# Adaptive page size tests
# ---------------------------------------------------------------------------


class TestAdaptivePageSize:
    """Tests for adaptive page size on timeout."""

    @pytest.mark.asyncio
    async def test_halves_page_size_on_remote_protocol_error(self) -> None:
        """Page size halves on RemoteProtocolError, retries same cursor."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100, "Song A", "Artist A")

        # First call at 1000: timeout. Second call at 500: success. Third: empty.
        connector.get_listens = AsyncMock(
            side_effect=[
                httpx.RemoteProtocolError("Server disconnected"),
                [listen1],
                [],
            ]
        )

        with (
            patch("resonance.sync.listenbrainz.asyncio.sleep", new_callable=AsyncMock),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        # Should have called get_listens 3 times:
        # 1. count=1000 (failed), 2. count=500 (success), 3. count=1000 (empty)
        calls = connector.get_listens.call_args_list
        assert calls[0].kwargs["count"] == 1000
        assert calls[1].kwargs["count"] == 500
        # After success, grows back to 1000
        assert calls[2].kwargs["count"] == 1000
        assert result["items_created"] == 1

    @pytest.mark.asyncio
    async def test_multiple_halves_down_to_minimum(self) -> None:
        """Page size halves multiple times down to _MIN_PAGE_SIZE."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100, "Song A", "Artist A")

        # Timeout at 1000, 500, 250 — success at 125
        connector.get_listens = AsyncMock(
            side_effect=[
                httpx.RemoteProtocolError("timeout"),
                httpx.RemoteProtocolError("timeout"),
                httpx.RemoteProtocolError("timeout"),
                [listen1],
                [],
            ]
        )

        with (
            patch("resonance.sync.listenbrainz.asyncio.sleep", new_callable=AsyncMock),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            connection = _make_connection()
            await strategy.execute(session, task, connector, connection)

        calls = connector.get_listens.call_args_list
        assert calls[0].kwargs["count"] == 1000
        assert calls[1].kwargs["count"] == 500
        assert calls[2].kwargs["count"] == 250
        assert calls[3].kwargs["count"] == 125
        # Grows back: 125 → 250
        assert calls[4].kwargs["count"] == 250

    @pytest.mark.asyncio
    async def test_raises_at_minimum_page_size(self) -> None:
        """Raises RemoteProtocolError if it fails at minimum page size."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        # Timeout all the way down to min, then fail again at min
        connector.get_listens = AsyncMock(
            side_effect=httpx.RemoteProtocolError("Server disconnected")
        )

        with (
            patch("resonance.sync.listenbrainz.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(httpx.RemoteProtocolError),
        ):
            connection = _make_connection()
            await strategy.execute(session, task, connector, connection)

        # Should have tried 1000, 500, 250, 125, 100, then raised at 100
        calls = connector.get_listens.call_args_list
        counts = [c.kwargs["count"] for c in calls]
        assert counts == [1000, 500, 250, 125, 100]

    @pytest.mark.asyncio
    async def test_read_timeout_also_triggers_adaptive(self) -> None:
        """ReadTimeout triggers the same adaptive behavior."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100, "Song A", "Artist A")

        connector.get_listens = AsyncMock(
            side_effect=[
                httpx.ReadTimeout("timeout"),
                [listen1],
                [],
            ]
        )

        with (
            patch("resonance.sync.listenbrainz.asyncio.sleep", new_callable=AsyncMock),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            connection = _make_connection()
            result = await strategy.execute(session, task, connector, connection)

        calls = connector.get_listens.call_args_list
        assert calls[0].kwargs["count"] == 1000
        assert calls[1].kwargs["count"] == 500
        assert result["items_created"] == 1

    @pytest.mark.asyncio
    async def test_backoff_increases_with_reduction_depth(self) -> None:
        """Backoff delay scales with number of reductions."""
        strategy = lb_sync_module.ListenBrainzSyncStrategy()
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"username": "testuser"})
        connector = _make_lb_connector()

        listen1 = _make_listen(1700000100, "Song A", "Artist A")

        connector.get_listens = AsyncMock(
            side_effect=[
                httpx.RemoteProtocolError("timeout"),
                httpx.RemoteProtocolError("timeout"),
                [listen1],
                [],
            ]
        )

        with (
            patch(
                "resonance.sync.listenbrainz.asyncio.sleep",
                new_callable=AsyncMock,
            ) as mock_sleep,
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_artists",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "bulk_fetch_tracks",
                new_callable=AsyncMock,
                return_value={},
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                lb_sync_module.runner_module,
                "_upsert_listening_event",
                new_callable=AsyncMock,
            ),
        ):
            connection = _make_connection()
            await strategy.execute(session, task, connector, connection)

        # Backoff: 5s * depth (5, 10)
        sleep_calls = [c.args[0] for c in mock_sleep.call_args_list]
        assert sleep_calls == [5.0, 10.0]


# ---------------------------------------------------------------------------
# _cast_connector tests
# ---------------------------------------------------------------------------


class TestCastConnector:
    """Tests for the _cast_connector helper."""

    def test_returns_lb_connector(self) -> None:
        connector = MagicMock(spec=listenbrainz_module.ListenBrainzConnector)
        result = lb_sync_module._cast_connector(connector)
        assert result is connector

    def test_raises_type_error_for_wrong_type(self) -> None:
        connector = MagicMock(spec=connector_base.BaseConnector)
        with pytest.raises(TypeError, match="Expected ListenBrainzConnector"):
            lb_sync_module._cast_connector(connector)
