"""Tests for the Concert Archives CSV import worker task."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.concerts.worker as concert_worker
import resonance.types as types_module

if TYPE_CHECKING:
    import datetime

# ---------------------------------------------------------------------------
# Sample CSV data for testing
# ---------------------------------------------------------------------------

_SAMPLE_CSV = (
    "Start Date,End Date,Status,Concert Name,"
    "Bands Seen,Bands Not Seen,Venue,Location,URL\n"
    "05/15/2023,,Past,Summer Fest,"
    "The National / Arcade Fire,,The Fillmore,"
    '"San Francisco, California, United States",'
    "https://concertarchives.org/u/concerts/123\n"
    "03/20/2024,,Past,,"
    "Radiohead,,Madison Square Garden,"
    '"New York, New York, United States",'
    "https://concertarchives.org/u/concerts/456\n"
)

_CANCELLED_CSV = """\
Start Date,End Date,Status,Concert Name,Bands Seen,Bands Not Seen,Venue,Location,URL
05/15/2023,,Cancelled,Cancelled Show,Some Band,,The Venue,"City, State, Country",https://example.com/cancelled
"""

_INVALID_CSV = "This is not a CSV file at all"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_connection(
    *,
    connection_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    service_type: types_module.ServiceType = types_module.ServiceType.CONCERT_ARCHIVES,
    enabled: bool = True,
    last_synced_at: datetime.datetime | None = None,
) -> MagicMock:
    """Create a mock ServiceConnection for Concert Archives."""
    conn = MagicMock()
    conn.id = connection_id or uuid.uuid4()
    conn.user_id = user_id or uuid.uuid4()
    conn.service_type = service_type
    conn.enabled = enabled
    conn.last_synced_at = last_synced_at
    return conn


def _make_task(
    *,
    task_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    service_connection_id: uuid.UUID | None = None,
    status: types_module.SyncStatus = types_module.SyncStatus.PENDING,
) -> MagicMock:
    """Create a mock Task."""
    task = MagicMock()
    task.id = task_id or uuid.uuid4()
    task.user_id = user_id or uuid.uuid4()
    task.service_connection_id = service_connection_id or uuid.uuid4()
    task.status = status
    task.started_at = None
    task.completed_at = None
    task.result = {}
    task.error_message = None
    task.parent_id = None
    return task


def _make_ctx(session: AsyncMock) -> dict[str, object]:
    """Build a minimal worker context with a mock session factory."""
    session_factory = MagicMock()
    session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
    session_factory.return_value.__aexit__ = AsyncMock(return_value=False)
    return {"session_factory": session_factory}


def _setup_session_queries(
    session: AsyncMock,
    task: MagicMock | None,
    connection: MagicMock | None,
) -> None:
    """Set up session.execute to return task on first call, connection on second."""
    task_result = MagicMock()
    task_result.scalar_one_or_none.return_value = task

    conn_result = MagicMock()
    conn_result.scalar_one_or_none.return_value = connection

    session.execute.side_effect = [task_result, conn_result]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSyncConcertArchivesLifecycle:
    """Tests for task lifecycle handling in sync_concert_archives."""

    @pytest.mark.anyio()
    async def test_task_not_found_returns_early(self) -> None:
        """Returns early without error when task ID is not found."""
        session = AsyncMock()
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = None
        session.execute.return_value = task_result
        ctx = _make_ctx(session)

        await concert_worker.sync_concert_archives(ctx, str(uuid.uuid4()), _SAMPLE_CSV)

        # Should only have one execute call (task lookup) — no further processing
        assert session.execute.await_count == 1

    @pytest.mark.anyio()
    async def test_connection_not_found_fails_task(self) -> None:
        """Calls fail_task when connection is not found."""
        task = _make_task()
        session = AsyncMock()
        _setup_session_queries(session, task, None)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_concert_archives(ctx, str(task.id), _SAMPLE_CSV)

            mock_fail.assert_awaited_once()
            error_msg = mock_fail.call_args.args[2]
            assert "not found" in error_msg

    @pytest.mark.anyio()
    async def test_connection_disabled_completes_with_skip(self) -> None:
        """Completes task with skip message when connection is disabled."""
        connection = _make_connection(enabled=False)
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with patch(
            "resonance.concerts.worker.lifecycle_module.complete_task"
        ) as mock_complete:
            await concert_worker.sync_concert_archives(ctx, str(task.id), _SAMPLE_CSV)

            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert result_arg == {"skipped": "connection disabled"}


class TestSyncConcertArchivesImport:
    """Tests for successful CSV import processing."""

    @pytest.mark.anyio()
    async def test_successful_import(self) -> None:
        """Parses CSV, upserts events/venues/candidates, completes task."""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch(
                "resonance.concerts.worker.concert_sync.upsert_venue"
            ) as mock_upsert_venue,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_event"
            ) as mock_upsert_event,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_candidates"
            ) as mock_upsert_candidates,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_attendance"
            ) as mock_upsert_attendance,
            patch(
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
            patch(
                "resonance.concerts.worker.lifecycle_module.complete_task"
            ) as mock_complete,
        ):
            mock_venue = MagicMock()
            mock_upsert_venue.return_value = mock_venue
            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_upsert_candidates.return_value = 2
            mock_match.return_value = 1

            await concert_worker.sync_concert_archives(ctx, str(task.id), _SAMPLE_CSV)

            # 2 events in CSV → 2 venue upserts, 2 event upserts
            assert mock_upsert_venue.await_count == 2
            assert mock_upsert_event.await_count == 2

            # Verify source_service is CONCERT_ARCHIVES
            first_event_call = mock_upsert_event.call_args_list[0]
            assert first_event_call.args[2] == types_module.ServiceType.CONCERT_ARCHIVES

            # Both events have "Past" status → attendance "going"
            assert mock_upsert_attendance.await_count == 2

            # Candidates: first event has 2 artists, second has 1
            assert mock_upsert_candidates.await_count == 2

            # match_candidates called for each event
            assert mock_match.await_count == 2

            # Verify task completed
            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert result_arg["total_events"] == 2
            assert result_arg["events_created"] == 2
            assert result_arg["candidates_created"] == 4  # 2 per event call
            assert result_arg["candidates_matched"] == 2  # 1 per event call

            # Connection last_synced_at updated
            assert connection.last_synced_at is not None

    @pytest.mark.anyio()
    async def test_cancelled_event_no_attendance(self) -> None:
        """Cancelled events do not create attendance records."""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch(
                "resonance.concerts.worker.concert_sync.upsert_venue"
            ) as mock_upsert_venue,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_event"
            ) as mock_upsert_event,
            patch("resonance.concerts.worker.concert_sync.upsert_candidates"),
            patch(
                "resonance.concerts.worker.concert_sync.upsert_attendance"
            ) as mock_upsert_attendance,
            patch(
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
            patch(
                "resonance.concerts.worker.lifecycle_module.complete_task"
            ) as mock_complete,
        ):
            mock_venue = MagicMock()
            mock_upsert_venue.return_value = mock_venue
            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_match.return_value = 0

            await concert_worker.sync_concert_archives(
                ctx, str(task.id), _CANCELLED_CSV
            )

            # Event should still be created
            mock_upsert_event.assert_awaited_once()

            # But no attendance for "Cancelled" status
            mock_upsert_attendance.assert_not_awaited()

            # Task should still complete successfully
            mock_complete.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_csv_parse_error_fails_task(self) -> None:
        """Fails task with traceback when CSV parsing raises an error."""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_concert_archives(ctx, str(task.id), _INVALID_CSV)

            mock_fail.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_warnings_included_in_result(self) -> None:
        """Warnings from CSV parsing are included in the task result."""
        # CSV with a missing date to trigger a warning
        csv_with_warning = """\
Start Date,End Date,Status,Concert Name,Bands Seen,Bands Not Seen,Venue,Location,URL
,,Past,Some Show,Band A,,Venue,"City, Country",https://example.com
"""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch(
                "resonance.concerts.worker.concert_sync.upsert_venue"
            ) as mock_upsert_venue,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_event"
            ) as mock_upsert_event,
            patch("resonance.concerts.worker.concert_sync.upsert_candidates"),
            patch("resonance.concerts.worker.concert_sync.upsert_attendance"),
            patch(
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
            patch(
                "resonance.concerts.worker.lifecycle_module.complete_task"
            ) as mock_complete,
        ):
            mock_upsert_venue.return_value = MagicMock()
            mock_upsert_event.return_value = (MagicMock(), True)
            mock_match.return_value = 0

            await concert_worker.sync_concert_archives(
                ctx, str(task.id), csv_with_warning
            )

            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert "warnings" in result_arg
            assert len(result_arg["warnings"]) > 0

    @pytest.mark.anyio()
    async def test_event_update_counts_correctly(self) -> None:
        """Tracks events_updated when upsert_event returns created=False."""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch(
                "resonance.concerts.worker.concert_sync.upsert_venue"
            ) as mock_upsert_venue,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_event"
            ) as mock_upsert_event,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_candidates"
            ) as mock_upsert_candidates,
            patch("resonance.concerts.worker.concert_sync.upsert_attendance"),
            patch(
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
            patch(
                "resonance.concerts.worker.lifecycle_module.complete_task"
            ) as mock_complete,
        ):
            mock_upsert_venue.return_value = MagicMock()
            # First event is new, second is existing
            mock_event = MagicMock()
            mock_upsert_event.side_effect = [
                (mock_event, True),
                (mock_event, False),
            ]
            mock_upsert_candidates.return_value = 0
            mock_match.return_value = 0

            await concert_worker.sync_concert_archives(ctx, str(task.id), _SAMPLE_CSV)

            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert result_arg["events_created"] == 1
            assert result_arg["events_updated"] == 1
