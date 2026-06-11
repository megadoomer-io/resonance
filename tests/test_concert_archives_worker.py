"""Tests for the Concert Archives CSV import worker task."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.concerts.worker as concert_worker
import resonance.types as types_module

if TYPE_CHECKING:
    import datetime
    from collections.abc import Iterator

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
    parent_id: uuid.UUID | None = None,
    status: types_module.SyncStatus = types_module.SyncStatus.PENDING,
) -> MagicMock:
    """Create a mock Task."""
    task = MagicMock()
    task.id = task_id or uuid.uuid4()
    task.user_id = user_id or uuid.uuid4()
    task.service_connection_id = service_connection_id or uuid.uuid4()
    task.parent_id = parent_id
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
    return {"session_factory": session_factory, "redis": AsyncMock()}


def _setup_session_queries(
    session: AsyncMock,
    task: MagicMock | None,
    connection: MagicMock | None,
    existing_children: list[MagicMock] | None = None,
) -> None:
    """Set up session.execute for the planner flow.

    Query order: (1) load task, (2) check children, (3) load connection.
    """
    task_result = MagicMock()
    task_result.scalar_one_or_none.return_value = task

    children_result = MagicMock()
    children_scalars = MagicMock()
    children_scalars.all.return_value = existing_children or []
    children_result.scalars.return_value = children_scalars

    conn_result = MagicMock()
    conn_result.scalar_one_or_none.return_value = connection

    session.execute.side_effect = [task_result, children_result, conn_result]


@contextmanager
def _patch_candidate_sync(
    *,
    event_results: list[tuple[MagicMock, bool]] | None = None,
) -> Iterator[dict[str, Any]]:
    """Patch the candidate-based sync functions used by the worker.

    Returns a dict of mock objects keyed by function name.
    """
    mock_venue_candidate = MagicMock()
    mock_venue = MagicMock()
    mock_event_candidate = MagicMock()
    mock_event = MagicMock()

    default_event_result = (mock_event, True)
    event_side_effect = event_results if event_results else None

    with (
        patch(
            "resonance.concerts.worker.concert_sync.upsert_venue_candidate"
        ) as mock_uvc,
        patch(
            "resonance.concerts.worker.concert_sync.resolve_venue_candidate"
        ) as mock_rvc,
        patch(
            "resonance.concerts.worker.concert_sync.upsert_event_candidate"
        ) as mock_uec,
        patch(
            "resonance.concerts.worker.concert_sync.resolve_event_candidate"
        ) as mock_rec,
        patch(
            "resonance.concerts.worker.concert_sync.upsert_candidates"
        ) as mock_candidates,
        patch(
            "resonance.concerts.worker.concert_sync.upsert_attendance"
        ) as mock_attendance,
        patch(
            "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
        ) as mock_match,
        patch(
            "resonance.concerts.worker.lifecycle_module.complete_task"
        ) as mock_complete,
    ):
        mock_uvc.return_value = mock_venue_candidate
        mock_rvc.return_value = mock_venue
        mock_uec.return_value = mock_event_candidate
        if event_side_effect:
            mock_rec.side_effect = event_side_effect
        else:
            mock_rec.return_value = default_event_result
        mock_candidates.return_value = 2
        mock_match.return_value = 1

        yield {
            "upsert_venue_candidate": mock_uvc,
            "resolve_venue_candidate": mock_rvc,
            "upsert_event_candidate": mock_uec,
            "resolve_event_candidate": mock_rec,
            "upsert_candidates": mock_candidates,
            "upsert_attendance": mock_attendance,
            "match_candidates": mock_match,
            "complete_task": mock_complete,
        }


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

    @pytest.mark.anyio()
    async def test_orphan_recovery_missing_csv_fails_gracefully(self) -> None:
        """Fails task gracefully when csv_content is None and no children."""
        task = _make_task()
        session = AsyncMock()
        _setup_session_queries(session, task, None)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_concert_archives(ctx, str(task.id))

            mock_fail.assert_awaited_once()
            error_msg = mock_fail.call_args.args[2]
            assert "CSV content unavailable" in error_msg
            assert "re-upload" in error_msg


class TestSyncConcertArchivesPlanner:
    """Tests for the planner that creates chunk children."""

    @pytest.mark.anyio()
    async def test_creates_chunk_children(self) -> None:
        """Parses CSV and creates chunk children with correct params."""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        task.params = {}
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        await concert_worker.sync_concert_archives(ctx, str(task.id), _SAMPLE_CSV)

        # 2 events in CSV, chunk size 25 → 1 chunk child
        added_objects = [
            call.args[0]
            for call in session.add.call_args_list
            if hasattr(call.args[0], "task_type")
            and str(getattr(call.args[0], "task_type", ""))
            == types_module.TaskType.CONCERT_ARCHIVES_CHUNK.value
        ]
        assert len(added_objects) == 1
        child = added_objects[0]
        assert child.params["chunk_index"] == 0

        # Parsed events stored in parent params
        assert "parsed_events" in task.params
        assert len(task.params["parsed_events"]) == 2

        # Progress total set
        assert task.progress_total == 2

        # First child enqueued
        arq_redis = ctx["redis"]
        arq_redis.enqueue_job.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_csv_parse_error_fails_task(self) -> None:
        """Fails task with traceback when CSV parsing raises an error."""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        task.params = {}
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_concert_archives(ctx, str(task.id), _INVALID_CSV)
            mock_fail.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_warnings_stored_in_params(self) -> None:
        """Warnings from CSV parsing are stored in parent task params."""
        csv_with_warning = """\
Start Date,End Date,Status,Concert Name,Bands Seen,Bands Not Seen,Venue,Location,URL
,,Past,Some Show,Band A,,Venue,"City, Country",https://example.com
"""
        connection = _make_connection()
        task = _make_task(
            user_id=connection.user_id,
            service_connection_id=connection.id,
        )
        task.params = {}
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        await concert_worker.sync_concert_archives(ctx, str(task.id), csv_with_warning)

        assert "warnings" in task.params
        assert len(task.params["warnings"]) > 0

    @pytest.mark.anyio()
    async def test_orphan_recovery_resumes_pending_child(self) -> None:
        """Orphan recovery with existing children re-enqueues first pending."""
        task = _make_task()
        pending_child = MagicMock()
        pending_child.id = uuid.uuid4()
        pending_child.status = types_module.SyncStatus.PENDING
        session = AsyncMock()
        _setup_session_queries(session, task, None, [pending_child])
        ctx = _make_ctx(session)

        await concert_worker.sync_concert_archives(ctx, str(task.id))

        arq_redis = ctx["redis"]
        arq_redis.enqueue_job.assert_awaited_once()
        call_args = arq_redis.enqueue_job.call_args
        assert call_args.args[0] == "sync_concert_archives_chunk"


# Chunk processor tests live in test_concert_archives_chunk.py
