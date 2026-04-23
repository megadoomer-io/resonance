"""End-to-end integration tests for the calendar feed sync pipeline.

These tests exercise the full sync pipeline — configure connection, fetch with
mocked HTTP, parse iCal, and verify orchestration of upsert calls.  The database
session is mocked (matching the project's existing test patterns), so these
validate the orchestration logic rather than true database behavior.
"""

from __future__ import annotations

import datetime
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import resonance.concerts.worker as concert_worker
import resonance.types as types_module

# ---------------------------------------------------------------------------
# Sample iCal data
# ---------------------------------------------------------------------------

SAMPLE_SONGKICK_FEED = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Songkick//Events//EN
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260511
SUMMARY:Puscifer at Golden Gate Theatre (11 May 26)
LOCATION:Golden Gate Theatre, San Francisco, CA, US
UID:songkick-event-12345@songkick.com
URL:https://www.songkick.com/concerts/12345
DESCRIPTION:You're going.
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260516
SUMMARY:Lagwagon, Strung Out, and Swingin' Utters at The Fillmore \
(16 May 26) with Western Addiction
LOCATION:The Fillmore, San Francisco, CA, US
UID:songkick-event-67890@songkick.com
URL:https://www.songkick.com/concerts/67890
DESCRIPTION:You're tracking this event.
END:VEVENT
END:VCALENDAR
"""

SAMPLE_GENERIC_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Generic//EN
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260701
SUMMARY:Neighborhood Block Party
UID:generic-001@example.com
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260815
SUMMARY:Company Picnic
LOCATION:Central Park, New York, NY, US
UID:generic-002@example.com
END:VEVENT
END:VCALENDAR
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_connection(
    *,
    connection_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    service_type: types_module.ServiceType = types_module.ServiceType.SONGKICK,
    external_user_id: str | None = "testuser",
    url: str | None = None,
    enabled: bool = True,
    last_synced_at: datetime.datetime | None = None,
) -> MagicMock:
    """Create a mock ServiceConnection."""
    conn = MagicMock()
    conn.id = connection_id or uuid.uuid4()
    conn.user_id = user_id or uuid.uuid4()
    conn.service_type = service_type
    conn.external_user_id = external_user_id
    conn.url = url
    conn.enabled = enabled
    conn.last_synced_at = last_synced_at
    return conn


def _make_task(
    *,
    task_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
) -> MagicMock:
    """Create a mock Task."""
    task = MagicMock()
    task.id = task_id or uuid.uuid4()
    task.user_id = user_id or uuid.uuid4()
    task.status = types_module.SyncStatus.PENDING
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
    task: MagicMock,
    connection: MagicMock,
) -> None:
    """Set up session.execute to return task then connection."""
    task_result = MagicMock()
    task_result.scalar_one_or_none.return_value = task
    conn_result = MagicMock()
    conn_result.scalar_one_or_none.return_value = connection
    session.execute.side_effect = [task_result, conn_result]


def _setup_http_mock(mock_client_cls: MagicMock, ical_text: str) -> MagicMock:
    """Configure httpx.AsyncClient mock to return given iCal text."""
    mock_response = MagicMock(spec=httpx.Response)
    mock_response.text = ical_text
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.get.return_value = mock_response
    mock_client_cls.return_value = mock_client

    return mock_response


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullSongkickSyncPipeline:
    """End-to-end test for the Songkick connection sync pipeline."""

    @pytest.mark.anyio()
    async def test_full_songkick_sync_pipeline(self) -> None:
        """Syncing a Songkick connection parses events and calls all upserts."""
        connection = _make_connection()
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls,
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
            _setup_http_mock(mock_client_cls, SAMPLE_SONGKICK_FEED)

            # Upsert mocks — 2 feeds * 2 events each = up to 4 venues
            mock_venue = MagicMock()
            mock_upsert_venue.return_value = mock_venue

            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_upsert_candidates.return_value = 1
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            # Both feeds fetched: 2 events per feed * 2 feeds = 4 events
            assert mock_upsert_event.await_count == 4

            # Verify all events use SONGKICK source service
            for event_call in mock_upsert_event.call_args_list:
                assert event_call.args[2] == types_module.ServiceType.SONGKICK

            # Attendance: only the attendance feed has DESCRIPTION, but both
            # feeds return events — attendance upserts for events with status
            # The attendance feed has 2 events with attendance, the tracked
            # feed has 0 (no attendance parsing for SONGKICK_TRACKED_ARTIST)
            assert mock_upsert_attendance.await_count == 2

            # Attendance status for first feed events
            att_call_1 = mock_upsert_attendance.call_args_list[0]
            assert att_call_1.args[1] == connection.user_id
            assert att_call_1.args[3] == "going"

            att_call_2 = mock_upsert_attendance.call_args_list[1]
            assert att_call_2.args[3] == "interested"

            # Lifecycle complete_task called
            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert result_arg["total_events"] == 4

            # connection last_synced_at updated
            assert connection.last_synced_at is not None
            assert isinstance(connection.last_synced_at, datetime.datetime)

            # Session committed
            session.commit.assert_awaited()


class TestIdempotentSync:
    """Verify that syncing the same connection data twice does not raise errors."""

    @pytest.mark.anyio()
    async def test_idempotent_sync(self) -> None:
        """Running sync twice with the same data succeeds both times."""
        connection = _make_connection()
        task_1 = _make_task(user_id=connection.user_id)
        task_2 = _make_task(user_id=connection.user_id)
        session = AsyncMock()

        # First call: task_1, connection
        task_result_1 = MagicMock()
        task_result_1.scalar_one_or_none.return_value = task_1
        conn_result_1 = MagicMock()
        conn_result_1.scalar_one_or_none.return_value = connection

        # Second call: task_2, connection
        task_result_2 = MagicMock()
        task_result_2.scalar_one_or_none.return_value = task_2
        conn_result_2 = MagicMock()
        conn_result_2.scalar_one_or_none.return_value = connection

        session.execute.side_effect = [
            task_result_1,
            conn_result_1,
            task_result_2,
            conn_result_2,
        ]

        ctx = _make_ctx(session)

        with (
            patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls,
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
            _setup_http_mock(mock_client_cls, SAMPLE_SONGKICK_FEED)

            mock_venue = MagicMock()
            mock_upsert_venue.return_value = mock_venue
            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_upsert_candidates.return_value = 0
            mock_match.return_value = 0

            # First sync
            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task_1.id)
            )

            first_sync_time = connection.last_synced_at
            assert first_sync_time is not None

            # Second sync (same data, no errors)
            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task_2.id)
            )

            second_sync_time = connection.last_synced_at
            assert second_sync_time is not None
            assert second_sync_time >= first_sync_time

            # complete_task called twice
            assert mock_complete.await_count == 2

            # Session committed: 2 commits per run (RUNNING status + completion)
            assert session.commit.await_count == 4


class TestGenericIcalSync:
    """Tests for syncing a generic iCal connection (no artist extraction)."""

    @pytest.mark.anyio()
    async def test_generic_ical_no_candidates_no_attendance(self) -> None:
        """Generic iCal feed does not extract artist candidates or attendance."""
        connection = _make_connection(
            service_type=types_module.ServiceType.ICAL,
            external_user_id=None,
            url="https://example.com/calendar.ics",
        )
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls,
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
            _setup_http_mock(mock_client_cls, SAMPLE_GENERIC_ICAL)

            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            # -- 2 events parsed --
            assert mock_upsert_event.await_count == 2

            # -- Event source service is ICAL --
            for event_call in mock_upsert_event.call_args_list:
                assert event_call.args[2] == types_module.ServiceType.ICAL

            # -- Generic iCal: no venue parsing (venue is None for both) --
            mock_upsert_venue.assert_not_awaited()

            for event_call in mock_upsert_event.call_args_list:
                assert event_call.args[3] is None  # venue arg is None

            # -- Generic feed: no artist candidate extraction --
            mock_upsert_candidates.assert_not_awaited()

            # -- Generic feed: no attendance status --
            mock_upsert_attendance.assert_not_awaited()

            # -- match_candidates_to_artists still called (handles empty state) --
            assert mock_match.await_count == 2

            # -- complete_task called and connection updated --
            mock_complete.assert_awaited_once()
            assert connection.last_synced_at is not None
            session.commit.assert_awaited()

    @pytest.mark.anyio()
    async def test_generic_ical_event_dates_parsed(self) -> None:
        """Generic iCal events have correct dates passed to upsert_event."""
        connection = _make_connection(
            service_type=types_module.ServiceType.ICAL,
            external_user_id=None,
            url="https://example.com/calendar.ics",
        )
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls,
            patch("resonance.concerts.worker.concert_sync.upsert_venue"),
            patch(
                "resonance.concerts.worker.concert_sync.upsert_event"
            ) as mock_upsert_event,
            patch("resonance.concerts.worker.concert_sync.upsert_candidates"),
            patch("resonance.concerts.worker.concert_sync.upsert_attendance"),
            patch(
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
            patch("resonance.concerts.worker.lifecycle_module.complete_task"),
        ):
            _setup_http_mock(mock_client_cls, SAMPLE_GENERIC_ICAL)

            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            # Event 1: 2026-07-01
            parsed_event_1 = mock_upsert_event.call_args_list[0].args[1]
            assert parsed_event_1.event_date == datetime.date(2026, 7, 1)

            # Event 2: 2026-08-15
            parsed_event_2 = mock_upsert_event.call_args_list[1].args[1]
            assert parsed_event_2.event_date == datetime.date(2026, 8, 15)
