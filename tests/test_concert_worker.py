"""Tests for the calendar feed sync worker task."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import resonance.concerts.worker as concert_worker
import resonance.types as types_module

if TYPE_CHECKING:
    import datetime

# ---------------------------------------------------------------------------
# Sample iCal data for testing
# ---------------------------------------------------------------------------

_SAMPLE_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:The National at The Fillmore (15 Mar 26)
DTSTART;VALUE=DATE:20260315
UID:songkick-evt-111
URL:https://songkick.com/concerts/111
LOCATION:The Fillmore, San Francisco, CA, US
DESCRIPTION:You're going.
END:VEVENT
BEGIN:VEVENT
SUMMARY:Radiohead at Madison Square Garden (20 Apr 26)
DTSTART;VALUE=DATE:20260420
UID:songkick-evt-222
URL:https://songkick.com/concerts/222
LOCATION:Madison Square Garden, New York, NY, US
DESCRIPTION:You're tracking this event.
END:VEVENT
END:VCALENDAR
"""

_EMPTY_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
END:VCALENDAR
"""

_TRACKED_ARTIST_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:Arcade Fire at Red Rocks (10 Jul 26)
DTSTART;VALUE=DATE:20260710
UID:songkick-tracked-001
URL:https://songkick.com/concerts/999
LOCATION:Red Rocks, Morrison, CO, US
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
    status: types_module.SyncStatus = types_module.SyncStatus.PENDING,
) -> MagicMock:
    """Create a mock Task."""
    task = MagicMock()
    task.id = task_id or uuid.uuid4()
    task.user_id = user_id or uuid.uuid4()
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


class TestSyncCalendarFeedSongkick:
    """Tests for sync_calendar_feed with Songkick connections."""

    @pytest.mark.anyio()
    async def test_songkick_fetches_both_feeds(self) -> None:
        """Songkick connections derive and process both attendance and tracked feeds."""
        connection = _make_connection()
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.text = _SAMPLE_ICAL
        mock_response.raise_for_status = MagicMock()

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
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            mock_venue = MagicMock()
            mock_upsert_venue.return_value = mock_venue
            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_upsert_candidates.return_value = 1
            mock_match.return_value = 1

            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            # Should fetch both URLs (attendance + tracked artist)
            assert mock_client.get.await_count == 2
            expected_base = (
                f"https://www.songkick.com/users/"
                f"{connection.external_user_id}/calendars.ics"
            )
            calls = [call.args[0] for call in mock_client.get.call_args_list]
            assert calls[0] == f"{expected_base}?filter=attendance"
            assert calls[1] == f"{expected_base}?filter=tracked_artist"

            # 2 events per feed * 2 feeds = 4 event upserts
            assert mock_upsert_event.await_count == 4

            # Verify source_service is SONGKICK
            first_event_call = mock_upsert_event.call_args_list[0]
            assert first_event_call.args[2] == types_module.ServiceType.SONGKICK

            # Verify lifecycle complete_task was called
            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert result_arg["total_events"] == 4

            # Verify connection last_synced_at was updated
            assert connection.last_synced_at is not None

    @pytest.mark.anyio()
    async def test_songkick_no_external_user_id_fails(self) -> None:
        """Fails when Songkick connection has no external_user_id."""
        connection = _make_connection(external_user_id=None)
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            mock_fail.assert_awaited_once()
            error_msg = mock_fail.call_args.args[2]
            assert "external_user_id" in error_msg


class TestSyncCalendarFeedIcal:
    """Tests for sync_calendar_feed with iCal connections."""

    @pytest.mark.anyio()
    async def test_ical_uses_url_directly(self) -> None:
        """iCal connections use connection.url directly for a single feed."""
        ical_url = "https://example.com/feed.ics"
        connection = _make_connection(
            service_type=types_module.ServiceType.ICAL,
            external_user_id=None,
            url=ical_url,
        )
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        ical_no_venue = """\
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:Local Show
DTSTART;VALUE=DATE:20260601
UID:generic-evt-001
END:VEVENT
END:VCALENDAR
"""
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.text = ical_no_venue
        mock_response.raise_for_status = MagicMock()

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
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
            patch(
                "resonance.concerts.worker.lifecycle_module.complete_task"
            ) as mock_complete,
        ):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_upsert_candidates.return_value = 0
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            # Should fetch exactly the URL from the connection
            mock_client.get.assert_awaited_once_with(ical_url)

            # Generic iCal: no venue upsert
            mock_upsert_venue.assert_not_awaited()

            # But event should be upserted (with venue=None)
            mock_upsert_event.assert_awaited_once()
            event_call = mock_upsert_event.call_args
            assert event_call.args[2] == types_module.ServiceType.ICAL
            assert event_call.args[3] is None  # venue arg is None

            # Lifecycle helper called
            mock_complete.assert_awaited_once()

    @pytest.mark.anyio()
    async def test_ical_no_url_fails(self) -> None:
        """Fails when iCal connection has no url."""
        connection = _make_connection(
            service_type=types_module.ServiceType.ICAL,
            external_user_id=None,
            url=None,
        )
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            mock_fail.assert_awaited_once()
            error_msg = mock_fail.call_args.args[2]
            assert "URL" in error_msg


class TestSyncCalendarFeedLifecycle:
    """Tests for task lifecycle handling in sync_calendar_feed."""

    @pytest.mark.anyio()
    async def test_connection_not_found_fails_task(self) -> None:
        """Calls fail_task when connection is not found."""
        task = _make_task()
        session = AsyncMock()
        # Task found, connection not found
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task
        conn_result = MagicMock()
        conn_result.scalar_one_or_none.return_value = None
        session.execute.side_effect = [task_result, conn_result]
        ctx = _make_ctx(session)

        fake_conn_id = str(uuid.uuid4())
        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_calendar_feed(ctx, fake_conn_id, str(task.id))

            mock_fail.assert_awaited_once()
            error_msg = mock_fail.call_args.args[2]
            assert fake_conn_id in error_msg

    @pytest.mark.anyio()
    async def test_disabled_connection_completes_with_skip(self) -> None:
        """Completes task with skip message when connection is disabled."""
        connection = _make_connection(enabled=False)
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with patch(
            "resonance.concerts.worker.lifecycle_module.complete_task"
        ) as mock_complete:
            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert result_arg == {"skipped": "connection disabled"}

    @pytest.mark.anyio()
    async def test_http_error_calls_fail_task(self) -> None:
        """Calls fail_task when HTTP request fails."""
        connection = _make_connection()
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with (
            patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls,
            patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail,
        ):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get.side_effect = httpx.HTTPStatusError(
                "Server Error",
                request=MagicMock(spec=httpx.Request),
                response=MagicMock(spec=httpx.Response, status_code=500),
            )
            mock_client_cls.return_value = mock_client

            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            # fail_task should be called
            mock_fail.assert_awaited_once()

            # connection last_synced_at should NOT be updated
            assert connection.last_synced_at is None

    @pytest.mark.anyio()
    async def test_task_not_found_returns_early(self) -> None:
        """Returns early without error when task ID is not found."""
        session = AsyncMock()
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = None
        session.execute.return_value = task_result
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls:
            await concert_worker.sync_calendar_feed(
                ctx, str(uuid.uuid4()), str(uuid.uuid4())
            )
            mock_client_cls.assert_not_called()

    @pytest.mark.anyio()
    async def test_empty_calendar_completes_successfully(self) -> None:
        """Completes successfully even when calendar has no events."""
        connection = _make_connection()
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        mock_response = MagicMock(spec=httpx.Response)
        mock_response.text = _EMPTY_ICAL
        mock_response.raise_for_status = MagicMock()

        with (
            patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_venue"
            ) as mock_upsert_venue,
            patch(
                "resonance.concerts.worker.concert_sync.upsert_event"
            ) as mock_upsert_event,
            patch(
                "resonance.concerts.worker.lifecycle_module.complete_task"
            ) as mock_complete,
        ):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            # No events means no upserts
            mock_upsert_venue.assert_not_awaited()
            mock_upsert_event.assert_not_awaited()

            # But complete_task should be called
            mock_complete.assert_awaited_once()
            result_arg = mock_complete.call_args.args[2]
            assert result_arg["total_events"] == 0

            # Connection last_synced_at should be updated
            assert connection.last_synced_at is not None

    @pytest.mark.anyio()
    async def test_unsupported_service_type_fails(self) -> None:
        """Fails when connection has an unsupported service type."""
        connection = _make_connection(
            service_type=types_module.ServiceType.SPOTIFY,
        )
        task = _make_task(user_id=connection.user_id)
        session = AsyncMock()
        _setup_session_queries(session, task, connection)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.lifecycle_module.fail_task") as mock_fail:
            await concert_worker.sync_calendar_feed(
                ctx, str(connection.id), str(task.id)
            )

            mock_fail.assert_awaited_once()
            error_msg = mock_fail.call_args.args[2]
            assert "Unsupported service type" in error_msg
