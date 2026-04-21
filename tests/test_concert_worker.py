"""Tests for the calendar feed sync worker task."""

from __future__ import annotations

import datetime
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

import resonance.concerts.worker as concert_worker
import resonance.types as types_module

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_feed(
    *,
    feed_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    enabled: bool = True,
    feed_type: types_module.FeedType = types_module.FeedType.SONGKICK_ATTENDANCE,
    url: str = "https://songkick.com/feed.ics",
    last_synced_at: datetime.datetime | None = None,
) -> MagicMock:
    """Create a mock UserCalendarFeed."""
    feed = MagicMock()
    feed.id = feed_id or uuid.uuid4()
    feed.user_id = user_id or uuid.uuid4()
    feed.enabled = enabled
    feed.feed_type = feed_type
    feed.url = url
    feed.last_synced_at = last_synced_at
    return feed


def _make_ctx(session: AsyncMock) -> dict[str, object]:
    """Build a minimal worker context with a mock session factory."""
    session_factory = MagicMock()
    session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
    session_factory.return_value.__aexit__ = AsyncMock(return_value=False)
    return {"session_factory": session_factory}


def _mock_feed_query(session: AsyncMock, feed: MagicMock | None) -> None:
    """Set up session.execute to return a feed on the first call."""
    feed_result = MagicMock()
    feed_result.scalar_one_or_none.return_value = feed
    session.execute.return_value = feed_result


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSyncCalendarFeed:
    """Tests for sync_calendar_feed worker task."""

    @pytest.mark.anyio()
    async def test_successful_sync(self) -> None:
        """Fetches feed, parses events, calls upserts, updates last_synced_at."""
        feed = _make_feed()
        session = AsyncMock()
        _mock_feed_query(session, feed)
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
            patch(
                "resonance.concerts.worker.concert_sync.upsert_attendance"
            ) as mock_upsert_attendance,
            patch(
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
        ):
            # Set up httpx mock
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            # Set up upsert mocks
            mock_venue = MagicMock()
            mock_upsert_venue.return_value = mock_venue

            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_upsert_candidates.return_value = 1
            mock_match.return_value = 1

            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # Verify HTTP fetch
            mock_client.get.assert_awaited_once_with(feed.url)
            mock_response.raise_for_status.assert_called_once()

            # Two events in the sample iCal, each should trigger upserts
            assert mock_upsert_venue.await_count == 2
            assert mock_upsert_event.await_count == 2
            assert mock_upsert_candidates.await_count == 2
            assert mock_upsert_attendance.await_count == 2
            assert mock_match.await_count == 2

            # Verify source_service is SONGKICK for songkick attendance feed
            first_event_call = mock_upsert_event.call_args_list[0]
            assert first_event_call.args[2] == types_module.ServiceType.SONGKICK

            # Verify last_synced_at was updated
            assert feed.last_synced_at is not None
            assert isinstance(feed.last_synced_at, datetime.datetime)

            # Verify session was committed
            session.commit.assert_awaited()

    @pytest.mark.anyio()
    async def test_disabled_feed_skips_sync(self) -> None:
        """Does not fetch or process a disabled feed."""
        feed = _make_feed(enabled=False)
        session = AsyncMock()
        _mock_feed_query(session, feed)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls:
            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # Should not have created an HTTP client
            mock_client_cls.assert_not_called()

    @pytest.mark.anyio()
    async def test_http_error_handled_gracefully(self) -> None:
        """Logs error and does not crash when HTTP request fails."""
        feed = _make_feed()
        session = AsyncMock()
        _mock_feed_query(session, feed)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get.side_effect = httpx.HTTPStatusError(
                "Server Error",
                request=MagicMock(spec=httpx.Request),
                response=MagicMock(spec=httpx.Response, status_code=500),
            )
            mock_client_cls.return_value = mock_client

            # Should not raise
            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # last_synced_at should NOT be updated on error
            assert feed.last_synced_at is None

    @pytest.mark.anyio()
    async def test_empty_calendar_updates_last_synced_at(self) -> None:
        """Updates last_synced_at even when calendar has no events."""
        feed = _make_feed()
        session = AsyncMock()
        _mock_feed_query(session, feed)
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
        ):
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # No events means no upserts
            mock_upsert_venue.assert_not_awaited()
            mock_upsert_event.assert_not_awaited()

            # But last_synced_at should be updated
            assert feed.last_synced_at is not None
            session.commit.assert_awaited()

    @pytest.mark.anyio()
    async def test_feed_not_found_returns_early(self) -> None:
        """Returns early without error when feed ID is not found."""
        session = AsyncMock()
        _mock_feed_query(session, None)
        ctx = _make_ctx(session)

        with patch("resonance.concerts.worker.httpx.AsyncClient") as mock_client_cls:
            await concert_worker.sync_calendar_feed(ctx, str(uuid.uuid4()))
            mock_client_cls.assert_not_called()

    @pytest.mark.anyio()
    async def test_event_without_venue_skips_venue_upsert(self) -> None:
        """Does not call upsert_venue when parsed event has no venue data."""
        feed = _make_feed(
            feed_type=types_module.FeedType.ICAL_GENERIC,
            url="https://example.com/feed.ics",
        )
        session = AsyncMock()
        _mock_feed_query(session, feed)
        ctx = _make_ctx(session)

        # A generic iCal feed with no LOCATION
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

            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # No venue data on generic ical events
            mock_upsert_venue.assert_not_awaited()
            # But event should still be upserted (with venue=None)
            mock_upsert_event.assert_awaited_once()
            event_call = mock_upsert_event.call_args
            assert event_call.args[3] is None  # venue arg is None

    @pytest.mark.anyio()
    async def test_event_without_attendance_skips_attendance_upsert(self) -> None:
        """Skips upsert_attendance when parsed event has no attendance."""
        feed = _make_feed(
            feed_type=types_module.FeedType.SONGKICK_TRACKED_ARTIST,
        )
        session = AsyncMock()
        _mock_feed_query(session, feed)
        ctx = _make_ctx(session)

        # Tracked artist feed: no DESCRIPTION with attendance
        ical_no_attendance = """\
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
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.text = ical_no_attendance
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
                "resonance.concerts.worker.concert_sync.upsert_attendance"
            ) as mock_upsert_attendance,
            patch(
                "resonance.concerts.worker.concert_sync.match_candidates_to_artists"
            ) as mock_match,
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
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # Tracked artist feed: no attendance in parsed output
            mock_upsert_attendance.assert_not_awaited()

            # But venue, event, and candidates should still be processed
            mock_upsert_venue.assert_awaited_once()
            mock_upsert_event.assert_awaited_once()
            mock_upsert_candidates.assert_awaited_once()


class TestFeedTypeToService:
    """Tests for _FEED_TYPE_TO_SERVICE mapping."""

    def test_songkick_attendance_maps_to_songkick(self) -> None:
        assert (
            concert_worker._FEED_TYPE_TO_SERVICE[
                types_module.FeedType.SONGKICK_ATTENDANCE
            ]
            == types_module.ServiceType.SONGKICK
        )

    def test_songkick_tracked_maps_to_songkick(self) -> None:
        assert (
            concert_worker._FEED_TYPE_TO_SERVICE[
                types_module.FeedType.SONGKICK_TRACKED_ARTIST
            ]
            == types_module.ServiceType.SONGKICK
        )

    def test_ical_generic_maps_to_ical(self) -> None:
        assert (
            concert_worker._FEED_TYPE_TO_SERVICE[types_module.FeedType.ICAL_GENERIC]
            == types_module.ServiceType.ICAL
        )
