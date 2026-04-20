"""End-to-end integration tests for the calendar feed sync pipeline.

These tests exercise the full sync pipeline — configure feed, fetch with mocked
HTTP, parse iCal, and verify orchestration of upsert calls. The database session
is mocked (matching the project's existing test patterns), so these validate the
orchestration logic rather than true database behavior.
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
    """End-to-end test for the Songkick attendance feed sync pipeline."""

    @pytest.mark.anyio()
    async def test_full_songkick_sync_pipeline(self) -> None:
        """Syncing a Songkick attendance feed parses events and calls all upserts."""
        feed = _make_feed()
        session = AsyncMock()
        _mock_feed_query(session, feed)
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
        ):
            _setup_http_mock(mock_client_cls, SAMPLE_SONGKICK_FEED)

            # Upsert mocks
            mock_venue_1 = MagicMock()
            mock_venue_2 = MagicMock()
            mock_upsert_venue.side_effect = [mock_venue_1, mock_venue_2]

            mock_event_1 = MagicMock()
            mock_event_2 = MagicMock()
            mock_upsert_event.side_effect = [
                (mock_event_1, True),
                (mock_event_2, True),
            ]
            mock_upsert_candidates.return_value = 1
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # -- Venues: both events have LOCATION, so 2 upsert_venue calls --
            assert mock_upsert_venue.await_count == 2

            # Event 1 venue: Golden Gate Theatre
            venue_call_1 = mock_upsert_venue.call_args_list[0]
            venue_data_1 = venue_call_1.args[1]
            assert venue_data_1.name == "Golden Gate Theatre"
            assert venue_data_1.city == "San Francisco"
            assert venue_data_1.state == "CA"
            assert venue_data_1.country == "US"

            # Event 2 venue: The Fillmore
            venue_call_2 = mock_upsert_venue.call_args_list[1]
            venue_data_2 = venue_call_2.args[1]
            assert venue_data_2.name == "The Fillmore"
            assert venue_data_2.city == "San Francisco"

            # -- Events: 2 upsert_event calls with SONGKICK source service --
            assert mock_upsert_event.await_count == 2
            for event_call in mock_upsert_event.call_args_list:
                assert event_call.args[2] == types_module.ServiceType.SONGKICK

            # Event 1 passed with venue_1
            event_call_1 = mock_upsert_event.call_args_list[0]
            assert event_call_1.args[3] is mock_venue_1

            # Event 2 passed with venue_2
            event_call_2 = mock_upsert_event.call_args_list[1]
            assert event_call_2.args[3] is mock_venue_2

            # -- Candidates: both events have Songkick artist parsing --
            assert mock_upsert_candidates.await_count == 2

            # Event 1 candidates: ["Puscifer"]
            cand_call_1 = mock_upsert_candidates.call_args_list[0]
            candidates_1 = cand_call_1.args[2]
            assert len(candidates_1) == 1
            assert candidates_1[0].name == "Puscifer"

            # Event 2: Lagwagon, Strung Out, Swingin' Utters, Western Addiction
            cand_call_2 = mock_upsert_candidates.call_args_list[1]
            candidates_2 = cand_call_2.args[2]
            assert len(candidates_2) == 4
            candidate_names = [c.name for c in candidates_2]
            assert "Lagwagon" in candidate_names
            assert "Strung Out" in candidate_names
            assert "Swingin' Utters" in candidate_names
            assert "Western Addiction" in candidate_names

            # -- Attendance: both events have DESCRIPTION --
            assert mock_upsert_attendance.await_count == 2

            # Event 1: "You're going." → "going"
            att_call_1 = mock_upsert_attendance.call_args_list[0]
            assert att_call_1.args[1] == feed.user_id
            assert att_call_1.args[3] == "going"

            # Event 2: "You're tracking this event." → "interested"
            att_call_2 = mock_upsert_attendance.call_args_list[1]
            assert att_call_2.args[1] == feed.user_id
            assert att_call_2.args[3] == "interested"

            # -- match_candidates_to_artists called for each event --
            assert mock_match.await_count == 2

            # -- last_synced_at was updated --
            assert feed.last_synced_at is not None
            assert isinstance(feed.last_synced_at, datetime.datetime)

            # -- Session was committed --
            session.commit.assert_awaited()


class TestIdempotentSync:
    """Verify that syncing the same feed data twice does not raise errors."""

    @pytest.mark.anyio()
    async def test_idempotent_sync(self) -> None:
        """Running sync twice with the same data succeeds both times."""
        feed = _make_feed()
        session = AsyncMock()
        _mock_feed_query(session, feed)
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
        ):
            _setup_http_mock(mock_client_cls, SAMPLE_SONGKICK_FEED)

            mock_venue = MagicMock()
            mock_upsert_venue.return_value = mock_venue
            mock_event = MagicMock()
            # First run: events are created; second run: events are updated
            mock_upsert_event.side_effect = [
                (mock_event, True),
                (mock_event, True),
                (mock_event, False),
                (mock_event, False),
            ]
            mock_upsert_candidates.return_value = 0
            mock_match.return_value = 0

            # First sync
            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            first_sync_time = feed.last_synced_at
            assert first_sync_time is not None

            # Second sync (same data, no errors)
            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            second_sync_time = feed.last_synced_at
            assert second_sync_time is not None
            assert second_sync_time >= first_sync_time

            # Both runs called upsert functions (upserts handle dedup internally)
            assert mock_upsert_venue.await_count == 4  # 2 events * 2 runs
            assert mock_upsert_event.await_count == 4
            assert mock_upsert_attendance.await_count == 4
            assert mock_match.await_count == 4

            # Session committed twice (once per sync)
            assert session.commit.await_count == 2


class TestGenericIcalSync:
    """Tests for syncing a generic iCal feed (no artist extraction, no attendance)."""

    @pytest.mark.anyio()
    async def test_generic_ical_no_candidates_no_attendance(self) -> None:
        """Generic iCal feed does not extract artist candidates or attendance."""
        feed = _make_feed(
            feed_type=types_module.FeedType.ICAL_GENERIC,
            url="https://example.com/calendar.ics",
        )
        session = AsyncMock()
        _mock_feed_query(session, feed)
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
        ):
            _setup_http_mock(mock_client_cls, SAMPLE_GENERIC_ICAL)

            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # -- 2 events parsed --
            assert mock_upsert_event.await_count == 2

            # -- Event source service is ICAL --
            for event_call in mock_upsert_event.call_args_list:
                assert event_call.args[2] == types_module.ServiceType.ICAL

            # -- Generic iCal: no venue parsing (venue is None for both) --
            mock_upsert_venue.assert_not_awaited()

            # Event 1 has no LOCATION; event 2 has LOCATION but generic feeds
            # don't parse it as Songkick format → venue=None
            for event_call in mock_upsert_event.call_args_list:
                assert event_call.args[3] is None  # venue arg is None

            # -- Generic feed: no artist candidate extraction --
            mock_upsert_candidates.assert_not_awaited()

            # -- Generic feed: no attendance status --
            mock_upsert_attendance.assert_not_awaited()

            # -- match_candidates_to_artists still called (handles empty state) --
            assert mock_match.await_count == 2

            # -- last_synced_at updated and session committed --
            assert feed.last_synced_at is not None
            session.commit.assert_awaited()

    @pytest.mark.anyio()
    async def test_generic_ical_event_dates_parsed(self) -> None:
        """Generic iCal events have correct dates passed to upsert_event."""
        feed = _make_feed(
            feed_type=types_module.FeedType.ICAL_GENERIC,
            url="https://example.com/calendar.ics",
        )
        session = AsyncMock()
        _mock_feed_query(session, feed)
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
        ):
            _setup_http_mock(mock_client_cls, SAMPLE_GENERIC_ICAL)

            mock_event = MagicMock()
            mock_upsert_event.return_value = (mock_event, True)
            mock_match.return_value = 0

            await concert_worker.sync_calendar_feed(ctx, str(feed.id))

            # Event 1: 2026-07-01
            parsed_event_1 = mock_upsert_event.call_args_list[0].args[1]
            assert parsed_event_1.event_date == datetime.date(2026, 7, 1)

            # Event 2: 2026-08-15
            parsed_event_2 = mock_upsert_event.call_args_list[1].args[1]
            assert parsed_event_2.event_date == datetime.date(2026, 8, 15)
