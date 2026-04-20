"""Tests for iCal feed parser with Songkick-specific extensions."""

import datetime

import resonance.concerts.ical as ical_module
import resonance.types as types_module

SAMPLE_SONGKICK_ATTENDANCE = """\
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
END:VCALENDAR
"""

SAMPLE_SONGKICK_TRACKED_ARTIST = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Songkick//Events//EN
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260604
SUMMARY:Sleepbomb at Bottom of the Hill (04 Jun 26) with Hazzard's Cure and Ominess
LOCATION:Bottom of the Hill, San Francisco, CA, US
UID:songkick-event-67890@songkick.com
URL:https://www.songkick.com/concerts/67890
DESCRIPTION:Event details at https://www.songkick.com/concerts/67890
END:VEVENT
END:VCALENDAR
"""

SAMPLE_GENERIC_ICAL = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//My Calendar//EN
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260701
SUMMARY:Jazz Night at Blue Note
LOCATION:Blue Note, New York
UID:generic-event-001@example.com
URL:https://example.com/events/001
DESCRIPTION:A fun jazz evening.
END:VEVENT
END:VCALENDAR
"""

SAMPLE_MULTIPLE_EVENTS = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Songkick//Events//EN
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260511
SUMMARY:Puscifer at Golden Gate Theatre (11 May 26)
LOCATION:Golden Gate Theatre, San Francisco, CA, US
UID:songkick-event-111@songkick.com
URL:https://www.songkick.com/concerts/111
DESCRIPTION:You're going.
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260516
SUMMARY:Lagwagon at The Fillmore (16 May 26)
LOCATION:The Fillmore, San Francisco, CA, US
UID:songkick-event-222@songkick.com
URL:https://www.songkick.com/concerts/222
DESCRIPTION:You're going.
END:VEVENT
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260604
SUMMARY:Sleepbomb at Bottom of the Hill (04 Jun 26)
LOCATION:Bottom of the Hill, San Francisco, CA, US
UID:songkick-event-333@songkick.com
URL:https://www.songkick.com/concerts/333
DESCRIPTION:You're tracking this event.
END:VEVENT
END:VCALENDAR
"""

SAMPLE_EMPTY_CALENDAR = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Empty//EN
END:VCALENDAR
"""

SAMPLE_NO_LOCATION = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Songkick//Events//EN
BEGIN:VEVENT
DTSTART;VALUE=DATE:20260511
SUMMARY:Puscifer at Golden Gate Theatre (11 May 26)
UID:songkick-event-999@songkick.com
URL:https://www.songkick.com/concerts/999
DESCRIPTION:You're going.
END:VEVENT
END:VCALENDAR
"""

SAMPLE_DATETIME_START = """\
BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Songkick//Events//EN
BEGIN:VEVENT
DTSTART:20260511T200000Z
SUMMARY:Puscifer at Golden Gate Theatre (11 May 26)
LOCATION:Golden Gate Theatre, San Francisco, CA, US
UID:songkick-event-dttime@songkick.com
URL:https://www.songkick.com/concerts/dttime
DESCRIPTION:You're going.
END:VEVENT
END:VCALENDAR
"""


class TestParseSongkickAttendanceEvent:
    """Parse a single Songkick attendance event."""

    def test_title(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_ATTENDANCE, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert len(events) == 1
        assert events[0].title == "Puscifer at Golden Gate Theatre (11 May 26)"

    def test_event_date(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_ATTENDANCE, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert events[0].event_date == datetime.date(2026, 5, 11)

    def test_venue_data(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_ATTENDANCE, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        venue = events[0].venue
        assert venue is not None
        assert venue.name == "Golden Gate Theatre"
        assert venue.city == "San Francisco"
        assert venue.state == "CA"
        assert venue.country == "US"

    def test_external_id(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_ATTENDANCE, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert events[0].external_id == "songkick-event-12345@songkick.com"

    def test_external_url(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_ATTENDANCE, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert events[0].external_url == "https://www.songkick.com/concerts/12345"

    def test_artist_candidates(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_ATTENDANCE, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        candidates = events[0].artist_candidates
        assert len(candidates) == 1
        assert candidates[0].name == "Puscifer"
        assert candidates[0].position == 0
        assert candidates[0].confidence == 90

    def test_attendance_status(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_ATTENDANCE, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert events[0].attendance_status == "going"


class TestParseGenericIcalEvent:
    """Parse a generic iCal event — no artist extraction, no attendance."""

    def test_title(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_GENERIC_ICAL, types_module.FeedType.ICAL_GENERIC
        )
        assert len(events) == 1
        assert events[0].title == "Jazz Night at Blue Note"

    def test_event_date(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_GENERIC_ICAL, types_module.FeedType.ICAL_GENERIC
        )
        assert events[0].event_date == datetime.date(2026, 7, 1)

    def test_no_artist_candidates(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_GENERIC_ICAL, types_module.FeedType.ICAL_GENERIC
        )
        assert events[0].artist_candidates == []

    def test_no_attendance(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_GENERIC_ICAL, types_module.FeedType.ICAL_GENERIC
        )
        assert events[0].attendance_status is None

    def test_external_id(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_GENERIC_ICAL, types_module.FeedType.ICAL_GENERIC
        )
        assert events[0].external_id == "generic-event-001@example.com"

    def test_external_url(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_GENERIC_ICAL, types_module.FeedType.ICAL_GENERIC
        )
        assert events[0].external_url == "https://example.com/events/001"


class TestMultipleEvents:
    """Parse a calendar with multiple events."""

    def test_event_count(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_MULTIPLE_EVENTS, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert len(events) == 3

    def test_event_dates_distinct(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_MULTIPLE_EVENTS, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        dates = [e.event_date for e in events]
        assert datetime.date(2026, 5, 11) in dates
        assert datetime.date(2026, 5, 16) in dates
        assert datetime.date(2026, 6, 4) in dates

    def test_attendance_varies(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_MULTIPLE_EVENTS, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        statuses = [e.attendance_status for e in events]
        assert "going" in statuses
        assert "interested" in statuses


class TestEmptyCalendar:
    """Parse an empty calendar — no VEVENT components."""

    def test_returns_empty_list(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_EMPTY_CALENDAR, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert events == []


class TestEventWithoutLocation:
    """Parse an event without a LOCATION field."""

    def test_venue_is_none(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_NO_LOCATION, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert len(events) == 1
        assert events[0].venue is None


class TestSongkickTrackedArtistFeed:
    """Tracked artist feeds have no attendance in DESCRIPTION."""

    def test_no_attendance_status(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_TRACKED_ARTIST,
            types_module.FeedType.SONGKICK_TRACKED_ARTIST,
        )
        assert len(events) == 1
        assert events[0].attendance_status is None

    def test_artist_candidates_extracted(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_SONGKICK_TRACKED_ARTIST,
            types_module.FeedType.SONGKICK_TRACKED_ARTIST,
        )
        names = [c.name for c in events[0].artist_candidates]
        assert "Sleepbomb" in names
        assert "Hazzard's Cure" in names
        assert "Ominess" in names


class TestDatetimeStart:
    """DTSTART as datetime should be normalized to date."""

    def test_datetime_normalized_to_date(self) -> None:
        events = ical_module.parse_ical_feed(
            SAMPLE_DATETIME_START, types_module.FeedType.SONGKICK_ATTENDANCE
        )
        assert len(events) == 1
        assert events[0].event_date == datetime.date(2026, 5, 11)
        assert isinstance(events[0].event_date, datetime.date)


class TestParseSongkickLocation:
    """Unit tests for the location parsing helper."""

    def test_full_location(self) -> None:
        result = ical_module._parse_songkick_location(
            "Golden Gate Theatre, San Francisco, CA, US"
        )
        assert result is not None
        assert result.name == "Golden Gate Theatre"
        assert result.city == "San Francisco"
        assert result.state == "CA"
        assert result.country == "US"

    def test_venue_only(self) -> None:
        result = ical_module._parse_songkick_location("The Fillmore")
        assert result is not None
        assert result.name == "The Fillmore"
        assert result.city is None
        assert result.state is None
        assert result.country is None

    def test_venue_and_city(self) -> None:
        result = ical_module._parse_songkick_location("Blue Note, New York")
        assert result is not None
        assert result.name == "Blue Note"
        assert result.city == "New York"
        assert result.state is None

    def test_empty_string(self) -> None:
        result = ical_module._parse_songkick_location("")
        assert result is None
