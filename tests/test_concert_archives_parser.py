"""Tests for Concert Archives CSV parser."""

from __future__ import annotations

import datetime
import pathlib

import pytest

import resonance.concerts.concert_archives as ca_module


class TestParseExportDate:
    """Tests for parse_export_date."""

    def test_standard_filename(self) -> None:
        filename = "mike.dougherty - Concert Archives Export - 05-19-2026.csv"
        assert ca_module.parse_export_date(filename) == datetime.date(2026, 5, 19)

    def test_no_date(self) -> None:
        assert ca_module.parse_export_date("random-file.csv") is None

    def test_different_date(self) -> None:
        filename = "user - Concert Archives Export - 12-31-2025.csv"
        assert ca_module.parse_export_date(filename) == datetime.date(2025, 12, 31)


class TestParseUsername:
    """Tests for parse_username."""

    def test_from_filename(self) -> None:
        filename = "mike.dougherty - Concert Archives Export - 05-19-2026.csv"
        assert ca_module.parse_username(filename, []) == "mike.dougherty"

    def test_from_url_fallback(self) -> None:
        urls = [
            "https://www.concertarchives.org/jdoe/concerts/some-event",
        ]
        assert ca_module.parse_username("random.csv", urls) == "jdoe"

    def test_no_source(self) -> None:
        assert ca_module.parse_username("random.csv", []) is None


class TestParseLocation:
    """Tests for parse_location."""

    def test_us_location(self) -> None:
        result = ca_module.parse_location("San Francisco, California, United States")
        assert result is not None
        assert result.name == ""
        assert result.city == "San Francisco"
        assert result.state == "California"
        assert result.country == "United States"

    def test_international_location(self) -> None:
        result = ca_module.parse_location("London, United Kingdom")
        assert result is not None
        assert result.name == ""
        assert result.city == "London"
        assert result.state is None
        assert result.country == "United Kingdom"

    def test_empty_string(self) -> None:
        assert ca_module.parse_location("") is None


class TestGenerateExternalId:
    """Tests for generate_external_id."""

    def test_basic(self) -> None:
        result = ca_module.generate_external_id(
            datetime.date(2026, 5, 19), "The Fillmore", "San Francisco"
        )
        assert result == "2026-05-19_the-fillmore_san-francisco"

    def test_normalization(self) -> None:
        result = ca_module.generate_external_id(
            datetime.date(2026, 1, 1),
            "  Great  American  Music Hall  ",
            "  San Francisco  ",
        )
        assert result == "2026-01-01_great-american-music-hall_san-francisco"

    def test_no_venue(self) -> None:
        result = ca_module.generate_external_id(datetime.date(2026, 5, 19), None, None)
        assert result == "2026-05-19__"


class TestParseArtists:
    """Tests for parse_artists."""

    def test_single_artist(self) -> None:
        result = ca_module.parse_artists("Beck", "")
        assert len(result) == 1
        assert result[0].name == "Beck"
        assert result[0].position == 0
        assert result[0].confidence == 90

    def test_multiple_artists(self) -> None:
        result = ca_module.parse_artists("The Sword / Red Fang", "")
        assert len(result) == 2
        assert result[0].name == "The Sword"
        assert result[1].name == "Red Fang"
        assert result[0].position == 0
        assert result[1].position == 1

    def test_both_fields_combined(self) -> None:
        result = ca_module.parse_artists("SLOW CRUSH / Faetooth", "Pure Hex")
        assert len(result) == 3
        assert result[0].name == "SLOW CRUSH"
        assert result[1].name == "Faetooth"
        assert result[2].name == "Pure Hex"

    def test_empty_fields(self) -> None:
        result = ca_module.parse_artists("", "")
        assert result == []

    def test_w_slash_in_name_preserved(self) -> None:
        """w/ is not the ' / ' separator and should stay in the name."""
        result = ca_module.parse_artists(
            "Lea Bertucci w/ Norbert Rodenkirchen / Brendan Glasson", ""
        )
        assert len(result) == 2
        assert result[0].name == "Lea Bertucci w/ Norbert Rodenkirchen"
        assert result[1].name == "Brendan Glasson"


class TestParseCsv:
    """Tests for parse_csv."""

    HEADERS = (
        "Start Date,End Date,Status,Concert Name,Bands Seen,"
        "Bands Not Seen,Venue,Location,URL"
    )

    def test_basic_event(self) -> None:
        csv = (
            f"{self.HEADERS}\n"
            '09/26/2026,,Upcoming,,Beck,"",SF Masonic Auditorium,'
            '"San Francisco, California, United States",'
            "https://www.concertarchives.org/user/concerts/beck-abc123\n"
        )
        result = ca_module.parse_csv(csv)
        assert len(result.events) == 1
        event = result.events[0]
        assert event.event_date == datetime.date(2026, 9, 26)
        assert event.attendance_status == "going"
        assert event.venue is not None
        assert event.venue.name == "SF Masonic Auditorium"
        assert event.venue.city == "San Francisco"
        assert event.venue.state == "California"
        assert event.venue.country == "United States"
        assert len(event.artist_candidates) == 1
        assert event.artist_candidates[0].name == "Beck"
        assert (
            event.external_url
            == "https://www.concertarchives.org/user/concerts/beck-abc123"
        )
        assert len(result.warnings) == 0

    def test_cancelled_no_attendance(self) -> None:
        csv = (
            f"{self.HEADERS}\n"
            '03/05/2023,,Cancelled,"","",Rage Against The Machine / Run the Jewels,'
            'Oakland Arena,"Oakland, California, United States",'
            "https://www.concertarchives.org/user/concerts/ratm\n"
        )
        result = ca_module.parse_csv(csv)
        assert len(result.events) == 1
        assert result.events[0].attendance_status is None

    def test_missing_date_sentinel_and_warning(self) -> None:
        csv = (
            f"{self.HEADERS}\n"
            ',,Past,,Some Band,"",The Venue,'
            '"City, State, Country",'
            "https://example.com\n"
        )
        result = ca_module.parse_csv(csv)
        assert len(result.events) == 1
        assert result.events[0].event_date == datetime.date(1970, 1, 1)
        assert len(result.warnings) == 1
        warning = result.warnings[0].lower()
        assert "row 2" in warning or "date" in warning

    def test_invalid_headers_raises_value_error(self) -> None:
        csv = "Name,Date,Place\nfoo,bar,baz\n"
        with pytest.raises(ValueError, match=r"[Mm]issing.*header"):
            ca_module.parse_csv(csv)

    def test_concert_name_as_title(self) -> None:
        csv = (
            f"{self.HEADERS}\n"
            "07/15/2026,,Upcoming,Celestial Blues 5 Year Anniversary Tour,"
            'King Woman,"",Great American Music Hall,'
            '"San Francisco, California, United States",'
            "https://example.com\n"
        )
        result = ca_module.parse_csv(csv)
        assert result.events[0].title == "Celestial Blues 5 Year Anniversary Tour"

    def test_synthesized_title_headliner_at_venue(self) -> None:
        csv = (
            f"{self.HEADERS}\n"
            '09/26/2026,,Upcoming,,Beck,"",SF Masonic Auditorium,'
            '"San Francisco, California, United States",'
            "https://example.com\n"
        )
        result = ca_module.parse_csv(csv)
        assert result.events[0].title == "Beck at SF Masonic Auditorium"

    def test_synthesized_title_headliner_only(self) -> None:
        csv = f'{self.HEADERS}\n09/26/2026,,Upcoming,,Beck,"",,,https://example.com\n'
        result = ca_module.parse_csv(csv)
        assert result.events[0].title == "Beck"

    def test_synthesized_title_date_fallback(self) -> None:
        csv = f'{self.HEADERS}\n09/26/2026,,Upcoming,,"","",,,https://example.com\n'
        result = ca_module.parse_csv(csv)
        assert result.events[0].title == "Concert on 2026-09-26"

    def test_real_csv_file(self) -> None:
        """Integration test using the real Concert Archives export."""
        csv_path = (
            pathlib.Path(__file__).parent.parent
            / "data"
            / "mike.dougherty - Concert Archives Export - 05-19-2026.csv"
        )
        content = csv_path.read_text()
        result = ca_module.parse_csv(content)
        assert len(result.events) == 290
        assert len(result.warnings) == 0

        # Spot-check a known cancelled event has no attendance
        cancelled = [e for e in result.events if e.attendance_status is None]
        assert len(cancelled) == 2  # 2 Cancelled rows in the real data

        # Spot-check artist parsing — multi-artist row
        multi_artist_events = [e for e in result.events if len(e.artist_candidates) > 1]
        assert len(multi_artist_events) > 0

        # All events should have external IDs
        assert all(e.external_id for e in result.events)
