"""Tests for Songkick event title parser."""

import resonance.concerts.parser as parser_module


class TestParseSongkickSummary:
    def test_single_artist_at_venue(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Puscifer at Golden Gate Theatre (11 May 26)"
        )
        assert len(result) == 1
        assert result[0].name == "Puscifer"
        assert result[0].position == 0
        assert result[0].confidence == 90

    def test_two_artists_with_and(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Puscifer and Dave Hill at Golden Gate Theatre (11 May 26)"
        )
        assert len(result) == 2
        assert result[0].name == "Puscifer"
        assert result[1].name == "Dave Hill"

    def test_multiple_headliners_with_support(self) -> None:
        summary = (
            "Lagwagon, Strung Out, and Swingin' Utters"
            " at The Fillmore (16 May 26)"
            " with Western Addiction"
        )
        result = parser_module.parse_songkick_summary(summary)
        assert len(result) == 4
        assert result[0].name == "Lagwagon"
        assert result[1].name == "Strung Out"
        assert result[2].name == "Swingin' Utters"
        assert result[3].name == "Western Addiction"
        assert [r.position for r in result] == [0, 1, 2, 3]

    def test_headliner_with_multiple_support(self) -> None:
        summary = (
            "Sleepbomb at Bottom of the Hill (04 Jun 26)"
            " with Hazzard's Cure and Ominess"
        )
        result = parser_module.parse_songkick_summary(summary)
        assert len(result) == 3
        assert result[0].name == "Sleepbomb"
        assert result[1].name == "Hazzard's Cure"
        assert result[2].name == "Ominess"

    def test_no_at_delimiter_returns_empty(self) -> None:
        assert parser_module.parse_songkick_summary("Just a random string") == []

    def test_ambiguous_multiple_at_low_confidence(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Panic! at the Disco at The Forum (01 Jan 27)"
        )
        assert len(result) >= 1
        assert result[0].confidence == 30

    def test_two_headliners_comma_separated(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Artist One, Artist Two at The Venue (01 Jan 27)"
        )
        assert len(result) == 2
        assert result[0].name == "Artist One"
        assert result[1].name == "Artist Two"
        assert result[0].position == 0
        assert result[1].position == 1

    def test_empty_string_returns_empty(self) -> None:
        assert parser_module.parse_songkick_summary("") == []

    def test_headliners_confidence_is_90(self) -> None:
        result = parser_module.parse_songkick_summary("Band at Venue (01 Jan 27)")
        assert all(r.confidence == 90 for r in result)

    def test_support_acts_confidence_matches_headliners(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Main Act at Venue (01 Jan 27) with Opener"
        )
        assert result[0].confidence == 90
        assert result[1].confidence == 90

    def test_three_headliners_oxford_comma(self) -> None:
        result = parser_module.parse_songkick_summary(
            "Alpha, Beta, and Gamma at Club (01 Jan 27)"
        )
        assert len(result) == 3
        assert result[0].name == "Alpha"
        assert result[1].name == "Beta"
        assert result[2].name == "Gamma"


class TestParseSongkickVenue:
    def test_venue_extraction(self) -> None:
        assert (
            parser_module.parse_songkick_venue(
                "Puscifer at Golden Gate Theatre (11 May 26)"
            )
            == "Golden Gate Theatre"
        )

    def test_venue_strips_date(self) -> None:
        assert (
            parser_module.parse_songkick_venue("Artist at The Fillmore (16 May 26)")
            == "The Fillmore"
        )

    def test_no_at_returns_none(self) -> None:
        assert parser_module.parse_songkick_venue("No venue here") is None

    def test_venue_with_support_acts(self) -> None:
        assert (
            parser_module.parse_songkick_venue(
                "Band at Great Hall (01 Jan 27) with Opener"
            )
            == "Great Hall"
        )

    def test_venue_ambiguous_at(self) -> None:
        result = parser_module.parse_songkick_venue(
            "Panic! at the Disco at The Forum (01 Jan 27)"
        )
        assert result == "The Forum"


class TestParseSongkickAttendance:
    def test_going(self) -> None:
        assert (
            parser_module.parse_songkick_attendance(
                "You're going.\n\nMore details: https://..."
            )
            == "going"
        )

    def test_tracking(self) -> None:
        assert (
            parser_module.parse_songkick_attendance(
                "You're tracking this event.\n\nMore details: https://..."
            )
            == "interested"
        )

    def test_unknown(self) -> None:
        assert (
            parser_module.parse_songkick_attendance("Some random description") is None
        )

    def test_empty_string(self) -> None:
        assert parser_module.parse_songkick_attendance("") is None

    def test_going_without_extra_content(self) -> None:
        assert parser_module.parse_songkick_attendance("You're going.") == "going"

    def test_tracking_without_extra_content(self) -> None:
        assert (
            parser_module.parse_songkick_attendance("You're tracking this event.")
            == "interested"
        )
