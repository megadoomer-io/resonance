"""Tests for concert data upsert helpers."""

import datetime
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.concerts.ical as ical_module
import resonance.concerts.parser as parser_module
import resonance.concerts.sync as sync_module
import resonance.types as types_module

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_venue_data(
    name: str = "The Fillmore",
    city: str | None = "San Francisco",
    state: str | None = "CA",
    country: str | None = "US",
) -> ical_module.VenueData:
    return ical_module.VenueData(name=name, city=city, state=state, country=country)


def _make_parsed_event(
    title: str = "Artist at Venue (01 Jan 26)",
    event_date: datetime.date | None = None,
    venue: ical_module.VenueData | None = None,
    external_id: str = "uid-123",
    external_url: str | None = "https://songkick.com/event/123",
    artist_candidates: list[parser_module.ArtistCandidate] | None = None,
    attendance_status: str | None = None,
) -> ical_module.ParsedEvent:
    return ical_module.ParsedEvent(
        title=title,
        event_date=event_date or datetime.date(2026, 1, 1),
        venue=venue,
        external_id=external_id,
        external_url=external_url,
        artist_candidates=artist_candidates or [],
        attendance_status=attendance_status,
    )


# ---------------------------------------------------------------------------
# upsert_venue
# ---------------------------------------------------------------------------


class TestUpsertVenue:
    """Tests for upsert_venue."""

    @pytest.mark.anyio()
    async def test_creates_new_venue(self) -> None:
        """Creates a new venue when no match exists."""
        session = AsyncMock()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        session.execute.return_value = no_result
        session.add = MagicMock()

        venue_data = _make_venue_data()
        venue = await sync_module.upsert_venue(session, venue_data)

        session.add.assert_called_once()
        added = session.add.call_args[0][0]
        assert added.name == "The Fillmore"
        assert added.city == "San Francisco"
        assert added.state == "CA"
        assert added.country == "US"
        assert venue is added

    @pytest.mark.anyio()
    async def test_returns_existing_venue(self) -> None:
        """Returns existing venue when lookup matches."""
        session = AsyncMock()

        existing_venue = MagicMock()
        existing_venue.name = "The Fillmore"
        found_result = MagicMock()
        found_result.scalar_one_or_none.return_value = existing_venue
        session.execute.return_value = found_result

        venue_data = _make_venue_data()
        venue = await sync_module.upsert_venue(session, venue_data)

        session.add.assert_not_called()
        assert venue is existing_venue

    @pytest.mark.anyio()
    async def test_handles_nullable_location_fields(self) -> None:
        """Creates venue with None for city/state/country."""
        session = AsyncMock()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        session.execute.return_value = no_result
        session.add = MagicMock()

        venue_data = _make_venue_data(city=None, state=None, country=None)
        venue = await sync_module.upsert_venue(session, venue_data)

        added = session.add.call_args[0][0]
        assert added.city is None
        assert added.state is None
        assert added.country is None
        assert venue is added


# ---------------------------------------------------------------------------
# upsert_event
# ---------------------------------------------------------------------------


class TestUpsertEvent:
    """Tests for upsert_event."""

    @pytest.mark.anyio()
    async def test_creates_new_event(self) -> None:
        """Creates a new event when no match exists."""
        session = AsyncMock()
        venue = MagicMock()
        venue.id = uuid.uuid4()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        session.execute.return_value = no_result
        session.add = MagicMock()

        parsed = _make_parsed_event()
        event, created = await sync_module.upsert_event(
            session, parsed, types_module.ServiceType.SONGKICK, venue
        )

        session.add.assert_called_once()
        added = session.add.call_args[0][0]
        assert added.title == parsed.title
        assert added.event_date == parsed.event_date
        assert added.external_id == "uid-123"
        assert added.source_service == types_module.ServiceType.SONGKICK
        assert added.venue_id == venue.id
        assert created is True
        assert event is added

    @pytest.mark.anyio()
    async def test_updates_existing_event(self) -> None:
        """Updates fields on an existing event and returns (event, False)."""
        session = AsyncMock()
        venue = MagicMock()
        venue.id = uuid.uuid4()

        existing_event = MagicMock()
        existing_event.title = "Old Title"
        existing_event.event_date = datetime.date(2025, 6, 1)
        existing_event.external_url = "https://old.com"
        existing_event.venue_id = None

        found_result = MagicMock()
        found_result.scalar_one_or_none.return_value = existing_event
        session.execute.return_value = found_result

        parsed = _make_parsed_event(
            title="New Title",
            event_date=datetime.date(2026, 1, 1),
            external_url="https://new.com",
        )
        event, created = await sync_module.upsert_event(
            session, parsed, types_module.ServiceType.SONGKICK, venue
        )

        assert created is False
        assert event is existing_event
        assert existing_event.title == "New Title"
        assert existing_event.event_date == datetime.date(2026, 1, 1)
        assert existing_event.external_url == "https://new.com"
        assert existing_event.venue_id == venue.id
        session.add.assert_not_called()

    @pytest.mark.anyio()
    async def test_creates_event_without_venue(self) -> None:
        """Creates event with venue_id=None when venue is None."""
        session = AsyncMock()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        session.execute.return_value = no_result
        session.add = MagicMock()

        parsed = _make_parsed_event()
        _event, created = await sync_module.upsert_event(
            session, parsed, types_module.ServiceType.ICAL, None
        )

        added = session.add.call_args[0][0]
        assert added.venue_id is None
        assert created is True


# ---------------------------------------------------------------------------
# upsert_candidates
# ---------------------------------------------------------------------------


class TestUpsertCandidates:
    """Tests for upsert_candidates."""

    @pytest.mark.anyio()
    async def test_creates_new_candidates(self) -> None:
        """Creates candidate rows for each artist candidate."""
        session = AsyncMock()
        event = MagicMock()
        event.id = uuid.uuid4()

        # No existing candidates found
        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        session.execute.return_value = no_result
        session.add = MagicMock()

        candidates = [
            parser_module.ArtistCandidate(name="Band A", position=0, confidence=90),
            parser_module.ArtistCandidate(name="Band B", position=1, confidence=90),
        ]

        count = await sync_module.upsert_candidates(session, event, candidates)

        assert count == 2
        assert session.add.call_count == 2

        # Verify first candidate fields
        first_added = session.add.call_args_list[0][0][0]
        assert first_added.event_id == event.id
        assert first_added.raw_name == "Band A"
        assert first_added.position == 0
        assert first_added.confidence_score == 90

    @pytest.mark.anyio()
    async def test_skips_duplicate_candidates(self) -> None:
        """Skips candidates that already exist for the event."""
        session = AsyncMock()
        event = MagicMock()
        event.id = uuid.uuid4()

        existing_candidate = MagicMock()

        # First candidate: already exists; second candidate: new
        found_result = MagicMock()
        found_result.scalar_one_or_none.return_value = existing_candidate
        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [found_result, no_result]
        session.add = MagicMock()

        candidates = [
            parser_module.ArtistCandidate(name="Existing", position=0, confidence=90),
            parser_module.ArtistCandidate(name="New Band", position=1, confidence=90),
        ]

        count = await sync_module.upsert_candidates(session, event, candidates)

        assert count == 1
        assert session.add.call_count == 1
        added = session.add.call_args[0][0]
        assert added.raw_name == "New Band"

    @pytest.mark.anyio()
    async def test_empty_candidates_returns_zero(self) -> None:
        """Returns 0 when no candidates are provided."""
        session = AsyncMock()
        event = MagicMock()
        event.id = uuid.uuid4()

        count = await sync_module.upsert_candidates(session, event, [])

        assert count == 0
        session.execute.assert_not_called()


# ---------------------------------------------------------------------------
# upsert_attendance
# ---------------------------------------------------------------------------


class TestUpsertAttendance:
    """Tests for upsert_attendance."""

    @pytest.mark.anyio()
    async def test_creates_new_attendance(self) -> None:
        """Creates attendance record when none exists."""
        session = AsyncMock()
        user_id = uuid.uuid4()
        event = MagicMock()
        event.id = uuid.uuid4()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        session.execute.return_value = no_result
        session.add = MagicMock()

        await sync_module.upsert_attendance(
            session, user_id, event, "going", types_module.ServiceType.SONGKICK
        )

        session.add.assert_called_once()
        added = session.add.call_args[0][0]
        assert added.user_id == user_id
        assert added.event_id == event.id
        assert added.status == types_module.AttendanceStatus.GOING
        assert added.source_service == types_module.ServiceType.SONGKICK

    @pytest.mark.anyio()
    async def test_updates_existing_attendance_status(self) -> None:
        """Updates status on existing attendance record."""
        session = AsyncMock()
        user_id = uuid.uuid4()
        event = MagicMock()
        event.id = uuid.uuid4()

        existing = MagicMock()
        existing.status = types_module.AttendanceStatus.INTERESTED
        found_result = MagicMock()
        found_result.scalar_one_or_none.return_value = existing
        session.execute.return_value = found_result

        await sync_module.upsert_attendance(
            session, user_id, event, "going", types_module.ServiceType.SONGKICK
        )

        assert existing.status == types_module.AttendanceStatus.GOING
        session.add.assert_not_called()

    @pytest.mark.anyio()
    async def test_maps_interested_status(self) -> None:
        """Correctly maps 'interested' string to AttendanceStatus.INTERESTED."""
        session = AsyncMock()
        user_id = uuid.uuid4()
        event = MagicMock()
        event.id = uuid.uuid4()

        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None
        session.execute.return_value = no_result
        session.add = MagicMock()

        await sync_module.upsert_attendance(
            session, user_id, event, "interested", types_module.ServiceType.SONGKICK
        )

        added = session.add.call_args[0][0]
        assert added.status == types_module.AttendanceStatus.INTERESTED


# ---------------------------------------------------------------------------
# match_candidates_to_artists
# ---------------------------------------------------------------------------


class TestMatchCandidatesToArtists:
    """Tests for match_candidates_to_artists."""

    @pytest.mark.anyio()
    async def test_matches_by_case_insensitive_name(self) -> None:
        """Matches candidate to artist via case-insensitive name lookup."""
        session = AsyncMock()
        event = MagicMock()
        event.id = uuid.uuid4()

        candidate = MagicMock()
        candidate.raw_name = "the national"
        candidate.matched_artist_id = None
        candidate.status = types_module.CandidateStatus.PENDING

        artist = MagicMock()
        artist.id = uuid.uuid4()

        # First query: get pending candidates
        candidates_result = MagicMock()
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [candidate]
        candidates_result.scalars.return_value = scalars_mock

        # Second query: artist name lookup -> match found
        artist_result = MagicMock()
        artist_result.scalar_one_or_none.return_value = artist

        session.execute.side_effect = [candidates_result, artist_result]

        count = await sync_module.match_candidates_to_artists(session, event)

        assert count == 1
        assert candidate.matched_artist_id == artist.id

    @pytest.mark.anyio()
    async def test_leaves_unmatched_candidates(self) -> None:
        """Candidates with no artist match remain unchanged."""
        session = AsyncMock()
        event = MagicMock()
        event.id = uuid.uuid4()

        candidate = MagicMock()
        candidate.raw_name = "Unknown Band"
        candidate.matched_artist_id = None
        candidate.status = types_module.CandidateStatus.PENDING

        # First query: get pending candidates
        candidates_result = MagicMock()
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [candidate]
        candidates_result.scalars.return_value = scalars_mock

        # Second query: artist name lookup -> no match
        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [candidates_result, no_result]

        count = await sync_module.match_candidates_to_artists(session, event)

        assert count == 0
        assert candidate.matched_artist_id is None

    @pytest.mark.anyio()
    async def test_no_pending_candidates(self) -> None:
        """Returns 0 when there are no pending candidates."""
        session = AsyncMock()
        event = MagicMock()
        event.id = uuid.uuid4()

        # No pending candidates
        candidates_result = MagicMock()
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = []
        candidates_result.scalars.return_value = scalars_mock

        session.execute.side_effect = [candidates_result]

        count = await sync_module.match_candidates_to_artists(session, event)

        assert count == 0

    @pytest.mark.anyio()
    async def test_mixed_matches_and_misses(self) -> None:
        """Correctly counts only matched candidates in a mixed batch."""
        session = AsyncMock()
        event = MagicMock()
        event.id = uuid.uuid4()

        candidate_a = MagicMock()
        candidate_a.raw_name = "Known Artist"
        candidate_a.matched_artist_id = None
        candidate_a.status = types_module.CandidateStatus.PENDING

        candidate_b = MagicMock()
        candidate_b.raw_name = "Unknown Artist"
        candidate_b.matched_artist_id = None
        candidate_b.status = types_module.CandidateStatus.PENDING

        artist = MagicMock()
        artist.id = uuid.uuid4()

        # First query: get pending candidates
        candidates_result = MagicMock()
        scalars_mock = MagicMock()
        scalars_mock.all.return_value = [candidate_a, candidate_b]
        candidates_result.scalars.return_value = scalars_mock

        # Second query: artist lookup for "Known Artist" -> found
        found_result = MagicMock()
        found_result.scalar_one_or_none.return_value = artist
        # Third query: artist lookup for "Unknown Artist" -> not found
        no_result = MagicMock()
        no_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [candidates_result, found_result, no_result]

        count = await sync_module.match_candidates_to_artists(session, event)

        assert count == 1
        assert candidate_a.matched_artist_id == artist.id
        assert candidate_b.matched_artist_id is None
