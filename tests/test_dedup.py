"""Tests for dedup functions: merge_artists, merge_tracks, venue/event dedup."""

from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

import resonance.dedup as dedup_module
from resonance.dedup import MergeStats, dedup_all, merge_artists


class FakeResult:
    """Fake DB result supporting common chains."""

    def __init__(self, items: list[Any] | None = None, rowcount: int = 0) -> None:
        self._items = items or []
        self.rowcount = rowcount

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


def _make_artist(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "Test Artist",
        "service_links": {},
        "created_at": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_event_artist(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "event_id": uuid.uuid4(),
        "artist_id": uuid.uuid4(),
        "position": 0,
        "raw_name": "Test Artist",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# merge_artists — EventArtist / EventArtistCandidate repointing (#67)
# ---------------------------------------------------------------------------


class TestMergeArtistsRepointsEventArtists:
    """merge_artists must repoint EventArtist and EventArtistCandidate
    records before deleting the duplicate, otherwise CASCADE deletes them."""

    @pytest.mark.asyncio
    async def test_repoints_event_artists_without_conflict(self) -> None:
        """EventArtist rows for the duplicate are repointed to canonical."""
        canonical = _make_artist(name="The Red Pears")
        duplicate = _make_artist(name="Red Pears")

        event_artist = _make_event_artist(artist_id=duplicate.id)

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(rowcount=0),  # UPDATE tracks
                FakeResult(items=[]),  # SELECT UserArtistRelation
                FakeResult(items=[event_artist]),  # SELECT EventArtist
                FakeResult(items=[]),  # SELECT EventArtist conflict check → none
                FakeResult(rowcount=1),  # UPDATE EventArtistCandidate
                FakeResult(),  # DELETE duplicate artist
            ]
        )

        stats = await merge_artists(session, canonical, duplicate)

        assert stats.event_artists_repointed == 1
        assert stats.event_artists_deleted == 0
        assert event_artist.artist_id == canonical.id

    @pytest.mark.asyncio
    async def test_deletes_conflicting_event_artists(self) -> None:
        """When canonical already has an EventArtist for the same event,
        the duplicate's EventArtist is deleted instead of repointed."""
        canonical = _make_artist(name="The Red Pears")
        duplicate = _make_artist(name="Red Pears")

        shared_event_id = uuid.uuid4()
        dup_event_artist = _make_event_artist(
            artist_id=duplicate.id, event_id=shared_event_id
        )
        existing_event_artist = _make_event_artist(
            artist_id=canonical.id, event_id=shared_event_id
        )

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(rowcount=0),  # UPDATE tracks
                FakeResult(items=[]),  # SELECT UserArtistRelation
                FakeResult(items=[dup_event_artist]),  # SELECT EventArtist
                FakeResult(items=[existing_event_artist]),  # conflict exists
                FakeResult(rowcount=0),  # UPDATE EventArtistCandidate
                FakeResult(),  # DELETE duplicate artist
            ]
        )

        stats = await merge_artists(session, canonical, duplicate)

        assert stats.event_artists_deleted == 1
        assert stats.event_artists_repointed == 0
        session.delete.assert_called_once_with(dup_event_artist)

    @pytest.mark.asyncio
    async def test_repoints_candidate_matched_artist_ids(self) -> None:
        """EventArtistCandidate.matched_artist_id pointing to duplicate
        is repointed to canonical."""
        canonical = _make_artist(name="The Red Pears")
        duplicate = _make_artist(name="Red Pears")

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(rowcount=0),  # UPDATE tracks
                FakeResult(items=[]),  # SELECT UserArtistRelation
                FakeResult(items=[]),  # SELECT EventArtist (none)
                FakeResult(rowcount=3),  # UPDATE EventArtistCandidate
                FakeResult(),  # DELETE duplicate artist
            ]
        )

        stats = await merge_artists(session, canonical, duplicate)

        assert stats.candidates_repointed == 3


# ---------------------------------------------------------------------------
# dedup_all orchestration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_calls_all_three_in_order() -> None:
    """dedup_all calls artist, track, and event dedup in that order."""
    call_order: list[str] = []

    async def mock_venues(session: object) -> MergeStats:
        call_order.append("venues")
        return MergeStats()

    async def mock_concerts(session: object) -> MergeStats:
        call_order.append("concerts")
        return MergeStats()

    async def mock_artists(session: object) -> MergeStats:
        call_order.append("artists")
        return MergeStats()

    async def mock_tracks(session: object) -> MergeStats:
        call_order.append("tracks")
        return MergeStats()

    async def mock_events(session: object) -> int:
        call_order.append("events")
        return 0

    session = AsyncMock()

    with (
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_venues",
            side_effect=mock_venues,
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_concerts",
            side_effect=mock_concerts,
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_artists",
            side_effect=mock_artists,
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_tracks",
            side_effect=mock_tracks,
        ),
        patch.object(
            dedup_module,
            "delete_cross_service_duplicate_events",
            side_effect=mock_events,
        ),
    ):
        await dedup_all(session)

    assert call_order == ["venues", "concerts", "artists", "tracks", "events"]


@pytest.mark.asyncio
async def test_returns_combined_stats() -> None:
    """dedup_all returns a dict combining stats from all operations."""
    artist_stats = MergeStats(
        artists_merged=3,
        tracks_repointed=5,
        artist_relations_repointed=2,
        artist_relations_deleted=1,
    )
    track_stats = MergeStats(
        tracks_merged=4,
        events_repointed=10,
        track_relations_repointed=6,
        track_relations_deleted=2,
    )
    events_deleted = 7

    session = AsyncMock()

    with (
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_venues",
            new_callable=AsyncMock,
            return_value=MergeStats(),
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_concerts",
            new_callable=AsyncMock,
            return_value=MergeStats(),
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_artists",
            new_callable=AsyncMock,
            return_value=artist_stats,
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_tracks",
            new_callable=AsyncMock,
            return_value=track_stats,
        ),
        patch.object(
            dedup_module,
            "delete_cross_service_duplicate_events",
            new_callable=AsyncMock,
            return_value=events_deleted,
        ),
    ):
        result = await dedup_all(session)

    assert result["artists_merged"] == 3
    assert result["tracks_merged"] == 4
    assert result["events_deleted"] == 7


# ---------------------------------------------------------------------------
# Venue factories and helpers
# ---------------------------------------------------------------------------


def _make_venue(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "name": "The Fillmore",
        "city": "San Francisco",
        "state": "California",
        "country": "United States",
        "address": None,
        "postal_code": None,
        "service_links": {},
        "created_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_event(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "title": "Test Concert",
        "event_date": datetime.date(2026, 5, 1),
        "venue_id": None,
        "source_service": "SONGKICK",
        "external_id": "test-123",
        "external_url": None,
        "service_links": {},
        "created_at": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        "artists": [],
        "artist_candidates": [],
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_candidate(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "event_id": uuid.uuid4(),
        "raw_name": "Test Artist",
        "matched_artist_id": None,
        "position": 0,
        "confidence_score": 90,
        "status": "PENDING",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_attendance(**overrides: Any) -> SimpleNamespace:
    defaults: dict[str, Any] = {
        "id": uuid.uuid4(),
        "user_id": uuid.uuid4(),
        "event_id": uuid.uuid4(),
        "status": "GOING",
        "source_service": "SONGKICK",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# pick_canonical_venue
# ---------------------------------------------------------------------------


class TestPickCanonicalVenue:
    def test_more_location_fields_wins(self) -> None:
        a = _make_venue(address="123 Main St", postal_code="94105")
        b = _make_venue()
        canonical, dup = dedup_module.pick_canonical_venue(a, b)
        assert canonical is a
        assert dup is b

    def test_more_service_links_wins(self) -> None:
        a = _make_venue(service_links={"songkick": "123"})
        b = _make_venue(service_links={"songkick": "123", "concert_archives": "456"})
        canonical, _dup = dedup_module.pick_canonical_venue(a, b)
        assert canonical is b

    def test_oldest_wins_as_tiebreaker(self) -> None:
        a = _make_venue(created_at=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC))
        b = _make_venue(created_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC))
        canonical, _dup = dedup_module.pick_canonical_venue(a, b)
        assert canonical is b


# ---------------------------------------------------------------------------
# merge_venues
# ---------------------------------------------------------------------------


class TestMergeVenues:
    @pytest.mark.asyncio
    async def test_merges_service_links(self) -> None:
        canonical = _make_venue(service_links={"songkick": "123"})
        duplicate = _make_venue(service_links={"concert_archives": "456"})

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(rowcount=2),  # UPDATE events
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_venues(session, canonical, duplicate)

        assert canonical.service_links == {
            "songkick": "123",
            "concert_archives": "456",
        }
        assert stats.venues_merged == 1

    @pytest.mark.asyncio
    async def test_fills_null_fields_from_duplicate(self) -> None:
        canonical = _make_venue(address=None, postal_code=None)
        duplicate = _make_venue(address="123 Main St", postal_code="94105")

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(rowcount=0),
                FakeResult(),
            ]
        )

        await dedup_module.merge_venues(session, canonical, duplicate)

        assert canonical.address == "123 Main St"
        assert canonical.postal_code == "94105"

    @pytest.mark.asyncio
    async def test_repoints_events(self) -> None:
        canonical = _make_venue()
        duplicate = _make_venue()

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(rowcount=3),  # UPDATE events
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_venues(session, canonical, duplicate)

        assert stats.events_venue_repointed == 3


# ---------------------------------------------------------------------------
# pick_canonical_event
# ---------------------------------------------------------------------------


class TestPickCanonicalEvent:
    def test_more_confirmed_artists_wins(self) -> None:
        a = _make_event(artists=[1, 2, 3])
        b = _make_event(artists=[1])
        canonical, _dup = dedup_module.pick_canonical_event(a, b)
        assert canonical is a

    def test_more_candidates_wins(self) -> None:
        a = _make_event(artist_candidates=[1])
        b = _make_event(artist_candidates=[1, 2, 3])
        canonical, _dup = dedup_module.pick_canonical_event(a, b)
        assert canonical is b

    def test_more_service_links_wins(self) -> None:
        a = _make_event(service_links={"songkick": "1", "ca": "2"})
        b = _make_event(service_links={"songkick": "1"})
        canonical, _dup = dedup_module.pick_canonical_event(a, b)
        assert canonical is a

    def test_has_external_url_wins(self) -> None:
        a = _make_event(external_url="https://songkick.com/event/1")
        b = _make_event(external_url=None)
        canonical, _dup = dedup_module.pick_canonical_event(a, b)
        assert canonical is a

    def test_oldest_tiebreaker(self) -> None:
        a = _make_event(created_at=datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC))
        b = _make_event(created_at=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC))
        canonical, _dup = dedup_module.pick_canonical_event(a, b)
        assert canonical is b


# ---------------------------------------------------------------------------
# merge_events
# ---------------------------------------------------------------------------


class TestMergeEvents:
    @pytest.mark.asyncio
    async def test_merges_service_links(self) -> None:
        canonical = _make_event(service_links={"songkick": "sk-1"})
        duplicate = _make_event(service_links={"concert_archives": "ca-1"})

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(items=[]),  # SELECT EventArtist
                FakeResult(items=[]),  # SELECT EventArtistCandidate
                FakeResult(items=[]),  # SELECT UserEventAttendance
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_events(session, canonical, duplicate)

        assert canonical.service_links == {
            "songkick": "sk-1",
            "concert_archives": "ca-1",
        }
        assert stats.concerts_merged == 1

    @pytest.mark.asyncio
    async def test_repoints_event_artists_without_conflict(self) -> None:
        canonical = _make_event()
        duplicate = _make_event()
        ea = _make_event_artist(event_id=duplicate.id, artist_id=uuid.uuid4())

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(items=[ea]),  # SELECT EventArtist for duplicate
                FakeResult(items=[]),  # conflict check → none
                FakeResult(items=[]),  # SELECT EventArtistCandidate
                FakeResult(items=[]),  # SELECT UserEventAttendance
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_events(session, canonical, duplicate)

        assert ea.event_id == canonical.id
        assert stats.concert_artists_repointed == 1

    @pytest.mark.asyncio
    async def test_deletes_conflicting_event_artists(self) -> None:
        shared_artist_id = uuid.uuid4()
        canonical = _make_event()
        duplicate = _make_event()

        dup_ea = _make_event_artist(event_id=duplicate.id, artist_id=shared_artist_id)
        existing_ea = _make_event_artist(
            event_id=canonical.id, artist_id=shared_artist_id
        )

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(items=[dup_ea]),  # SELECT EventArtist
                FakeResult(items=[existing_ea]),  # conflict exists
                FakeResult(items=[]),  # SELECT EventArtistCandidate
                FakeResult(items=[]),  # SELECT UserEventAttendance
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_events(session, canonical, duplicate)

        assert stats.concert_artists_deleted == 1
        session.delete.assert_any_call(dup_ea)

    @pytest.mark.asyncio
    async def test_repoints_candidates_without_conflict(self) -> None:
        canonical = _make_event()
        duplicate = _make_event()
        cand = _make_candidate(event_id=duplicate.id, raw_name="Iron Maiden")

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(items=[]),  # SELECT EventArtist
                FakeResult(items=[cand]),  # SELECT EventArtistCandidate
                FakeResult(items=[]),  # conflict check → none
                FakeResult(items=[]),  # SELECT UserEventAttendance
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_events(session, canonical, duplicate)

        assert cand.event_id == canonical.id
        assert stats.concert_candidates_repointed == 1

    @pytest.mark.asyncio
    async def test_enriches_conflicting_candidates(self) -> None:
        canonical = _make_event()
        duplicate = _make_event()
        matched_id = uuid.uuid4()

        existing_cand = _make_candidate(
            event_id=canonical.id,
            raw_name="Iron Maiden",
            matched_artist_id=None,
            confidence_score=80,
            status="PENDING",
        )
        dup_cand = _make_candidate(
            event_id=duplicate.id,
            raw_name="Iron Maiden",
            matched_artist_id=matched_id,
            confidence_score=95,
            status="ACCEPTED",
        )

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(items=[]),  # SELECT EventArtist
                FakeResult(items=[dup_cand]),  # SELECT EventArtistCandidate
                FakeResult(items=[existing_cand]),  # conflict exists
                FakeResult(items=[]),  # SELECT UserEventAttendance
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_events(session, canonical, duplicate)

        assert existing_cand.matched_artist_id == matched_id
        assert existing_cand.confidence_score == 95
        assert existing_cand.status == "ACCEPTED"
        assert stats.concert_candidates_deleted == 1

    @pytest.mark.asyncio
    async def test_repoints_attendance_without_conflict(self) -> None:
        canonical = _make_event()
        duplicate = _make_event()
        user_id = uuid.uuid4()
        att = _make_attendance(user_id=user_id, event_id=duplicate.id)

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(items=[]),  # SELECT EventArtist
                FakeResult(items=[]),  # SELECT EventArtistCandidate
                FakeResult(items=[att]),  # SELECT UserEventAttendance
                FakeResult(items=[]),  # conflict check → none
                FakeResult(),  # DELETE duplicate
            ]
        )

        stats = await dedup_module.merge_events(session, canonical, duplicate)

        assert att.event_id == canonical.id
        assert stats.attendance_repointed == 1

    @pytest.mark.asyncio
    async def test_keeps_richer_title(self) -> None:
        canonical = _make_event(title="Concert on 2026-05-01")
        duplicate = _make_event(title="Iron Maiden at The Fillmore (01 May 26)")

        session = AsyncMock()
        session.execute = AsyncMock(
            side_effect=[
                FakeResult(items=[]),
                FakeResult(items=[]),
                FakeResult(items=[]),
                FakeResult(),
            ]
        )

        await dedup_module.merge_events(session, canonical, duplicate)

        assert canonical.title == "Iron Maiden at The Fillmore (01 May 26)"


# ---------------------------------------------------------------------------
# dedup_all — updated sequence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedup_all_includes_venues_and_concerts() -> None:
    """dedup_all calls all five dedup steps in the correct order."""
    call_order: list[str] = []

    async def mock_venues(session: object) -> MergeStats:
        call_order.append("venues")
        return MergeStats()

    async def mock_concerts(session: object) -> MergeStats:
        call_order.append("concerts")
        return MergeStats()

    async def mock_artists(session: object) -> MergeStats:
        call_order.append("artists")
        return MergeStats()

    async def mock_tracks(session: object) -> MergeStats:
        call_order.append("tracks")
        return MergeStats()

    async def mock_events(session: object) -> int:
        call_order.append("listening_events")
        return 0

    session = AsyncMock()

    with (
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_venues",
            side_effect=mock_venues,
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_concerts",
            side_effect=mock_concerts,
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_artists",
            side_effect=mock_artists,
        ),
        patch.object(
            dedup_module,
            "find_and_merge_duplicate_tracks",
            side_effect=mock_tracks,
        ),
        patch.object(
            dedup_module,
            "delete_cross_service_duplicate_events",
            side_effect=mock_events,
        ),
    ):
        result = await dedup_all(session)

    assert call_order == [
        "venues",
        "concerts",
        "artists",
        "tracks",
        "listening_events",
    ]
    assert "venues_merged" in result
    assert "concerts_merged" in result
