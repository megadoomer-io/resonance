"""Tests for dedup functions: merge_artists, merge_tracks, dedup_all."""

from __future__ import annotations

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

    assert call_order == ["artists", "tracks", "events"]


@pytest.mark.asyncio
async def test_returns_combined_stats() -> None:
    """dedup_all returns a dict combining stats from all three operations."""
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

    assert result == {
        "artists_merged": 3,
        "tracks_repointed": 5,
        "artist_relations_repointed": 2,
        "artist_relations_deleted": 1,
        "tracks_merged": 4,
        "events_repointed": 10,
        "track_relations_repointed": 6,
        "track_relations_deleted": 2,
        "events_deleted": 7,
    }
