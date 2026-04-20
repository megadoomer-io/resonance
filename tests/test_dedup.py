"""Tests for dedup_all orchestration function."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

import resonance.dedup as dedup_module
from resonance.dedup import MergeStats, dedup_all


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
