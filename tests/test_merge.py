"""Tests for account merge functions."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa

import resonance.merge as merge_module

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _scalar_result(value: Any) -> MagicMock:
    """Create a mock result whose .scalar_one() returns *value*."""
    result = MagicMock()
    result.scalar_one.return_value = value
    return result


def _scalars_result(values: list[Any]) -> MagicMock:
    """Create a mock result whose .scalars().all() returns *values*."""
    result = MagicMock()
    scalars_mock = MagicMock()
    scalars_mock.all.return_value = values
    result.scalars.return_value = scalars_mock
    return result


def _rowcount_result(count: int) -> MagicMock:
    """Create a mock result with a .rowcount attribute."""
    result = MagicMock()
    result.rowcount = count
    return result


# ---------------------------------------------------------------------------
# get_account_summary tests
# ---------------------------------------------------------------------------


class TestGetAccountSummary:
    """Tests for get_account_summary."""

    @pytest.mark.asyncio
    async def test_returns_correct_counts(self) -> None:
        """Should query each model and return a dict of counts."""
        session = AsyncMock()
        user_id = uuid.uuid4()

        expected_counts = {
            "connections": 2,
            "listening_events": 500,
            "artist_relations": 30,
            "track_relations": 200,
            "sync_jobs": 3,
        }

        # The function issues 5 count queries in order; map them to results.
        session.execute.side_effect = [
            _scalar_result(expected_counts["connections"]),
            _scalar_result(expected_counts["listening_events"]),
            _scalar_result(expected_counts["artist_relations"]),
            _scalar_result(expected_counts["track_relations"]),
            _scalar_result(expected_counts["sync_jobs"]),
        ]

        result = await merge_module.get_account_summary(session, user_id)

        assert result == expected_counts
        assert session.execute.call_count == 5


# ---------------------------------------------------------------------------
# merge_accounts tests
# ---------------------------------------------------------------------------


class TestMergeAccounts:
    """Tests for merge_accounts."""

    def _setup_session(
        self,
        *,
        connections_moved: int = 0,
        events_moved: int = 0,
        source_artist_rels: list[Any] | None = None,
        target_artist_rels: list[Any] | None = None,
        source_track_rels: list[Any] | None = None,
        target_track_rels: list[Any] | None = None,
        sync_jobs_moved: int = 0,
    ) -> tuple[AsyncMock, uuid.UUID, uuid.UUID]:
        """Build a mock session with configurable return values."""
        session = AsyncMock()
        target_id = uuid.uuid4()
        source_id = uuid.uuid4()

        if source_artist_rels is None:
            source_artist_rels = []
        if target_artist_rels is None:
            target_artist_rels = []
        if source_track_rels is None:
            source_track_rels = []
        if target_track_rels is None:
            target_track_rels = []

        # Order of execute calls in merge_accounts:
        # 1. UPDATE service_connections
        # 2. UPDATE listening_events
        # 3. SELECT source artist relations
        # 4. SELECT target artist relations
        # 5..N. DELETE / UPDATE individual artist relations (dynamic)
        # N+1. SELECT source track relations
        # N+2. SELECT target track relations
        # N+3..M. DELETE / UPDATE individual track relations (dynamic)
        # M+1. UPDATE sync_jobs
        # M+2. DELETE source user

        results: list[Any] = [
            _rowcount_result(connections_moved),  # 1
            _rowcount_result(events_moved),  # 2
            _scalars_result(source_artist_rels),  # 3
            _scalars_result(target_artist_rels),  # 4
        ]

        # Artist relation individual ops (delete for dups, update for moves)
        # handled internally, we add no extra results here — individual
        # updates/deletes don't need specific return values.

        # Build target keys set for dedup logic
        target_artist_keys = {
            (r.artist_id, r.relation_type, r.source_service) for r in target_artist_rels
        }
        for rel in source_artist_rels:
            key = (rel.artist_id, rel.relation_type, rel.source_service)
            if key in target_artist_keys:
                results.append(_rowcount_result(1))  # DELETE
            else:
                results.append(_rowcount_result(1))  # UPDATE

        results.append(_scalars_result(source_track_rels))  # source tracks
        results.append(_scalars_result(target_track_rels))  # target tracks

        target_track_keys = {
            (r.track_id, r.relation_type, r.source_service) for r in target_track_rels
        }
        for rel in source_track_rels:
            key = (rel.track_id, rel.relation_type, rel.source_service)
            if key in target_track_keys:
                results.append(_rowcount_result(1))  # DELETE
            else:
                results.append(_rowcount_result(1))  # UPDATE

        results.append(_rowcount_result(sync_jobs_moved))  # sync_jobs
        results.append(_rowcount_result(1))  # DELETE user

        session.execute.side_effect = results
        return session, target_id, source_id

    @pytest.mark.asyncio
    async def test_reassigns_connections(self) -> None:
        session, target_id, source_id = self._setup_session(connections_moved=3)

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats.connections_moved == 3

    @pytest.mark.asyncio
    async def test_reassigns_listening_events(self) -> None:
        session, target_id, source_id = self._setup_session(events_moved=42)

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats.events_moved == 42

    @pytest.mark.asyncio
    async def test_skips_duplicate_artist_relations(self) -> None:
        """When a source artist relation duplicates a target one, delete it."""
        artist_id = uuid.uuid4()

        @dataclass
        class FakeRel:
            id: uuid.UUID
            artist_id: uuid.UUID
            relation_type: str
            source_service: str

        source_rel = FakeRel(
            id=uuid.uuid4(),
            artist_id=artist_id,
            relation_type="follow",
            source_service="spotify",
        )
        target_rel = FakeRel(
            id=uuid.uuid4(),
            artist_id=artist_id,
            relation_type="follow",
            source_service="spotify",
        )

        session, target_id, source_id = self._setup_session(
            source_artist_rels=[source_rel],
            target_artist_rels=[target_rel],
        )

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats.artist_relations_skipped == 1
        assert stats.artist_relations_moved == 0

    @pytest.mark.asyncio
    async def test_moves_non_duplicate_artist_relations(self) -> None:
        @dataclass
        class FakeRel:
            id: uuid.UUID
            artist_id: uuid.UUID
            relation_type: str
            source_service: str

        source_rel = FakeRel(
            id=uuid.uuid4(),
            artist_id=uuid.uuid4(),
            relation_type="follow",
            source_service="spotify",
        )

        session, target_id, source_id = self._setup_session(
            source_artist_rels=[source_rel],
        )

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats.artist_relations_moved == 1
        assert stats.artist_relations_skipped == 0

    @pytest.mark.asyncio
    async def test_skips_duplicate_track_relations(self) -> None:
        track_id = uuid.uuid4()

        @dataclass
        class FakeRel:
            id: uuid.UUID
            track_id: uuid.UUID
            relation_type: str
            source_service: str

        source_rel = FakeRel(
            id=uuid.uuid4(),
            track_id=track_id,
            relation_type="like",
            source_service="spotify",
        )
        target_rel = FakeRel(
            id=uuid.uuid4(),
            track_id=track_id,
            relation_type="like",
            source_service="spotify",
        )

        session, target_id, source_id = self._setup_session(
            source_track_rels=[source_rel],
            target_track_rels=[target_rel],
        )

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats.track_relations_skipped == 1
        assert stats.track_relations_moved == 0

    @pytest.mark.asyncio
    async def test_moves_non_duplicate_track_relations(self) -> None:
        @dataclass
        class FakeRel:
            id: uuid.UUID
            track_id: uuid.UUID
            relation_type: str
            source_service: str

        source_rel = FakeRel(
            id=uuid.uuid4(),
            track_id=uuid.uuid4(),
            relation_type="like",
            source_service="spotify",
        )

        session, target_id, source_id = self._setup_session(
            source_track_rels=[source_rel],
        )

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats.track_relations_moved == 1
        assert stats.track_relations_skipped == 0

    @pytest.mark.asyncio
    async def test_reassigns_sync_jobs(self) -> None:
        session, target_id, source_id = self._setup_session(sync_jobs_moved=5)

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats.sync_jobs_moved == 5

    @pytest.mark.asyncio
    async def test_deletes_source_user(self) -> None:
        session, target_id, source_id = self._setup_session()

        await merge_module.merge_accounts(session, target_id, source_id)

        # The last execute call should be the DELETE for the source user.
        last_call = session.execute.call_args_list[-1]
        stmt = last_call.args[0]
        # Verify it's a DELETE targeting the users table.
        assert isinstance(stmt, sa.Delete)

    @pytest.mark.asyncio
    async def test_returns_merge_stats(self) -> None:
        """Merge returns a fully populated MergeStats."""
        artist_id = uuid.uuid4()
        track_id = uuid.uuid4()

        @dataclass
        class FakeArtistRel:
            id: uuid.UUID
            artist_id: uuid.UUID
            relation_type: str
            source_service: str

        @dataclass
        class FakeTrackRel:
            id: uuid.UUID
            track_id: uuid.UUID
            relation_type: str
            source_service: str

        source_artist_dup = FakeArtistRel(
            id=uuid.uuid4(),
            artist_id=artist_id,
            relation_type="follow",
            source_service="spotify",
        )
        target_artist_dup = FakeArtistRel(
            id=uuid.uuid4(),
            artist_id=artist_id,
            relation_type="follow",
            source_service="spotify",
        )
        source_artist_unique = FakeArtistRel(
            id=uuid.uuid4(),
            artist_id=uuid.uuid4(),
            relation_type="follow",
            source_service="spotify",
        )

        source_track_dup = FakeTrackRel(
            id=uuid.uuid4(),
            track_id=track_id,
            relation_type="like",
            source_service="spotify",
        )
        target_track_dup = FakeTrackRel(
            id=uuid.uuid4(),
            track_id=track_id,
            relation_type="like",
            source_service="spotify",
        )
        source_track_unique = FakeTrackRel(
            id=uuid.uuid4(),
            track_id=uuid.uuid4(),
            relation_type="like",
            source_service="spotify",
        )

        session, target_id, source_id = self._setup_session(
            connections_moved=1,
            events_moved=500,
            source_artist_rels=[source_artist_dup, source_artist_unique],
            target_artist_rels=[target_artist_dup],
            source_track_rels=[source_track_dup, source_track_unique],
            target_track_rels=[target_track_dup],
            sync_jobs_moved=3,
        )

        stats = await merge_module.merge_accounts(session, target_id, source_id)

        assert stats == merge_module.MergeStats(
            connections_moved=1,
            events_moved=500,
            artist_relations_moved=1,
            artist_relations_skipped=1,
            track_relations_moved=1,
            track_relations_skipped=1,
            sync_jobs_moved=3,
        )
