"""Tests for the Concert Archives chunk processor."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.concerts.concert_archives as concert_archives_module
import resonance.concerts.worker as concert_worker
import resonance.types as types_module

_SAMPLE_CSV = """\
Start Date,End Date,Status,Concert Name,Bands Seen,Bands Not Seen,Venue,Location,URL
2024-01-15,,Past,Show 1,Band A,,Venue A,"City A, US",https://example.com/1
2024-02-20,,Past,Show 2,Band B,,Venue B,"City B, US",https://example.com/2
"""

_CANCELLED_CSV = """\
Start Date,End Date,Status,Concert Name,Bands Seen,Bands Not Seen,Venue,Location,URL
2024-03-10,,Cancelled,Cancelled Show,Band C,,Venue C,"City C, US",https://example.com/3
"""

_SYNC_PREFIX = "resonance.concerts.worker.concert_sync"
_LIFECYCLE_PREFIX = "resonance.concerts.worker.lifecycle_module"


def _build_chunk_ctx(
    csv: str = _SAMPLE_CSV,
) -> tuple[MagicMock, dict[str, object]]:
    """Build a chunk task mock and worker ctx with all DB queries pre-wired."""
    connection = MagicMock()
    connection.id = uuid.uuid4()
    connection.user_id = uuid.uuid4()
    connection.service_type = types_module.ServiceType.CONCERT_ARCHIVES
    connection.enabled = True

    parent = MagicMock()
    parent.id = uuid.uuid4()
    parent.user_id = connection.user_id
    parent.service_connection_id = connection.id
    parse_result = concert_archives_module.parse_csv(csv)
    parent.params = {
        "parsed_events": [e.model_dump(mode="json") for e in parse_result.events],
    }

    chunk = MagicMock()
    chunk.id = uuid.uuid4()
    chunk.user_id = connection.user_id
    chunk.service_connection_id = connection.id
    chunk.parent_id = parent.id
    chunk.status = types_module.SyncStatus.PENDING
    chunk.params = {"chunk_index": 0, "chunk_size": 25}
    chunk.started_at = None

    session = AsyncMock()
    tr = MagicMock()
    tr.scalar_one_or_none.return_value = chunk
    pr = MagicMock()
    pr.scalar_one_or_none.return_value = parent
    cr = MagicMock()
    cr.scalar_one_or_none.return_value = connection
    session.execute.side_effect = [tr, pr, cr]

    sf = MagicMock()
    sf.return_value.__aenter__ = AsyncMock(return_value=session)
    sf.return_value.__aexit__ = AsyncMock(return_value=False)
    ctx: dict[str, object] = {"session_factory": sf, "redis": AsyncMock()}

    return chunk, ctx


class TestSyncConcertArchivesChunk:
    """Tests for sync_concert_archives_chunk."""

    @pytest.mark.anyio()
    async def test_processes_correct_slice(self) -> None:
        """Chunk processes all events from the parent's parsed_events."""
        chunk, ctx = _build_chunk_ctx()

        mock_event = MagicMock()
        with (
            patch(
                f"{_LIFECYCLE_PREFIX}.is_cancelled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(f"{_LIFECYCLE_PREFIX}.complete_task", new_callable=AsyncMock) as mc,
            patch(
                f"{_SYNC_PREFIX}.upsert_venue_candidate", new_callable=AsyncMock
            ) as muvc,
            patch(f"{_SYNC_PREFIX}.resolve_venue_candidate", new_callable=AsyncMock),
            patch(f"{_SYNC_PREFIX}.upsert_event_candidate", new_callable=AsyncMock),
            patch(
                f"{_SYNC_PREFIX}.resolve_event_candidate",
                new_callable=AsyncMock,
                return_value=(mock_event, True),
            ),
            patch(
                f"{_SYNC_PREFIX}.upsert_candidates",
                new_callable=AsyncMock,
                return_value=2,
            ),
            patch(f"{_SYNC_PREFIX}.upsert_attendance", new_callable=AsyncMock) as matt,
            patch(
                f"{_SYNC_PREFIX}.match_candidates_to_artists",
                new_callable=AsyncMock,
                return_value=1,
            ),
            patch("resonance.worker._check_parent_completion", new_callable=AsyncMock),
        ):
            await concert_worker.sync_concert_archives_chunk(ctx, str(chunk.id))

            assert muvc.await_count == 2
            assert matt.await_count == 2

            mc.assert_awaited_once()
            result = mc.call_args.args[2]
            assert result["total_events"] == 2
            assert result["events_created"] == 2
            assert result["candidates_created"] == 4
            assert result["candidates_matched"] == 2

    @pytest.mark.anyio()
    async def test_cancelled_event_no_attendance(self) -> None:
        """Cancelled events do not create attendance records."""
        chunk, ctx = _build_chunk_ctx(csv=_CANCELLED_CSV)

        mock_event = MagicMock()
        with (
            patch(
                f"{_LIFECYCLE_PREFIX}.is_cancelled",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch(f"{_LIFECYCLE_PREFIX}.complete_task", new_callable=AsyncMock) as mc,
            patch(f"{_SYNC_PREFIX}.upsert_venue_candidate", new_callable=AsyncMock),
            patch(f"{_SYNC_PREFIX}.resolve_venue_candidate", new_callable=AsyncMock),
            patch(
                f"{_SYNC_PREFIX}.upsert_event_candidate", new_callable=AsyncMock
            ) as muec,
            patch(
                f"{_SYNC_PREFIX}.resolve_event_candidate",
                new_callable=AsyncMock,
                return_value=(mock_event, True),
            ),
            patch(
                f"{_SYNC_PREFIX}.upsert_candidates",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch(f"{_SYNC_PREFIX}.upsert_attendance", new_callable=AsyncMock) as matt,
            patch(
                f"{_SYNC_PREFIX}.match_candidates_to_artists",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch("resonance.worker._check_parent_completion", new_callable=AsyncMock),
        ):
            await concert_worker.sync_concert_archives_chunk(ctx, str(chunk.id))

            muec.assert_awaited_once()
            matt.assert_not_awaited()
            mc.assert_awaited_once()
