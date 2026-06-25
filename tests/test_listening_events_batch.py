"""Tests for batched listening-event upsert + dedup core (#6)."""

from __future__ import annotations

import datetime
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.connectors.base as base_module
import resonance.sync.runner as runner_module
import resonance.types as types_module


def _dt(epoch: int) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(epoch, tz=datetime.UTC)


class TestDedupWindowSeconds:
    def test_uses_duration_plus_buffer(self) -> None:
        # 240s track -> 240 + 60 buffer
        assert runner_module._dedup_window_seconds(240_000) == 300

    def test_falls_back_to_ten_minutes(self) -> None:
        assert runner_module._dedup_window_seconds(None) == 600
        assert runner_module._dedup_window_seconds(0) == 600


class TestSelectNewEvents:
    """Pure dedup core: R1 (intra-page), existing anchors, per-track windows."""

    def test_all_unique_survive(self) -> None:
        t = uuid.uuid4()
        resolved = [(t, _dt(1000), 180_000), (t, _dt(2000), 180_000)]
        survivors = runner_module._select_new_events(resolved, {})
        assert len(survivors) == 2

    def test_intra_page_near_duplicate_dropped(self) -> None:
        """R1: two same-track events within window, no existing rows -> one wins.

        This is the regression the batch must not introduce: a naive batch that
        only checks against pre-loaded DB rows would let both survive.
        """
        t = uuid.uuid4()
        # 180s track -> window 240s. 1000 and 1100 are 100s apart -> dup.
        resolved = [(t, _dt(1000), 180_000), (t, _dt(1100), 180_000)]
        survivors = runner_module._select_new_events(resolved, {})
        assert survivors == [(t, _dt(1000))]

    def test_intra_page_far_apart_both_survive(self) -> None:
        t = uuid.uuid4()
        # 1000 and 5000 are 4000s apart, well beyond the 240s window.
        resolved = [(t, _dt(1000), 180_000), (t, _dt(5000), 180_000)]
        survivors = runner_module._select_new_events(resolved, {})
        assert len(survivors) == 2

    def test_different_tracks_same_time_both_survive(self) -> None:
        t1, t2 = uuid.uuid4(), uuid.uuid4()
        resolved = [(t1, _dt(1000), 180_000), (t2, _dt(1000), 180_000)]
        survivors = runner_module._select_new_events(resolved, {})
        assert len(survivors) == 2

    def test_dropped_against_existing_db_row(self) -> None:
        t = uuid.uuid4()
        existing = {t: [_dt(1000)]}
        resolved = [(t, _dt(1150), 180_000)]  # within 240s of existing 1000
        survivors = runner_module._select_new_events(resolved, existing)
        assert survivors == []

    def test_kept_just_outside_existing_window(self) -> None:
        t = uuid.uuid4()
        existing = {t: [_dt(1000)]}
        # window 240s; 1000 + 241 = 1241 is just outside -> kept.
        resolved = [(t, _dt(1241), 180_000)]
        survivors = runner_module._select_new_events(resolved, existing)
        assert survivors == [(t, _dt(1241))]

    def test_window_uses_each_events_duration(self) -> None:
        t = uuid.uuid4()
        # No-duration fallback = 600s window; 1000 and 1500 (500s) -> dup.
        resolved = [(t, _dt(1000), None), (t, _dt(1500), None)]
        survivors = runner_module._select_new_events(resolved, {})
        assert survivors == [(t, _dt(1000))]


class TestUpsertListeningEventsBatch:
    @pytest.mark.anyio()
    async def test_empty_events_no_queries(self) -> None:
        session = AsyncMock()
        n = await runner_module.upsert_listening_events_batch(
            session,
            uuid.uuid4(),
            [],
            service_type=types_module.ServiceType.LISTENBRAINZ,
        )
        assert n == 0
        session.execute.assert_not_called()

    @pytest.mark.anyio()
    async def test_resolves_dedupes_and_inserts(self, monkeypatch) -> None:  # type: ignore[no-untyped-def]
        """Two same-track near-dups in a page -> one INSERT row, returns 1."""
        user_id = uuid.uuid4()
        track = MagicMock()
        track.id = uuid.uuid4()
        track.duration_ms = 180_000

        td = base_module.TrackData(
            external_id="lb-track-1",
            title="Song",
            artist_external_id="lb-art-1",
            artist_name="Artist",
            service=types_module.ServiceType.LISTENBRAINZ,
        )

        # bulk_fetch_tracks resolves the external id to our track.
        async def fake_bulk_fetch_tracks(_session, _key, _ids):  # type: ignore[no-untyped-def]
            return {"lb-track-1": track}

        monkeypatch.setattr(runner_module, "bulk_fetch_tracks", fake_bulk_fetch_tracks)

        session = AsyncMock()
        # First execute() = existing-events range query (no prior rows).
        existing_result = MagicMock()
        existing_result.all.return_value = []
        # Second execute() = the bulk insert ... returning -> one inserted id.
        insert_result = MagicMock()
        insert_result.all.return_value = [(uuid.uuid4(),)]
        session.execute.side_effect = [existing_result, insert_result]

        events = [(td, 1000), (td, 1100)]  # 100s apart, within 240s window
        n = await runner_module.upsert_listening_events_batch(
            session, user_id, events, service_type=types_module.ServiceType.LISTENBRAINZ
        )

        assert n == 1
        # range query + one bulk insert == exactly two executes (not per-listen).
        assert session.execute.await_count == 2
