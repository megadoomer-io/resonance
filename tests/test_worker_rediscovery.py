"""Tests for the worker's rediscovery selection glue (#rediscovery, T4/T8).

``worker._select_rediscovery`` is the pure derivation step: from a profile's
input_references + the already-fetched candidates + lifetime listen counts, it
derives the new-artist set (via_seed discoveries), each seed artist's deep-cut
track ids, and the window-driven freshness exemption, then delegates to
``rediscovery.score_and_select`` (tested separately). These tests exercise the
derivation, not the SQL (the worker tests are mock-based).
"""

from __future__ import annotations

import uuid

import resonance.generators.concert_prep as concert_prep_module
import resonance.types as types_module
import resonance.worker as worker_module


def _cand(
    *,
    artist_id: uuid.UUID,
    track_id: uuid.UUID,
    listen_count: int,
    in_library: bool,
    source: types_module.TrackSource,
) -> concert_prep_module.CandidateTrack:
    return concert_prep_module.CandidateTrack(
        track_id=track_id,
        title="t",
        artist_name="a",
        artist_id=artist_id,
        is_target_artist=True,
        listen_count=listen_count,
        in_library=in_library,
        popularity_score=50,
        source=source,
    )


def _relative_refs(new_artist: uuid.UUID) -> dict[str, object]:
    return {
        "sources": [
            {
                "kind": "listening_range",
                "enabled": True,
                "window": {"kind": "relative", "lookback_days": 14},
            },
            {
                "kind": "artist",
                "artist_id": str(new_artist),
                "enabled": True,
                "via_seed": "lineup",
            },
        ]
    }


class TestSelectRediscovery:
    def test_partitions_via_seed_new_from_listened_seeds(self) -> None:
        new_artist = uuid.uuid4()
        seed_artist = uuid.uuid4()
        # New (discovery) tracks: never heard.
        new_tracks = [
            _cand(
                artist_id=new_artist,
                track_id=uuid.uuid4(),
                listen_count=0,
                in_library=False,
                source=types_module.TrackSource.DISCOVERY,
            )
            for _ in range(3)
        ]
        # Seed (listened) tracks with a play distribution.
        seed_tids = [uuid.uuid4() for _ in range(5)]
        seed_counts = [1, 2, 3, 8, 20]
        seed_tracks = [
            _cand(
                artist_id=seed_artist,
                track_id=tid,
                listen_count=count,
                in_library=True,
                source=types_module.TrackSource.LIBRARY,
            )
            for tid, count in zip(seed_tids, seed_counts, strict=True)
        ]
        listen_counts = dict(zip(seed_tids, seed_counts, strict=True))

        result = worker_module._select_rediscovery(
            candidates=[*new_tracks, *seed_tracks],
            input_references=_relative_refs(new_artist),
            listen_counts=listen_counts,
            params={
                "familiarity": 50,
                "hit_depth": 50,
                "new_ratio": 50,
                "less_heard_percentile": 33,
            },
            max_tracks=4,
            previous_track_ids=set(),
            freshness_target=None,
        )

        picked = {t.track_id for t in result.tracks}
        new_picked = picked & {c.track_id for c in new_tracks}
        deep_picked = picked & set(seed_tids)
        # 50/50 split of 4 -> 2 new + 2 deep.
        assert len(new_picked) == 2
        assert len(deep_picked) == 2
        # Deep cuts are the bottom-third of the seed distribution (counts 1 and 2),
        # never the heavy-rotation tracks (8, 20).
        assert deep_picked == {seed_tids[0], seed_tids[1]}

    def test_thin_seed_artist_contributes_no_deep_cuts(self) -> None:
        new_artist = uuid.uuid4()
        thin_seed = uuid.uuid4()
        # Only 3 distinct played tracks -> thin-seed guard drops it from deep cuts.
        thin_tids = [uuid.uuid4() for _ in range(3)]
        thin_tracks = [
            _cand(
                artist_id=thin_seed,
                track_id=tid,
                listen_count=1,
                in_library=True,
                source=types_module.TrackSource.LIBRARY,
            )
            for tid in thin_tids
        ]
        new_tracks = [
            _cand(
                artist_id=new_artist,
                track_id=uuid.uuid4(),
                listen_count=0,
                in_library=False,
                source=types_module.TrackSource.DISCOVERY,
            )
            for _ in range(3)
        ]
        result = worker_module._select_rediscovery(
            candidates=[*new_tracks, *thin_tracks],
            input_references=_relative_refs(new_artist),
            listen_counts=dict.fromkeys(thin_tids, 1),
            params={"familiarity": 50, "hit_depth": 50, "new_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        picked = {t.track_id for t in result.tracks}
        # The thin seed contributes NO deep cuts (guard drops it) -- none of its
        # tracks are selected, even though new_ratio=0 asked for all deep cuts.
        assert picked & set(thin_tids) == set()
        # The deep stream's empty slack redistributes to the new stream, so the
        # playlist still fills rather than coming back empty.
        assert picked == {c.track_id for c in new_tracks}

    def test_absolute_window_exempts_deep_cuts_from_freshness(self) -> None:
        seed_artist = uuid.uuid4()
        seed_tids = [uuid.uuid4() for _ in range(4)]
        seed_tracks = [
            _cand(
                artist_id=seed_artist,
                track_id=tid,
                listen_count=1,
                in_library=True,
                source=types_module.TrackSource.LIBRARY,
            )
            for tid in seed_tids
        ]
        refs = {
            "sources": [
                {
                    "kind": "listening_range",
                    "enabled": True,
                    "window": {
                        "kind": "absolute",
                        "start": "2025-07-01T00:00:00+00:00",
                        "end": "2025-07-15T00:00:00+00:00",
                    },
                }
            ]
        }
        result = worker_module._select_rediscovery(
            candidates=list(seed_tracks),
            input_references=refs,
            listen_counts=dict.fromkeys(seed_tids, 1),
            params={"familiarity": 50, "hit_depth": 50, "new_ratio": 0},
            max_tracks=4,
            # every deep cut was in the prior version; freshness_target=100 would
            # normally drop them all, but the absolute window exempts deep cuts.
            previous_track_ids=set(seed_tids),
            freshness_target=100,
        )
        assert len(result.tracks) == 4
