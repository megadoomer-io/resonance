"""Tests for the pure rediscovery generator logic (#rediscovery, design T3)."""

from __future__ import annotations

import uuid

import resonance.generators.rediscovery as rediscovery_module
import resonance.types as types_module


def _cand(
    *,
    artist_id: uuid.UUID,
    track_id: uuid.UUID | None = None,
    title: str = "t",
    artist_name: str = "a",
    listen_count: int = 0,
    in_library: bool = True,
    popularity_score: int = 50,
    source: types_module.TrackSource = types_module.TrackSource.LIBRARY,
) -> rediscovery_module.CandidateTrack:
    return rediscovery_module.CandidateTrack(
        track_id=track_id or uuid.uuid4(),
        title=title,
        artist_name=artist_name,
        artist_id=artist_id,
        is_target_artist=True,
        listen_count=listen_count,
        in_library=in_library,
        popularity_score=popularity_score,
        source=source,
    )


class TestSelectDeepCutTrackIds:
    def test_bottom_third_selected(self) -> None:
        # 8 tracks; 33rd pctile -> nearest-rank idx 2 (value 2); count<=2 -> 3 tracks.
        tids = [uuid.uuid4() for _ in range(8)]
        counts = [1, 1, 2, 3, 5, 8, 13, 21]
        play_counts = dict(zip(tids, counts, strict=True))
        deep = rediscovery_module.select_deep_cut_track_ids(play_counts, percentile=33)
        assert deep == {tids[0], tids[1], tids[2]}

    def test_play_count_floor_excludes_unplayed(self) -> None:
        # A never-played track (count 0) is not a rediscovery -- excluded entirely,
        # and it doesn't count toward the distinct-track guard either.
        played = [uuid.uuid4() for _ in range(4)]
        unplayed = uuid.uuid4()
        play_counts = {**dict.fromkeys(played, 1), unplayed: 0}
        # bump so they aren't all equal
        play_counts[played[3]] = 9
        deep = rediscovery_module.select_deep_cut_track_ids(play_counts, percentile=50)
        assert unplayed not in deep

    def test_thin_seed_guard_drops_artist(self) -> None:
        # Fewer than 4 distinct played tracks -> no deep cuts (degenerate pctile).
        tids = [uuid.uuid4() for _ in range(3)]
        play_counts = dict(zip(tids, [1, 1, 2], strict=True))
        assert (
            rediscovery_module.select_deep_cut_track_ids(play_counts, percentile=33)
            == set()
        )

    def test_guard_boundary_exactly_four_passes(self) -> None:
        tids = [uuid.uuid4() for _ in range(4)]
        play_counts = dict(zip(tids, [1, 2, 3, 4], strict=True))
        deep = rediscovery_module.select_deep_cut_track_ids(play_counts, percentile=33)
        # nearest-rank idx 1 -> value 2 -> count<=2 -> 2 tracks.
        assert deep == {tids[0], tids[1]}

    def test_heavy_rotation_artist_still_yields_deep_cuts(self) -> None:
        # An artist you play a lot still has a "bottom third" -- deep cuts are
        # relative to the artist's own distribution, not an absolute play threshold.
        tids = [uuid.uuid4() for _ in range(6)]
        play_counts = dict(zip(tids, [40, 55, 60, 90, 120, 200], strict=True))
        deep = rediscovery_module.select_deep_cut_track_ids(play_counts, percentile=33)
        assert tids[0] in deep and tids[5] not in deep


class TestSplitBudget:
    def test_balanced(self) -> None:
        assert rediscovery_module.split_budget(new_ratio=50, max_tracks=10) == (5, 5)

    def test_all_deep_cuts(self) -> None:
        assert rediscovery_module.split_budget(new_ratio=0, max_tracks=10) == (0, 10)

    def test_all_new(self) -> None:
        assert rediscovery_module.split_budget(new_ratio=100, max_tracks=10) == (10, 0)

    def test_rounding_remainder_to_deep(self) -> None:
        # round(3.3) = 3 -> 3 new, 7 deep; total exact.
        assert rediscovery_module.split_budget(new_ratio=33, max_tracks=10) == (3, 7)


class TestScoreAndSelect:
    def test_partitions_new_and_deep_streams(self) -> None:
        new_artist = uuid.uuid4()
        seed_artist = uuid.uuid4()
        new_tracks = [
            _cand(artist_id=new_artist, in_library=False, popularity_score=80)
            for _ in range(5)
        ]
        seed_tracks = [_cand(artist_id=seed_artist, listen_count=2) for _ in range(5)]
        deep_ids = {c.track_id for c in seed_tracks}
        result = rediscovery_module.score_and_select(
            candidates=[*new_tracks, *seed_tracks],
            new_artist_ids={new_artist},
            deep_cut_track_ids=deep_ids,
            params={"familiarity": 50, "hit_depth": 50},
            new_ratio=50,
            max_tracks=6,
            previous_track_ids=set(),
            freshness_target=None,
            exempt_deep_cuts_from_freshness=False,
        )
        picked = {t.track_id for t in result.tracks}
        assert len(result.tracks) == 6
        # 3 from each stream at 50/50.
        new_picked = picked & {c.track_id for c in new_tracks}
        deep_picked = picked & deep_ids
        assert len(new_picked) == 3
        assert len(deep_picked) == 3

    def test_deep_track_not_in_deep_ids_is_excluded(self) -> None:
        # A seed artist's track that didn't qualify as a deep cut is not a candidate.
        seed_artist = uuid.uuid4()
        deep = _cand(artist_id=seed_artist, listen_count=1)
        loud = _cand(artist_id=seed_artist, listen_count=99)  # not a deep cut
        result = rediscovery_module.score_and_select(
            candidates=[deep, loud],
            new_artist_ids=set(),
            deep_cut_track_ids={deep.track_id},
            params={"familiarity": 50, "hit_depth": 50},
            new_ratio=0,
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
            exempt_deep_cuts_from_freshness=False,
        )
        picked = {t.track_id for t in result.tracks}
        assert deep.track_id in picked
        assert loud.track_id not in picked

    def test_short_new_stream_redistributes_to_deep(self) -> None:
        # new_ratio wants 3 new but only 1 new artist track exists; deep absorbs
        # the slack so the playlist still fills to max_tracks.
        new_artist = uuid.uuid4()
        seed_artist = uuid.uuid4()
        new_tracks = [_cand(artist_id=new_artist, in_library=False)]
        seed_tracks = [_cand(artist_id=seed_artist, listen_count=1) for _ in range(9)]
        result = rediscovery_module.score_and_select(
            candidates=[*new_tracks, *seed_tracks],
            new_artist_ids={new_artist},
            deep_cut_track_ids={c.track_id for c in seed_tracks},
            params={"familiarity": 50, "hit_depth": 50},
            new_ratio=50,
            max_tracks=6,
            previous_track_ids=set(),
            freshness_target=None,
            exempt_deep_cuts_from_freshness=False,
        )
        assert len(result.tracks) == 6  # 1 new + 5 deep (slack redistributed)

    def test_all_new_ratio_selects_only_new(self) -> None:
        new_artist = uuid.uuid4()
        seed_artist = uuid.uuid4()
        new_tracks = [_cand(artist_id=new_artist, in_library=False) for _ in range(4)]
        seed_tracks = [_cand(artist_id=seed_artist, listen_count=1) for _ in range(4)]
        result = rediscovery_module.score_and_select(
            candidates=[*new_tracks, *seed_tracks],
            new_artist_ids={new_artist},
            deep_cut_track_ids={c.track_id for c in seed_tracks},
            params={"familiarity": 50, "hit_depth": 50},
            new_ratio=100,
            max_tracks=4,
            previous_track_ids=set(),
            freshness_target=None,
            exempt_deep_cuts_from_freshness=False,
        )
        picked = {t.track_id for t in result.tracks}
        assert picked == {c.track_id for c in new_tracks}

    def test_absolute_window_exempts_deep_cuts_from_freshness(self) -> None:
        # freshness_target=100 (all new) but deep cuts are exempt -> a previously
        # surfaced deep cut persists across regenerate.
        seed_artist = uuid.uuid4()
        deep_tracks = [_cand(artist_id=seed_artist, listen_count=1) for _ in range(4)]
        prev = {c.track_id for c in deep_tracks}  # all were in the prior version
        result = rediscovery_module.score_and_select(
            candidates=list(deep_tracks),
            new_artist_ids=set(),
            deep_cut_track_ids=prev,
            params={"familiarity": 50, "hit_depth": 50},
            new_ratio=0,
            max_tracks=4,
            previous_track_ids=prev,
            freshness_target=100,
            exempt_deep_cuts_from_freshness=True,
        )
        # Exempt: repeats survive despite freshness_target=100.
        assert len(result.tracks) == 4

    def test_relative_window_applies_freshness_to_deep_cuts(self) -> None:
        # Same setup but NOT exempt (relative window) -> repeats are filtered out.
        seed_artist = uuid.uuid4()
        deep_tracks = [_cand(artist_id=seed_artist, listen_count=1) for _ in range(4)]
        prev = {c.track_id for c in deep_tracks}
        result = rediscovery_module.score_and_select(
            candidates=list(deep_tracks),
            new_artist_ids=set(),
            deep_cut_track_ids=prev,
            params={"familiarity": 50, "hit_depth": 50},
            new_ratio=0,
            max_tracks=4,
            previous_track_ids=prev,
            freshness_target=100,
            exempt_deep_cuts_from_freshness=False,
        )
        # All were repeats and freshness=100 allows 0 repeats -> empty.
        assert result.tracks == []

    def test_empty_candidates(self) -> None:
        result = rediscovery_module.score_and_select(
            candidates=[],
            new_artist_ids=set(),
            deep_cut_track_ids=set(),
            params={"familiarity": 50, "hit_depth": 50},
            new_ratio=50,
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
            exempt_deep_cuts_from_freshness=False,
        )
        assert result.tracks == []
