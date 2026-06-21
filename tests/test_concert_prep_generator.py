"""Tests for the concert prep generator."""

from __future__ import annotations

import math
import uuid

import resonance.generators.concert_prep as concert_prep_module
import resonance.types as types_module


class TestBuildCandidateList:
    def test_library_tracks_included(self) -> None:
        artist_id = uuid.uuid4()
        track_id = uuid.uuid4()
        library_tracks = [
            concert_prep_module.CandidateTrack(
                track_id=track_id,
                title="Known Song",
                artist_name="Band A",
                artist_id=artist_id,
                is_target_artist=True,
                listen_count=50,
                in_library=True,
                popularity_score=0,
                source=types_module.TrackSource.LIBRARY,
            )
        ]
        result = concert_prep_module.score_and_select(
            candidates=library_tracks,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 1
        assert result.tracks[0].track_id == track_id

    def test_respects_max_tracks(self) -> None:
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title=f"Song {i}",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=i,
                in_library=True,
                popularity_score=50,
                source=types_module.TrackSource.LIBRARY,
            )
            for i in range(50)
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=20,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 20


class TestFreshnessFilter:
    def test_full_freshness_excludes_previous(self) -> None:
        prev_id = uuid.uuid4()
        new_id = uuid.uuid4()
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=prev_id,
                title="Old Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=100,
                in_library=True,
                popularity_score=90,
                source=types_module.TrackSource.LIBRARY,
            ),
            concert_prep_module.CandidateTrack(
                track_id=new_id,
                title="New Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=10,
                in_library=True,
                popularity_score=50,
                source=types_module.TrackSource.LIBRARY,
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids={prev_id},
            freshness_target=100,
        )
        track_ids = {t.track_id for t in result.tracks}
        assert prev_id not in track_ids
        assert new_id in track_ids

    def test_zero_freshness_allows_all(self) -> None:
        prev_id = uuid.uuid4()
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=prev_id,
                title="Old Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=100,
                in_library=True,
                popularity_score=90,
                source=types_module.TrackSource.LIBRARY,
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids={prev_id},
            freshness_target=0,
        )
        assert len(result.tracks) == 1

    def test_partial_freshness_limits_repeats(self) -> None:
        """With freshness_target=50, half the tracks can be repeats."""
        prev_ids = [uuid.uuid4() for _ in range(10)]
        new_ids = [uuid.uuid4() for _ in range(10)]
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=pid,
                title=f"Old Song {i}",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=100 + i,
                in_library=True,
                popularity_score=80,
                source=types_module.TrackSource.LIBRARY,
            )
            for i, pid in enumerate(prev_ids)
        ] + [
            concert_prep_module.CandidateTrack(
                track_id=nid,
                title=f"New Song {i}",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=50 + i,
                in_library=True,
                popularity_score=60,
                source=types_module.TrackSource.LIBRARY,
            )
            for i, nid in enumerate(new_ids)
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(prev_ids),
            freshness_target=50,
        )
        # With freshness_target=50, at most 50% can be repeats => at most 5 repeats
        repeat_count = sum(1 for t in result.tracks if t.track_id in set(prev_ids))
        assert repeat_count <= 5

    def test_freshness_none_allows_all(self) -> None:
        """When freshness_target is None, no filtering applied."""
        prev_id = uuid.uuid4()
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=prev_id,
                title="Old Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=100,
                in_library=True,
                popularity_score=90,
                source=types_module.TrackSource.LIBRARY,
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids={prev_id},
            freshness_target=None,
        )
        assert len(result.tracks) == 1


class TestSelectionResult:
    def test_tracks_ordered_by_position(self) -> None:
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title=f"Song {i}",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=i * 10,
                in_library=True,
                popularity_score=50,
                source=types_module.TrackSource.LIBRARY,
            )
            for i in range(5)
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 80, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        positions = [t.position for t in result.tracks]
        assert positions == list(range(1, len(result.tracks) + 1))

    def test_source_summary_computed(self) -> None:
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title="Lib Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=10,
                in_library=True,
                popularity_score=50,
                source=types_module.TrackSource.LIBRARY,
            ),
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title="Disc Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=0,
                in_library=False,
                popularity_score=60,
                source=types_module.TrackSource.DISCOVERY,
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert result.sources_summary[types_module.TrackSource.LIBRARY] == 1
        assert result.sources_summary[types_module.TrackSource.DISCOVERY] == 1

    def test_freshness_actual_no_previous(self) -> None:
        """When no previous tracks, freshness_actual should be None."""
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title="Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=10,
                in_library=True,
                popularity_score=50,
                source=types_module.TrackSource.LIBRARY,
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert result.freshness_actual is None

    def test_freshness_actual_with_previous(self) -> None:
        """Freshness actual should reflect percentage of new tracks."""
        prev_id = uuid.uuid4()
        new_id = uuid.uuid4()
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=prev_id,
                title="Old Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=100,
                in_library=True,
                popularity_score=90,
                source=types_module.TrackSource.LIBRARY,
            ),
            concert_prep_module.CandidateTrack(
                track_id=new_id,
                title="New Song",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=50,
                in_library=True,
                popularity_score=50,
                source=types_module.TrackSource.LIBRARY,
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids={prev_id},
            freshness_target=None,
        )
        # 1 of 2 tracks is new => freshness = 50.0
        assert result.freshness_actual == 50.0

    def test_empty_candidates(self) -> None:
        """Empty candidate list produces empty result."""
        result = concert_prep_module.score_and_select(
            candidates=[],
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 0
        assert result.sources_summary == {}
        assert result.freshness_actual is None

    def test_scores_are_descending(self) -> None:
        """Tracks should be ordered by score descending."""
        candidates = [
            concert_prep_module.CandidateTrack(
                track_id=uuid.uuid4(),
                title=f"Song {i}",
                artist_name="Band",
                artist_id=uuid.uuid4(),
                is_target_artist=True,
                listen_count=i * 10,
                in_library=True,
                popularity_score=i * 10,
                source=types_module.TrackSource.LIBRARY,
            )
            for i in range(10)
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        scores = [t.score for t in result.tracks]
        assert scores == sorted(scores, reverse=True)


def _candidate(*, is_target: bool) -> concert_prep_module.CandidateTrack:
    """Build a candidate track with a unique id for blend-quota tests."""
    return concert_prep_module.CandidateTrack(
        track_id=uuid.uuid4(),
        title="Song",
        artist_name="Target" if is_target else "Adjacent",
        artist_id=uuid.uuid4(),
        is_target_artist=is_target,
        listen_count=10,
        in_library=True,
        popularity_score=0,
        source=types_module.TrackSource.LIBRARY,
    )


class TestBlendQuota:
    """similar_artist_ratio sets the fraction of slots from the adjacent pool.

    Familiarity/hit_depth rank within a pool; the ratio picks the mix. The two
    are orthogonal (issue #111/#57).
    """

    def test_ratio_zero_excludes_adjacent(self) -> None:
        target = [_candidate(is_target=True) for _ in range(10)]
        adjacent = [_candidate(is_target=False) for _ in range(10)]
        target_ids = {t.track_id for t in target}
        result = concert_prep_module.score_and_select(
            candidates=target + adjacent,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 10
        assert all(t.track_id in target_ids for t in result.tracks)

    def test_ratio_hundred_excludes_target(self) -> None:
        target = [_candidate(is_target=True) for _ in range(10)]
        adjacent = [_candidate(is_target=False) for _ in range(10)]
        adjacent_ids = {t.track_id for t in adjacent}
        result = concert_prep_module.score_and_select(
            candidates=target + adjacent,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 100},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 10
        assert all(t.track_id in adjacent_ids for t in result.tracks)

    def test_ratio_thirty_splits_pools(self) -> None:
        target = [_candidate(is_target=True) for _ in range(20)]
        adjacent = [_candidate(is_target=False) for _ in range(20)]
        adjacent_ids = {t.track_id for t in adjacent}
        result = concert_prep_module.score_and_select(
            candidates=target + adjacent,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 30},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 10
        adj_count = sum(1 for t in result.tracks if t.track_id in adjacent_ids)
        # round_half_up(10 * 30 / 100) = 3 adjacent, 7 target
        assert adj_count == 3

    def test_ratio_rounds_half_up(self) -> None:
        target = [_candidate(is_target=True) for _ in range(20)]
        adjacent = [_candidate(is_target=False) for _ in range(20)]
        adjacent_ids = {t.track_id for t in adjacent}
        result = concert_prep_module.score_and_select(
            candidates=target + adjacent,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 35},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        adj_count = sum(1 for t in result.tracks if t.track_id in adjacent_ids)
        # round_half_up(10 * 35 / 100) = round_half_up(3.5) = 4 adjacent
        assert adj_count == 4

    def test_adjacent_underflow_backfills_from_target(self) -> None:
        target = [_candidate(is_target=True) for _ in range(20)]
        adjacent = [_candidate(is_target=False) for _ in range(2)]
        adjacent_ids = {t.track_id for t in adjacent}
        result = concert_prep_module.score_and_select(
            candidates=target + adjacent,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 50},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        # quota is 5 adjacent, but only 2 exist => 2 adjacent + 8 target = 10
        assert len(result.tracks) == 10
        adj_count = sum(1 for t in result.tracks if t.track_id in adjacent_ids)
        assert adj_count == 2

    def test_target_underflow_backfills_from_adjacent(self) -> None:
        target = [_candidate(is_target=True) for _ in range(2)]
        adjacent = [_candidate(is_target=False) for _ in range(20)]
        target_ids = {t.track_id for t in target}
        result = concert_prep_module.score_and_select(
            candidates=target + adjacent,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 50},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        # quota is 5 target, but only 2 exist => 2 target + 8 adjacent = 10
        assert len(result.tracks) == 10
        tgt_count = sum(1 for t in result.tracks if t.track_id in target_ids)
        assert tgt_count == 2

    def test_ratio_zero_no_target_yields_empty(self) -> None:
        # ratio=0 is target-only; with no target tracks, no adjacent backfill.
        adjacent = [_candidate(is_target=False) for _ in range(10)]
        result = concert_prep_module.score_and_select(
            candidates=adjacent,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 0

    def test_ratio_hundred_no_adjacent_yields_empty(self) -> None:
        # ratio=100 is adjacent-only; with no adjacent tracks, no target backfill.
        target = [_candidate(is_target=True) for _ in range(10)]
        result = concert_prep_module.score_and_select(
            candidates=target,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 100},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 0


# Parameters that make track score a strict monotonic function of listen_count:
# familiarity=100 gives familiarity full positive weight, hit_depth=50 makes
# popularity neutral. So higher listen_count => strictly higher score, which lets
# the per-artist fairness tests control ranking deterministically.
_FAMILIARITY_DRIVEN = {"familiarity": 100, "hit_depth": 50}


def _artist_candidate(
    *,
    artist_id: uuid.UUID,
    listen_count: int,
    is_target: bool,
) -> concert_prep_module.CandidateTrack:
    """Build a candidate whose score is driven solely by listen_count.

    Used by the per-artist fairness tests, where ranking must be deterministic
    and grouping by artist_id matters.
    """
    return concert_prep_module.CandidateTrack(
        track_id=uuid.uuid4(),
        title=f"Song {listen_count}",
        artist_name=str(artist_id),
        artist_id=artist_id,
        is_target_artist=is_target,
        listen_count=listen_count,
        in_library=True,
        popularity_score=0,
        source=types_module.TrackSource.LIBRARY,
    )


class TestPerArtistFairness:
    """Round-robin interleave spreads quota slots across artists in a pool.

    The fix replaces "sort-by-score-and-slice" with a round-robin fill: round 0
    takes each artist's best track, round 1 their second-best, etc. No artist
    gets a 2nd track until every artist has had a 1st (issue #115 / PR #121).
    """

    def test_anti_flooding_one_artist_cannot_dominate(self) -> None:
        # One artist with 15 high-scoring tracks; 5 others with enough tracks that
        # the quota can be filled fairly (so the constraint is fairness, not
        # scarcity). Pool = 15 + 5*8 = 55 tracks for a quota of 30.
        flooder = uuid.uuid4()
        others = [uuid.uuid4() for _ in range(5)]
        candidates: list[concert_prep_module.CandidateTrack] = [
            # Flooder's tracks all outscore everyone else's.
            _artist_candidate(artist_id=flooder, listen_count=90 + i, is_target=True)
            for i in range(15)
        ]
        for other in others:
            candidates += [
                _artist_candidate(artist_id=other, listen_count=10 + i, is_target=True)
                for i in range(8)
            ]

        max_tracks = 30
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={**_FAMILIARITY_DRIVEN, "similar_artist_ratio": 0},
            max_tracks=max_tracks,
            previous_track_ids=set(),
            freshness_target=None,
        )

        # Map each returned track back to its artist via title (carries listen_count
        # is not unique enough) -- instead rebuild an id->artist map.
        artist_by_track = {c.track_id: c.artist_id for c in candidates}
        per_artist: dict[uuid.UUID, int] = {}
        for t in result.tracks:
            aid = artist_by_track[t.track_id]
            per_artist[aid] = per_artist.get(aid, 0) + 1

        # The pool has 6 artists. Round-robin guarantees no artist exceeds
        # ceil(quota / num_artists) by more than the rounding remainder; with 6
        # artists and quota 30, the cap is ceil(30/6) = 5. The flooder should be
        # held to roughly that, not ~15. Compute the cap from the pool's distinct
        # artist count (NOT the result's) -- a flooding bug collapses the result
        # to one artist and would otherwise hide itself.
        num_artists_in_pool = len({c.artist_id for c in candidates})
        cap = math.ceil(max_tracks / num_artists_in_pool)
        assert per_artist[flooder] <= cap + 1, per_artist
        # And the quota is fully filled (enough tracks exist: 15 + 15 = 30).
        assert len(result.tracks) == max_tracks

    def test_few_artists_alternate_without_starvation(self) -> None:
        # Only 2 artists, quota of 10: must alternate and fill all 10.
        a1 = uuid.uuid4()
        a2 = uuid.uuid4()
        candidates: list[concert_prep_module.CandidateTrack] = []
        for i in range(10):
            candidates.append(
                _artist_candidate(artist_id=a1, listen_count=80 + i, is_target=True)
            )
            candidates.append(
                _artist_candidate(artist_id=a2, listen_count=20 + i, is_target=True)
            )

        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={**_FAMILIARITY_DRIVEN, "similar_artist_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )

        artist_by_track = {c.track_id: c.artist_id for c in candidates}
        per_artist: dict[uuid.UUID, int] = {}
        for t in result.tracks:
            aid = artist_by_track[t.track_id]
            per_artist[aid] = per_artist.get(aid, 0) + 1

        assert len(result.tracks) == 10
        # Two artists, quota 10 -> 5 each (round-robin, no starvation).
        assert per_artist[a1] == 5
        assert per_artist[a2] == 5

    def test_round_robin_in_adjacent_pool(self) -> None:
        # Fairness also applies to the adjacent pool. One adjacent flooder, several
        # adjacent others; ratio=100 routes the whole playlist through adjacent.
        flooder = uuid.uuid4()
        others = [uuid.uuid4() for _ in range(4)]
        candidates: list[concert_prep_module.CandidateTrack] = [
            _artist_candidate(artist_id=flooder, listen_count=90 + i, is_target=False)
            for i in range(15)
        ]
        for other in others:
            candidates += [
                _artist_candidate(artist_id=other, listen_count=10 + i, is_target=False)
                for i in range(3)
            ]

        max_tracks = 10
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={**_FAMILIARITY_DRIVEN, "similar_artist_ratio": 100},
            max_tracks=max_tracks,
            previous_track_ids=set(),
            freshness_target=None,
        )

        artist_by_track = {c.track_id: c.artist_id for c in candidates}
        per_artist: dict[uuid.UUID, int] = {}
        for t in result.tracks:
            aid = artist_by_track[t.track_id]
            per_artist[aid] = per_artist.get(aid, 0) + 1

        # Cap from the pool's distinct artist count, not the result's, so a
        # flooding regression cannot hide by collapsing the result to one artist.
        num_artists_in_pool = len({c.artist_id for c in candidates})
        cap = math.ceil(max_tracks / num_artists_in_pool)
        assert per_artist[flooder] <= cap + 1, per_artist
        assert len(result.tracks) == max_tracks

    def test_backfill_uses_round_robin_from_donor_pool(self) -> None:
        # Adjacent pool underflows its quota, so the target pool backfills. The
        # backfilled target tracks must come via round-robin, not a naive top-N
        # slice that would let the target flooder dominate the backfill.
        tgt_flooder = uuid.uuid4()
        tgt_others = [uuid.uuid4() for _ in range(4)]
        target: list[concert_prep_module.CandidateTrack] = [
            _artist_candidate(
                artist_id=tgt_flooder, listen_count=90 + i, is_target=True
            )
            for i in range(15)
        ]
        for other in tgt_others:
            target += [
                _artist_candidate(artist_id=other, listen_count=30 + i, is_target=True)
                for i in range(3)
            ]
        # Only 1 adjacent track -> adjacent quota of 5 underflows by 4.
        adjacent = [
            _artist_candidate(artist_id=uuid.uuid4(), listen_count=50, is_target=False)
        ]

        max_tracks = 10
        result = concert_prep_module.score_and_select(
            candidates=target + adjacent,
            params={**_FAMILIARITY_DRIVEN, "similar_artist_ratio": 50},
            max_tracks=max_tracks,
            previous_track_ids=set(),
            freshness_target=None,
        )

        assert len(result.tracks) == max_tracks
        artist_by_track = {c.track_id: c.artist_id for c in (target + adjacent)}
        # Count how many slots the target flooder grabbed across base + backfill.
        flooder_count = sum(
            1 for t in result.tracks if artist_by_track[t.track_id] == tgt_flooder
        )
        # tgt quota is 5, backfill adds 4 more from target = 9 target slots over
        # 5 target artists. Round-robin caps the flooder well below a top-N slice
        # (which would give the flooder all 9).
        assert flooder_count <= 3, flooder_count

    def test_deterministic_same_input_same_output(self) -> None:
        flooder = uuid.uuid4()
        others = [uuid.uuid4() for _ in range(3)]
        candidates: list[concert_prep_module.CandidateTrack] = [
            _artist_candidate(artist_id=flooder, listen_count=90 + i, is_target=True)
            for i in range(10)
        ]
        for other in others:
            candidates += [
                _artist_candidate(artist_id=other, listen_count=20 + i, is_target=True)
                for i in range(4)
            ]
        params = {**_FAMILIARITY_DRIVEN, "similar_artist_ratio": 0}
        run1 = concert_prep_module.score_and_select(
            candidates=candidates,
            params=params,
            max_tracks=15,
            previous_track_ids=set(),
            freshness_target=None,
        )
        run2 = concert_prep_module.score_and_select(
            candidates=candidates,
            params=params,
            max_tracks=15,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert [t.track_id for t in run1.tracks] == [t.track_id for t in run2.tracks]

    def test_strongest_artist_still_leads(self) -> None:
        # Round-robin orders artist groups by their best track's score, so the
        # top-scoring track overall still appears (first in score-desc output).
        strong = uuid.uuid4()
        weak = uuid.uuid4()
        candidates = [
            _artist_candidate(artist_id=strong, listen_count=99, is_target=True),
            _artist_candidate(artist_id=strong, listen_count=98, is_target=True),
            _artist_candidate(artist_id=weak, listen_count=5, is_target=True),
            _artist_candidate(artist_id=weak, listen_count=4, is_target=True),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={**_FAMILIARITY_DRIVEN, "similar_artist_ratio": 0},
            max_tracks=4,
            previous_track_ids=set(),
            freshness_target=None,
        )
        artist_by_track = {c.track_id: c.artist_id for c in candidates}
        # Final output is score-desc; the very first track is the strong artist's best.
        assert artist_by_track[result.tracks[0].track_id] == strong
