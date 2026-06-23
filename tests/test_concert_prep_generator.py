"""Tests for the concert prep generator."""

from __future__ import annotations

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


# Parameters that make a track's score a strict monotonic function of
# listen_count: familiarity=100 gives familiarity full positive weight, hit_depth=50
# makes popularity neutral. So higher listen_count => strictly higher score, which
# lets the selection tests control ranking deterministically.
_FAMILIARITY_DRIVEN = {"familiarity": 100, "hit_depth": 50, "similar_artist_ratio": 0}


def _artist_candidate(
    *,
    artist_id: uuid.UUID,
    listen_count: int,
    is_target: bool = True,
    in_library: bool = True,
    popularity_score: int = 0,
) -> concert_prep_module.CandidateTrack:
    """Build a candidate whose score is driven by listen_count under the
    familiarity-driven params, grouped by ``artist_id``."""
    return concert_prep_module.CandidateTrack(
        track_id=uuid.uuid4(),
        title=f"Song {listen_count}",
        artist_name=str(artist_id),
        artist_id=artist_id,
        is_target_artist=is_target,
        listen_count=listen_count,
        in_library=in_library,
        popularity_score=popularity_score,
        source=(
            types_module.TrackSource.LIBRARY
            if in_library
            else types_module.TrackSource.DISCOVERY
        ),
    )


def _artist_count(
    result: concert_prep_module.SelectionResult,
    candidates: list[concert_prep_module.CandidateTrack],
) -> dict[uuid.UUID, int]:
    """Map selected tracks back to their artist and count per artist."""
    artist_by_track = {c.track_id: c.artist_id for c in candidates}
    per_artist: dict[uuid.UUID, int] = {}
    for t in result.tracks:
        aid = artist_by_track[t.track_id]
        per_artist[aid] = per_artist.get(aid, 0) + 1
    return per_artist


class TestOnePoolSelection:
    """One ranked pool: round-0 per-artist guarantee, then fill by pure score.

    Provenance (target vs adjacent) is metadata only and never affects selection.
    This replaces the #122 forced target/adjacent quota and full round-robin
    spread (issue #128 / #111).
    """

    def test_round_zero_guarantees_each_artist(self) -> None:
        # Six artists, each with several tracks; plenty of room. Every artist must
        # appear at least once -- never silently absent.
        artists = [uuid.uuid4() for _ in range(6)]
        candidates: list[concert_prep_module.CandidateTrack] = []
        for rank, aid in enumerate(artists):
            candidates += [
                _artist_candidate(artist_id=aid, listen_count=10 + rank * 10 + i)
                for i in range(5)
            ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params=_FAMILIARITY_DRIVEN,
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        per_artist = _artist_count(result, candidates)
        for aid in artists:
            assert per_artist.get(aid, 0) >= 1, per_artist

    def test_high_score_artist_gets_depth_in_fill(self) -> None:
        # A prolific, high-scoring artist SHOULD take many fill slots beyond its
        # round-0 token -- "heard artists get depth". This is the deliberate
        # reversal of the old anti-flooding round-robin.
        deep = uuid.uuid4()
        others = [uuid.uuid4() for _ in range(3)]
        candidates: list[concert_prep_module.CandidateTrack] = [
            _artist_candidate(artist_id=deep, listen_count=90 + i) for i in range(15)
        ]
        for other in others:
            candidates += [
                _artist_candidate(artist_id=other, listen_count=5 + i) for i in range(2)
            ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params=_FAMILIARITY_DRIVEN,
            max_tracks=20,
            previous_track_ids=set(),
            freshness_target=None,
        )
        per_artist = _artist_count(result, candidates)
        # Each of the 3 others is guaranteed exactly its catalog (1-2 each); the
        # deep artist takes the rest of the 20 slots. Round 0 = 4 tracks (one per
        # artist), fill 16 by score -> the deep artist's 14 remaining all outrank
        # the others, so it gets 1 + 14 = 15.
        assert per_artist[deep] == 15, per_artist
        for other in others:
            assert per_artist.get(other, 0) >= 1, per_artist

    def test_pool_larger_than_max_tracks_graceful_drop(self) -> None:
        # More distinct artists than slots: round 0 cannot seat everyone. The
        # highest-scoring artist groups keep their guaranteed slot; lowest drop.
        artists = [uuid.uuid4() for _ in range(20)]
        candidates = [
            _artist_candidate(artist_id=aid, listen_count=rank + 1)
            for rank, aid in enumerate(artists)
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params=_FAMILIARITY_DRIVEN,
            max_tracks=5,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert len(result.tracks) == 5
        per_artist = _artist_count(result, candidates)
        # The five highest-listen artists are the ones kept (graceful drop).
        kept = set(per_artist)
        top_five = set(artists[-5:])
        assert kept == top_five, per_artist

    def test_provenance_does_not_affect_selection(self) -> None:
        # is_target_artist must not influence selection. Two artists with equal
        # scores, one tagged target and one adjacent: both are seated by the
        # round-0 guarantee regardless of the tag.
        target = uuid.uuid4()
        adjacent = uuid.uuid4()
        candidates = [
            _artist_candidate(artist_id=target, listen_count=50, is_target=True),
            _artist_candidate(artist_id=adjacent, listen_count=50, is_target=False),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params=_FAMILIARITY_DRIVEN,
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        per_artist = _artist_count(result, candidates)
        assert per_artist.get(target, 0) == 1
        assert per_artist.get(adjacent, 0) == 1

    def test_deterministic_same_input_same_output(self) -> None:
        deep = uuid.uuid4()
        others = [uuid.uuid4() for _ in range(3)]
        candidates: list[concert_prep_module.CandidateTrack] = [
            _artist_candidate(artist_id=deep, listen_count=90 + i) for i in range(10)
        ]
        for other in others:
            candidates += [
                _artist_candidate(artist_id=other, listen_count=20 + i)
                for i in range(4)
            ]
        run1 = concert_prep_module.score_and_select(
            candidates=candidates,
            params=_FAMILIARITY_DRIVEN,
            max_tracks=15,
            previous_track_ids=set(),
            freshness_target=None,
        )
        run2 = concert_prep_module.score_and_select(
            candidates=candidates,
            params=_FAMILIARITY_DRIVEN,
            max_tracks=15,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert [t.track_id for t in run1.tracks] == [t.track_id for t in run2.tracks]

    def test_strongest_track_leads(self) -> None:
        # Final output is score-desc, so the very first track is the single
        # highest-scoring candidate overall.
        strong = uuid.uuid4()
        weak = uuid.uuid4()
        candidates = [
            _artist_candidate(artist_id=strong, listen_count=99),
            _artist_candidate(artist_id=strong, listen_count=98),
            _artist_candidate(artist_id=weak, listen_count=5),
            _artist_candidate(artist_id=weak, listen_count=4),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params=_FAMILIARITY_DRIVEN,
            max_tracks=4,
            previous_track_ids=set(),
            freshness_target=None,
        )
        artist_by_track = {c.track_id: c.artist_id for c in candidates}
        assert artist_by_track[result.tracks[0].track_id] == strong


class TestKnownVsDiscoveryDistribution:
    """Familiarity shifts depth between heard and unheard artists, but the
    round-0 guarantee keeps every artist present at both extremes (issue #128)."""

    def _mixed_pool(self) -> list[concert_prep_module.CandidateTrack]:
        heard = uuid.uuid4()
        unheard = uuid.uuid4()
        candidates = [
            _artist_candidate(artist_id=heard, listen_count=80 + i, in_library=True)
            for i in range(8)
        ]
        candidates += [
            _artist_candidate(
                artist_id=unheard,
                listen_count=0,
                in_library=False,
                popularity_score=50,
            )
            for _ in range(8)
        ]
        return candidates

    def test_mostly_known_keeps_unheard_present_but_light(self) -> None:
        candidates = self._mixed_pool()
        heard = candidates[0].artist_id
        unheard = candidates[-1].artist_id
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            # Favor known tracks: heard artist wins the fill, unheard stays at its
            # one guaranteed token.
            params={"familiarity": 100, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=8,
            previous_track_ids=set(),
            freshness_target=None,
        )
        per_artist = _artist_count(result, candidates)
        assert per_artist.get(unheard, 0) >= 1, per_artist
        assert per_artist[heard] > per_artist[unheard], per_artist

    def test_mostly_discovery_lets_unheard_fill(self) -> None:
        candidates = self._mixed_pool()
        heard = candidates[0].artist_id
        unheard = candidates[-1].artist_id
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            # Favor discovery: unheard tracks win the fill slots.
            params={"familiarity": 0, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=8,
            previous_track_ids=set(),
            freshness_target=None,
        )
        per_artist = _artist_count(result, candidates)
        assert per_artist.get(heard, 0) >= 1, per_artist
        assert per_artist[unheard] > per_artist[heard], per_artist


class TestHitDepthReorders:
    """#114: hit_depth re-ranks the pool by external popularity_score.

    A single artist (so the round-0 guarantee doesn't interfere) with tracks of
    varying popularity. With familiarity neutral, hit_depth alone drives order.
    """

    def _popularity_pool(self) -> list[concert_prep_module.CandidateTrack]:
        artist = uuid.uuid4()
        # Same listen_count so familiarity is constant; popularity varies.
        return [
            _artist_candidate(
                artist_id=artist,
                listen_count=20,
                popularity_score=pop,
            )
            for pop in (10, 40, 70, 95)
        ]

    def _ordered_popularity(
        self,
        result: concert_prep_module.SelectionResult,
        candidates: list[concert_prep_module.CandidateTrack],
    ) -> list[int]:
        pop_by_track = {c.track_id: c.popularity_score for c in candidates}
        return [pop_by_track[t.track_id] for t in result.tracks]

    def test_high_hit_depth_orders_by_popularity_desc(self) -> None:
        candidates = self._popularity_pool()
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 100, "similar_artist_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        pops = self._ordered_popularity(result, candidates)
        assert pops == sorted(pops, reverse=True), pops
        assert pops[0] == 95

    def test_low_hit_depth_prefers_deep_cuts(self) -> None:
        candidates = self._popularity_pool()
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 0, "similar_artist_ratio": 0},
            max_tracks=10,
            previous_track_ids=set(),
            freshness_target=None,
        )
        pops = self._ordered_popularity(result, candidates)
        assert pops == sorted(pops), pops
        assert pops[0] == 10


class TestEventProfileRegression:
    """CRITICAL (T3): existing event-style profiles still generate sensibly under
    the one-pool model. An event resolves a headliner + openers (all target) into
    the pool; selection must keep them all present and fill by score."""

    def test_event_lineup_all_artists_present_and_filled(self) -> None:
        headliner = uuid.uuid4()
        openers = [uuid.uuid4() for _ in range(3)]
        candidates: list[concert_prep_module.CandidateTrack] = [
            _artist_candidate(artist_id=headliner, listen_count=70 + i)
            for i in range(10)
        ]
        for opener in openers:
            candidates += [
                _artist_candidate(artist_id=opener, listen_count=20 + i)
                for i in range(6)
            ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=20,
            previous_track_ids=set(),
            freshness_target=None,
        )
        per_artist = _artist_count(result, candidates)
        # Every act on the bill appears.
        assert per_artist.get(headliner, 0) >= 1, per_artist
        for opener in openers:
            assert per_artist.get(opener, 0) >= 1, per_artist
        # Playlist is filled to the requested size and stays score-desc.
        assert len(result.tracks) == 20
        scores = [t.score for t in result.tracks]
        assert scores == sorted(scores, reverse=True)
