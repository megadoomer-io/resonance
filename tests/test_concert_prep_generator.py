"""Tests for the concert prep generator."""

from __future__ import annotations

import uuid

import resonance.generators.concert_prep as concert_prep_module


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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
                source="discovery",
            ),
        ]
        result = concert_prep_module.score_and_select(
            candidates=candidates,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
            max_tracks=30,
            previous_track_ids=set(),
            freshness_target=None,
        )
        assert result.sources_summary["library"] == 1
        assert result.sources_summary["discovery"] == 1

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
                source="library",
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
                source="library",
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
                source="library",
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
                source="library",
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
