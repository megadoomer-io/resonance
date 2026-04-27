"""Tests for the playlist scoring engine."""

import resonance.generators.scoring as scoring_module


class TestFamiliaritySignal:
    def test_never_heard_returns_zero(self) -> None:
        result = scoring_module.familiarity_signal(listen_count=0, in_library=False)
        assert result == 0.0

    def test_high_listen_count(self) -> None:
        score = scoring_module.familiarity_signal(listen_count=100, in_library=True)
        assert score > 0.8

    def test_in_library_low_listens(self) -> None:
        score = scoring_module.familiarity_signal(listen_count=1, in_library=True)
        assert 0.0 < score < 0.5


class TestPopularitySignal:
    def test_zero_popularity(self) -> None:
        assert scoring_module.popularity_signal(popularity_score=0) == 0.0

    def test_max_popularity(self) -> None:
        assert scoring_module.popularity_signal(popularity_score=100) == 1.0

    def test_mid_popularity(self) -> None:
        score = scoring_module.popularity_signal(popularity_score=50)
        assert 0.4 <= score <= 0.6


class TestArtistRelevanceSignal:
    def test_target_artist(self) -> None:
        assert scoring_module.artist_relevance_signal(is_target_artist=True) == 1.0

    def test_adjacent_artist(self) -> None:
        assert scoring_module.artist_relevance_signal(is_target_artist=False) == 0.0


class TestBipolarWeight:
    def test_neutral_returns_zero(self) -> None:
        assert scoring_module.bipolar_weight(50) == 0.0

    def test_max_returns_positive(self) -> None:
        assert scoring_module.bipolar_weight(100) == 1.0

    def test_min_returns_negative(self) -> None:
        assert scoring_module.bipolar_weight(0) == -1.0

    def test_seventy_five(self) -> None:
        assert scoring_module.bipolar_weight(75) == 0.5


class TestCompositeScore:
    def test_neutral_params_score_from_relevance(self) -> None:
        score = scoring_module.composite_score(
            familiarity_val=0.5,
            popularity_val=0.5,
            is_target_artist=True,
            params={"familiarity": 50, "hit_depth": 50, "similar_artist_ratio": 0},
        )
        assert 0.0 <= score <= 1.0

    def test_high_familiarity_boosts_known_tracks(self) -> None:
        known = scoring_module.composite_score(
            familiarity_val=0.9,
            popularity_val=0.5,
            is_target_artist=True,
            params={"familiarity": 90, "hit_depth": 50, "similar_artist_ratio": 0},
        )
        unknown = scoring_module.composite_score(
            familiarity_val=0.1,
            popularity_val=0.5,
            is_target_artist=True,
            params={"familiarity": 90, "hit_depth": 50, "similar_artist_ratio": 0},
        )
        assert known > unknown
