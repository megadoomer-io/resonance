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
    """composite_score reflects familiarity + hit_depth only.

    Artist relevance (target vs adjacent) is NOT a score factor; it is applied
    as a blend quota at selection time (see concert_prep.score_and_select).
    """

    def test_neutral_params_midpoint(self) -> None:
        score = scoring_module.composite_score(
            familiarity_val=0.5,
            popularity_val=0.5,
            params={"familiarity": 50, "hit_depth": 50},
        )
        assert score == 0.5

    def test_high_familiarity_boosts_known_tracks(self) -> None:
        known = scoring_module.composite_score(
            familiarity_val=0.9,
            popularity_val=0.5,
            params={"familiarity": 90, "hit_depth": 50},
        )
        unknown = scoring_module.composite_score(
            familiarity_val=0.1,
            popularity_val=0.5,
            params={"familiarity": 90, "hit_depth": 50},
        )
        assert known > unknown

    def test_discovery_profile_boosts_unheard(self) -> None:
        # familiarity=0 (all discovery) => an unheard track outranks a heard one.
        unheard = scoring_module.composite_score(
            familiarity_val=0.0,
            popularity_val=0.5,
            params={"familiarity": 0, "hit_depth": 50},
        )
        heard = scoring_module.composite_score(
            familiarity_val=1.0,
            popularity_val=0.5,
            params={"familiarity": 0, "hit_depth": 50},
        )
        assert unheard > heard

    def test_missing_params_default_to_neutral(self) -> None:
        score = scoring_module.composite_score(
            familiarity_val=0.5,
            popularity_val=0.5,
            params={},
        )
        assert score == 0.5

    def test_clamped_to_unit_range(self) -> None:
        high = scoring_module.composite_score(
            familiarity_val=1.0,
            popularity_val=1.0,
            params={"familiarity": 100, "hit_depth": 100},
        )
        low = scoring_module.composite_score(
            familiarity_val=0.0,
            popularity_val=0.0,
            params={"familiarity": 100, "hit_depth": 100},
        )
        assert high == 1.0
        assert low == 0.0
