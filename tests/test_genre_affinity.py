"""Tests for the genre-affinity primitive (#136 genre model, Arc 1 phase 3)."""

from __future__ import annotations

import math

import pytest

import resonance.generators.genre as genre

_APPROX = {"abs": 1e-9}


class TestBuildVector:
    def test_drops_non_genre_tags(self) -> None:
        # Only genre_mbid-bearing tags participate; folksonomy tags are noise.
        vec = genre.build_vector([("g-rock", 5), (None, 99), ("g-jazz", 2), (None, 1)])
        assert vec == {"g-rock": 5.0, "g-jazz": 2.0}

    def test_sums_repeated_genres(self) -> None:
        assert genre.build_vector([("g", 3), ("g", 4)]) == {"g": 7.0}

    def test_floors_negative_counts(self) -> None:
        assert genre.build_vector([("g", -5)]) == {"g": 0.0}

    def test_drops_non_finite_counts(self) -> None:
        # inf/nan must not survive into a downstream cosine (NaN poisons sort).
        assert genre.build_vector([("g", float("inf"))]) == {}
        assert genre.build_vector([("g", float("nan"))]) == {}

    def test_empty(self) -> None:
        assert genre.build_vector([]) == {}


class TestCosine:
    def test_identical_vectors_is_one(self) -> None:
        v = {"g-rock": 3.0, "g-jazz": 1.0}
        assert genre.cosine(v, v) == pytest.approx(1.0, **_APPROX)

    def test_disjoint_genres_is_zero(self) -> None:
        assert genre.cosine({"g-rock": 5.0}, {"g-jazz": 5.0}) == 0.0

    def test_empty_is_zero(self) -> None:
        assert genre.cosine({}, {"g": 1.0}) == 0.0
        assert genre.cosine({"g": 1.0}, {}) == 0.0

    def test_partial_overlap(self) -> None:
        # a=(1,0), b=(1,1) -> cos = 1/sqrt(2)
        a = {"metal": 1.0}
        b = {"metal": 1.0, "rock": 1.0}
        assert genre.cosine(a, b) == pytest.approx(1.0 / math.sqrt(2), **_APPROX)

    def test_never_exceeds_one(self) -> None:
        a = {"a": 3.0, "b": 4.0, "c": 5.0}
        assert genre.cosine(a, a) <= 1.0


class TestSparseSingleTag:
    """The deliberate binary low-end behavior, pinned by the design."""

    def test_same_single_genre_is_one(self) -> None:
        assert genre.cosine({"metal": 1.0}, {"metal": 1.0}) == pytest.approx(
            1.0, **_APPROX
        )

    def test_different_single_genre_is_zero(self) -> None:
        assert genre.cosine({"metal": 1.0}, {"jazz": 1.0}) == 0.0


class TestAffinityScore:
    def test_metal_candidate_beats_electronic_for_metal_seeds(self) -> None:
        # The #136 case: seeds are metal bands; the metal candidate outranks the
        # electronic one, so disambiguation prefers the right artist.
        seeds = [
            [("g-metal", 8), ("g-thrash", 3), (None, 20)],
            [("g-metal", 5), ("g-death", 2)],
        ]
        metal = [("g-metal", 9), ("g-thrash", 1)]
        electronic = [("g-house", 7), ("g-techno", 4)]
        m = genre.affinity_score(metal, seeds)
        e = genre.affinity_score(electronic, seeds)
        assert m is not None and e is not None
        assert m > e

    def test_seed_majority_beats_one_heavy_off_genre_seed(self) -> None:
        # Three metal seeds (small counts) + one pop seed with a huge count.
        # Per-seed L2 normalization must keep the metal *consensus* winning over
        # the single loud pop seed -- the finding-#1 regression guard.
        seeds = [
            [("metal", 5)],
            [("metal", 5)],
            [("metal", 5)],
            [("pop", 1000)],
        ]
        metal = genre.affinity_score([("metal", 5)], seeds)
        pop = genre.affinity_score([("pop", 5)], seeds)
        assert metal is not None and pop is not None
        assert metal > pop

    def test_no_candidate_genre_is_none_not_zero(self) -> None:
        # "unknown genre" (None) must stay distinct from "wrong genre" (0.0) so a
        # ranker never ties an untagged true match with a confirmed mismatch.
        seeds = [[("g-metal", 5)]]
        assert genre.affinity_score([], seeds) is None
        assert genre.affinity_score([(None, 3)], seeds) is None

    def test_wrong_genre_is_zero_not_none(self) -> None:
        # Both sides have genre data but share nothing -> a real 0.0 mismatch.
        assert genre.affinity_score([("g-house", 5)], [[("g-metal", 5)]]) == 0.0

    def test_empty_seed_set_is_none(self) -> None:
        assert genre.affinity_score([("g-metal", 5)], []) is None

    def test_bounded_zero_to_one(self) -> None:
        seeds = [[("g-metal", 8)], [("g-metal", 3), ("g-rock", 2)]]
        score = genre.affinity_score([("g-metal", 5), ("g-rock", 1)], seeds)
        assert score is not None
        assert 0.0 <= score <= 1.0


class TestL2Normalize:
    def test_unit_norm(self) -> None:
        out = genre.l2_normalize({"a": 3.0, "b": 4.0})
        assert math.isclose(math.sqrt(sum(w * w for w in out.values())), 1.0)

    def test_zero_norm_is_empty(self) -> None:
        assert genre.l2_normalize({}) == {}
        assert genre.l2_normalize({"a": 0.0}) == {}
