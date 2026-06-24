"""Regression tests for the strip-similar_artist_ratio data migration (#133 T1).

The migration removes the dead ``similar_artist_ratio`` parameter from existing
profiles' ``parameter_values`` so they keep loading after the registry removal
(``apply_defaults`` raises on unknown parameter names). These tests exercise the
pure transform helper, and the cross-check that a stripped profile still passes
``apply_defaults`` -- the deploy-safety property the migration exists to protect.
"""

from __future__ import annotations

import importlib.util
import pathlib

import resonance.generators.parameters as parameters_module

_MIGRATION_PATH = (
    pathlib.Path(__file__).parent.parent
    / "alembic"
    / "versions"
    / "o6p7q8r9s0t1_strip_similar_artist_ratio.py"
)


def _load_migration() -> object:
    spec = importlib.util.spec_from_file_location("_mig_strip_ratio", _MIGRATION_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_mig = _load_migration()


class TestStripSimilarRatio:
    def test_strips_the_key(self) -> None:
        result = _mig._strip_similar_ratio(
            {"familiarity": 50, "hit_depth": 75, "similar_artist_ratio": 40}
        )
        assert result == {"familiarity": 50, "hit_depth": 75}

    def test_absent_key_returns_none(self) -> None:
        # None => "no change needed"; the migration skips the row.
        assert _mig._strip_similar_ratio({"familiarity": 50}) is None

    def test_empty_dict_returns_none(self) -> None:
        assert _mig._strip_similar_ratio({}) is None

    def test_non_dict_returns_none(self) -> None:
        assert _mig._strip_similar_ratio(None) is None
        assert _mig._strip_similar_ratio("nope") is None
        assert _mig._strip_similar_ratio([1, 2, 3]) is None

    def test_only_key_strips_to_empty(self) -> None:
        assert _mig._strip_similar_ratio({"similar_artist_ratio": 0}) == {}

    def test_does_not_mutate_input(self) -> None:
        original = {"hit_depth": 10, "similar_artist_ratio": 5}
        _mig._strip_similar_ratio(original)
        assert original == {"hit_depth": 10, "similar_artist_ratio": 5}


class TestStrippedProfileStillValidates:
    """The deploy-safety property: stripping unblocks apply_defaults.

    After T6 removes similar_artist_ratio from the registry, apply_defaults
    raises on any profile that still carries the key. A stripped profile must
    pass cleanly. (Today the registry still has the param, so we assert the
    stripped dict contains no unknown keys against the registry.)
    """

    def test_stripped_values_have_no_unknown_keys(self) -> None:
        stripped = _mig._strip_similar_ratio(
            {"familiarity": 30, "similar_artist_ratio": 90}
        )
        assert stripped is not None
        unknown = set(stripped) - set(parameters_module.PARAMETER_REGISTRY)
        assert unknown == set()
        # apply_defaults accepts the stripped set without raising.
        parameters_module.apply_defaults(stripped)
