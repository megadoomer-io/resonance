"""Tests for the listening_range pool source + seed-window primitive (#rediscovery).

Covers the pure half of the seed-window resolver: parsing/serializing the
listening_range source and its window, the ``resolve_window_bounds`` primitive
(relative rolls forward, absolute stays frozen), and the ``find_listening_range_source``
accessor. The DB seed query lives in the worker and is covered there.
"""

from __future__ import annotations

import datetime

import pytest

import resonance.generators.parameters as params_module
import resonance.generators.pool as pool_module
import resonance.types as types_module


class TestParseListeningRangeSource:
    def test_relative_window_parses_with_defaults(self) -> None:
        sources = pool_module.normalize_sources(
            {
                "sources": [
                    {
                        "kind": "listening_range",
                        "window": {"kind": "relative", "lookback_days": 14},
                    }
                ]
            }
        )
        assert sources == [
            pool_module.ListeningRangeSource(
                window=pool_module.ListeningWindow(kind="relative", lookback_days=14),
                seed_artist_count=20,
                deep_cut_basis="lifetime",
                novelty_basis="lifetime",
                enabled=True,
            )
        ]

    def test_absolute_window_parses(self) -> None:
        raw = {
            "sources": [
                {
                    "kind": "listening_range",
                    "enabled": True,
                    "window": {
                        "kind": "absolute",
                        "start": "2025-07-01T00:00:00+00:00",
                        "end": "2025-07-15T00:00:00+00:00",
                    },
                    "seed_artist_count": 10,
                    "deep_cut_basis": "window",
                    "novelty_basis": "lifetime",
                }
            ]
        }
        (source,) = pool_module.normalize_sources(raw)
        assert isinstance(source, pool_module.ListeningRangeSource)
        assert source.window.kind == "absolute"
        assert source.window.start == datetime.datetime(2025, 7, 1, tzinfo=datetime.UTC)
        assert source.window.end == datetime.datetime(2025, 7, 15, tzinfo=datetime.UTC)
        assert source.seed_artist_count == 10
        assert source.deep_cut_basis == "window"
        assert source.novelty_basis == "lifetime"

    def test_unknown_window_kind_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unknown window kind"):
            pool_module.normalize_sources(
                {
                    "sources": [
                        {"kind": "listening_range", "window": {"kind": "forever"}}
                    ]
                }
            )

    def test_relative_requires_positive_lookback(self) -> None:
        with pytest.raises(ValueError, match="lookback_days must be a positive"):
            pool_module.normalize_sources(
                {
                    "sources": [
                        {
                            "kind": "listening_range",
                            "window": {"kind": "relative", "lookback_days": 0},
                        }
                    ]
                }
            )

    def test_absolute_end_must_be_after_start(self) -> None:
        with pytest.raises(ValueError, match=r"window\.end must be after"):
            pool_module.normalize_sources(
                {
                    "sources": [
                        {
                            "kind": "listening_range",
                            "window": {
                                "kind": "absolute",
                                "start": "2025-07-15T00:00:00+00:00",
                                "end": "2025-07-01T00:00:00+00:00",
                            },
                        }
                    ]
                }
            )

    def test_bad_basis_rejected(self) -> None:
        with pytest.raises(ValueError, match="deep_cut_basis must be"):
            pool_module.normalize_sources(
                {
                    "sources": [
                        {
                            "kind": "listening_range",
                            "window": {"kind": "relative", "lookback_days": 14},
                            "deep_cut_basis": "yesterday",
                        }
                    ]
                }
            )

    def test_seed_artist_count_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="seed_artist_count must be a positive"):
            pool_module.normalize_sources(
                {
                    "sources": [
                        {
                            "kind": "listening_range",
                            "window": {"kind": "relative", "lookback_days": 14},
                            "seed_artist_count": -1,
                        }
                    ]
                }
            )


class TestSerializeRoundTrip:
    def test_relative_round_trips(self) -> None:
        source = pool_module.ListeningRangeSource(
            window=pool_module.ListeningWindow(kind="relative", lookback_days=30),
            seed_artist_count=15,
            deep_cut_basis="lifetime",
            novelty_basis="lifetime",
        )
        raw = pool_module.serialize_source(source)
        assert pool_module.normalize_sources({"sources": [raw]}) == [source]

    def test_absolute_round_trips(self) -> None:
        source = pool_module.ListeningRangeSource(
            window=pool_module.ListeningWindow(
                kind="absolute",
                start=datetime.datetime(2024, 7, 10, tzinfo=datetime.UTC),
                end=datetime.datetime(2024, 7, 24, tzinfo=datetime.UTC),
            ),
        )
        raw = pool_module.serialize_source(source)
        assert pool_module.normalize_sources({"sources": [raw]}) == [source]

    def test_survives_enrich_replace_round_trip(self) -> None:
        # replace_via_seed_sources re-serializes every non-via_seed source; the
        # listening_range source (window + basis flags) must survive so enrich
        # (create -> enrich -> generate) doesn't drop the recipe.
        lr = pool_module.serialize_source(
            pool_module.ListeningRangeSource(
                window=pool_module.ListeningWindow(kind="relative", lookback_days=14),
            )
        )
        refs = {"sources": [lr], "exclude_artist_ids": []}
        import uuid

        discovered = uuid.uuid4()
        merged = pool_module.replace_via_seed_sources(refs, "lineup", [discovered])
        source = pool_module.find_listening_range_source(merged)
        assert source is not None
        assert source.window == pool_module.ListeningWindow(
            kind="relative", lookback_days=14
        )
        # the discovered artist landed as a via_seed source alongside it
        assert pool_module.scope_artist_ids(merged, "lineup") == [discovered]


class TestResolveWindowBounds:
    def test_relative_rolls_from_now(self) -> None:
        now = datetime.datetime(2026, 7, 10, 12, 0, tzinfo=datetime.UTC)
        window = pool_module.ListeningWindow(kind="relative", lookback_days=14)
        start, end = pool_module.resolve_window_bounds(window, now)
        assert end == now
        assert start == now - datetime.timedelta(days=14)

    def test_absolute_is_frozen(self) -> None:
        now = datetime.datetime(2026, 7, 10, tzinfo=datetime.UTC)
        window = pool_module.ListeningWindow(
            kind="absolute",
            start=datetime.datetime(2025, 7, 1, tzinfo=datetime.UTC),
            end=datetime.datetime(2025, 7, 15, tzinfo=datetime.UTC),
        )
        start, end = pool_module.resolve_window_bounds(window, now)
        assert start == datetime.datetime(2025, 7, 1, tzinfo=datetime.UTC)
        assert end == datetime.datetime(2025, 7, 15, tzinfo=datetime.UTC)


class TestFindListeningRangeSource:
    def test_finds_enabled_source(self) -> None:
        refs = {
            "sources": [
                {
                    "kind": "listening_range",
                    "window": {"kind": "relative", "lookback_days": 14},
                }
            ]
        }
        source = pool_module.find_listening_range_source(refs)
        assert source is not None
        assert source.window.lookback_days == 14

    def test_skips_disabled_source(self) -> None:
        refs = {
            "sources": [
                {
                    "kind": "listening_range",
                    "enabled": False,
                    "window": {"kind": "relative", "lookback_days": 14},
                }
            ]
        }
        assert pool_module.find_listening_range_source(refs) is None

    def test_none_when_absent(self) -> None:
        assert pool_module.find_listening_range_source({"sources": []}) is None


class TestRediscoveryParameters:
    def test_type_registered_with_listening_range_seed(self) -> None:
        config = params_module.GENERATOR_TYPE_CONFIG[
            types_module.GeneratorType.REDISCOVERY
        ]
        assert (
            config.default_pool_seed == pool_module.PoolSourceKind.LISTENING_RANGE.value
        )
        assert "new_ratio" in config.featured_parameters
        assert "less_heard_percentile" in config.featured_parameters

    def test_dials_registered_with_balanced_defaults(self) -> None:
        assert params_module.PARAMETER_REGISTRY["new_ratio"].default_value == 50
        assert (
            params_module.PARAMETER_REGISTRY["less_heard_percentile"].default_value
            == 33
        )

    def test_apply_defaults_fills_new_dials(self) -> None:
        applied = params_module.apply_defaults({})
        assert applied["new_ratio"] == 50
        assert applied["less_heard_percentile"] == 33

    def test_dials_are_bounded_0_100(self) -> None:
        with pytest.raises(ValueError, match="must be 0-100"):
            params_module.apply_defaults({"new_ratio": 150})
