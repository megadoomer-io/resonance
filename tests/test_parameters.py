"""Tests for the generator parameter registry."""

import resonance.generators.parameters as params_module
import resonance.types as types_module


class TestParameterDefinition:
    def test_bipolar_parameter(self) -> None:
        param = params_module.PARAMETER_REGISTRY["hit_depth"]
        assert param.scale_type == types_module.ParameterScaleType.BIPOLAR
        assert param.default_value == 50
        assert param.labels == ("Deep Cuts", "Big Hits")

    def test_similar_artist_ratio_removed(self) -> None:
        # Removed in #133: related artists are added explicitly via enrichment,
        # not folded in at generation time by a slider.
        assert "similar_artist_ratio" not in params_module.PARAMETER_REGISTRY

    def test_familiarity_parameter(self) -> None:
        param = params_module.PARAMETER_REGISTRY["familiarity"]
        assert param.scale_type == types_module.ParameterScaleType.BIPOLAR
        assert param.default_value == 50
        assert param.labels == ("All Discovery", "All Known Tracks")


class TestGeneratorTypeConfig:
    def test_concert_prep_featured_params(self) -> None:
        gen_type = types_module.GeneratorType.CONCERT_PREP
        config = params_module.GENERATOR_TYPE_CONFIG[gen_type]
        assert "familiarity" in config.featured_parameters
        assert "hit_depth" in config.featured_parameters

    def test_concert_prep_no_legacy_required_key(self) -> None:
        # Pool sufficiency is structural now (#128): concert_prep no longer
        # hard-requires the legacy "event_id" key.
        gen_type = types_module.GeneratorType.CONCERT_PREP
        config = params_module.GENERATOR_TYPE_CONFIG[gen_type]
        assert config.required_inputs == frozenset()

    def test_concert_prep_seeds_from_event(self) -> None:
        gen_type = types_module.GeneratorType.CONCERT_PREP
        config = params_module.GENERATOR_TYPE_CONFIG[gen_type]
        assert config.default_pool_seed == "event"


class TestApplyDefaults:
    def test_fills_missing_with_defaults(self) -> None:
        result = params_module.apply_defaults({"hit_depth": 75})
        assert result["hit_depth"] == 75
        assert result["familiarity"] == 50

    def test_preserves_all_provided(self) -> None:
        # apply_defaults returns every registry parameter, so provided values are
        # preserved (a superset that also carries the rediscovery dials' defaults).
        provided = {"hit_depth": 25, "familiarity": 80}
        result = params_module.apply_defaults(provided)
        assert result.items() >= provided.items()
        assert result["new_ratio"] == 50
        assert result["less_heard_percentile"] == 33

    def test_rejects_removed_similar_artist_ratio(self) -> None:
        # The dead parameter must be rejected, not silently accepted (#133).
        import pytest

        with pytest.raises(ValueError, match="Unknown parameter"):
            params_module.apply_defaults({"similar_artist_ratio": 30})
