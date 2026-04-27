"""Tests for the generator parameter registry."""

import resonance.generators.parameters as params_module
import resonance.types as types_module


class TestParameterDefinition:
    def test_bipolar_parameter(self) -> None:
        param = params_module.PARAMETER_REGISTRY["hit_depth"]
        assert param.scale_type == types_module.ParameterScaleType.BIPOLAR
        assert param.default_value == 50
        assert param.labels == ("Deep Cuts", "Big Hits")

    def test_unipolar_parameter(self) -> None:
        param = params_module.PARAMETER_REGISTRY["similar_artist_ratio"]
        assert param.scale_type == types_module.ParameterScaleType.UNIPOLAR
        assert param.default_value == 0

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

    def test_concert_prep_required_inputs(self) -> None:
        gen_type = types_module.GeneratorType.CONCERT_PREP
        config = params_module.GENERATOR_TYPE_CONFIG[gen_type]
        assert "event_id" in config.required_inputs


class TestApplyDefaults:
    def test_fills_missing_with_defaults(self) -> None:
        result = params_module.apply_defaults({"hit_depth": 75})
        assert result["hit_depth"] == 75
        assert result["familiarity"] == 50
        assert result["similar_artist_ratio"] == 0

    def test_preserves_all_provided(self) -> None:
        provided = {"hit_depth": 25, "familiarity": 80, "similar_artist_ratio": 30}
        result = params_module.apply_defaults(provided)
        assert result == provided
