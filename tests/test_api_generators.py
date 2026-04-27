"""Tests for generator profile API endpoints."""

from __future__ import annotations

import uuid

import pytest

import resonance.api.v1.generators as generators_module
import resonance.types as types_module


class TestCreateProfileRequest:
    """Tests for CreateProfileRequest Pydantic model."""

    def test_valid_concert_prep(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
            "parameter_values": {"hit_depth": 75},
        }
        request = generators_module.CreateProfileRequest(**body)
        assert request.generator_type == types_module.GeneratorType.CONCERT_PREP
        assert request.name == "Show Prep"
        assert request.parameter_values == {"hit_depth": 75}

    def test_default_parameter_values(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
        }
        request = generators_module.CreateProfileRequest(**body)
        assert request.parameter_values == {}

    def test_missing_required_input_raises(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="event_id"):
            generators_module.validate_profile_inputs(request)

    def test_valid_inputs_pass_validation(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        # Should not raise
        generators_module.validate_profile_inputs(request)

    def test_unknown_generator_type_passes_validation(self) -> None:
        """Generator types not in GENERATOR_TYPE_CONFIG skip validation."""
        body = {
            "name": "Deep Dive",
            "generator_type": "artist_deep_dive",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        # Should not raise — no config means no required inputs
        generators_module.validate_profile_inputs(request)


class TestUpdateProfileRequest:
    """Tests for UpdateProfileRequest Pydantic model."""

    def test_partial_update(self) -> None:
        body = {"parameter_values": {"hit_depth": 25}}
        request = generators_module.UpdateProfileRequest(**body)
        assert request.name is None
        assert request.parameter_values == {"hit_depth": 25}
        assert request.input_references is None

    def test_name_only_update(self) -> None:
        body = {"name": "New Name"}
        request = generators_module.UpdateProfileRequest(**body)
        assert request.name == "New Name"
        assert request.parameter_values is None

    def test_empty_update(self) -> None:
        request = generators_module.UpdateProfileRequest()
        assert request.name is None
        assert request.parameter_values is None
        assert request.input_references is None


class TestGenerateRequest:
    """Tests for GenerateRequest Pydantic model."""

    def test_freshness_target(self) -> None:
        body = {"freshness_target": 50}
        request = generators_module.GenerateRequest(**body)
        assert request.freshness_target == 50

    def test_default_no_freshness(self) -> None:
        request = generators_module.GenerateRequest()
        assert request.freshness_target is None

    def test_default_max_tracks(self) -> None:
        request = generators_module.GenerateRequest()
        assert request.max_tracks == 30

    def test_custom_max_tracks(self) -> None:
        body = {"max_tracks": 50, "freshness_target": 75}
        request = generators_module.GenerateRequest(**body)
        assert request.max_tracks == 50
        assert request.freshness_target == 75


class TestValidateProfileInputs:
    """Tests for validate_profile_inputs function."""

    def test_multiple_missing_inputs_reports_first(self) -> None:
        """When multiple required inputs are missing, reports one."""
        body = {
            "name": "Test",
            "generator_type": "concert_prep",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="Missing required input"):
            generators_module.validate_profile_inputs(request)

    def test_extra_inputs_are_allowed(self) -> None:
        """Extra input references beyond what's required are fine."""
        body = {
            "name": "Test",
            "generator_type": "concert_prep",
            "input_references": {
                "event_id": str(uuid.uuid4()),
                "extra_ref": "some-value",
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        # Should not raise
        generators_module.validate_profile_inputs(request)
