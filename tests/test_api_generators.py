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

    def test_empty_pool_raises(self) -> None:
        # No sources at all -> structurally empty pool -> clear error (#128).
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )

    def test_valid_legacy_inputs_pass_validation(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {"event_id": str(uuid.uuid4())},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        # Legacy single-event shape resolves to one enabled source -> passes.
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )

    def test_valid_layered_sources_pass_validation(self) -> None:
        body = {
            "name": "Show Prep",
            "generator_type": "concert_prep",
            "input_references": {
                "sources": [
                    {"kind": "event", "event_id": str(uuid.uuid4()), "enabled": True}
                ]
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )

    def test_unknown_generator_type_still_requires_pool(self) -> None:
        """No type config skips the legacy key-check but still needs a pool."""
        valid = {
            "name": "Deep Dive",
            "generator_type": "artist_deep_dive",
            "input_references": {
                "sources": [
                    {"kind": "artist", "artist_id": str(uuid.uuid4()), "enabled": True}
                ]
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**valid)
        # A valid source passes even with no type config.
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )
        # But an empty pool still raises, config or not.
        empty = generators_module.CreateProfileRequest(
            name="Deep Dive",
            generator_type="artist_deep_dive",
            input_references={},
        )
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                empty.input_references, empty.generator_type
            )

    def test_malformed_source_raises(self) -> None:
        body = {
            "name": "Bad",
            "generator_type": "concert_prep",
            "input_references": {"sources": [{"kind": "nonsense"}]},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="Invalid input_references"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )


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

    def test_empty_input_references_raise_empty_pool(self) -> None:
        """An input spec with no sources reports a clear empty-pool error."""
        body = {
            "name": "Test",
            "generator_type": "concert_prep",
            "input_references": {},
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )

    def test_disabled_only_sources_raise_empty_pool(self) -> None:
        """All-disabled sources resolve to an empty pool."""
        body = {
            "name": "Test",
            "generator_type": "concert_prep",
            "input_references": {
                "sources": [
                    {"kind": "event", "event_id": str(uuid.uuid4()), "enabled": False}
                ]
            },
            "parameter_values": {},
        }
        request = generators_module.CreateProfileRequest(**body)
        with pytest.raises(ValueError, match="empty pool"):
            generators_module.validate_profile_inputs(
                request.input_references, request.generator_type
            )

    def test_extra_legacy_keys_are_allowed(self) -> None:
        """Extra keys alongside a legacy event_id are ignored, not rejected."""
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
        generators_module.validate_profile_inputs(
            request.input_references, request.generator_type
        )
