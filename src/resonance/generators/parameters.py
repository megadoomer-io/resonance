"""Generator parameter registry and type configurations."""

from __future__ import annotations

import dataclasses

import resonance.types as types_module


@dataclasses.dataclass(frozen=True)
class ParameterDefinition:
    """A named parameter with display and scale metadata."""

    name: str
    display_name: str
    description: str
    scale_type: types_module.ParameterScaleType
    default_value: int
    labels: tuple[str, str]


@dataclasses.dataclass(frozen=True)
class GeneratorTypeConfig:
    """Configuration for a generator type."""

    featured_parameters: frozenset[str]
    required_inputs: frozenset[str]
    description: str


PARAMETER_REGISTRY: dict[str, ParameterDefinition] = {
    "familiarity": ParameterDefinition(
        name="familiarity",
        display_name="Familiarity",
        description="Balance between tracks you know and new discovery",
        scale_type=types_module.ParameterScaleType.BIPOLAR,
        default_value=50,
        labels=("All Discovery", "All Known Tracks"),
    ),
    "hit_depth": ParameterDefinition(
        name="hit_depth",
        display_name="Hit Depth",
        description="Balance between deep cuts and popular tracks",
        scale_type=types_module.ParameterScaleType.BIPOLAR,
        default_value=50,
        labels=("Deep Cuts", "Big Hits"),
    ),
    "similar_artist_ratio": ParameterDefinition(
        name="similar_artist_ratio",
        display_name="Similar Artists",
        description="How much to include tracks from adjacent/similar artists",
        scale_type=types_module.ParameterScaleType.UNIPOLAR,
        default_value=0,
        labels=("Target Artists Only", "Heavy Adjacent Artists"),
    ),
}

GENERATOR_TYPE_CONFIG: dict[types_module.GeneratorType, GeneratorTypeConfig] = {
    types_module.GeneratorType.CONCERT_PREP: GeneratorTypeConfig(
        featured_parameters=frozenset({"familiarity", "hit_depth"}),
        required_inputs=frozenset({"event_id"}),
        description="Generate a playlist to prepare for a concert",
    ),
}


def apply_defaults(
    provided: dict[str, object],
) -> dict[str, int]:
    """Fill in missing parameter values with registry defaults."""
    result: dict[str, int] = {}
    for name, defn in PARAMETER_REGISTRY.items():
        raw = provided.get(name)
        if raw is not None:
            if not isinstance(raw, (int, float, str)):
                msg = f"Parameter {name} must be numeric, got {type(raw)}"
                raise TypeError(msg)
            result[name] = int(raw)
        else:
            result[name] = defn.default_value
    return result
