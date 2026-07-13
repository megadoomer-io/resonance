"""Generator parameter registry and type configurations."""

from __future__ import annotations

import dataclasses

import resonance.generators.pool as pool_module
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
    """Configuration / preset for a generator type.

    A generator type is a *preset* over the single generation engine, not a
    distinct engine (#128). It declares which parameters the UI features, the
    default parameter values and default pool seed a new profile of this type
    starts from, and a human description.

    ``required_inputs`` is retained for any type that still wants a specific key
    present, but pool sufficiency is now enforced structurally (a non-empty set of
    enabled sources) rather than by a hard-coded key like ``event_id`` -- see
    ``api.v1.generators.validate_profile_inputs``.
    """

    featured_parameters: frozenset[str]
    required_inputs: frozenset[str]
    description: str
    # Human-readable name for the type, shown in the new-playlist type selector
    # (#rediscovery-ui). Falls back to the title-cased enum key when empty.
    display_name: str = ""
    # Parameter values a new profile of this type starts from (overrides registry
    # defaults). Empty means "use the registry defaults".
    default_param_values: dict[str, int] = dataclasses.field(default_factory=dict)
    # The source kind a new profile of this type seeds its pool from (e.g. an
    # event for concert prep). None means no default seed -- the caller supplies
    # sources. Value is a ``pool.PoolSourceKind``.
    default_pool_seed: str | None = None
    # Editor slider ordering (#rediscovery-ui). ``lead_parameters`` render first,
    # ``advanced_parameters`` behind an "Advanced" disclosure. Both are ordered
    # (unlike the unordered ``featured_parameters`` set) and together should equal
    # ``featured_parameters``: only a type's featured dials render in the editor,
    # so inert dials (e.g. new_ratio for concert_prep) never show. Empty
    # ``lead_parameters`` falls back to the featured set in registry order.
    lead_parameters: tuple[str, ...] = ()
    advanced_parameters: tuple[str, ...] = ()

    def ordered_lead(self) -> tuple[str, ...]:
        """Lead slider names, defaulting to featured params in registry order."""
        if self.lead_parameters:
            return self.lead_parameters
        return tuple(n for n in PARAMETER_REGISTRY if n in self.featured_parameters)


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
    # NOTE: the old "similar_artist_ratio" parameter was removed in #133. Related
    # artists are now added to the pool explicitly via the enrich endpoint as
    # concrete artist sources, not folded in at generation time by a slider.
    "new_ratio": ParameterDefinition(
        name="new_ratio",
        display_name="New vs Deep Cuts",
        description=(
            "Balance between never-heard new artists and less-heard deep cuts "
            "from artists you already spin (#rediscovery)"
        ),
        # Unipolar: the value IS the fraction of the track budget given to the
        # new-artist stream (0 = all deep cuts, 100 = all new), not a -1..1 weight.
        scale_type=types_module.ParameterScaleType.UNIPOLAR,
        default_value=50,
        labels=("All Deep Cuts", "All New Artists"),
    ),
    "less_heard_percentile": ParameterDefinition(
        name="less_heard_percentile",
        display_name="Deep Cut Depth",
        description=(
            "How far down each seed artist's play distribution counts as a deep "
            "cut -- 33 = bottom third of that artist's own tracks (#rediscovery)"
        ),
        scale_type=types_module.ParameterScaleType.UNIPOLAR,
        default_value=33,
        labels=("Rarest Only", "Broader Catalog"),
    ),
}

GENERATOR_TYPE_CONFIG: dict[types_module.GeneratorType, GeneratorTypeConfig] = {
    types_module.GeneratorType.CONCERT_PREP: GeneratorTypeConfig(
        featured_parameters=frozenset({"familiarity", "hit_depth"}),
        display_name="Concert Prep",
        # Pool sufficiency is checked structurally now (#128); concert_prep no
        # longer hard-requires the legacy "event_id" key -- it seeds from an event
        # source by default but accepts any non-empty pool.
        required_inputs=frozenset(),
        description="Generate a playlist to prepare for a concert",
        default_pool_seed=pool_module.PoolSourceKind.EVENT.value,
        lead_parameters=("familiarity", "hit_depth"),
    ),
    types_module.GeneratorType.REDISCOVERY: GeneratorTypeConfig(
        featured_parameters=frozenset(
            {"familiarity", "hit_depth", "new_ratio", "less_heard_percentile"}
        ),
        display_name="Rediscovery",
        required_inputs=frozenset(),
        description=(
            "Rediscover a slice of your listening history: never-heard new artists "
            "on that period's genres, mixed with less-heard deep cuts from the "
            "artists you were spinning"
        ),
        # A new rediscovery profile seeds its pool from a listening-history window
        # (the reusable seed-window primitive), not an event.
        default_pool_seed=pool_module.PoolSourceKind.LISTENING_RANGE.value,
        # Balanced 50/50 new-vs-deep-cut by default (design premise 2). These match
        # the registry defaults; stated explicitly so the type's intended vibe is
        # documented at the config, not inferred from the registry.
        default_param_values={"new_ratio": 50, "less_heard_percentile": 33},
        # Lead with the two dials that define rediscovery (new-vs-deep-cut split +
        # deep-cut depth); familiarity/hit_depth tuck behind "Advanced" so four peer
        # sliders don't flatten the hierarchy (#rediscovery-ui design decision 2).
        lead_parameters=("new_ratio", "less_heard_percentile"),
        advanced_parameters=("familiarity", "hit_depth"),
    ),
}


def apply_defaults(
    provided: dict[str, object],
) -> dict[str, int]:
    """Fill in missing parameter values with registry defaults.

    Validates that all provided parameter names are recognized and
    that values are integers in the 0-100 range.

    Raises:
        ValueError: If a parameter name is unknown or value is out of range.
        TypeError: If a parameter value is not numeric.
    """
    unknown = set(provided) - set(PARAMETER_REGISTRY)
    if unknown:
        msg = f"Unknown parameter(s): {', '.join(sorted(unknown))}"
        raise ValueError(msg)

    result: dict[str, int] = {}
    for name, defn in PARAMETER_REGISTRY.items():
        raw = provided.get(name)
        if raw is not None:
            if not isinstance(raw, (int, float, str)):
                msg = f"Parameter {name} must be numeric, got {type(raw)}"
                raise TypeError(msg)
            value = int(raw)
            if value < 0 or value > 100:
                msg = f"Parameter {name} must be 0-100, got {value}"
                raise ValueError(msg)
            result[name] = value
        else:
            result[name] = defn.default_value
    return result
