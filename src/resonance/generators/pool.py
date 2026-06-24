"""Pool source spec: pure parsing, validation, and assembly of generation inputs.

A generator profile's ``input_references`` is a layered list of *sources* that each
resolve to artist IDs at generation time, plus a global ``exclude_artist_ids`` set
applied last to the union (issue #128). The stored shape is::

    {
      "sources": [
        {"kind": "event",   "event_id": "<uuid>", "enabled": true},
        {"kind": "artist",  "artist_id": "<uuid>", "enabled": true},
        {"kind": "artist",  "artist_id": "<uuid>", "enabled": true,
         "via_seed": "lineup"},
        {"kind": "related", "seed": "target", "amount": 5, "enabled": true}
      ],
      "exclude_artist_ids": ["<uuid>"]
    }

This module is **pure** -- no database, no connectors. It does two jobs:

1. Parse the stored/client shape into typed sources (:func:`normalize_sources`),
   tolerating the legacy single-event shape ``{"event_id": "<uuid>"}`` so a profile
   written before #128 still resolves during the migration window.
2. Assemble a deduplicated, exclude-filtered pool from already-resolved
   ``(artist_id, provenance)`` pairs (:func:`build_pool`).

The DB/connector resolution step (event -> artists, related -> similar artists)
lives in the worker's ``resolve_pool`` helper, which calls into this module for the
pure parts.
"""

from __future__ import annotations

import dataclasses
import enum
import uuid
from collections.abc import Mapping, Sequence


class PoolSourceKind(enum.StrEnum):
    """The kind of a pool source (also used as resolved-artist provenance)."""

    EVENT = "event"
    ARTIST = "artist"
    RELATED = "related"


# Provenance shares the source vocabulary: an artist enters the pool *via* the kind
# of source that produced it (event lineup, manual artist add, or related expansion).
PoolProvenance = PoolSourceKind

# The only ``seed`` selector today: expand related artists from the non-related
# ("target") artists already resolved from event + artist sources.
SEED_TARGET = "target"


@dataclasses.dataclass(frozen=True)
class EventSource:
    """Resolve an event's lineup (confirmed + accepted candidates) to artists."""

    event_id: uuid.UUID
    enabled: bool = True


@dataclasses.dataclass(frozen=True)
class ArtistSource:
    """Add a single artist directly to the pool.

    ``via_seed`` records enrichment provenance (#133): ``None`` for a manual or
    event-origin artist the user added, ``"<artist_id>"`` for one discovered by
    "find similar" from that seed artist, and ``"lineup"`` for one discovered by
    the global "add N related from the whole lineup" sweep. It drives
    replace-by-scope (re-running a scope's enrich drops only that scope's prior
    discovered sources) and lets the builder group discovered artists under the
    seed that produced them.
    """

    artist_id: uuid.UUID
    enabled: bool = True
    via_seed: str | None = None


@dataclasses.dataclass(frozen=True)
class RelatedSource:
    """Fold in artists similar to the already-resolved seed artists.

    ``amount`` caps how many related artists are folded in. ``seed`` selects which
    already-resolved artists to expand from; only ``"target"`` (the non-related
    artists) is supported today.
    """

    amount: int
    seed: str = SEED_TARGET
    enabled: bool = True


PoolSource = EventSource | ArtistSource | RelatedSource


@dataclasses.dataclass(frozen=True)
class ResolvedArtist:
    """An artist resolved into the pool, tagged with how it got there."""

    artist_id: uuid.UUID
    via: PoolProvenance


def _parse_uuid(value: object, field: str) -> uuid.UUID:
    """Parse a JSON-stored UUID string, raising a clear error on bad input."""
    try:
        return uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError) as exc:
        msg = f"Invalid UUID for {field}: {value!r}"
        raise ValueError(msg) from exc


def _parse_enabled(raw: Mapping[str, object]) -> bool:
    """Read the optional ``enabled`` flag, defaulting to True."""
    value = raw.get("enabled", True)
    if not isinstance(value, bool):
        msg = f"Source 'enabled' must be a boolean, got {type(value).__name__}"
        raise ValueError(msg)
    return value


def _parse_via_seed(value: object) -> str | None:
    """Parse the optional ``via_seed`` provenance tag on an artist source.

    Absent or null means a user-added (manual/event-origin) artist. A present
    value must be a non-empty string (a seed ``<artist_id>`` or ``"lineup"``).
    """
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        msg = f"Artist source 'via_seed' must be a non-empty string, got {value!r}"
        raise ValueError(msg)
    return value


def _parse_source(raw: object) -> PoolSource:
    """Parse one source entry into a typed source.

    Raises:
        ValueError: If the entry is malformed or the kind is unknown.
    """
    if not isinstance(raw, Mapping):
        msg = f"Each source must be an object, got {type(raw).__name__}"
        raise ValueError(msg)

    kind_raw = raw.get("kind")
    try:
        kind = PoolSourceKind(str(kind_raw))
    except ValueError as exc:
        msg = f"Unknown source kind: {kind_raw!r}"
        raise ValueError(msg) from exc

    enabled = _parse_enabled(raw)

    if kind is PoolSourceKind.EVENT:
        return EventSource(
            event_id=_parse_uuid(raw.get("event_id"), "event_id"), enabled=enabled
        )
    if kind is PoolSourceKind.ARTIST:
        return ArtistSource(
            artist_id=_parse_uuid(raw.get("artist_id"), "artist_id"),
            enabled=enabled,
            via_seed=_parse_via_seed(raw.get("via_seed")),
        )
    # RELATED
    amount_raw = raw.get("amount")
    if not isinstance(amount_raw, int) or isinstance(amount_raw, bool):
        msg = f"Related source 'amount' must be an integer, got {amount_raw!r}"
        raise ValueError(msg)
    if amount_raw < 0:
        msg = f"Related source 'amount' must be non-negative, got {amount_raw}"
        raise ValueError(msg)
    seed = str(raw.get("seed", SEED_TARGET))
    if seed != SEED_TARGET:
        msg = f"Unsupported related 'seed': {seed!r} (only {SEED_TARGET!r})"
        raise ValueError(msg)
    return RelatedSource(amount=amount_raw, seed=seed, enabled=enabled)


def normalize_sources(raw: Mapping[str, object]) -> list[PoolSource]:
    """Parse ``input_references`` into a list of typed sources.

    Accepts the layered ``{"sources": [...]}`` shape and the legacy single-event
    shape ``{"event_id": "<uuid>"}`` (so pre-#128 profiles still resolve during the
    migration window). If both are present, the explicit ``sources`` list wins. An
    input with neither yields an empty list -- emptiness is validated post-resolution
    (a profile may resolve to an empty pool, which is a higher-layer error).

    Raises:
        ValueError: If ``sources`` is not a list or any entry is malformed.
    """
    sources_raw = raw.get("sources")
    if sources_raw is not None:
        if not isinstance(sources_raw, Sequence) or isinstance(
            sources_raw, (str, bytes)
        ):
            msg = f"'sources' must be a list, got {type(sources_raw).__name__}"
            raise ValueError(msg)
        return [_parse_source(entry) for entry in sources_raw]

    # Legacy single-event shape.
    if raw.get("event_id"):
        return [EventSource(event_id=_parse_uuid(raw.get("event_id"), "event_id"))]

    return []


def extract_excludes(raw: Mapping[str, object]) -> set[uuid.UUID]:
    """Parse the global ``exclude_artist_ids`` set from ``input_references``.

    Raises:
        ValueError: If ``exclude_artist_ids`` is present but not a list, or any
            entry is not a valid UUID.
    """
    raw_excludes = raw.get("exclude_artist_ids")
    if raw_excludes is None:
        return set()
    if not isinstance(raw_excludes, Sequence) or isinstance(raw_excludes, (str, bytes)):
        msg = f"'exclude_artist_ids' must be a list, got {type(raw_excludes).__name__}"
        raise ValueError(msg)
    return {_parse_uuid(item, "exclude_artist_ids[]") for item in raw_excludes}


def build_pool(
    resolved: Sequence[ResolvedArtist],
    exclude_ids: set[uuid.UUID],
) -> list[ResolvedArtist]:
    """Assemble the final pool from resolved artists: dedup, then exclude last.

    ``resolved`` is the in-order concatenation of every enabled source's resolved
    artists (event sources first, then artist sources, then related expansion).
    The first occurrence of an artist wins, so provenance precedence is
    event > artist > related by construction. The global exclude set is applied
    last, so "this event but not the opener" works regardless of which source the
    opener came from.

    Returns the deduplicated, exclude-filtered artists in first-seen order.
    """
    seen: set[uuid.UUID] = set()
    pool: list[ResolvedArtist] = []
    for entry in resolved:
        if entry.artist_id in exclude_ids:
            continue
        if entry.artist_id in seen:
            continue
        seen.add(entry.artist_id)
        pool.append(entry)
    return pool


def serialize_source(source: PoolSource) -> dict[str, object]:
    """Serialize a typed source back to its stored JSON shape."""
    if isinstance(source, EventSource):
        return {
            "kind": PoolSourceKind.EVENT.value,
            "event_id": str(source.event_id),
            "enabled": source.enabled,
        }
    if isinstance(source, ArtistSource):
        payload: dict[str, object] = {
            "kind": PoolSourceKind.ARTIST.value,
            "artist_id": str(source.artist_id),
            "enabled": source.enabled,
        }
        # Omit via_seed when None so user-added artists keep the lean shape and
        # round-trip cleanly (None -> absent -> None).
        if source.via_seed is not None:
            payload["via_seed"] = source.via_seed
        return payload
    return {
        "kind": PoolSourceKind.RELATED.value,
        "seed": source.seed,
        "amount": source.amount,
        "enabled": source.enabled,
    }


def serialize_input_references(
    sources: Sequence[PoolSource],
    exclude_ids: Sequence[uuid.UUID] = (),
) -> dict[str, object]:
    """Build a stored ``input_references`` dict from typed sources + excludes."""
    return {
        "sources": [serialize_source(s) for s in sources],
        "exclude_artist_ids": [str(aid) for aid in exclude_ids],
    }
