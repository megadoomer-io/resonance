"""Pool source spec: pure parsing, validation, and assembly of generation inputs.

A generator profile's ``input_references`` is a layered list of *sources* that each
resolve to artist IDs at generation time, plus a global ``exclude_artist_ids`` set
applied last to the union (issue #128). The stored shape is::

    {
      "sources": [
        {"kind": "event",   "event_id": "<uuid>", "enabled": true},
        {"kind": "artist",  "artist_id": "<uuid>", "enabled": true},
        {"kind": "artist",  "artist_id": "<uuid>", "enabled": true,
         "via_seed": "lineup"}
      ],
      "exclude_artist_ids": ["<uuid>"],
      "exclude_track_ids": ["<uuid>"]
    }

``exclude_track_ids`` (optional, #track-exclude) is applied at track-selection time
as a candidate filter in the worker -- this module only parses it; it never sees
tracks. ``exclude_artist_ids`` is applied here at pool build (artist level).

Related artists are no longer a live source kind: enrichment (#133) resolves them
up front and persists them as concrete ``artist`` sources tagged with ``via_seed``.

This module is **pure** -- no database, no connectors. It does two jobs:

1. Parse the stored/client shape into typed sources (:func:`normalize_sources`),
   tolerating the legacy single-event shape ``{"event_id": "<uuid>"}`` so a profile
   written before #128 still resolves during the migration window.
2. Assemble a deduplicated, exclude-filtered pool from already-resolved
   ``(artist_id, provenance)`` pairs (:func:`build_pool`).

The DB/connector resolution step (event -> artists) lives in the worker's
``resolve_pool`` helper, which calls into this module for the pure parts.
"""

from __future__ import annotations

import dataclasses
import datetime
import enum
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Literal


class PoolSourceKind(enum.StrEnum):
    """The kind of a pool source (also used as resolved-artist provenance)."""

    EVENT = "event"
    ARTIST = "artist"
    # A time window over listening history that resolves to the user's top seed
    # artists in that window (#rediscovery). The reusable "seed-window" primitive:
    # scoped as a source kind so a future generic generator can consume it too
    # (deferred Approach C). Its window kind drives regenerate semantics (see
    # ListeningWindow).
    LISTENING_RANGE = "listening_range"


# Provenance shares the source vocabulary: an artist enters the pool *via* the kind
# of source that produced it (event lineup or manual/discovered artist add).
PoolProvenance = PoolSourceKind


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
class ListeningWindow:
    """A time window over listening history, selecting seed artists (#rediscovery).

    ``kind`` drives regenerate semantics (design R4):

    * ``relative`` -- re-resolves to ``[now - lookback_days, now]`` on every
      generation, so the seed set rolls forward. A living feed of the current
      rotation ("last 2 weeks").
    * ``absolute`` -- fixed ``start``/``end`` (stored once at create), so the seed
      set is frozen. A stable artifact you return to ("this time last year"), and
      the kind under which deep cuts are exempt from the freshness drop so a
      rediscovered cut persists across regenerates.

    Exactly one shape is populated: ``lookback_days`` for ``relative``, or
    ``start`` + ``end`` for ``absolute``. This is a pure recipe object; the DB
    query that turns bounds into seed artists lives in the worker.
    """

    kind: Literal["relative", "absolute"]
    lookback_days: int | None = None
    start: datetime.datetime | None = None
    end: datetime.datetime | None = None


@dataclasses.dataclass(frozen=True)
class ListeningRangeSource:
    """Resolve a listening-history window to the user's top seed artists.

    The window defines *which* artists seed the pool; ``seed_artist_count`` caps
    how many (ranked by distinct-track listens in the window). ``deep_cut_basis``
    and ``novelty_basis`` are the non-numeric recipe flags the int-only parameter
    system can't hold (design R1); they live here on the source rather than at the
    top level so the enrich round-trip (which re-serializes sources) can't drop
    them. v1 scores lifetime-only; the ``window`` basis path is a plumbed seam.
    """

    window: ListeningWindow
    seed_artist_count: int = 20
    deep_cut_basis: Literal["lifetime", "window"] = "lifetime"
    novelty_basis: Literal["lifetime", "window"] = "lifetime"
    enabled: bool = True


PoolSource = EventSource | ArtistSource | ListeningRangeSource


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


def _parse_positive_int(value: object, field: str) -> int:
    """Parse a positive integer field, raising a clear error on bad input.

    Accepts an int or a numeric string (JSON round-trips ints, but the CLI may
    pass strings). ``bool`` is rejected explicitly (it is an ``int`` subclass, so
    ``True`` would silently parse to 1).
    """
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        msg = f"{field} must be an integer, got {value!r}"
        raise ValueError(msg)
    try:
        parsed = int(value)
    except ValueError as exc:
        msg = f"{field} must be an integer, got {value!r}"
        raise ValueError(msg) from exc
    if parsed <= 0:
        msg = f"{field} must be a positive integer, got {parsed}"
        raise ValueError(msg)
    return parsed


def _parse_datetime(value: object, field: str) -> datetime.datetime:
    """Parse an ISO 8601 datetime string (or passthrough a datetime)."""
    if isinstance(value, datetime.datetime):
        return value
    try:
        return datetime.datetime.fromisoformat(str(value))
    except (ValueError, TypeError) as exc:
        msg = f"Invalid datetime for {field}: {value!r}"
        raise ValueError(msg) from exc


def _parse_basis(value: object, field: str) -> Literal["lifetime", "window"]:
    """Parse a scoring-basis flag, defaulting to ``lifetime`` when absent."""
    if value is None:
        return "lifetime"
    if value in ("lifetime", "window"):
        return value  # type: ignore[return-value]
    msg = f"{field} must be 'lifetime' or 'window', got {value!r}"
    raise ValueError(msg)


def _parse_window(raw: object) -> ListeningWindow:
    """Parse a listening window object into a typed :class:`ListeningWindow`.

    Requires ``kind`` of ``relative`` (with a positive ``lookback_days``) or
    ``absolute`` (with ``start`` before ``end``). Raises on any other shape.
    """
    if not isinstance(raw, Mapping):
        msg = f"'window' must be an object, got {type(raw).__name__}"
        raise ValueError(msg)
    kind = raw.get("kind")
    if kind == "relative":
        return ListeningWindow(
            kind="relative",
            lookback_days=_parse_positive_int(
                raw.get("lookback_days"), "window.lookback_days"
            ),
        )
    if kind == "absolute":
        start = _parse_datetime(raw.get("start"), "window.start")
        end = _parse_datetime(raw.get("end"), "window.end")
        if end <= start:
            msg = (
                "window.end must be after window.start "
                f"(start={start.isoformat()}, end={end.isoformat()})"
            )
            raise ValueError(msg)
        return ListeningWindow(kind="absolute", start=start, end=end)
    msg = f"Unknown window kind: {kind!r} (expected 'relative' or 'absolute')"
    raise ValueError(msg)


def resolve_window_bounds(
    window: ListeningWindow, now: datetime.datetime
) -> tuple[datetime.datetime, datetime.datetime]:
    """Resolve a window to concrete ``(start, end)`` bounds against ``now``.

    ``relative`` computes ``[now - lookback_days, now]`` (rolls forward on every
    call, so the seed set follows the current rotation); ``absolute`` returns its
    stored ``start``/``end`` (frozen). Pure -- the caller supplies ``now`` so the
    resolution is deterministic and unit-testable. This is the reusable
    seed-window primitive's pure half; the DB seed query lives in the worker.

    Raises:
        ValueError: If the window's fields are inconsistent with its kind.
    """
    if window.kind == "relative":
        if window.lookback_days is None:
            msg = "relative window requires lookback_days"
            raise ValueError(msg)
        return (now - datetime.timedelta(days=window.lookback_days), now)
    if window.start is None or window.end is None:
        msg = "absolute window requires start and end"
        raise ValueError(msg)
    return (window.start, window.end)


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
    if kind is PoolSourceKind.LISTENING_RANGE:
        return ListeningRangeSource(
            window=_parse_window(raw.get("window")),
            seed_artist_count=_parse_positive_int(
                raw.get("seed_artist_count", 20), "seed_artist_count"
            ),
            deep_cut_basis=_parse_basis(raw.get("deep_cut_basis"), "deep_cut_basis"),
            novelty_basis=_parse_basis(raw.get("novelty_basis"), "novelty_basis"),
            enabled=enabled,
        )
    # ARTIST (the only remaining kind; unknown kinds raised above).
    return ArtistSource(
        artist_id=_parse_uuid(raw.get("artist_id"), "artist_id"),
        enabled=enabled,
        via_seed=_parse_via_seed(raw.get("via_seed")),
    )


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


def extract_track_excludes(raw: Mapping[str, object]) -> set[uuid.UUID]:
    """Parse the ``exclude_track_ids`` set from ``input_references`` (#track-exclude).

    Track exclusions are applied at track-selection time (a candidate filter in the
    scoring path), NOT at pool build time -- this module never sees tracks. This
    helper only parses the stored ids; the worker applies them.

    Raises:
        ValueError: If ``exclude_track_ids`` is present but not a list, or any
            entry is not a valid UUID.
    """
    raw_excludes = raw.get("exclude_track_ids")
    if raw_excludes is None:
        return set()
    if not isinstance(raw_excludes, Sequence) or isinstance(raw_excludes, (str, bytes)):
        msg = f"'exclude_track_ids' must be a list, got {type(raw_excludes).__name__}"
        raise ValueError(msg)
    return {_parse_uuid(item, "exclude_track_ids[]") for item in raw_excludes}


def with_track_excludes(
    input_references: Mapping[str, object],
    track_ids: Iterable[uuid.UUID],
) -> dict[str, object]:
    """Return a copy of ``input_references`` with ``exclude_track_ids`` set to
    ``track_ids`` (deduped, order-preserving), leaving every other key intact.

    Used by the playlist-detail refine actions (#track-exclude) to mark/unmark a
    track on the recipe. The key is omitted when the set is empty so a profile
    that excludes nothing keeps its lean shape (matching
    :func:`serialize_input_references`).
    """
    refs = dict(input_references)
    ids = list(dict.fromkeys(str(t) for t in track_ids))
    if ids:
        refs["exclude_track_ids"] = ids
    else:
        refs.pop("exclude_track_ids", None)
    return refs


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


def _serialize_window(window: ListeningWindow) -> dict[str, object]:
    """Serialize a :class:`ListeningWindow` to its stored JSON shape."""
    if window.kind == "relative":
        return {"kind": "relative", "lookback_days": window.lookback_days}
    return {
        "kind": "absolute",
        "start": window.start.isoformat() if window.start is not None else None,
        "end": window.end.isoformat() if window.end is not None else None,
    }


def serialize_source(source: PoolSource) -> dict[str, object]:
    """Serialize a typed source back to its stored JSON shape."""
    if isinstance(source, EventSource):
        return {
            "kind": PoolSourceKind.EVENT.value,
            "event_id": str(source.event_id),
            "enabled": source.enabled,
        }
    if isinstance(source, ListeningRangeSource):
        return {
            "kind": PoolSourceKind.LISTENING_RANGE.value,
            "enabled": source.enabled,
            "window": _serialize_window(source.window),
            "seed_artist_count": source.seed_artist_count,
            "deep_cut_basis": source.deep_cut_basis,
            "novelty_basis": source.novelty_basis,
        }
    # ARTIST (the only remaining kind).
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


def find_listening_range_source(
    input_references: Mapping[str, object],
) -> ListeningRangeSource | None:
    """Return the first enabled listening_range source, or None (#rediscovery).

    A rediscovery profile carries exactly one. The worker reads it to get the
    window (which drives freshness/regenerate semantics) and the basis flags,
    without re-parsing the whole source list at each use site.
    """
    for source in normalize_sources(input_references):
        if isinstance(source, ListeningRangeSource) and source.enabled:
            return source
    return None


def serialize_input_references(
    sources: Sequence[PoolSource],
    exclude_ids: Sequence[uuid.UUID] = (),
    exclude_track_ids: Sequence[uuid.UUID] = (),
) -> dict[str, object]:
    """Build a stored ``input_references`` dict from typed sources + excludes.

    ``exclude_track_ids`` is emitted only when non-empty, so profiles that never
    exclude a track keep their existing lean shape (and existing round-trips are
    unchanged).
    """
    refs: dict[str, object] = {
        "sources": [serialize_source(s) for s in sources],
        "exclude_artist_ids": [str(aid) for aid in exclude_ids],
    }
    if exclude_track_ids:
        refs["exclude_track_ids"] = [str(tid) for tid in exclude_track_ids]
    return refs


def scope_artist_ids(
    input_references: Mapping[str, object], scope: str
) -> list[uuid.UUID]:
    """Return the artist ids of every ``ArtistSource`` tagged ``via_seed == scope``.

    Used by the enrich worker to know which previously-discovered artists belong
    to a scope (so they can be re-discovered on replace rather than excluded).
    """
    return [
        s.artist_id
        for s in normalize_sources(input_references)
        if isinstance(s, ArtistSource) and s.via_seed == scope
    ]


def replace_via_seed_sources(
    input_references: Mapping[str, object],
    scope: str,
    artist_ids: Sequence[uuid.UUID],
) -> dict[str, object]:
    """Replace a scope's discovered artist sources with a fresh batch (#133).

    Drops every ``ArtistSource`` whose ``via_seed == scope``, then appends one
    enabled ``ArtistSource(via_seed=scope)`` per id in ``artist_ids`` (in order).
    All other sources (events, manual artists, other scopes) and the global
    ``exclude_artist_ids`` set are preserved. Pure: returns a new stored
    ``input_references`` dict, leaving the input untouched.

    This is how per-seed and global enrichment are "batch + replace": re-running a
    scope removes only that scope's prior discoveries before adding the new ones,
    so curation in other scopes and the core pool is never disturbed.
    """
    kept: list[PoolSource] = [
        s
        for s in normalize_sources(input_references)
        if not (isinstance(s, ArtistSource) and s.via_seed == scope)
    ]
    kept.extend(
        ArtistSource(artist_id=aid, enabled=True, via_seed=scope) for aid in artist_ids
    )
    excludes = extract_excludes(input_references)
    track_excludes = extract_track_excludes(input_references)
    return serialize_input_references(
        kept,
        sorted(excludes, key=str),
        sorted(track_excludes, key=str),
    )
