"""Scoring engine for playlist track selection."""

from __future__ import annotations

import math
from collections import deque
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import uuid


def familiarity_signal(*, listen_count: int, in_library: bool) -> float:
    """Score from 0.0 (never heard) to 1.0 (most played).

    Uses a logarithmic curve so early listens matter more than
    the difference between 80 and 100 listens.
    """
    if not in_library and listen_count == 0:
        return 0.0
    return min(1.0, math.log1p(listen_count) / math.log1p(100))


def popularity_signal(*, popularity_score: int) -> float:
    """Score from 0.0 (obscure) to 1.0 (biggest hit).

    Linear mapping from the 0-100 external popularity score.
    """
    return max(0.0, min(1.0, popularity_score / 100.0))


def bipolar_weight(param_value: int) -> float:
    """Convert a 0-100 bipolar parameter to a -1.0 to 1.0 weight.

    50 = neutral (0.0), 0 = full negative (-1.0), 100 = full positive (1.0).
    """
    return (param_value - 50) / 50.0


def composite_score(
    *,
    familiarity_val: float,
    popularity_val: float,
    params: dict[str, int],
) -> float:
    """Compute composite score for a candidate track from familiarity and hit_depth.

    Returns a value clamped to [0.0, 1.0]. Artist relevance (target vs adjacent)
    is intentionally NOT part of this score. There is one ranked pool: provenance
    is metadata only. Pool *composition* (which related artists are in the pool) is
    decided upstream by enrichment (#133), not by scoring or selection.
    """
    base = 0.5

    fam_weight = bipolar_weight(params.get("familiarity", 50))
    hit_weight = bipolar_weight(params.get("hit_depth", 50))

    score = base
    score += fam_weight * (familiarity_val - 0.5)
    score += hit_weight * (popularity_val - 0.5)

    return max(0.0, min(1.0, score))


def score_track(
    *,
    listen_count: int,
    in_library: bool,
    popularity_score: int,
    params: dict[str, int],
) -> float:
    """Composite score for one track from its raw per-track signals.

    The scoring glue shared by every generator (concert_prep, rediscovery): turns
    listen count / library membership / popularity into a familiarity + popularity
    composite via the familiarity and hit_depth dials.
    """
    fam_val = familiarity_signal(listen_count=listen_count, in_library=in_library)
    pop_val = popularity_signal(popularity_score=popularity_score)
    return composite_score(
        familiarity_val=fam_val, popularity_val=pop_val, params=params
    )


class _Selectable(Protocol):
    """Structural type for tracks the selection helpers operate on.

    Declared as read-only properties (not bare attributes) so a *frozen* dataclass
    field satisfies the protocol -- a mutable protocol attribute would reject a
    frozen dataclass. Any track type exposing ``artist_id`` and ``track_id`` (e.g.
    concert_prep's ``CandidateTrack``) matches, so the round-robin and freshness
    primitives stay generator-agnostic without importing a concrete track type
    (which would be circular).
    """

    @property
    def artist_id(self) -> uuid.UUID: ...

    @property
    def track_id(self) -> uuid.UUID: ...


def round_robin_select[T: _Selectable](
    scored: list[tuple[T, float]],
    max_tracks: int,
    weights: dict[uuid.UUID, int] | None = None,
) -> list[tuple[T, float]]:
    """Select ``max_tracks`` by **weighted round-robin** across the pool's artists.

    ``scored`` must already be sorted by score descending. Tracks are grouped by
    artist (each group stays score-desc); artists are dealt in best-track-score
    order (first-seen order == best-score order because ``scored`` is score-desc).
    Each round every artist with tracks left contributes its next-best track --
    ``weights[artist_id]`` of them per round (default 1 = even). Dealing stops at
    ``max_tracks`` or when every artist is exhausted; an artist that runs out drops
    from later rounds, so its unused share redistributes to the others.

    This is the shared spread primitive extracted from concert_prep's
    ``_select_one_pool`` (#128 round-robin); it decides HOW MANY tracks each artist
    gets, while the caller's score decides WHICH. ``weights`` is the plumbed seam
    for future seed/headliner emphasis; unset today (all 1 = even).

    Edge cases (all handled by the best-track-score deal order):
    - ``max_tracks < n_artists``: only the highest-scoring artists get a round-1
      slot; the lowest never get dealt (keeps the top of the bill).
    - not divisible: the partial final round deals to the highest-scoring first.
    - one artist: deals its top ``max_tracks`` by score.
    - empty pool: returns empty.
    """
    weights = weights or {}
    groups: dict[uuid.UUID, deque[tuple[T, float]]] = {}
    order: list[uuid.UUID] = []
    for pair in scored:
        artist_id = pair[0].artist_id
        if artist_id not in groups:
            groups[artist_id] = deque()
            order.append(artist_id)
        groups[artist_id].append(pair)

    selected: list[tuple[T, float]] = []
    while len(selected) < max_tracks:
        dealt_this_round = False
        for artist_id in order:
            if len(selected) >= max_tracks:
                break
            group = groups[artist_id]
            for _ in range(max(1, weights.get(artist_id, 1))):
                if not group or len(selected) >= max_tracks:
                    break
                selected.append(group.popleft())
                dealt_this_round = True
        if not dealt_this_round:
            break  # every artist exhausted before reaching max_tracks
    return selected


def apply_freshness_filter[T: _Selectable](
    scored: list[tuple[T, float]],
    previous_track_ids: set[uuid.UUID],
    freshness_target: int | None,
    max_tracks: int,
) -> list[tuple[T, float]]:
    """Filter scored candidates to meet a freshness target.

    If ``freshness_target`` is set, > 0, and there are ``previous_track_ids``,
    limits how many previous tracks may appear: a target of 100 means all new, 0
    means repeats are fine. The repeat allowance is
    ``floor(max_tracks * (100 - freshness_target) / 100)``.

    Shared primitive extracted from concert_prep's ``_apply_freshness_filter``.
    """
    if freshness_target is None or freshness_target <= 0 or not previous_track_ids:
        return scored

    max_repeats = int(max_tracks * (100 - freshness_target) / 100)
    repeat_count = 0
    result: list[tuple[T, float]] = []

    for candidate, score in scored:
        is_repeat = candidate.track_id in previous_track_ids
        if is_repeat:
            if repeat_count >= max_repeats:
                continue
            repeat_count += 1
        result.append((candidate, score))

    return result
