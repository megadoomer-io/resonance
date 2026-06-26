"""Concert prep generator -- selects and scores tracks for concert playlists.

This module is pure logic (no database access). It takes pre-fetched candidate
tracks as input, scores them using the scoring engine, and selects the best
tracks for playlist inclusion.
"""

from __future__ import annotations

import dataclasses
from collections import Counter, deque
from typing import TYPE_CHECKING

import resonance.generators.scoring as scoring_module
import resonance.types as types_module

if TYPE_CHECKING:
    import uuid


@dataclasses.dataclass(frozen=True)
class CandidateTrack:
    """A track candidate for playlist inclusion."""

    track_id: uuid.UUID
    title: str
    artist_name: str
    artist_id: uuid.UUID
    # Provenance metadata only: whether this track's artist was an event/seed
    # ("target") vs a resolved related artist. Selection ignores this -- there is
    # one ranked pool. Retained for UI provenance display and source summaries.
    is_target_artist: bool
    listen_count: int
    in_library: bool
    popularity_score: int
    source: types_module.TrackSource


@dataclasses.dataclass(frozen=True)
class ScoredTrack:
    """A track with its composite score and position."""

    track_id: uuid.UUID
    title: str
    artist_name: str
    position: int
    score: float
    source: types_module.TrackSource


@dataclasses.dataclass(frozen=True)
class SelectionResult:
    """The output of score_and_select."""

    tracks: list[ScoredTrack]
    sources_summary: dict[types_module.TrackSource, int]
    freshness_actual: float | None


def _score_candidate(
    candidate: CandidateTrack,
    params: dict[str, int],
) -> float:
    """Compute the composite score for a single candidate track."""
    fam_val = scoring_module.familiarity_signal(
        listen_count=candidate.listen_count,
        in_library=candidate.in_library,
    )
    pop_val = scoring_module.popularity_signal(
        popularity_score=candidate.popularity_score,
    )
    return scoring_module.composite_score(
        familiarity_val=fam_val,
        popularity_val=pop_val,
        params=params,
    )


def _select_one_pool(
    scored: list[tuple[CandidateTrack, float]],
    max_tracks: int,
    weights: dict[uuid.UUID, int] | None = None,
) -> list[tuple[CandidateTrack, float]]:
    """Select ``max_tracks`` by **weighted round-robin** across the pool's artists.

    Provenance (event / manual / related) never touches selection -- there is one
    pool. ``composite_score`` (familiarity + hit_depth) decides WHICH of an artist's
    tracks fill its slots; round-robin decides HOW MANY each artist gets, so every
    artist on the bill is represented (not buried by a heavy-rotation neighbor).
    This restores the per-artist spread that #128 dropped (round-0 + global fill),
    which let well-listened artists monopolize every post-round-0 slot.

    ``scored`` must already be sorted by score descending. Tracks are grouped by
    artist (each group stays score-desc); artists are dealt in best-track-score
    order. Each round, every artist with tracks left contributes its next-best
    track -- ``weights[artist_id]`` of them per round (default 1 = even). Dealing
    stops at ``max_tracks`` or when every artist is exhausted; an artist that runs
    out simply drops from later rounds, so its unused share redistributes to the
    others automatically (a 6-track band among five gives 6; the rest absorb the
    slack).

    ``weights`` is the plumbed seam for a future seed/headliner emphasis (and
    jitter): a higher weight = more tracks per round. Today it is unset (all 1), so
    the deal is even -- the behavior the owner asked for (#round-robin, D7).

    Edge cases, all handled by the deal order (best-track-score desc):
    - ``max_tracks < n_artists``: only the highest-scoring artists get a slot in
      round 1; the lowest never get dealt (graceful, keeps the top of the bill).
    - ``max_tracks`` not divisible by artist count: the partial final round deals
      to the highest-scoring artists first, so they get the extra slots.
    - one artist: deals its top ``max_tracks`` by score.
    - empty pool: returns empty (handled by the caller's no-candidates guard).
    """
    weights = weights or {}
    # Group tracks by artist, preserving score-desc order within each group.
    # First-seen order == best-track-score order (``scored`` is score-desc), so the
    # artist deal order is best-score desc without a separate sort.
    groups: dict[uuid.UUID, deque[tuple[CandidateTrack, float]]] = {}
    order: list[uuid.UUID] = []
    for pair in scored:
        artist_id = pair[0].artist_id
        if artist_id not in groups:
            groups[artist_id] = deque()
            order.append(artist_id)
        groups[artist_id].append(pair)

    selected: list[tuple[CandidateTrack, float]] = []
    while len(selected) < max_tracks:
        dealt_this_round = False
        for artist_id in order:
            if len(selected) >= max_tracks:
                break
            group = groups[artist_id]
            # weight = tracks this artist deals per round (>=1); default even.
            for _ in range(max(1, weights.get(artist_id, 1))):
                if not group or len(selected) >= max_tracks:
                    break
                selected.append(group.popleft())
                dealt_this_round = True
        if not dealt_this_round:
            break  # every artist exhausted before reaching max_tracks
    return selected


def _apply_freshness_filter(
    scored: list[tuple[CandidateTrack, float]],
    previous_track_ids: set[uuid.UUID],
    freshness_target: int | None,
    max_tracks: int,
) -> list[tuple[CandidateTrack, float]]:
    """Filter scored candidates to meet the freshness target.

    If freshness_target is set and >0 and there are previous_track_ids,
    limits how many previous tracks can appear. A freshness_target of 100
    means all tracks should be new; a target of 0 means repeats are fine.

    The repeat allowance is computed as:
        max_repeats = floor(max_tracks * (100 - freshness_target) / 100)
    """
    if freshness_target is None or freshness_target <= 0 or not previous_track_ids:
        return scored

    max_repeats = int(max_tracks * (100 - freshness_target) / 100)
    repeat_count = 0
    result: list[tuple[CandidateTrack, float]] = []

    for candidate, score in scored:
        is_repeat = candidate.track_id in previous_track_ids
        if is_repeat:
            if repeat_count >= max_repeats:
                continue
            repeat_count += 1
        result.append((candidate, score))

    return result


def score_and_select(
    *,
    candidates: list[CandidateTrack],
    params: dict[str, int],
    max_tracks: int,
    previous_track_ids: set[uuid.UUID],
    freshness_target: int | None,
    weights: dict[uuid.UUID, int] | None = None,
) -> SelectionResult:
    """Score candidates, apply freshness filtering, and select top tracks.

    Args:
        candidates: Pre-fetched candidate tracks to evaluate.
        params: Generator parameter values (familiarity, hit_depth, etc.).
        max_tracks: Maximum number of tracks to include in the result.
        previous_track_ids: Track IDs from a previous generation (for freshness).
        freshness_target: Target freshness percentage (0-100), or None to skip.
        weights: Optional per-artist deal weight for the round-robin selection
            (artist_id -> tracks per round, default 1 = even). The plumbed seam for
            a future seed/headliner emphasis; unset today.

    Returns:
        A SelectionResult with scored, positioned tracks and metadata.
    """
    if not candidates:
        return SelectionResult(
            tracks=[],
            sources_summary={},
            freshness_actual=None,
        )

    # 1. Score each candidate
    scored = [(c, _score_candidate(c, params)) for c in candidates]

    # 2. Sort by score descending
    scored.sort(key=lambda pair: pair[1], reverse=True)

    # 3. Apply freshness filter
    scored = _apply_freshness_filter(
        scored, previous_track_ids, freshness_target, max_tracks
    )

    # 4. Select by weighted round-robin across the pool's artists: every artist on
    #    the bill is represented; composite_score decides which of each artist's
    #    tracks fill its slots. Provenance (target vs adjacent) is metadata only.
    #    Pool composition (which related artists are present) is decided upstream by
    #    enrichment (#133), not here.
    selected = _select_one_pool(scored, max_tracks, weights)

    # 5. Re-sort the merged selection by score for final ordering, then assign
    #    1-indexed positions and build the ScoredTrack list.
    selected.sort(key=lambda pair: pair[1], reverse=True)
    tracks = [
        ScoredTrack(
            track_id=candidate.track_id,
            title=candidate.title,
            artist_name=candidate.artist_name,
            position=i + 1,
            score=score,
            source=candidate.source,
        )
        for i, (candidate, score) in enumerate(selected)
    ]

    # 6. Compute source summary
    source_counts = Counter(t.source for t in tracks)
    sources_summary = dict(source_counts)

    # 7. Compute actual freshness
    freshness_actual: float | None = None
    if previous_track_ids and tracks:
        new_count = sum(1 for t in tracks if t.track_id not in previous_track_ids)
        freshness_actual = (new_count / len(tracks)) * 100.0

    return SelectionResult(
        tracks=tracks,
        sources_summary=sources_summary,
        freshness_actual=freshness_actual,
    )
