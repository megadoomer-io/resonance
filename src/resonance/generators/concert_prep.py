"""Concert prep generator -- selects and scores tracks for concert playlists.

This module is pure logic (no database access). It takes pre-fetched candidate
tracks as input, scores them using the scoring engine, and selects the best
tracks for playlist inclusion.
"""

from __future__ import annotations

import dataclasses
from collections import Counter
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


def _round_half_up(value: float) -> int:
    """Round to the nearest integer, halves rounding up (not banker's rounding).

    Used for slot-count math so a 3.5 quota becomes 4, predictably.
    """
    return int(value + 0.5)


def _select_with_quota(
    target_pool: list[tuple[CandidateTrack, float]],
    adjacent_pool: list[tuple[CandidateTrack, float]],
    similar_artist_ratio: int,
    max_tracks: int,
) -> list[tuple[CandidateTrack, float]]:
    """Fill ``max_tracks`` slots, drawing ~ratio% from the adjacent pool.

    Both pools must already be sorted by score descending. The adjacent quota is
    ``round_half_up(max_tracks * ratio / 100)``; the rest go to the target pool.

    When a pool cannot fill its quota, the shortfall is backfilled from the other
    pool -- but only if the other pool was allocated at least one slot. This keeps
    the extremes literal: at ratio=0 no adjacent track is ever added, and at
    ratio=100 no target track is ever added (matching the design intent that 0 is
    target-only and 100 is adjacent-only).
    """
    ratio = max(0, min(100, similar_artist_ratio))
    adj_quota = _round_half_up(max_tracks * ratio / 100)
    tgt_quota = max_tracks - adj_quota

    tgt_take = target_pool[:tgt_quota]
    adj_take = adjacent_pool[:adj_quota]

    tgt_short = tgt_quota - len(tgt_take)
    adj_short = adj_quota - len(adj_take)

    # Backfill a pool's shortfall from the other pool's remainder, but only when
    # the other pool was allocated slots (so 0/100 stay pure).
    if tgt_short > 0 and adj_quota > 0:
        adj_take += adjacent_pool[adj_quota : adj_quota + tgt_short]
    if adj_short > 0 and tgt_quota > 0:
        tgt_take += target_pool[tgt_quota : tgt_quota + adj_short]

    return (tgt_take + adj_take)[:max_tracks]


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
) -> SelectionResult:
    """Score candidates, apply freshness filtering, and select top tracks.

    Args:
        candidates: Pre-fetched candidate tracks to evaluate.
        params: Generator parameter values (familiarity, hit_depth, etc.).
        max_tracks: Maximum number of tracks to include in the result.
        previous_track_ids: Track IDs from a previous generation (for freshness).
        freshness_target: Target freshness percentage (0-100), or None to skip.

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

    # 4. Partition into target / adjacent pools (each stays score-desc) and
    #    select per the similar_artist_ratio blend quota.
    target_pool = [pair for pair in scored if pair[0].is_target_artist]
    adjacent_pool = [pair for pair in scored if not pair[0].is_target_artist]
    selected = _select_with_quota(
        target_pool,
        adjacent_pool,
        params.get("similar_artist_ratio", 0),
        max_tracks,
    )

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
