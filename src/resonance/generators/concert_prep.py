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
) -> list[tuple[CandidateTrack, float]]:
    """Select ``max_tracks`` from a single scored pool with a per-artist floor.

    Provenance (event / manual / related) never touches selection -- there is one
    pool, ranked by ``composite_score``. The input must already be sorted by score
    descending. Selection is two phases:

    Round 0 -- per-artist guarantee: each artist in the pool contributes its single
    highest-scoring track. Because ``scored`` is score-desc, the first time an
    artist is seen is its best track, so round 0 is the set of those firsts, itself
    in score-desc order. This guarantees every pool artist is represented by at
    least one track and is never silently absent.

    Fill: the remaining ``max_tracks - num_artists`` slots are filled by pure score
    across all not-yet-selected tracks. A prolific, high-scoring artist *can* take
    several fill slots here -- that is the intended "heard artists get depth"
    behavior, replacing the old forced round-robin spread.

    Edge case: when the pool has more distinct artists than ``max_tracks``, round 0
    cannot seat everyone. The highest-scoring artist groups keep their guaranteed
    slot; the lowest are dropped (graceful best-effort), since round 0 is already
    in score-desc order.
    """
    round_zero: list[tuple[CandidateTrack, float]] = []
    remaining: list[tuple[CandidateTrack, float]] = []
    seen_artists: set[uuid.UUID] = set()
    for pair in scored:
        artist_id = pair[0].artist_id
        if artist_id not in seen_artists:
            seen_artists.add(artist_id)
            round_zero.append(pair)
        else:
            remaining.append(pair)

    # More artists than slots: keep the highest-scoring guaranteed tracks, drop
    # the rest. round_zero is score-desc, so a head slice is the graceful choice.
    if len(round_zero) >= max_tracks:
        return round_zero[:max_tracks]

    fill_slots = max_tracks - len(round_zero)
    return round_zero + remaining[:fill_slots]


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

    # 4. Select from one pool: round-0 per-artist guarantee, then fill by score.
    #    Provenance (target vs adjacent) is metadata only -- it does not affect
    #    selection. Pool composition (which related artists are present) is decided
    #    upstream by enrichment (#133), not here.
    selected = _select_one_pool(scored, max_tracks)

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
