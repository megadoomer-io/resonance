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

    # 1. Score each candidate (shared scoring glue).
    scored = [
        (
            c,
            scoring_module.score_track(
                listen_count=c.listen_count,
                in_library=c.in_library,
                popularity_score=c.popularity_score,
                params=params,
            ),
        )
        for c in candidates
    ]

    # 2. Sort by score descending
    scored.sort(key=lambda pair: pair[1], reverse=True)

    # 3. Apply freshness filter
    scored = scoring_module.apply_freshness_filter(
        scored, previous_track_ids, freshness_target, max_tracks
    )

    # 4. Select by weighted round-robin across the pool's artists: every artist on
    #    the bill is represented; composite_score decides which of each artist's
    #    tracks fill its slots. Provenance (target vs adjacent) is metadata only.
    #    Pool composition (which related artists are present) is decided upstream by
    #    enrichment (#133), not here.
    selected = scoring_module.round_robin_select(scored, max_tracks, weights)

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
