"""Rediscovery generator -- "more but different" from a listening-history window.

Pure logic (no DB). A rediscovery playlist mixes two streams:

* **new** -- never-heard on-genre artists materialized into the pool up front by
  related-artist enrichment (#133), tagged ``via_seed``. These are the "different."
* **deep-cut** -- less-heard tracks from the seed artists the user already spins
  (the listening-range seeds). These are the "more."

``new_ratio`` (0-100) splits the *track budget* between the two streams; within a
stream, the shared weighted round-robin (:func:`scoring.round_robin_select`) spreads
tracks across that stream's artists so no single artist monopolizes its half. The
worker (:func:`worker.score_and_build_playlist`) supplies the candidate tracks, the
via_seed artist-id set, and each seed artist's play distribution; this module
decides which of a seed's tracks are deep cuts, splits the budget, and assembles
the final ordered selection.

The deep-cut definition is *relative* per artist (design premise 4): a track whose
lifetime play count sits at or below the ``less_heard_percentile`` of that artist's
own per-track play distribution, floored at ``play_count >= 1`` (rediscovery, not
cold strangers). A thin-seed guard drops an artist with too few distinct played
tracks, since the percentile of ``[1, 1, 2]`` is degenerate (design R5).
"""

from __future__ import annotations

import math
from collections import Counter
from typing import TYPE_CHECKING

import resonance.generators.concert_prep as concert_prep_module
import resonance.generators.scoring as scoring_module
import resonance.types as types_module

if TYPE_CHECKING:
    import uuid
    from collections.abc import Mapping, Sequence

# Reuse concert_prep's track dataclasses so the worker builds one candidate shape
# and consumes one selection shape regardless of generator type.
CandidateTrack = concert_prep_module.CandidateTrack
ScoredTrack = concert_prep_module.ScoredTrack
SelectionResult = concert_prep_module.SelectionResult

# Minimum distinct played tracks a seed artist needs before the deep-cut percentile
# is meaningful; below this the artist is dropped from the deep-cut stream (R5).
DEFAULT_MIN_DISTINCT_TRACKS = 4


def select_deep_cut_track_ids(
    play_counts: Mapping[uuid.UUID, int],
    *,
    percentile: int,
    min_distinct: int = DEFAULT_MIN_DISTINCT_TRACKS,
) -> set[uuid.UUID]:
    """Deep-cut track ids for ONE seed artist from its per-track play distribution.

    ``play_counts`` maps each of the artist's tracks to its lifetime play count. A
    deep cut is a track whose count is at or below the ``percentile`` of the
    artist's own played-track distribution, with ``count >= 1`` (a rediscovery, not
    a never-heard track). Uses the nearest-rank percentile method.

    The thin-seed guard (design R5): if the artist has fewer than ``min_distinct``
    distinct played tracks, the distribution is too small to have a meaningful
    "bottom third" (the 33rd percentile of ``[1, 1, 2]`` is 1, which would make
    every track a deep cut), so the artist contributes NO deep cuts and is dropped
    from the deep-cut stream. It still seeds enrichment and can appear via the pool.

    Returns the set of qualifying track ids (empty when the guard trips).
    """
    played = {tid: count for tid, count in play_counts.items() if count >= 1}
    if len(played) < min_distinct:
        return set()
    counts = sorted(played.values())
    n = len(counts)
    # Nearest-rank: index of the percentile value, clamped into range.
    idx = max(0, min(n - 1, math.ceil(percentile / 100 * n) - 1))
    threshold = counts[idx]
    return {tid for tid, count in played.items() if count <= threshold}


def split_budget(*, new_ratio: int, max_tracks: int) -> tuple[int, int]:
    """Split ``max_tracks`` between the new and deep-cut streams by ``new_ratio``.

    ``new_ratio`` is a 0-100 percentage of the budget given to the new-artist
    stream (0 = all deep cuts, 100 = all new). Returns ``(new_slots, deep_slots)``
    with ``new_slots + deep_slots == max_tracks``. Rounding favors whichever side
    ``round`` lands on; the remainder always goes to deep cuts so the total is
    exact.
    """
    new_slots = round(new_ratio / 100 * max_tracks)
    new_slots = max(0, min(max_tracks, new_slots))
    return new_slots, max_tracks - new_slots


def _score_stream(
    candidates: Sequence[CandidateTrack],
    params: dict[str, int],
) -> list[tuple[CandidateTrack, float]]:
    """Score a stream's candidates and return them sorted best-first."""
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
    scored.sort(key=lambda pair: pair[1], reverse=True)
    return scored


def score_and_select(
    *,
    candidates: Sequence[CandidateTrack],
    new_artist_ids: set[uuid.UUID],
    deep_cut_track_ids: set[uuid.UUID],
    params: dict[str, int],
    new_ratio: int,
    max_tracks: int,
    previous_track_ids: set[uuid.UUID],
    freshness_target: int | None,
    exempt_deep_cuts_from_freshness: bool,
) -> SelectionResult:
    """Two-stream rediscovery selection (design R2/R4).

    Partitions ``candidates`` into the new stream (artist in ``new_artist_ids``)
    and the deep-cut stream (artist NOT in ``new_artist_ids`` AND track in
    ``deep_cut_track_ids``), scores each, applies freshness, splits the budget by
    ``new_ratio``, and deals each stream by weighted round-robin. A short stream's
    unused slots redistribute to the other so the playlist still fills.

    Freshness: the new stream is always subject to the freshness target; the
    deep-cut stream is exempt when ``exempt_deep_cuts_from_freshness`` is true
    (absolute window kind) so rediscovered cuts persist across regenerates, and
    subject to normal freshness otherwise (relative window). Freshness is applied
    per stream against that stream's own budget.

    Args:
        candidates: Pre-fetched candidate tracks (both streams, mixed).
        new_artist_ids: Artist ids materialized by enrichment (the new stream).
        deep_cut_track_ids: Track ids that qualify as deep cuts (deep-cut stream).
        params: Generator parameter values (familiarity, hit_depth, ...).
        new_ratio: 0-100 budget share for the new stream.
        max_tracks: Total tracks to select.
        previous_track_ids: Track ids from the prior generation (freshness).
        freshness_target: Target freshness percentage (0-100), or None to skip.
        exempt_deep_cuts_from_freshness: When true, deep cuts bypass the freshness
            filter (absolute window kind).

    Returns:
        A SelectionResult with scored, positioned tracks and metadata.
    """
    if not candidates:
        return SelectionResult(tracks=[], sources_summary={}, freshness_actual=None)

    new_candidates = [c for c in candidates if c.artist_id in new_artist_ids]
    deep_candidates = [
        c
        for c in candidates
        if c.artist_id not in new_artist_ids and c.track_id in deep_cut_track_ids
    ]

    new_slots, deep_slots = split_budget(new_ratio=new_ratio, max_tracks=max_tracks)

    new_scored = _score_stream(new_candidates, params)
    deep_scored = _score_stream(deep_candidates, params)

    # New stream: always freshness-filtered. Deep stream: exempt under an absolute
    # window (rediscovered cuts persist), normal freshness under relative.
    new_scored = scoring_module.apply_freshness_filter(
        new_scored, previous_track_ids, freshness_target, max(1, new_slots)
    )
    if not exempt_deep_cuts_from_freshness:
        deep_scored = scoring_module.apply_freshness_filter(
            deep_scored, previous_track_ids, freshness_target, max(1, deep_slots)
        )

    # Deal each stream, then redistribute a short stream's slack to the other so
    # the playlist fills to max_tracks when candidates allow (mirrors how
    # concert_prep redistributes a short band's slack).
    new_selected = scoring_module.round_robin_select(new_scored, new_slots)
    deep_selected = scoring_module.round_robin_select(deep_scored, deep_slots)
    new_short = new_slots - len(new_selected)
    deep_short = deep_slots - len(deep_selected)
    if new_short > 0 and deep_short <= 0:
        deep_selected = scoring_module.round_robin_select(
            deep_scored, deep_slots + new_short
        )
    elif deep_short > 0 and new_short <= 0:
        new_selected = scoring_module.round_robin_select(
            new_scored, new_slots + deep_short
        )

    selected = new_selected + deep_selected
    # Final order by score (matches concert_prep: the merged selection is
    # re-sorted so the strongest tracks lead regardless of stream).
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

    source_counts: Counter[types_module.TrackSource] = Counter(t.source for t in tracks)
    sources_summary = dict(source_counts)

    freshness_actual: float | None = None
    if previous_track_ids and tracks:
        new_count = sum(1 for t in tracks if t.track_id not in previous_track_ids)
        freshness_actual = (new_count / len(tracks)) * 100.0

    return SelectionResult(
        tracks=tracks,
        sources_summary=sources_summary,
        freshness_actual=freshness_actual,
    )
