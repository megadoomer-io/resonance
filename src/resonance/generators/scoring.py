"""Scoring engine for playlist track selection."""

from __future__ import annotations

import math


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


def artist_relevance_signal(*, is_target_artist: bool) -> float:
    """1.0 for target artists, 0.0 for adjacent artists."""
    return 1.0 if is_target_artist else 0.0


def adjacent_multiplier(*, is_target_artist: bool, similar_artist_ratio: int) -> float:
    """Score multiplier from artist relevance, scaled by similar_artist_ratio.

    Target artists always score at full weight (1.0). Adjacent artists are
    scaled by similar_artist_ratio (a 0-100 unipolar parameter): 0 excludes
    them entirely, 100 applies no penalty, 50 halves their score.
    """
    relevance = artist_relevance_signal(is_target_artist=is_target_artist)
    ratio = max(0.0, min(1.0, similar_artist_ratio / 100.0))
    return relevance + (1.0 - relevance) * ratio


def bipolar_weight(param_value: int) -> float:
    """Convert a 0-100 bipolar parameter to a -1.0 to 1.0 weight.

    50 = neutral (0.0), 0 = full negative (-1.0), 100 = full positive (1.0).
    """
    return (param_value - 50) / 50.0


def composite_score(
    *,
    familiarity_val: float,
    popularity_val: float,
    is_target_artist: bool,
    params: dict[str, int],
) -> float:
    """Compute composite score for a candidate track.

    Returns a value clamped to [0.0, 1.0].
    """
    base = 0.5

    fam_weight = bipolar_weight(params.get("familiarity", 50))
    hit_weight = bipolar_weight(params.get("hit_depth", 50))

    score = base
    score += fam_weight * (familiarity_val - 0.5)
    score += hit_weight * (popularity_val - 0.5)
    score *= adjacent_multiplier(
        is_target_artist=is_target_artist,
        similar_artist_ratio=params.get("similar_artist_ratio", 0),
    )

    return max(0.0, min(1.0, score))
