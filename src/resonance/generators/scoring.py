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
    is metadata only, and similar_artist_ratio governs pool *composition* (how many
    related artists are resolved in), not scoring or selection.
    """
    base = 0.5

    fam_weight = bipolar_weight(params.get("familiarity", 50))
    hit_weight = bipolar_weight(params.get("hit_depth", 50))

    score = base
    score += fam_weight * (familiarity_val - 0.5)
    score += hit_weight * (popularity_val - 0.5)

    return max(0.0, min(1.0, score))
