"""Genre-affinity primitive for artist disambiguation and ranking (#136).

The shared core of the genre model (Arc 1) that every Arc 2 consumer
(genre-aware generation, discovery, concert lens) plugs into. Pure functions, no
ORM or I/O -- callers adapt ArtistTag rows to ``(genre_mbid, count)`` pairs.

Model: an artist's genre profile is a sparse vector keyed by ``genre_mbid`` with
folksonomy ``count`` weights. Only canonical-genre tags participate
(``genre_mbid`` present); free folksonomy tags ("seen live") are dropped as noise.

Affinity of a candidate to a seed *set* is the cosine of the candidate vector
against the aggregated seed profile. Each seed vector is L2-normalized BEFORE
aggregation, so every seed artist contributes equally regardless of its raw
folksonomy count magnitude -- a MusicBrainz vote tally is not comparable across
artists (a famous act accrues hundreds of votes, an obscure one gets two), so
summing raw counts would let one heavily-tagged seed hijack the profile. Intra
-artist relative counts (metal:9 vs thrash:1) are preserved; only the cross
-artist scale is normalized away.

No-data vs wrong-genre: ``affinity_score`` returns ``None`` when there is no basis
to compare (the candidate has no canonical-genre tags, or the seed set has none),
and a float in ``[0.0, 1.0]`` otherwise. This keeps "unknown genre" (None)
distinct from "known mismatch" (0.0) so a ranking consumer never suppresses an
untagged true match down to the same rank as a confirmed off-genre one. Callers
choose the neutral for None (e.g. skip the genre term entirely).

Sparse behavior: when both sides carry a single genre, cosine is 1.0 on a shared
genre and 0.0 otherwise -- a binary, lossy signal at the sparse end (two single
-tag candidates sharing the seed genre tie at 1.0, discarding count detail). This
is tolerated because multi-tag artists are the common case; it is information loss
at the tail, not a correctness claim. Smoothing (a floor, or blending shared-tag
overlap) can be added behind ``affinity_score`` in Arc 2 -- consumers depend on
the function, not the cosine internals.
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import collections.abc as abc

# A genre profile: genre_mbid -> summed folksonomy count weight.
GenreVector = dict[str, float]


def build_vector(tags: abc.Iterable[tuple[str | None, float]]) -> GenreVector:
    """Build a genre vector from ``(genre_mbid, count)`` pairs.

    Tags with no ``genre_mbid`` (free folksonomy tags) are dropped -- only
    canonical genres shape the vector. Negative counts are floored to 0;
    non-finite counts (inf/nan) are dropped so they cannot poison a downstream
    cosine with NaN. Repeated genres are summed.
    """
    vec: GenreVector = {}
    for genre_mbid, count in tags:
        if not genre_mbid:
            continue
        weight = float(count)
        if not math.isfinite(weight):
            continue
        vec[genre_mbid] = vec.get(genre_mbid, 0.0) + max(0.0, weight)
    return vec


def l2_normalize(vec: GenreVector) -> GenreVector:
    """Return ``vec`` scaled to unit L2 norm (empty/zero-norm -> empty)."""
    norm = math.sqrt(sum(w * w for w in vec.values()))
    if norm == 0.0:
        return {}
    return {k: w / norm for k, w in vec.items()}


def aggregate_vectors(vectors: abc.Iterable[GenreVector]) -> GenreVector:
    """Sum genre vectors component-wise into one profile.

    A plain component-wise sum -- callers that need equal per-vector weight should
    ``l2_normalize`` each input before aggregating (``affinity_score`` does).
    """
    out: GenreVector = {}
    for vec in vectors:
        for genre_mbid, weight in vec.items():
            out[genre_mbid] = out.get(genre_mbid, 0.0) + weight
    return out


def cosine(a: GenreVector, b: GenreVector) -> float:
    """Cosine similarity of two genre vectors, in ``[0.0, 1.0]``.

    Returns 0.0 when either vector is empty/zero-magnitude, or when the two share
    no genre. Weights are non-negative, so the result never goes below 0 and is
    clamped to 1.0 against float rounding.
    """
    if not a or not b:
        return 0.0
    shared = a.keys() & b.keys()
    if not shared:
        return 0.0
    dot = sum(a[k] * b[k] for k in shared)
    # Weights are non-negative, so dot >= 0; == 0 only if every shared genre has
    # zero weight on one side (division would be fine, but short-circuit anyway).
    if dot == 0.0:
        return 0.0
    norm_a = math.sqrt(sum(w * w for w in a.values()))
    norm_b = math.sqrt(sum(w * w for w in b.values()))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return min(1.0, dot / (norm_a * norm_b))


def affinity_score(
    candidate_tags: abc.Iterable[tuple[str | None, float]],
    seed_tag_lists: abc.Iterable[abc.Iterable[tuple[str | None, float]]],
) -> float | None:
    """Genre affinity of a candidate artist to a seed set.

    The public entry point for #136 ranking. Each seed vector is L2-normalized
    before aggregation so seeds contribute equally regardless of raw count scale.

    Returns:
        A cosine similarity in ``[0.0, 1.0]``, or ``None`` when there is no basis
        to compare (candidate has no canonical-genre tags, or no seed does). A
        consumer maps ``None`` to its own neutral (typically: omit the genre term)
        and keeps it distinct from a genuine 0.0 mismatch.
    """
    candidate = build_vector(candidate_tags)
    seed_profile = aggregate_vectors(
        l2_normalize(build_vector(t)) for t in seed_tag_lists
    )
    if not candidate or not seed_profile:
        return None
    return cosine(candidate, seed_profile)
