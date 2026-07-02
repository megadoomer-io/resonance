"""Taste & genre-discovery API routes (#154 Arc 2, Phase 1).

Read-only aggregation over the ``artist_tags`` data shipped in Arc 1. The
top-genres aggregation here is dual-purpose: it is BOTH the "taste stats" view
(which genres dominate the library) AND the option list that populates the
genre-browse filter on the artist library. One query, two consumers.

Genre identity is the MusicBrainz ``genre_mbid`` (stable); the human label is the
representative ``tag`` string for that mbid. No genre-label catalog table exists
(deferred) -- the tag is the label.

Scope: the library is single-tenant (like the artist list), so these aggregates
are library-wide. The user dependency on the route is authentication only -- there
is no per-user genre scoping.
"""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dependencies as deps_module
import resonance.models.music as music_models

router = fastapi.APIRouter(prefix="/taste", tags=["taste"])

# Cap the top-genres list. The library has a few hundred distinct canonical
# genres at most; this bounds the browse-filter option list and the stats page.
_MAX_GENRES = 500


async def get_top_genres(
    db: sa_async.AsyncSession, limit: int = _MAX_GENRES
) -> list[dict[str, Any]]:
    """Top canonical genres across the library, by distinct-artist count.

    Two exact queries, both served by ``ix_artist_tags_genre_mbid``:

    1. Per-``genre_mbid`` distinct-artist count + ``total_votes`` (the summed
       MusicBrainz tag-vote ``count`` across all rows for the genre -- a popularity
       weight, not a distinct measure). Grouping by ``genre_mbid`` (not by tag)
       makes the artist count exact -- a tag-string variant of the same genre can't
       double-count an artist.
    2. Per-``genre_mbid`` representative label: the ``tag`` string carried by the
       most artists for that mbid (ties broken alphabetically). Genre labels are
       near-constant per mbid, but this is robust to case/spacing variants.

    Returns rows ordered by ``artist_count`` DESC, then label, each shaped
    ``{genre_mbid, label, artist_count, total_votes}``.
    """
    stats_stmt = (
        sa.select(
            music_models.ArtistTag.genre_mbid,
            sa.func.count(sa.distinct(music_models.ArtistTag.artist_id)).label(
                "artist_count"
            ),
            sa.func.coalesce(sa.func.sum(music_models.ArtistTag.count), 0).label(
                "total_votes"
            ),
        )
        .where(music_models.ArtistTag.genre_mbid.isnot(None))
        .group_by(music_models.ArtistTag.genre_mbid)
        # genre_mbid is the deterministic tie-break so the LIMIT cutoff is stable
        # when many genres share an artist_count at the boundary.
        .order_by(sa.desc("artist_count"), music_models.ArtistTag.genre_mbid)
        .limit(limit)
    )
    stats_rows = (await db.execute(stats_stmt)).all()
    mbids = [r.genre_mbid for r in stats_rows]
    if not mbids:
        return []

    # Representative label per mbid: the tag borne by the most artists.
    label_stmt = (
        sa.select(
            music_models.ArtistTag.genre_mbid,
            music_models.ArtistTag.tag,
            sa.func.count(sa.distinct(music_models.ArtistTag.artist_id)).label("n"),
        )
        .where(music_models.ArtistTag.genre_mbid.in_(mbids))
        .group_by(music_models.ArtistTag.genre_mbid, music_models.ArtistTag.tag)
    )
    labels: dict[str, str] = {}
    label_keys: dict[str, tuple[int, str, str]] = {}
    for row in (await db.execute(label_stmt)).all():
        # Fully deterministic pick: most artists first, then case-insensitive alpha,
        # then the raw tag as a final tiebreak (so two variants differing ONLY by
        # case -- e.g. "metal"/"Metal" -- resolve stably instead of by DB row order).
        key = (-row.n, row.tag.lower(), row.tag)
        best = label_keys.get(row.genre_mbid)
        if best is None or key < best:
            label_keys[row.genre_mbid] = key
            labels[row.genre_mbid] = row.tag

    out: list[dict[str, Any]] = []
    for r in stats_rows:
        label = labels.get(r.genre_mbid, r.genre_mbid)
        out.append(
            {
                "genre_mbid": r.genre_mbid,
                "label": label,
                "artist_count": int(r.artist_count),
                "total_votes": int(r.total_votes),
            }
        )
    # Final ordering: artist_count DESC (from SQL), then label for a stable tie-break.
    out.sort(key=lambda g: (-g["artist_count"], g["label"].lower()))
    return out


@router.get(
    "/genres",
    summary="Top genres across the library",
    description=(
        "Canonical genres ranked by how many artists carry them. Powers the "
        "taste-stats view and the genre-browse filter's option list."
    ),
)
async def list_top_genres(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    limit: Annotated[int, fastapi.Query(ge=1, le=_MAX_GENRES)] = _MAX_GENRES,
) -> dict[str, Any]:
    genres = await get_top_genres(db, limit=limit)
    return {"items": genres, "count": len(genres)}
