"""Track API routes — list and detail endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.dependencies as deps_module
import resonance.models.music as music_models

_PAGE_SIZE = 50

router = fastapi.APIRouter(prefix="/tracks", tags=["tracks"])


@router.get(
    "",
    summary="List tracks",
    description="Paginated list of tracks, alphabetical by title.",
)
async def list_tracks(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
    q: str | None = None,
) -> dict[str, Any]:
    offset = (page - 1) * _PAGE_SIZE

    stmt = (
        sa.select(music_models.Track)
        .join(music_models.Artist)
        .options(sa_orm.joinedload(music_models.Track.artist))
        .order_by(music_models.Track.title)
    )

    if q:
        stmt = stmt.where(music_models.Track.title.ilike(f"%{q}%"))

    stmt = stmt.offset(offset).limit(_PAGE_SIZE + 1)

    result = await db.execute(stmt)
    tracks = list(result.scalars().unique().all())

    has_next = len(tracks) > _PAGE_SIZE
    tracks = tracks[:_PAGE_SIZE]

    return {
        "items": [
            {
                "id": str(t.id),
                "title": t.title,
                "artist_name": t.artist.name,
                "artist_id": str(t.artist_id),
                "duration_ms": t.duration_ms,
                "service_links": t.service_links,
            }
            for t in tracks
        ],
        "page": page,
        "page_size": _PAGE_SIZE,
        "has_next": has_next,
    }


@router.get(
    "/{track_id}",
    summary="Get track detail",
    description="Get a track with artist info and service links.",
)
async def get_track(
    track_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    stmt = (
        sa.select(music_models.Track)
        .where(music_models.Track.id == track_id)
        .options(sa_orm.joinedload(music_models.Track.artist))
    )
    result = await db.execute(stmt)
    track = result.unique().scalar_one_or_none()

    if track is None:
        raise fastapi.HTTPException(status_code=404, detail="Track not found")

    return {
        "id": str(track.id),
        "title": track.title,
        "artist": {
            "id": str(track.artist.id),
            "name": track.artist.name,
            "service_links": track.artist.service_links,
        },
        "duration_ms": track.duration_ms,
        "service_links": track.service_links,
        "created_at": track.created_at.isoformat(),
        "updated_at": track.updated_at.isoformat(),
    }
