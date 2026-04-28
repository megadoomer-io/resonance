"""Artist API routes — list and detail endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dependencies as deps_module
import resonance.models.music as music_models

_PAGE_SIZE = 50

router = fastapi.APIRouter(prefix="/artists", tags=["artists"])


def _format_artist_summary(artist: music_models.Artist | Any) -> dict[str, Any]:
    return {
        "id": str(artist.id),
        "name": artist.name,
        "origin": artist.origin,
        "service_links": artist.service_links,
    }


@router.get(
    "",
    summary="List artists",
    description="Paginated list of artists, alphabetical by name.",
)
async def list_artists(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
    q: str | None = None,
) -> dict[str, Any]:
    offset = (page - 1) * _PAGE_SIZE

    stmt = sa.select(music_models.Artist).order_by(music_models.Artist.name)

    if q:
        stmt = stmt.where(music_models.Artist.name.ilike(f"%{q}%"))

    stmt = stmt.offset(offset).limit(_PAGE_SIZE + 1)

    result = await db.execute(stmt)
    artists = list(result.scalars().all())

    has_next = len(artists) > _PAGE_SIZE
    artists = artists[:_PAGE_SIZE]

    return {
        "items": [_format_artist_summary(a) for a in artists],
        "page": page,
        "page_size": _PAGE_SIZE,
        "has_next": has_next,
    }


@router.get(
    "/{artist_id}",
    summary="Get artist detail",
    description="Get an artist with service links.",
)
async def get_artist(
    artist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    stmt = sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
    result = await db.execute(stmt)
    artist = result.scalar_one_or_none()

    if artist is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    return {
        "id": str(artist.id),
        "name": artist.name,
        "origin": artist.origin,
        "service_links": artist.service_links,
        "created_at": artist.created_at.isoformat(),
        "updated_at": artist.updated_at.isoformat(),
    }
