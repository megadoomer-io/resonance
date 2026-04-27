"""Listening history API routes — paginated listening events."""

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

router = fastapi.APIRouter(prefix="/history", tags=["history"])


@router.get(
    "",
    summary="List listening history",
    description="Paginated listening history, most recent first.",
)
async def list_history(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
) -> dict[str, Any]:
    offset = (page - 1) * _PAGE_SIZE

    stmt = (
        sa.select(music_models.ListeningEvent)
        .where(music_models.ListeningEvent.user_id == user_id)
        .order_by(music_models.ListeningEvent.listened_at.desc())
        .options(
            sa_orm.joinedload(music_models.ListeningEvent.track).joinedload(
                music_models.Track.artist
            )
        )
        .offset(offset)
        .limit(_PAGE_SIZE + 1)
    )

    result = await db.execute(stmt)
    events = list(result.scalars().unique().all())

    has_next = len(events) > _PAGE_SIZE
    events = events[:_PAGE_SIZE]

    return {
        "items": [
            {
                "id": str(e.id),
                "listened_at": e.listened_at.isoformat(),
                "source_service": str(e.source_service),
                "track": {
                    "id": str(e.track.id),
                    "title": e.track.title,
                    "artist_name": e.track.artist.name,
                    "artist_id": str(e.track.artist_id),
                    "duration_ms": e.track.duration_ms,
                },
            }
            for e in events
        ],
        "page": page,
        "page_size": _PAGE_SIZE,
        "has_next": has_next,
    }
