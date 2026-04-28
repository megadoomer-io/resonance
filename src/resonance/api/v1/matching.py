"""Entity matching API routes — merge preview and execution."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dedup as dedup_module
import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.music as music_models

router = fastapi.APIRouter(prefix="/matching", tags=["matching"])


async def _get_artist_or_404(
    db: sa_async.AsyncSession,
    artist_id: uuid.UUID,
) -> music_models.Artist:
    stmt = sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
    artist = (await db.execute(stmt)).scalar_one_or_none()
    if artist is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")
    return artist


async def _get_track_or_404(
    db: sa_async.AsyncSession,
    track_id: uuid.UUID,
) -> music_models.Track:
    stmt = sa.select(music_models.Track).where(music_models.Track.id == track_id)
    track = (await db.execute(stmt)).scalar_one_or_none()
    if track is None:
        raise fastapi.HTTPException(status_code=404, detail="Track not found")
    return track


@router.post(
    "/artists/{artist_id}/merge/{other_id}",
    summary="Preview artist merge",
    description="Show what would be affected by merging two artists.",
)
async def preview_artist_merge(
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    canonical = await _get_artist_or_404(db, artist_id)
    other = await _get_artist_or_404(db, other_id)

    # Count tracks for each
    canonical_tracks = (
        await db.execute(
            sa.select(sa.func.count()).where(
                music_models.Track.artist_id == canonical.id
            )
        )
    ).scalar_one()
    other_tracks = (
        await db.execute(
            sa.select(sa.func.count()).where(music_models.Track.artist_id == other.id)
        )
    ).scalar_one()

    # Count events (via EventArtist) for each
    canonical_events = (
        await db.execute(
            sa.select(sa.func.count()).where(
                concert_models.EventArtist.artist_id == canonical.id
            )
        )
    ).scalar_one()
    other_events = (
        await db.execute(
            sa.select(sa.func.count()).where(
                concert_models.EventArtist.artist_id == other.id
            )
        )
    ).scalar_one()

    # Count listening events (via tracks)
    canonical_listens = (
        await db.execute(
            sa.select(sa.func.count()).where(
                music_models.ListeningEvent.track_id.in_(
                    sa.select(music_models.Track.id).where(
                        music_models.Track.artist_id == canonical.id
                    )
                )
            )
        )
    ).scalar_one()
    other_listens = (
        await db.execute(
            sa.select(sa.func.count()).where(
                music_models.ListeningEvent.track_id.in_(
                    sa.select(music_models.Track.id).where(
                        music_models.Track.artist_id == other.id
                    )
                )
            )
        )
    ).scalar_one()

    # Merged service_links preview
    merged_links = dict(canonical.service_links or {})
    for k, v in (other.service_links or {}).items():
        if v and k not in merged_links:
            merged_links[k] = v

    return {
        "canonical": {
            "id": str(canonical.id),
            "name": canonical.name,
            "tracks": canonical_tracks,
            "events": canonical_events,
            "listening_events": canonical_listens,
            "service_links": canonical.service_links,
        },
        "other": {
            "id": str(other.id),
            "name": other.name,
            "tracks": other_tracks,
            "events": other_events,
            "listening_events": other_listens,
            "service_links": other.service_links,
        },
        "merged_service_links": merged_links,
    }


@router.post(
    "/artists/{artist_id}/merge/{other_id}/confirm",
    summary="Confirm artist merge",
    description="Execute the merge of two artists.",
)
async def confirm_artist_merge(
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    canonical = await _get_artist_or_404(db, artist_id)
    other = await _get_artist_or_404(db, other_id)

    stats = await dedup_module.merge_artists(db, canonical, other)
    await db.commit()

    return {
        "status": "merged",
        "canonical_id": str(canonical.id),
        "duplicate_id": str(other_id),
        "stats": {
            "artists_merged": stats.artists_merged,
            "tracks_repointed": stats.tracks_repointed,
            "events_repointed": stats.events_repointed,
        },
    }


@router.post(
    "/tracks/{track_id}/merge/{other_id}/confirm",
    summary="Confirm track merge",
    description="Execute the merge of two tracks.",
)
async def confirm_track_merge(
    track_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    canonical = await _get_track_or_404(db, track_id)
    other = await _get_track_or_404(db, other_id)

    stats = await dedup_module.merge_tracks(db, canonical, other)
    await db.commit()

    return {
        "status": "merged",
        "canonical_id": str(canonical.id),
        "duplicate_id": str(other_id),
        "stats": {
            "tracks_merged": stats.tracks_merged,
            "events_repointed": stats.events_repointed,
        },
    }
