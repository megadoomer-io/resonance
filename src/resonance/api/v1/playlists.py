"""Playlist API routes — list, detail, and diff endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm
import structlog

import resonance.dependencies as deps_module
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models

logger = structlog.get_logger()

router = fastapi.APIRouter(
    prefix="/playlists", tags=["playlists"], redirect_slashes=False
)


def format_playlist_summary(playlist: playlist_models.Playlist) -> dict[str, Any]:
    """Format a playlist into a summary dict for list responses.

    Args:
        playlist: The Playlist ORM object.

    Returns:
        A dict with playlist metadata.
    """
    return {
        "id": str(playlist.id),
        "name": playlist.name,
        "description": playlist.description,
        "track_count": playlist.track_count,
        "is_pinned": playlist.is_pinned,
        "created_at": playlist.created_at.isoformat(),
    }


def build_diff_response(
    *,
    playlist_a_id: uuid.UUID,
    playlist_b_id: uuid.UUID,
    track_ids_a: set[uuid.UUID],
    track_ids_b: set[uuid.UUID],
) -> dict[str, Any]:
    """Build a diff response comparing two sets of track IDs.

    Args:
        playlist_a_id: The UUID of the first playlist.
        playlist_b_id: The UUID of the second playlist.
        track_ids_a: Track IDs in the first playlist.
        track_ids_b: Track IDs in the second playlist.

    Returns:
        A dict with added, removed, and common track IDs plus counts.
    """
    added = track_ids_b - track_ids_a
    removed = track_ids_a - track_ids_b
    common = track_ids_a & track_ids_b

    return {
        "playlist_a_id": str(playlist_a_id),
        "playlist_b_id": str(playlist_b_id),
        "added": sorted(str(t) for t in added),
        "removed": sorted(str(t) for t in removed),
        "common": sorted(str(t) for t in common),
        "added_count": len(added),
        "removed_count": len(removed),
        "common_count": len(common),
    }


@router.get(
    "/",
    summary="List playlists",
    description="List all playlists for the authenticated user.",
)
async def list_playlists(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[dict[str, Any]]:
    """List all playlists for the current user, newest first.

    Args:
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A list of playlist summary dicts.
    """
    stmt = (
        sa.select(playlist_models.Playlist)
        .where(playlist_models.Playlist.user_id == user_id)
        .order_by(playlist_models.Playlist.created_at.desc())
    )
    result = await db.execute(stmt)
    playlists = result.scalars().all()

    return [format_playlist_summary(p) for p in playlists]


@router.get(
    "/{playlist_id}",
    summary="Get playlist detail",
    description="Get a playlist with its tracks and generation info.",
)
async def get_playlist(
    playlist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    """Get a playlist with tracks and optional generation record.

    Args:
        playlist_id: The playlist UUID.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with playlist details, tracks, and generation info.

    Raises:
        HTTPException: 404 if playlist not found.
    """
    stmt = (
        sa.select(playlist_models.Playlist)
        .where(
            playlist_models.Playlist.id == playlist_id,
            playlist_models.Playlist.user_id == user_id,
        )
        .options(
            sa_orm.selectinload(playlist_models.Playlist.tracks)
            .joinedload(playlist_models.PlaylistTrack.track)
            .joinedload(music_models.Track.artist),
        )
    )
    result = await db.execute(stmt)
    playlist = result.scalar_one_or_none()

    if playlist is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

    # Look up generation record if one exists
    gen_stmt = sa.select(generator_models.GenerationRecord).where(
        generator_models.GenerationRecord.playlist_id == playlist_id,
    )
    gen_result = await db.execute(gen_stmt)
    gen_record = gen_result.scalar_one_or_none()

    generation_info: dict[str, Any] | None = None
    if gen_record is not None:
        # Load the profile for the generation record
        profile_stmt = sa.select(generator_models.GeneratorProfile).where(
            generator_models.GeneratorProfile.id == gen_record.profile_id,
        )
        profile_result = await db.execute(profile_stmt)
        profile = profile_result.scalar_one_or_none()

        generation_info = {
            "profile_id": str(gen_record.profile_id),
            "profile_name": profile.name if profile is not None else None,
            "parameter_snapshot": gen_record.parameter_snapshot,
            "freshness_actual": gen_record.freshness_actual,
        }

    detail = format_playlist_summary(playlist)
    detail["tracks"] = [
        {
            "position": pt.position,
            "title": pt.track.title,
            "artist_name": pt.track.artist.name,
            "track_id": str(pt.track_id),
            "score": pt.score,
            "source": pt.source,
        }
        for pt in playlist.tracks
    ]
    detail["generation"] = generation_info

    return detail


@router.get(
    "/{playlist_id}/diff/{other_id}",
    summary="Diff two playlists",
    description="Compare two playlists and return added, removed, and common tracks.",
)
async def diff_playlists(
    playlist_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    """Compare two playlists and return track ID differences.

    Args:
        playlist_id: The first playlist UUID (A).
        other_id: The second playlist UUID (B).
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with added, removed, and common track IDs plus counts.

    Raises:
        HTTPException: 404 if either playlist not found.
    """
    # Verify both playlists belong to the user
    stmt_a = sa.select(playlist_models.Playlist).where(
        playlist_models.Playlist.id == playlist_id,
        playlist_models.Playlist.user_id == user_id,
    )
    stmt_b = sa.select(playlist_models.Playlist).where(
        playlist_models.Playlist.id == other_id,
        playlist_models.Playlist.user_id == user_id,
    )

    result_a = await db.execute(stmt_a)
    result_b = await db.execute(stmt_b)

    if result_a.scalar_one_or_none() is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist A not found")
    if result_b.scalar_one_or_none() is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist B not found")

    # Get track IDs for each playlist
    tracks_stmt_a = sa.select(playlist_models.PlaylistTrack.track_id).where(
        playlist_models.PlaylistTrack.playlist_id == playlist_id,
    )
    tracks_stmt_b = sa.select(playlist_models.PlaylistTrack.track_id).where(
        playlist_models.PlaylistTrack.playlist_id == other_id,
    )

    tracks_result_a = await db.execute(tracks_stmt_a)
    tracks_result_b = await db.execute(tracks_stmt_b)

    track_ids_a = set(tracks_result_a.scalars().all())
    track_ids_b = set(tracks_result_b.scalars().all())

    return build_diff_response(
        playlist_a_id=playlist_id,
        playlist_b_id=other_id,
        track_ids_a=track_ids_a,
        track_ids_b=track_ids_b,
    )
