"""Playlist API routes — list, detail, diff, and export endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm
import structlog

import resonance.dependencies as deps_module
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module

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
        "service_links": playlist.service_links,
        "created_at": playlist.created_at.isoformat(),
        "updated_at": playlist.updated_at.isoformat(),
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


@router.delete(
    "/{playlist_id}",
    summary="Delete playlist",
    description="Delete a playlist and its tracks.",
)
async def delete_playlist(
    playlist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Delete a playlist owned by the authenticated user.

    Args:
        playlist_id: The playlist UUID.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        Confirmation dict.

    Raises:
        HTTPException: 404 if playlist not found.
    """
    result = await db.execute(
        sa.select(playlist_models.Playlist).where(
            playlist_models.Playlist.id == playlist_id,
            playlist_models.Playlist.user_id == user_id,
        )
    )
    playlist = result.scalar_one_or_none()

    if playlist is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

    # Find associated profile before deleting the playlist
    gen_result = await db.execute(
        sa.select(generator_models.GenerationRecord.profile_id).where(
            generator_models.GenerationRecord.playlist_id == playlist_id
        )
    )
    profile_id = gen_result.scalar_one_or_none()

    await db.delete(playlist)
    await db.flush()

    # Clean up orphaned profile if no playlists remain
    if profile_id is not None:
        remaining = await db.execute(
            sa.select(sa.func.count()).where(
                generator_models.GenerationRecord.profile_id == profile_id
            )
        )
        if remaining.scalar_one() == 0:
            profile_result = await db.execute(
                sa.select(generator_models.GeneratorProfile).where(
                    generator_models.GeneratorProfile.id == profile_id
                )
            )
            orphan = profile_result.scalar_one_or_none()
            if orphan is not None:
                await db.delete(orphan)
                logger.info(
                    "orphan_profile_deleted",
                    profile_id=str(profile_id),
                )

    await db.commit()
    logger.info("playlist_deleted", playlist_id=str(playlist_id))
    return {"status": "deleted"}


class ExportRequest(pydantic.BaseModel):
    """Request body for playlist export."""

    connection_ids: list[uuid.UUID] | None = None


@router.post(
    "/{playlist_id}/export",
    summary="Export playlist to external services",
    description="Export a playlist to connected Spotify accounts.",
    status_code=202,
)
async def export_playlist(
    request: fastapi.Request,
    playlist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    body: ExportRequest | None = None,
) -> dict[str, Any]:
    """Export a playlist to one or more Spotify connections.

    Creates a background task per connection and enqueues them for processing.

    Args:
        request: The incoming HTTP request (for arq access).
        playlist_id: The playlist UUID to export.
        user_id: The authenticated user's ID.
        db: The async database session.
        body: Optional request body with specific connection IDs.

    Returns:
        A dict with a list of created task/connection pairs.

    Raises:
        HTTPException: 404 if playlist not found, 400 if no valid connections.
    """
    # Verify playlist exists and belongs to user
    result = await db.execute(
        sa.select(playlist_models.Playlist).where(
            playlist_models.Playlist.id == playlist_id,
            playlist_models.Playlist.user_id == user_id,
        )
    )
    playlist = result.scalar_one_or_none()

    if playlist is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

    # Resolve target Spotify connections
    requested_ids = body.connection_ids if body is not None else None

    if requested_ids is not None:
        # Validate provided connection IDs are Spotify connections owned by user
        conn_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id.in_(requested_ids),
                user_models.ServiceConnection.user_id == user_id,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.SPOTIFY,
            )
        )
        connections = list(conn_result.scalars().all())

        if len(connections) != len(requested_ids):
            found_ids = {c.id for c in connections}
            invalid_ids = [str(cid) for cid in requested_ids if cid not in found_ids]
            raise fastapi.HTTPException(
                status_code=400,
                detail=f"Invalid or non-Spotify connection IDs: {invalid_ids}",
            )
    else:
        # Find all Spotify connections for the user
        conn_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_id,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.SPOTIFY,
            )
        )
        connections = list(conn_result.scalars().all())

        if not connections:
            raise fastapi.HTTPException(
                status_code=400,
                detail="No Spotify connections found for user",
            )

    # Create one task per connection and enqueue
    tasks_info: list[dict[str, str]] = []
    arq_redis = request.app.state.arq_redis

    for connection in connections:
        task = task_models.Task(
            user_id=user_id,
            service_connection_id=connection.id,
            task_type=types_module.TaskType.PLAYLIST_EXPORT,
            status=types_module.SyncStatus.PENDING,
            params={
                "playlist_id": str(playlist_id),
                "connection_id": str(connection.id),
            },
        )
        db.add(task)
        await db.flush()

        await arq_redis.enqueue_job(
            "export_playlist",
            str(task.id),
            _job_id=f"export_playlist:{task.id}",
        )

        tasks_info.append(
            {
                "task_id": str(task.id),
                "connection_id": str(connection.id),
            }
        )

        logger.info(
            "playlist_export_enqueued",
            playlist_id=str(playlist_id),
            task_id=str(task.id),
            connection_id=str(connection.id),
        )

    await db.commit()

    return {"tasks": tasks_info}
