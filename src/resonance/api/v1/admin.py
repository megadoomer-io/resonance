"""Admin API routes."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.crypto as crypto_module
import resonance.database as database_module
import resonance.dependencies as deps_module
import resonance.models.music as music_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module

router = fastapi.APIRouter(
    prefix="/admin",
    tags=["admin"],
    dependencies=[fastapi.Depends(deps_module.verify_admin_access)],
)

_VALID_DEDUP_OPERATIONS = frozenset({"events", "artists", "tracks"})


# ---------------------------------------------------------------------------
# Query functions (shared by API routes and UI routes)
# ---------------------------------------------------------------------------


async def get_sync_status(
    db: sa_async.AsyncSession,
) -> dict[str, object]:
    """Last 10 parent tasks with children summaries."""
    result = await db.execute(
        sa.select(task_models.Task)
        .where(task_models.Task.parent_id.is_(None))
        .order_by(task_models.Task.created_at.desc())
        .options(
            sa_orm.joinedload(task_models.Task.service_connection),
            sa_orm.joinedload(task_models.Task.children),
        )
        .limit(10)
    )
    jobs = result.scalars().unique().all()

    tasks_list: list[dict[str, object]] = []
    for job in jobs:
        conn = job.service_connection
        service = conn.service_type.value if conn else "unknown"
        children_summary = [
            {
                "type": c.task_type.value,
                "status": c.status.value,
                "progress": c.progress_current,
                "total": c.progress_total,
                "description": c.description,
                "error": c.error_message,
            }
            for c in sorted(job.children, key=lambda c: c.created_at)
        ]
        tasks_list.append(
            {
                "id": str(job.id),
                "service": service,
                "status": job.status.value,
                "created_at": job.created_at.isoformat(),
                "completed_at": (
                    job.completed_at.isoformat() if job.completed_at else None
                ),
                "children": children_summary,
            }
        )

    return {"sync_jobs": tasks_list}


async def get_db_stats(
    db: sa_async.AsyncSession,
) -> dict[str, object]:
    """Database aggregate counts."""
    artists = await database_module.count_rows(db, music_models.Artist)
    tracks_total = await database_module.count_rows(db, music_models.Track)
    events_total = await database_module.count_rows(db, music_models.ListeningEvent)

    dur_result = await db.execute(
        sa.select(
            sa.func.count()
            .filter(music_models.Track.duration_ms.isnot(None))
            .label("with_duration"),
            sa.func.count()
            .filter(music_models.Track.duration_ms.is_(None))
            .label("without_duration"),
        )
    )
    dur_row = dur_result.one()

    events_by_svc = await db.execute(
        sa.select(
            music_models.ListeningEvent.source_service,
            sa.func.count(),
        ).group_by(music_models.ListeningEvent.source_service)
    )

    dup_artists_result = await db.execute(
        sa.text(
            "SELECT COUNT(*) FROM ("
            "  SELECT LOWER(name) "
            "  FROM artists "
            "  GROUP BY LOWER(name) "
            "  HAVING COUNT(*) > 1"
            ") sub"
        )
    )
    dup_tracks_result = await db.execute(
        sa.text(
            "SELECT COUNT(*) FROM ("
            "  SELECT LOWER(title), artist_id "
            "  FROM tracks "
            "  GROUP BY LOWER(title), artist_id "
            "  HAVING COUNT(*) > 1"
            ") sub"
        )
    )

    return {
        "artists": artists,
        "tracks": tracks_total,
        "tracks_with_duration": dur_row.with_duration,
        "tracks_without_duration": dur_row.without_duration,
        "events_total": events_total,
        "events_by_service": {row[0]: row[1] for row in events_by_svc.all()},
        "duplicate_artist_groups": dup_artists_result.scalar() or 0,
        "duplicate_track_groups": dup_tracks_result.scalar() or 0,
    }


async def _mb_status_counts(
    db: sa_async.AsyncSession, status_col: Any
) -> dict[str, int]:
    """Count rows grouped by an mb_match_status column (None -> 'unattempted')."""
    rows = (
        await db.execute(sa.select(status_col, sa.func.count()).group_by(status_col))
    ).all()
    return {
        (status.value if status is not None else "unattempted"): count
        for status, count in rows
    }


async def get_backfill_coverage(db: sa_async.AsyncSession) -> dict[str, object]:
    """MBID-backfill coverage by entity type and outcome (#71, the T3-A gate).

    For tracks and artists, counts rows by ``mb_match_status``. The None bucket
    (not yet attempted, or left for retry after a transient error) is reported as
    ``"unattempted"``. This is the decision input for whether hosted-mapper
    coverage is good enough or the local MB DB (Phase B) is worth standing up.
    """
    return {
        "track": {
            "by_status": await _mb_status_counts(
                db, music_models.Track.mb_match_status
            ),
        },
        "artist": {
            "by_status": await _mb_status_counts(
                db, music_models.Artist.mb_match_status
            ),
        },
    }


async def get_task_detail(
    db: sa_async.AsyncSession,
    task_id: uuid.UUID,
) -> dict[str, object]:
    """Single task lookup with status/progress/result."""
    result = await db.execute(
        sa.select(task_models.Task).where(task_models.Task.id == task_id)
    )
    task = result.scalar_one_or_none()

    if task is None:
        raise fastapi.HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": str(task.id),
        "status": task.status.value,
        "operation": task.params.get("operation") if task.params else None,
        "progress_current": task.progress_current,
        "progress_total": task.progress_total,
        "result": task.result,
        "error": task.error_message,
        "started_at": (task.started_at.isoformat() if task.started_at else None),
        "completed_at": (task.completed_at.isoformat() if task.completed_at else None),
    }


async def search_tracks(
    db: sa_async.AsyncSession,
    query: str,
) -> dict[str, object]:
    """Case-insensitive track title search with listening event data."""
    if not query.strip():
        raise fastapi.HTTPException(
            status_code=422, detail="Query parameter 'q' is required."
        )

    result = await db.execute(
        sa.select(music_models.Track)
        .options(sa_orm.joinedload(music_models.Track.artist))
        .where(
            music_models.Track.title.ilike(
                f"%{database_module.escape_ilike(query.strip())}%"
            )
        )
        .order_by(music_models.Track.title)
        .limit(20)
    )
    tracks = result.scalars().unique().all()

    tracks_list: list[dict[str, object]] = []
    for t in tracks:
        ev_result = await db.execute(
            sa.select(
                music_models.ListeningEvent.source_service,
                sa.func.count(),
            )
            .where(music_models.ListeningEvent.track_id == t.id)
            .group_by(music_models.ListeningEvent.source_service)
        )
        recent = await db.execute(
            sa.select(
                music_models.ListeningEvent.listened_at,
                music_models.ListeningEvent.source_service,
            )
            .where(music_models.ListeningEvent.track_id == t.id)
            .order_by(music_models.ListeningEvent.listened_at.desc())
            .limit(5)
        )

        dur_str = None
        if t.duration_ms:
            mins = t.duration_ms // 60000
            secs = (t.duration_ms % 60000) // 1000
            dur_str = f"{mins}m{secs:02d}s"

        tracks_list.append(
            {
                "id": str(t.id),
                "title": t.title,
                "artist": t.artist.name if t.artist else None,
                "duration_ms": t.duration_ms,
                "duration": dur_str,
                "service_links": t.service_links,
                "events_by_service": {row[0]: row[1] for row in ev_result.all()},
                "recent_events": [
                    {
                        "listened_at": row[0].isoformat(),
                        "service": row[1],
                    }
                    for row in recent.all()
                ],
            }
        )

    return {"query": query, "results": tracks_list}


async def enqueue_dedup(
    request: fastapi.Request,
    db: sa_async.AsyncSession,
    operation: str,
) -> dict[str, str]:
    """Create a BULK_JOB task and enqueue it to arq."""
    task = task_models.Task(
        task_type=types_module.TaskType.BULK_JOB,
        status=types_module.SyncStatus.PENDING,
        params={"operation": operation},
        description=operation.replace("_", " ").title(),
    )
    db.add(task)
    await db.commit()
    task_id = str(task.id)

    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "run_bulk_job",
        task_id,
        _job_id=f"bulk:{task_id}",
    )
    return {"task_id": task_id, "status": "started"}


# ---------------------------------------------------------------------------
# API route endpoints
# ---------------------------------------------------------------------------


@router.get(
    "/status",
    summary="Recent sync job overview",
)
async def admin_status_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_sync_status(db)


@router.get(
    "/stats",
    summary="Database statistics",
)
async def admin_stats_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_db_stats(db)


@router.get(
    "/tasks/{task_id}",
    summary="Task detail",
)
async def admin_task_endpoint(
    task_id: uuid.UUID,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_task_detail(db, task_id)


@router.get(
    "/tracks",
    summary="Track search",
)
async def admin_tracks_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str = "",
) -> dict[str, object]:
    return await search_tracks(db, q)


@router.post(
    "/dedup/{operation}",
    summary="Enqueue dedup job",
)
async def admin_dedup_endpoint(
    operation: str,
    request: fastapi.Request,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    if operation not in _VALID_DEDUP_OPERATIONS:
        raise fastapi.HTTPException(
            status_code=400,
            detail=(
                f"Invalid operation '{operation}'."
                f" Must be one of: {', '.join(sorted(_VALID_DEDUP_OPERATIONS))}"
            ),
        )
    return await enqueue_dedup(request, db, f"dedup_{operation}")


@router.post(
    "/backfill-mbids",
    summary="Enqueue MusicBrainz MBID backfill",
)
async def admin_backfill_mbids_endpoint(
    request: fastapi.Request,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    retry: bool = False,
    entity_types: Annotated[list[str] | None, fastapi.Query()] = None,
) -> dict[str, str]:
    """Enqueue an MBID_BACKFILL task (#71).

    Query params: ``retry`` re-attempts prior no_match/below_similarity rows;
    ``entity_types`` (repeatable) limits to "track" and/or "artist" (default both).
    """
    params: dict[str, object] = {}
    if entity_types:
        params["entity_types"] = entity_types
    if retry:
        params["retry"] = True
    task = task_models.Task(
        task_type=types_module.TaskType.MBID_BACKFILL,
        status=types_module.SyncStatus.PENDING,
        params=params,
        description="MBID Backfill",
    )
    db.add(task)
    await db.commit()
    task_id = str(task.id)

    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "backfill_mbids",
        task_id,
        _job_id=f"mbid-backfill:{task_id}",
    )
    return {"task_id": task_id, "status": "started"}


@router.get(
    "/backfill-mbids",
    summary="MusicBrainz MBID backfill coverage",
)
async def admin_backfill_coverage_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_backfill_coverage(db)


async def get_popularity_coverage(db: sa_async.AsyncSession) -> dict[str, object]:
    """Popularity-backfill coverage: MB-recording-linked tracks vs. scored tracks.

    ``mb_linked`` is how many tracks carry a MusicBrainz recording MBID at
    ``service_links["musicbrainz"]["id"]`` (the backfill candidate set, since LB
    popularity is keyed by recording MBID); ``scored`` is how many of those
    already have a ``popularity_score``.
    """
    mb_link = music_models.Track.service_links["musicbrainz"]["id"].as_string()
    linked = await db.scalar(
        sa.select(sa.func.count())
        .select_from(music_models.Track)
        .where(mb_link.isnot(None))
    )
    scored = await db.scalar(
        sa.select(sa.func.count())
        .select_from(music_models.Track)
        .where(
            mb_link.isnot(None),
            music_models.Track.popularity_score.isnot(None),
        )
    )
    return {"mb_linked": linked or 0, "scored": scored or 0}


@router.post(
    "/backfill-popularity",
    summary="Enqueue ListenBrainz popularity backfill",
)
async def admin_backfill_popularity_endpoint(
    request: fastapi.Request,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Enqueue a POPULARITY_BACKFILL task.

    Refreshes ``Track.popularity_score`` from ListenBrainz recording popularity
    (global listen counts, normalized to 0-100) for every track carrying a
    MusicBrainz recording MBID, overwriting discovery-sourced synthetic values.
    """
    task = task_models.Task(
        task_type=types_module.TaskType.POPULARITY_BACKFILL,
        status=types_module.SyncStatus.PENDING,
        params={},
        description="Popularity Backfill",
    )
    db.add(task)
    await db.commit()
    task_id = str(task.id)

    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "backfill_popularity",
        task_id,
        _job_id=f"popularity-backfill:{task_id}",
    )
    return {"task_id": task_id, "status": "started"}


@router.get(
    "/backfill-popularity",
    summary="ListenBrainz popularity backfill coverage",
)
async def admin_backfill_popularity_coverage_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_popularity_coverage(db)


# ---------------------------------------------------------------------------
# Legacy endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/test/connect",
    summary="Connect test service",
)
async def connect_test_service(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Admin-only: instantly connect the test service."""
    existing = (
        await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_id,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.TEST,
            )
        )
    ).scalar_one_or_none()

    if existing:
        return {"status": "already_connected"}

    settings = request.app.state.settings
    connection = user_models.ServiceConnection(
        user_id=user_id,
        service_type=types_module.ServiceType.TEST,
        external_user_id="test",
        encrypted_access_token=crypto_module.encrypt_token(
            "test-token", settings.token_encryption_key
        ),
    )
    db.add(connection)
    await db.commit()

    return {"status": "connected"}
