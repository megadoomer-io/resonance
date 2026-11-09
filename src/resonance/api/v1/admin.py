"""Admin API routes."""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING, Annotated, Any

import fastapi
import sqlalchemy as sa
import sqlalchemy.exc as sa_exc
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.crypto as crypto_module
import resonance.database as database_module
import resonance.dependencies as deps_module
import resonance.models.music as music_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.services.artist_mbid as artist_mbid_module
import resonance.services.dev_seed as dev_seed_module
import resonance.sync.lifecycle as lifecycle_module
import resonance.sync.reaper as reaper_module
import resonance.types as types_module

if TYPE_CHECKING:
    import arq

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
        **await _mbid_duplicate_counts(db),
    }


async def _mbid_duplicate_counts(db: sa_async.AsyncSession) -> dict[str, int]:
    groups = await artist_mbid_module.find_mbid_duplicate_groups(db)
    return {
        "mbid_duplicate_artist_groups": len(groups),
        "mbid_duplicate_artist_groups_names_differ": sum(
            1 for g in groups if g.names_differ
        ),
    }


def _artist_json(artist: artist_mbid_module.DuplicateArtist) -> dict[str, str]:
    return {
        "id": str(artist.id),
        "name": artist.name,
        "created_at": artist.created_at.isoformat(),
    }


async def get_mbid_duplicate_artists(db: sa_async.AsyncSession) -> dict[str, object]:
    """Artists sharing an MBID, and same-name artists with different MBIDs (#203).

    ``groups`` with ``names_differ`` are not merged by dedup (manual review).
    ``name_conflicts`` are same-name artists the name dedup won't merge because
    their MBIDs differ.
    """
    groups = await artist_mbid_module.find_mbid_duplicate_groups(db)
    conflicts = await artist_mbid_module.find_name_conflicts(db)
    return {
        "group_count": len(groups),
        "groups": [
            {
                "mbid": group.mbid,
                "count": group.count,
                "names_differ": group.names_differ,
                "artists": [_artist_json(a) for a in group.artists],
            }
            for group in groups
        ],
        "name_conflicts": [
            {
                "name": conflict.name,
                "artists": [
                    {**_artist_json(artist), "mbid": mbid}
                    for artist, mbid in conflict.artists
                ],
            }
            for conflict in conflicts
        ],
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


_HEALTH_RUNNING_LIMIT = 50


def _utc(value: datetime.datetime | None) -> datetime.datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=datetime.UTC)


def _iso(value: datetime.datetime | None) -> str | None:
    aware = _utc(value)
    return aware.isoformat() if aware else None


def _age_seconds(then: datetime.datetime | None, now: datetime.datetime) -> int | None:
    aware = _utc(then)
    if aware is None:
        return None
    return max(0, int((now - aware).total_seconds()))


async def get_tasks_health(
    db: sa_async.AsyncSession,
    redis: arq.ArqRedis,
    now: datetime.datetime | None = None,
) -> dict[str, object]:
    """Reaper liveness, queue backlog, running tasks and recent reaper decisions."""
    now = now or datetime.datetime.now(datetime.UTC)
    task = task_models.Task

    counts: dict[str, int] = {s.value: 0 for s in types_module.SyncStatus}
    for status, count in (
        await db.execute(sa.select(task.status, sa.func.count()).group_by(task.status))
    ).all():
        counts[types_module.SyncStatus(status).value] = count

    oldest = (
        await db.execute(
            sa.select(task)
            .where(
                task.status.in_(
                    (types_module.SyncStatus.PENDING, types_module.SyncStatus.DEFERRED)
                )
            )
            .order_by(task.created_at)
            .limit(1)
        )
    ).scalar_one_or_none()

    running = (
        (
            await db.execute(
                sa.select(task)
                .where(task.status == types_module.SyncStatus.RUNNING)
                .order_by(task.started_at)
                .limit(_HEALTH_RUNNING_LIMIT)
            )
        )
        .scalars()
        .all()
    )

    state = await reaper_module.read_state(redis)
    stale_after = 2 * reaper_module.INTERVAL
    last_tick_at: datetime.datetime | None = None
    if state.last_tick is not None:
        last_tick_at = datetime.datetime.fromisoformat(str(state.last_tick["at"]))
    tick_age = _age_seconds(last_tick_at, now)

    return {
        "reaper": {
            "last_tick_at": _iso(last_tick_at),
            "age_seconds": tick_age,
            "stale": tick_age is None or tick_age > stale_after.total_seconds(),
            "stale_after_seconds": int(stale_after.total_seconds()),
            "last_candidates": (state.last_tick or {}).get("candidates"),
            "last_redelivered": (state.last_tick or {}).get("redelivered"),
            "last_lease_expired": (state.last_tick or {}).get("lease_expired"),
            "last_lease_recovered": (state.last_tick or {}).get("lease_recovered"),
            "last_holder_frozen": (state.last_tick or {}).get("holder_frozen"),
            "last_holder_terminated": (state.last_tick or {}).get("holder_terminated"),
        },
        "oldest_pending": (
            {
                "task_id": str(oldest.id),
                "task_type": oldest.task_type.value,
                "status": oldest.status.value,
                "created_at": _iso(oldest.created_at),
                "age_seconds": _age_seconds(oldest.created_at, now),
            }
            if oldest is not None
            else None
        ),
        "counts": counts,
        "running": [
            {
                "task_id": str(t.id),
                "task_type": t.task_type.value,
                "started_at": _iso(t.started_at),
                "age_seconds": _age_seconds(t.started_at, now),
                "progress_current": t.progress_current,
                "progress_total": t.progress_total,
            }
            for t in running
        ],
        "recent_decisions": state.decisions,
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
    await lifecycle_module.enqueue_task(
        db, request.app.state.arq_redis, task, "run_bulk_job", str(task.id)
    )
    task_id = str(task.id)
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
    "/artists/mbid-duplicates",
    summary="Artists sharing a MusicBrainz MBID",
    description=(
        "Read-only. Groups artists by MBID, read from"
        " service_links.musicbrainz.id or else the legacy flat"
        " service_links.listenbrainz string, trimmed and lowercased."
        " Artists without an MBID are ignored. Merge with POST"
        " /admin/dedup/artists."
    ),
)
async def admin_mbid_duplicates_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_mbid_duplicate_artists(db)


@router.get(
    "/tasks/health",
    summary="Task queue health",
    description=(
        "Reaper last tick (stale past twice its interval), oldest pending or"
        " deferred task, task counts for every status, running tasks with age"
        " and progress, and the most recent reaper decisions."
    ),
)
async def admin_tasks_health_endpoint(
    request: fastapi.Request,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_tasks_health(db, request.app.state.arq_redis)


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
    await lifecycle_module.enqueue_task(
        db, request.app.state.arq_redis, task, "backfill_mbids", str(task.id)
    )
    task_id = str(task.id)
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
    await lifecycle_module.enqueue_task(
        db, request.app.state.arq_redis, task, "backfill_popularity", str(task.id)
    )
    task_id = str(task.id)
    return {"task_id": task_id, "status": "started"}


@router.get(
    "/backfill-popularity",
    summary="ListenBrainz popularity backfill coverage",
)
async def admin_backfill_popularity_coverage_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_popularity_coverage(db)


async def get_genre_coverage(db: sa_async.AsyncSession) -> dict[str, object]:
    """Genre-tag backfill coverage over MBID-bearing artists (#136).

    ``mb_linked`` is how many artists carry a MusicBrainz artist MBID at
    ``service_links["musicbrainz"]["id"]`` (the backfill candidate set);
    ``attempted`` is how many of those have ``genre_attempted_at`` set; ``with_tags``
    is how many have at least one ArtistTag row. The gap between ``attempted`` and
    ``with_tags`` is the "attempted, no tags" set (genuinely tag-less on MB),
    distinct from the unattempted remainder (``mb_linked - attempted``).
    """
    mb_link = music_models.Artist.service_links["musicbrainz"]["id"].as_string()
    linked = await db.scalar(
        sa.select(sa.func.count())
        .select_from(music_models.Artist)
        .where(mb_link.isnot(None))
    )
    attempted = await db.scalar(
        sa.select(sa.func.count())
        .select_from(music_models.Artist)
        .where(
            mb_link.isnot(None),
            music_models.Artist.genre_attempted_at.isnot(None),
        )
    )
    with_tags = await db.scalar(
        sa.select(sa.func.count(sa.distinct(music_models.ArtistTag.artist_id)))
        .select_from(music_models.ArtistTag)
        .join(
            music_models.Artist,
            music_models.Artist.id == music_models.ArtistTag.artist_id,
        )
        .where(mb_link.isnot(None))
    )
    return {
        "mb_linked": linked or 0,
        "attempted": attempted or 0,
        "with_tags": with_tags or 0,
    }


@router.post(
    "/backfill-genres",
    summary="Enqueue artist genre-tag backfill",
)
async def admin_backfill_genres_endpoint(
    request: fastapi.Request,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Enqueue a GENRE_BACKFILL task (#136).

    Fetches MusicBrainz genre/folksonomy tags for every MBID-bearing artist from
    ListenBrainz's public artist metadata endpoint and stores them in artist_tags.
    """
    task = task_models.Task(
        task_type=types_module.TaskType.GENRE_BACKFILL,
        status=types_module.SyncStatus.PENDING,
        params={},
        description="Genre Backfill",
    )
    db.add(task)
    await lifecycle_module.enqueue_task(
        db, request.app.state.arq_redis, task, "backfill_genres", str(task.id)
    )
    task_id = str(task.id)
    return {"task_id": task_id, "status": "started"}


@router.get(
    "/backfill-genres",
    summary="Artist genre-tag backfill coverage",
)
async def admin_backfill_genres_coverage_endpoint(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    return await get_genre_coverage(db)


def _require_dev_seed(request: fastapi.Request) -> None:
    # A dependency, so a disabled route 404s before the body is validated.
    if not request.app.state.settings.dev_seed_enabled:
        raise fastapi.HTTPException(status_code=404, detail="Not Found")


@router.post(
    "/seed",
    dependencies=[fastapi.Depends(_require_dev_seed)],
    summary="Seed a test scenario (dev environments only)",
    description=(
        "Creates a user (or uses the owner), artists with arbitrary"
        " service_links, tags, similarity edges, tracks and listens from one"
        " JSON document. 404 unless DEV_SEED_ENABLED is set. Constraint"
        " violations roll back the whole scenario (409)."
    ),
)
async def admin_seed_endpoint(
    scenario: dev_seed_module.SeedScenario,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    try:
        return await dev_seed_module.seed(db, scenario)
    except sa_exc.IntegrityError as exc:
        await db.rollback()
        raise fastapi.HTTPException(
            status_code=409, detail=f"Constraint violation: {exc.orig}"
        ) from exc


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
