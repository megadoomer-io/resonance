from __future__ import annotations

import datetime
import pathlib
import uuid
import zoneinfo
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import fastapi
import fastapi.requests
import fastapi.responses
import fastapi.templating
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.dependencies as deps_module
import resonance.merge as merge_module
import resonance.middleware.session as session_module
import resonance.models.music as music_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

_PAGE_SIZE = 50

_TEMPLATE_DIR = pathlib.Path(__file__).resolve().parent.parent / "templates"
templates = fastapi.templating.Jinja2Templates(directory=str(_TEMPLATE_DIR))


def _localtime(
    value: datetime.datetime | None,
    tz_name: str | None,
) -> datetime.datetime | None:
    """Convert a UTC datetime to the user's local timezone."""
    if value is None:
        return None
    if tz_name is None:
        return value
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except KeyError, zoneinfo.ZoneInfoNotFoundError:
        return value
    return value.astimezone(tz)


templates.env.filters["localtime"] = _localtime

router = fastapi.APIRouter(tags=["ui"])


@asynccontextmanager
async def _get_db(
    request: fastapi.Request,
) -> AsyncIterator[sa_async.AsyncSession]:
    """Yield a DB session from the app's session factory."""
    factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = (
        request.app.state.session_factory
    )
    async with factory() as db:
        yield db


async def _count(
    db: sa_async.AsyncSession,
    model: type[sa.orm.DeclarativeBase],
    *filters: sa.ColumnElement[bool],
) -> int:
    """Return the row count for *model*, optionally filtered."""
    stmt = sa.select(sa.func.count()).select_from(model)
    for f in filters:
        stmt = stmt.where(f)
    result = await db.execute(stmt)
    return int(result.scalar_one())


def _user_tz(request: fastapi.Request) -> str | None:
    """Return the user's timezone from session, or None."""
    return request.state.session.get("user_tz")  # type: ignore[no-any-return]


def _user_role(request: fastapi.Request) -> str:
    """Return the user's role from session, defaulting to 'user'."""
    return request.state.session.get("user_role", "user")  # type: ignore[no-any-return]


@router.get("/login", response_class=fastapi.responses.HTMLResponse)
async def login(request: fastapi.Request) -> fastapi.responses.HTMLResponse:
    """Render the login page."""
    return templates.TemplateResponse(request, "login.html")


@router.get("/", response_model=None)
async def dashboard(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render dashboard with stats and sync controls, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        artist_count = await _count(db, music_models.Artist)
        track_count = await _count(db, music_models.Track)
        event_count = await _count(
            db,
            music_models.ListeningEvent,
            music_models.ListeningEvent.user_id == user_uuid,
        )

        connections_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_uuid
            )
        )
        connections: Sequence[user_models.ServiceConnection] = (
            connections_result.scalars().all()
        )

        latest_sync_result = await db.execute(
            sa.select(task_models.SyncTask)
            .where(
                task_models.SyncTask.user_id == user_uuid,
                task_models.SyncTask.task_type == types_module.SyncTaskType.SYNC_JOB,
            )
            .order_by(task_models.SyncTask.created_at.desc())
            .limit(1)
        )
        latest_sync: task_models.SyncTask | None = (
            latest_sync_result.scalar_one_or_none()
        )

        # Build active sync lookup for conditional button
        active_syncs: dict[str, task_models.SyncTask] = {}
        for conn in connections:
            active_stmt = sa.select(task_models.SyncTask).where(
                task_models.SyncTask.user_id == user_uuid,
                task_models.SyncTask.service_connection_id == conn.id,
                task_models.SyncTask.task_type == types_module.SyncTaskType.SYNC_JOB,
                task_models.SyncTask.status.in_(
                    [
                        types_module.SyncStatus.PENDING,
                        types_module.SyncStatus.RUNNING,
                        types_module.SyncStatus.DEFERRED,
                    ]
                ),
            )
            active_result = await db.execute(active_stmt)
            active_task = active_result.scalar_one_or_none()
            if active_task is not None:
                active_syncs[str(conn.id)] = active_task

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "artist_count": artist_count,
            "track_count": track_count,
            "event_count": event_count,
            "connections": connections,
            "latest_sync": latest_sync,
            "active_syncs": active_syncs,
        },
    )


@router.get("/artists", response_model=None)
async def artists_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated artists list, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(music_models.Artist)
            .order_by(music_models.Artist.name)
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        artists = list(result.scalars().all())

    has_next = len(artists) > _PAGE_SIZE
    artists = artists[:_PAGE_SIZE]

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "artists": artists,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/artist_list.html", context)
    return templates.TemplateResponse(request, "artists.html", context)


@router.get("/tracks", response_model=None)
async def tracks_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated tracks list with artist names, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(music_models.Track)
            .join(music_models.Artist)
            .order_by(music_models.Track.title)
            .options(sa_orm.joinedload(music_models.Track.artist))
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        tracks = list(result.scalars().unique().all())

    has_next = len(tracks) > _PAGE_SIZE
    tracks = tracks[:_PAGE_SIZE]

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "tracks": tracks,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/track_list.html", context)
    return templates.TemplateResponse(request, "tracks.html", context)


@router.get("/history", response_model=None)
async def history_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated listening history, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(music_models.ListeningEvent)
            .where(music_models.ListeningEvent.user_id == user_uuid)
            .order_by(music_models.ListeningEvent.listened_at.desc())
            .options(
                sa_orm.joinedload(music_models.ListeningEvent.track).joinedload(
                    music_models.Track.artist
                )
            )
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        events = list(result.scalars().unique().all())

    has_next = len(events) > _PAGE_SIZE
    events = events[:_PAGE_SIZE]

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "events": events,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/history_list.html", context
        )
    return templates.TemplateResponse(request, "history.html", context)


@router.get("/account", response_model=None)
async def account_page(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render account page with profile and connection management."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        user_result = await db.execute(
            sa.select(user_models.User).where(user_models.User.id == user_uuid)
        )
        user = user_result.scalar_one_or_none()

        if user is None:
            # User was deleted (e.g., merged into another account).
            # Clear the stale session and redirect to login.
            request.state.session.clear()
            return fastapi.responses.RedirectResponse(url="/login", status_code=307)

        connections_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_uuid
            )
        )
        connections: Sequence[user_models.ServiceConnection] = (
            connections_result.scalars().all()
        )

    return templates.TemplateResponse(
        request,
        "account.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "user": user,
            "connections": connections,
        },
    )


@router.get("/partials/sync-status", response_model=None)
async def sync_status_partial(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Return the sync status partial for HTMX polling."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        sync_jobs_result = await db.execute(
            sa.select(task_models.SyncTask)
            .where(
                task_models.SyncTask.user_id == user_uuid,
                task_models.SyncTask.task_type == types_module.SyncTaskType.SYNC_JOB,
            )
            .order_by(task_models.SyncTask.created_at.desc())
            .options(
                sa_orm.joinedload(task_models.SyncTask.service_connection),
                sa_orm.subqueryload(task_models.SyncTask.children),
            )
            .limit(5)
        )
        sync_jobs: Sequence[task_models.SyncTask] = sync_jobs_result.scalars().all()

        # Aggregate progress from children into parent for display
        for job in sync_jobs:
            if job.status in (
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
                types_module.SyncStatus.DEFERRED,
            ):
                child_progress_result = await db.execute(
                    sa.select(sa.func.sum(task_models.SyncTask.progress_current)).where(
                        task_models.SyncTask.parent_id == job.id
                    )
                )
                child_total = child_progress_result.scalar_one_or_none()
                if child_total is not None:
                    job.progress_current = int(child_total)

    has_active_sync = any(
        j.status
        in (
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.RUNNING,
            types_module.SyncStatus.DEFERRED,
        )
        for j in sync_jobs
    )

    return templates.TemplateResponse(
        request,
        "partials/sync_status.html",
        {
            "user_tz": _user_tz(request),
            "sync_jobs": sync_jobs,
            "has_active_sync": has_active_sync,
            "now": datetime.datetime.now(datetime.UTC),
        },
    )


@router.get("/merge", response_model=None)
async def merge_page(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render merge confirmation page with source account data summary."""
    session = request.state.session
    user_id = session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    source_user_id = session.get("merge_source_user_id")
    if not source_user_id:
        return fastapi.responses.RedirectResponse(url="/account", status_code=307)

    async with _get_db(request) as db:
        source_summary = await merge_module.get_account_summary(
            db, uuid.UUID(source_user_id)
        )

    service_type = session.get("merge_service_type", "unknown")
    return templates.TemplateResponse(
        request,
        "merge.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "source_summary": source_summary,
            "service_type": service_type,
        },
    )


@router.post("/merge", response_model=None)
async def merge_confirm(
    request: fastapi.Request,
) -> fastapi.responses.RedirectResponse:
    """Execute account merge and redirect to account page."""
    session = request.state.session
    user_id = session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    source_user_id = session.get("merge_source_user_id")
    if not source_user_id:
        return fastapi.responses.RedirectResponse(url="/account", status_code=307)

    async with _get_db(request) as db:
        await merge_module.merge_accounts(
            db, uuid.UUID(user_id), uuid.UUID(source_user_id)
        )
        await db.commit()

    # Invalidate all sessions belonging to the deleted source user so other
    # devices aren't left with a dangling user_id reference.
    redis: session_module.RedisClient = request.app.state.redis
    await session_module.invalidate_user_sessions(redis, source_user_id)

    session["merge_source_user_id"] = None
    session["merge_service_type"] = None
    session["merge_connection_id"] = None

    # 303 See Other — browser follows redirect with GET (not POST)
    return fastapi.responses.RedirectResponse(url="/account", status_code=303)


@router.get("/admin", response_model=None)
async def admin_dashboard(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render admin dashboard with user management controls."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_role = _user_role(request)
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        user_count = await _count(db, user_models.User)
        users_result = await db.execute(
            sa.select(user_models.User).order_by(user_models.User.created_at)
        )
        users: Sequence[user_models.User] = users_result.scalars().all()

        tasks_result = await db.execute(
            sa.select(task_models.SyncTask)
            .where(task_models.SyncTask.parent_id.is_(None))
            .order_by(task_models.SyncTask.created_at.desc())
            .options(
                sa_orm.joinedload(task_models.SyncTask.service_connection).joinedload(
                    user_models.ServiceConnection.user
                )
            )
            .limit(20)
        )
        tasks: Sequence[task_models.SyncTask] = tasks_result.scalars().unique().all()

    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": user_role,
            "user_count": user_count,
            "users": users,
            "tasks": tasks,
        },
    )


@router.post("/admin/users/{target_user_id}/role", response_model=None)
async def change_user_role(
    target_user_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.RedirectResponse:
    """Change a user's role (admin/owner only)."""
    user_id = request.state.session.get("user_id")
    user_role = _user_role(request)
    if not user_id or user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    form = await request.form()
    new_role_str = form.get("role", "user")

    if user_role != "owner" and new_role_str == "owner":
        raise fastapi.HTTPException(
            status_code=403, detail="Only owner can promote to owner"
        )

    if str(target_user_id) == user_id:
        raise fastapi.HTTPException(
            status_code=400, detail="Cannot change your own role"
        )

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(user_models.User).where(user_models.User.id == target_user_id)
        )
        target_user = result.scalar_one_or_none()
        if target_user is None:
            raise fastapi.HTTPException(status_code=404)

        target_user.role = types_module.UserRole(str(new_role_str))
        await db.commit()

    return fastapi.responses.RedirectResponse(url="/admin", status_code=303)


@router.post("/admin/tasks/{task_id}/clone", response_model=None)
async def clone_task(
    task_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.RedirectResponse:
    """Clone a sync task, optionally enabling step-through mode."""
    user_id = request.state.session.get("user_id")
    user_role = _user_role(request)
    if not user_id or user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    form = await request.form()
    step_mode = form.get("step_mode") == "true"

    async with _get_db(request) as db:
        original = (
            await db.execute(
                sa.select(task_models.SyncTask).where(
                    task_models.SyncTask.id == task_id
                )
            )
        ).scalar_one_or_none()
        if original is None:
            raise fastapi.HTTPException(status_code=404)

        params = dict(original.params or {})
        if step_mode:
            params["step_mode"] = True

        cloned = task_models.SyncTask(
            user_id=uuid.UUID(user_id),
            service_connection_id=original.service_connection_id,
            task_type=original.task_type,
            params=params,
            status=types_module.SyncStatus.PENDING,
            progress_total=original.progress_total,
        )
        db.add(cloned)
        await db.commit()

        # Enqueue via arq if available (not present in web-only mode)
        arq_redis = getattr(request.app.state, "arq_redis", None)
        if arq_redis:
            job_name = (
                "plan_sync"
                if original.task_type == types_module.SyncTaskType.SYNC_JOB
                else "sync_range"
            )
            await arq_redis.enqueue_job(
                job_name, str(cloned.id), _job_id=f"{job_name}:{cloned.id}"
            )

    return fastapi.responses.RedirectResponse(url="/admin", status_code=303)


@router.post("/admin/tasks/{task_id}/resume", response_model=None)
async def resume_task(
    task_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.RedirectResponse:
    """Resume a deferred step-mode task."""
    user_id = request.state.session.get("user_id")
    user_role = _user_role(request)
    if not user_id or user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    async with _get_db(request) as db:
        task = (
            await db.execute(
                sa.select(task_models.SyncTask).where(
                    task_models.SyncTask.id == task_id
                )
            )
        ).scalar_one_or_none()
        if task is None:
            raise fastapi.HTTPException(status_code=404)

        arq_redis = getattr(request.app.state, "arq_redis", None)

        if task.status == types_module.SyncStatus.DEFERRED:
            # Resume a deferred task directly
            import time

            task.status = types_module.SyncStatus.PENDING
            await db.commit()
            if arq_redis:
                job_id = f"sync_range:{task.id}:{int(time.time())}"
                await arq_redis.enqueue_job(
                    "sync_range",
                    str(task.id),
                    _job_id=job_id,
                )
        else:
            # Step mode: find the next pending child or sibling
            # If this is a parent (SYNC_JOB), look for pending children
            # If this is a child, look for pending siblings
            parent_id = task.id if task.parent_id is None else task.parent_id
            next_task = (
                await db.execute(
                    sa.select(task_models.SyncTask)
                    .where(
                        task_models.SyncTask.parent_id == parent_id,
                        task_models.SyncTask.status == types_module.SyncStatus.PENDING,
                    )
                    .order_by(task_models.SyncTask.created_at)
                    .limit(1)
                )
            ).scalar_one_or_none()
            if next_task is None:
                # No more pending tasks — complete the parent
                if task.parent_id is None:
                    task.status = types_module.SyncStatus.COMPLETED
                    task.completed_at = datetime.datetime.now(datetime.UTC)
                    await db.commit()
                return fastapi.responses.RedirectResponse(url="/admin", status_code=303)
            if arq_redis:
                # Use a unique job ID to avoid arq dedup with previous runs
                import time

                job_id = f"sync_range:{next_task.id}:{int(time.time())}"
                await arq_redis.enqueue_job(
                    "sync_range",
                    str(next_task.id),
                    _job_id=job_id,
                )

    return fastapi.responses.RedirectResponse(url="/admin", status_code=303)


@router.post("/admin/dedup-events", response_model=None)
async def dedup_listening_events(
    request: fastapi.Request,
) -> dict[str, int | str]:
    """Admin-only: remove duplicate cross-service listening events.

    Finds events where the same user listened to the same track on
    different services within a dedup window (track duration + 60s,
    or 10 minutes when duration is unknown). Keeps the earliest event,
    deletes the rest.
    """
    deps_module.verify_admin_access(request)

    async with _get_db(request) as db:
        # Find IDs of duplicate events to delete.
        # For each pair of events on the same track by the same user
        # from different services within the dedup window, mark the
        # later one for deletion.
        result = await db.execute(
            sa.text(
                "DELETE FROM listening_events "
                "WHERE id IN ("
                "  SELECT e2.id "
                "  FROM listening_events e1 "
                "  JOIN listening_events e2 "
                "    ON e1.track_id = e2.track_id "
                "    AND e1.user_id = e2.user_id "
                "    AND e1.source_service != e2.source_service "
                "    AND e1.listened_at < e2.listened_at "
                "  JOIN tracks t ON e1.track_id = t.id "
                "  WHERE e2.listened_at - e1.listened_at < "
                "    CASE "
                "      WHEN t.duration_ms IS NOT NULL "
                "      THEN make_interval(secs => t.duration_ms / 1000 + 60) "
                "      ELSE interval '10 minutes' "
                "    END"
                ")"
            )
        )
        deleted = result.rowcount if hasattr(result, "rowcount") else 0
        await db.commit()

    return {"status": "completed", "events_deleted": deleted}


@router.post("/admin/dedup-artists", response_model=None)
async def dedup_artists(
    request: fastapi.Request,
) -> dict[str, int | str]:
    """Admin-only: merge duplicate artist records."""
    deps_module.verify_admin_access(request)

    import resonance.dedup as dedup_module

    async with _get_db(request) as db:
        stats = await dedup_module.find_and_merge_duplicate_artists(db)

    return {
        "status": "completed",
        "artists_merged": stats.artists_merged,
        "tracks_repointed": stats.tracks_repointed,
        "relations_repointed": stats.artist_relations_repointed,
        "relations_deleted": stats.artist_relations_deleted,
    }


@router.post("/admin/dedup-tracks", response_model=None)
async def dedup_tracks(
    request: fastapi.Request,
) -> dict[str, int | str]:
    """Admin-only: merge duplicate track records."""
    deps_module.verify_admin_access(request)

    import resonance.dedup as dedup_module

    async with _get_db(request) as db:
        stats = await dedup_module.find_and_merge_duplicate_tracks(db)

    return {
        "status": "completed",
        "tracks_merged": stats.tracks_merged,
        "events_repointed": stats.events_repointed,
        "relations_repointed": stats.track_relations_repointed,
        "relations_deleted": stats.track_relations_deleted,
    }


@router.get("/admin/status", response_model=None)
async def admin_status(
    request: fastapi.Request,
) -> dict[str, object]:
    """Admin-only: overview of recent sync tasks."""
    deps_module.verify_admin_access(request)

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(task_models.SyncTask)
            .where(task_models.SyncTask.parent_id.is_(None))
            .order_by(task_models.SyncTask.created_at.desc())
            .options(
                sa_orm.joinedload(task_models.SyncTask.service_connection),
                sa_orm.joinedload(task_models.SyncTask.children),
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


@router.get("/admin/stats", response_model=None)
async def admin_stats(
    request: fastapi.Request,
) -> dict[str, object]:
    """Admin-only: database statistics overview."""
    deps_module.verify_admin_access(request)

    async with _get_db(request) as db:
        artists = await _count(db, music_models.Artist)
        tracks_total = await _count(db, music_models.Track)
        events_total = await _count(db, music_models.ListeningEvent)

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


@router.get("/admin/track", response_model=None)
async def admin_track_search(
    request: fastapi.Request,
    q: str = "",
) -> dict[str, object]:
    """Admin-only: search tracks by title (fuzzy match)."""
    deps_module.verify_admin_access(request)

    if not q.strip():
        return {"error": "Query parameter 'q' is required."}

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(music_models.Track)
            .options(sa_orm.joinedload(music_models.Track.artist))
            .where(sa.func.lower(music_models.Track.title).contains(q.strip().lower()))
            .order_by(music_models.Track.title)
            .limit(20)
        )
        tracks = result.scalars().unique().all()

        tracks_list: list[dict[str, object]] = []
        for t in tracks:
            # Event counts per service
            ev_result = await db.execute(
                sa.select(
                    music_models.ListeningEvent.source_service,
                    sa.func.count(),
                )
                .where(music_models.ListeningEvent.track_id == t.id)
                .group_by(music_models.ListeningEvent.source_service)
            )
            # Recent events
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

    return {"query": q, "results": tracks_list}
