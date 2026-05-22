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
import httpx
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.merge as merge_module
import resonance.middleware.session as session_module
import resonance.models.concert as concert_models
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module
import resonance.ui.filters as filters_module
import resonance.ui.view_filters as view_filters_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

_PAGE_SIZE = 50


def _escape_ilike(q: str) -> str:
    return q.replace("%", r"\%").replace("_", r"\_")


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
            sa.select(task_models.Task)
            .where(
                task_models.Task.user_id == user_uuid,
                task_models.Task.task_type.in_(
                    [
                        types_module.TaskType.SYNC_JOB,
                        types_module.TaskType.CALENDAR_SYNC,
                        types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
                    ]
                ),
            )
            .order_by(task_models.Task.created_at.desc())
            .limit(1)
        )
        latest_sync: task_models.Task | None = latest_sync_result.scalar_one_or_none()

        # Build active sync lookup — single query for all connection types
        conn_ids = [conn.id for conn in connections]
        active_syncs: dict[str, task_models.Task] = {}
        if conn_ids:
            active_stmt = sa.select(task_models.Task).where(
                task_models.Task.user_id == user_uuid,
                task_models.Task.service_connection_id.in_(conn_ids),
                task_models.Task.status.in_(
                    [
                        types_module.SyncStatus.PENDING,
                        types_module.SyncStatus.RUNNING,
                        types_module.SyncStatus.DEFERRED,
                    ]
                ),
            )
            active_result = await db.execute(active_stmt)
            for active_task in active_result.scalars().all():
                active_syncs[str(active_task.service_connection_id)] = active_task

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


# Artists, tracks, and history routes moved to ui/artists.py and ui/tracks.py


@router.get("/playlists", response_model=None)
async def playlists_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated playlists list with filtering."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * _PAGE_SIZE

    # Parse filter parameters
    params = dict(request.query_params)
    presets = view_filters_module.PLAYLIST_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    # Parse filters to get active_filters for template context
    applied = filters_module.parse_filter_params(
        view_filters_module.PLAYLIST_FILTERS, params
    )

    async with _get_db(request) as db:
        query = sa.select(playlist_models.Playlist).where(
            playlist_models.Playlist.user_id == user_uuid
        )

        # Apply registered filter fields (name, created, tracks)
        query = filters_module.apply_filters(
            query, view_filters_module.PLAYLIST_FILTERS, params
        )

        query = (
            query.order_by(playlist_models.Playlist.created_at.desc())
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )

        result = await db.execute(query)
        playlists = list(result.scalars().all())

        has_next = len(playlists) > _PAGE_SIZE
        playlists = playlists[:_PAGE_SIZE]

        playlist_ids = [p.id for p in playlists]
        gen_type_map: dict[uuid.UUID, str] = {}
        if playlist_ids:
            gen_result = await db.execute(
                sa.select(
                    generator_models.GenerationRecord.playlist_id,
                    generator_models.GeneratorProfile.generator_type,
                )
                .join(
                    generator_models.GeneratorProfile,
                    generator_models.GenerationRecord.profile_id
                    == generator_models.GeneratorProfile.id,
                )
                .where(generator_models.GenerationRecord.playlist_id.in_(playlist_ids))
            )
            gen_type_map = {row[0]: row[1].value for row in gen_result.all()}

        for p in playlists:
            p._generator_type = gen_type_map.get(p.id)  # type: ignore[attr-defined]

    # Build filter query string for pagination links
    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.PLAYLIST_FILTERS
    )
    # Include quick search in filter_qs
    q_value = params.get("q", "").strip()
    if q_value:
        if filter_qs:
            filter_qs += f"&q={q_value}"
        else:
            filter_qs = f"q={q_value}"

    # Build flat active_filters dict for the template
    template_active_filters: dict[str, object] = {}
    for key, value in applied.active_filters.items():
        if isinstance(value, dict):
            for dk, dv in value.items():
                if dv is not None:
                    template_active_filters[dk] = str(dv)
        else:
            template_active_filters[key] = value
    if q_value:
        template_active_filters["q"] = q_value

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "playlists": playlists,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
        "active_filters": template_active_filters,
        "presets": presets,
        "filters": view_filters_module.PLAYLIST_TEMPLATE_FILTERS,
        "active_preset": active_preset,
        "list_url": "/playlists",
        "list_target": "#playlist-list",
        "filter_qs": filter_qs,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/playlist_list.html", context
        )
    return templates.TemplateResponse(request, "playlists.html", context)


@router.get("/playlists/new", response_model=None)
async def new_playlist_page(
    request: fastapi.Request,
    event_id: str = "",
    type: str = "",
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render the New Playlist form."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    import resonance.generators.parameters as params_module

    async with _get_db(request) as db:
        events_result = await db.execute(
            sa.select(concert_models.Event)
            .where(concert_models.Event.event_date >= datetime.date.today())
            .options(
                sa_orm.joinedload(concert_models.Event.venue),
                sa_orm.subqueryload(concert_models.Event.artists),
            )
            .order_by(concert_models.Event.event_date)
        )
        all_events = events_result.unique().scalars().all()
        events = [e for e in all_events if len(e.artists) > 0]

    return templates.TemplateResponse(
        request,
        "playlists_new.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "events": events,
            "generator_types": params_module.GENERATOR_TYPE_CONFIG,
            "parameters": params_module.PARAMETER_REGISTRY,
            "selected_event_id": event_id,
            "selected_type": type or "",
        },
    )


@router.post("/playlists/new", response_model=None)
async def create_playlist(
    request: fastapi.Request,
) -> fastapi.responses.RedirectResponse:
    """Handle New Playlist form submission."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    form = await request.form()

    gen_type = str(form.get("generator_type", ""))
    event_id_str = str(form.get("event_id", ""))
    max_tracks = int(str(form.get("max_tracks", "30")))
    name = str(form.get("name", "")).strip()

    param_values: dict[str, int] = {}
    for key, val in form.items():
        if key.startswith("param_"):
            param_name = key[6:]
            param_values[param_name] = int(str(val))

    if not name:
        async with _get_db(request) as db:
            event_result = await db.execute(
                sa.select(concert_models.Event)
                .where(concert_models.Event.id == uuid.UUID(event_id_str))
                .options(sa_orm.joinedload(concert_models.Event.venue))
            )
            event = event_result.unique().scalar_one_or_none()
            if event:
                venue_str = f" @ {event.venue.name}" if event.venue else ""
                name = f"Concert Prep: {event.title}{venue_str}"
            else:
                now_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M")
                name = f"Playlist {now_str}"

    async with _get_db(request) as db:
        profile = generator_models.GeneratorProfile(
            user_id=user_uuid,
            name=name,
            generator_type=types_module.GeneratorType(gen_type),
            input_references={"event_id": event_id_str},
            parameter_values=param_values,
        )
        db.add(profile)
        await db.flush()

        task = task_models.Task(
            user_id=user_uuid,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={
                "profile_id": str(profile.id),
                "max_tracks": max_tracks,
            },
            description=name,
        )
        db.add(task)
        await db.commit()

    arq_redis = request.app.state.arq_redis
    await arq_redis.enqueue_job(
        "generate_playlist",
        str(task.id),
        _job_id=f"generate_playlist:{task.id}",
    )

    return fastapi.responses.RedirectResponse(
        url=f"/playlists/generating/{task.id}", status_code=303
    )


@router.get("/playlists/generating/{task_id}", response_model=None)
async def generating_page(
    request: fastapi.Request,
    task_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render the playlist generation status page."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(task_models.Task).where(task_models.Task.id == task_id)
        )
        task = result.scalar_one_or_none()

    if task is None:
        raise fastapi.HTTPException(status_code=404, detail="Task not found")

    return templates.TemplateResponse(
        request,
        "playlists_generating.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "task_id": str(task_id),
            "playlist_name": task.description or "New Playlist",
        },
    )


@router.get("/partials/generating-status/{task_id}", response_model=None)
async def generating_status_partial(
    request: fastapi.Request,
    task_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse:
    """Polled partial for playlist generation progress."""
    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(task_models.Task).where(task_models.Task.id == task_id)
        )
        task = result.scalar_one_or_none()

    if task is None:
        return fastapi.responses.HTMLResponse("<p>Task not found</p>")

    playlist_id = None
    if task.status == types_module.SyncStatus.COMPLETED:
        playlist_id = (task.result or {}).get("playlist_id")

    return templates.TemplateResponse(
        request,
        "partials/playlist_generating_status.html",
        {
            "task_id": str(task_id),
            "status": task.status.value,
            "playlist_id": playlist_id,
            "description": task.description,
            "progress_current": task.progress_current,
            "progress_total": task.progress_total,
            "error": task.error_message,
        },
    )


@router.post("/playlists/{playlist_id}/export", response_model=None)
async def export_playlist_submit(
    request: fastapi.Request,
    playlist_id: uuid.UUID,
) -> fastapi.responses.RedirectResponse:
    """Handle export form submission.

    Enqueue export tasks and redirect to status page.
    """
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    form = await request.form()
    connection_id = form.get("connection_id")

    async with _get_db(request) as db:
        # Verify playlist
        playlist_result = await db.execute(
            sa.select(playlist_models.Playlist).where(
                playlist_models.Playlist.id == playlist_id,
                playlist_models.Playlist.user_id == user_uuid,
            )
        )
        playlist = playlist_result.scalar_one_or_none()
        if playlist is None:
            raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

        # Determine connections
        if connection_id:
            conn_result = await db.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.id == uuid.UUID(str(connection_id)),
                    user_models.ServiceConnection.user_id == user_uuid,
                    user_models.ServiceConnection.service_type
                    == types_module.ServiceType.SPOTIFY,
                )
            )
            connections = list(conn_result.scalars().all())
        else:
            conn_result = await db.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.user_id == user_uuid,
                    user_models.ServiceConnection.service_type
                    == types_module.ServiceType.SPOTIFY,
                )
            )
            connections = list(conn_result.scalars().all())

        if not connections:
            raise fastapi.HTTPException(
                status_code=400, detail="No Spotify connections found"
            )

        # Create tasks
        task_ids: list[str] = []
        for conn in connections:
            task = task_models.Task(
                id=uuid.uuid4(),
                user_id=user_uuid,
                task_type=types_module.TaskType.PLAYLIST_EXPORT,
                status=types_module.SyncStatus.PENDING,
                params={
                    "playlist_id": str(playlist_id),
                    "connection_id": str(conn.id),
                },
                description=f"Export to Spotify ({conn.external_user_id or 'account'})",
            )
            db.add(task)
            task_ids.append(str(task.id))
        await db.commit()

    # Enqueue tasks
    arq_redis = request.app.state.arq_redis
    for tid in task_ids:
        await arq_redis.enqueue_job(
            "export_playlist",
            tid,
            _job_id=f"export_playlist:{tid}",
        )

    task_ids_param = ",".join(task_ids)
    return fastapi.responses.RedirectResponse(
        url=f"/playlists/exporting/{playlist_id}?task_ids={task_ids_param}",
        status_code=303,
    )


@router.get("/playlists/exporting/{playlist_id}", response_model=None)
async def export_status_page(
    request: fastapi.Request,
    playlist_id: uuid.UUID,
    task_ids: str = "",
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render the playlist export status page."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(playlist_models.Playlist).where(
                playlist_models.Playlist.id == playlist_id
            )
        )
        playlist = result.scalar_one_or_none()

    playlist_name = playlist.name if playlist else "Playlist"

    return templates.TemplateResponse(
        request,
        "playlists_exporting.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "playlist_id": str(playlist_id),
            "playlist_name": playlist_name,
            "task_ids": task_ids,
        },
    )


@router.get("/playlists/{playlist_id}", response_model=None)
async def playlist_detail_page(
    request: fastapi.Request,
    playlist_id: uuid.UUID,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render playlist detail with tracks and generation metadata."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        playlist_result = await db.execute(
            sa.select(playlist_models.Playlist).where(
                playlist_models.Playlist.id == playlist_id,
                playlist_models.Playlist.user_id == user_uuid,
            )
        )
        playlist = playlist_result.scalar_one_or_none()

        if playlist is None:
            raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

        tracks_result = await db.execute(
            sa.select(playlist_models.PlaylistTrack)
            .where(playlist_models.PlaylistTrack.playlist_id == playlist_id)
            .order_by(playlist_models.PlaylistTrack.position)
            .options(
                sa_orm.joinedload(playlist_models.PlaylistTrack.track).joinedload(
                    music_models.Track.artist
                )
            )
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        tracks = list(tracks_result.scalars().unique().all())

        has_next = len(tracks) > _PAGE_SIZE
        tracks = tracks[:_PAGE_SIZE]

        gen_result = await db.execute(
            sa.select(generator_models.GenerationRecord)
            .where(generator_models.GenerationRecord.playlist_id == playlist_id)
            .options(sa_orm.joinedload(generator_models.GenerationRecord.profile))
            .limit(1)
        )
        generation = gen_result.scalar_one_or_none()

        # Load Spotify connections for export section
        spotify_conn_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_uuid,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.SPOTIFY,
            )
        )
        spotify_connections = list(spotify_conn_result.scalars().all())

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "playlist": playlist,
        "playlist_id": playlist_id,
        "tracks": tracks,
        "generation": generation,
        "spotify_connections": spotify_connections,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/playlist_detail_tracks.html", context
        )
    return templates.TemplateResponse(request, "playlist_detail.html", context)


@router.get("/partials/export-status/{playlist_id}", response_model=None)
async def export_status_partial(
    request: fastapi.Request,
    playlist_id: uuid.UUID,
    task_ids: str = "",
) -> fastapi.responses.HTMLResponse:
    """Polled partial for playlist export progress."""
    task_id_list = [tid.strip() for tid in task_ids.split(",") if tid.strip()]

    task_results: list[dict[str, object]] = []
    all_completed = True
    any_failed = False

    async with _get_db(request) as db:
        for tid in task_id_list:
            result = await db.execute(
                sa.select(task_models.Task).where(task_models.Task.id == uuid.UUID(tid))
            )
            task = result.scalar_one_or_none()
            if task is None:
                continue

            task_info: dict[str, object] = {
                "description": task.description or "Export",
                "status": task.status.value,
            }

            if task.status == types_module.SyncStatus.COMPLETED:
                task_result = task.result or {}
                task_info["exported"] = task_result.get("exported", 0)
                task_info["skipped"] = task_result.get("skipped", 0)
                task_info["spotify_playlist_id"] = task_result.get(
                    "spotify_playlist_id"
                )
            elif task.status == types_module.SyncStatus.FAILED:
                task_info["error"] = task.error_message or "Unknown error"
                any_failed = True
                all_completed = False
            else:
                all_completed = False

            task_results.append(task_info)

    if not task_results:
        all_completed = False

    return templates.TemplateResponse(
        request,
        "partials/playlist_export_status.html",
        {
            "playlist_id": str(playlist_id),
            "task_ids": task_ids,
            "task_results": task_results,
            "all_completed": all_completed,
            "any_failed": any_failed,
        },
    )


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
            "state": "button",
        },
    )


@router.get("/partials/songkick-connect", response_model=None)
async def songkick_connect_button(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Return the Songkick connect button partial."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    return templates.TemplateResponse(
        request,
        "partials/songkick_connect.html",
        {"state": "button"},
    )


@router.get("/partials/songkick-lookup", response_model=None)
async def songkick_lookup_form(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Return the Songkick username lookup form partial."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    return templates.TemplateResponse(
        request,
        "partials/songkick_connect.html",
        {"state": "form"},
    )


@router.post("/partials/songkick-lookup", response_model=None)
async def songkick_lookup_submit(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Validate a Songkick username and return confirm or error state."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    form = await request.form()
    username = str(form.get("username", "")).strip()
    if not username:
        return templates.TemplateResponse(
            request,
            "partials/songkick_connect.html",
            {"state": "error", "error_message": "Please enter a username."},
        )

    base = f"https://www.songkick.com/users/{username}/calendars.ics"
    try:
        async with httpx.AsyncClient() as client:
            att_resp = await client.get(f"{base}?filter=attendance")
            att_resp.raise_for_status()
            trk_resp = await client.get(f"{base}?filter=tracked_artist")
            trk_resp.raise_for_status()
    except httpx.HTTPStatusError:
        return templates.TemplateResponse(
            request,
            "partials/songkick_connect.html",
            {"state": "error"},
        )
    except httpx.ConnectError:
        return templates.TemplateResponse(
            request,
            "partials/songkick_connect.html",
            {
                "state": "error",
                "error_message": (
                    "Could not connect to Songkick. Please try again later."
                ),
            },
        )

    return templates.TemplateResponse(
        request,
        "partials/songkick_connect.html",
        {
            "state": "confirm",
            "username": username,
            "plans_count": att_resp.text.count("BEGIN:VEVENT"),
            "tracked_artist_count": trk_resp.text.count("BEGIN:VEVENT"),
        },
    )


@router.post("/partials/songkick-confirm", response_model=None)
async def songkick_confirm(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Create a Songkick ServiceConnection and reload the page."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    form = await request.form()
    username = str(form.get("username", "")).strip()
    if not username:
        return fastapi.responses.HTMLResponse("")

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        # Check for duplicate Songkick connection with same username
        dup_stmt = sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_uuid,
            user_models.ServiceConnection.service_type
            == types_module.ServiceType.SONGKICK,
            user_models.ServiceConnection.external_user_id == username,
        )
        dup_result = await db.execute(dup_stmt)
        if dup_result.scalar_one_or_none() is not None:
            msg = "Songkick connection already exists for this username."
            return fastapi.responses.HTMLResponse(f"<p><mark>{msg}</mark></p>")

        conn = user_models.ServiceConnection(
            user_id=user_uuid,
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id=username,
            enabled=True,
        )
        db.add(conn)
        await db.commit()

    return fastapi.responses.HTMLResponse("<script>location.reload()</script>")


@router.post("/partials/songkick-sync/{username}", response_model=None)
async def songkick_sync_trigger(
    username: str, request: fastapi.Request
) -> fastapi.responses.HTMLResponse:
    """Trigger sync for a Songkick connection by username."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        raise fastapi.HTTPException(status_code=401)

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_uuid,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.SONGKICK,
                user_models.ServiceConnection.external_user_id == username,
            )
        )
        connection = result.scalar_one_or_none()
        if connection is None:
            raise fastapi.HTTPException(status_code=404)

        task = task_models.Task(
            user_id=user_uuid,
            service_connection_id=connection.id,
            task_type=types_module.TaskType.CALENDAR_SYNC,
            status=types_module.SyncStatus.PENDING,
        )
        db.add(task)
        await db.flush()

        arq_redis = request.app.state.arq_redis
        await arq_redis.enqueue_job(
            "sync_calendar_feed",
            str(connection.id),
            str(task.id),
            _job_id=f"sync_calendar_feed:{task.id}",
        )
        await db.commit()

    return fastapi.responses.HTMLResponse("")


@router.get("/partials/concert-archives-connect", response_model=None)
async def concert_archives_connect_button(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Return the Concert Archives connect button partial."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    return templates.TemplateResponse(
        request,
        "partials/concert_archives_connect.html",
        {"state": "button"},
    )


@router.get("/partials/concert-archives-upload", response_model=None)
async def concert_archives_upload_form(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Return the Concert Archives CSV upload form partial."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("")

    return templates.TemplateResponse(
        request,
        "partials/concert_archives_connect.html",
        {"state": "form"},
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
            sa.select(task_models.Task)
            .where(
                task_models.Task.user_id == user_uuid,
                task_models.Task.task_type.in_(
                    [
                        types_module.TaskType.SYNC_JOB,
                        types_module.TaskType.CALENDAR_SYNC,
                        types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
                    ]
                ),
            )
            .order_by(task_models.Task.created_at.desc())
            .options(
                sa_orm.joinedload(task_models.Task.service_connection),
                sa_orm.subqueryload(task_models.Task.children),
            )
            .limit(5)
        )
        sync_jobs: Sequence[task_models.Task] = sync_jobs_result.scalars().all()

        # Aggregate progress from eagerly-loaded children (no extra queries)
        for job in sync_jobs:
            if job.status in (
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
                types_module.SyncStatus.DEFERRED,
            ):
                child_total = sum(child.progress_current for child in job.children)
                if child_total:
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
