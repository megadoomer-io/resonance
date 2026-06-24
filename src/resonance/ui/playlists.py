"""Playlist routes: list, detail, new, generating, exporting."""

from __future__ import annotations

import datetime
import json
import uuid
from typing import Annotated

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.api.v1.generators as generators_api
import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.types as types_module
import resonance.ui.common as common
import resonance.ui.filters as filters_module
import resonance.ui.htmx as htmx
import resonance.ui.view_filters as view_filters_module

router = fastapi.APIRouter(tags=["ui"])


# ---------------------------------------------------------------------------
# Playlist list
# ---------------------------------------------------------------------------


@router.get("/playlists", response_model=None)
async def playlists_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Render paginated playlists list with filtering."""
    offset = common.page_offset(page)

    params = dict(request.query_params)
    presets = view_filters_module.PLAYLIST_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    applied = filters_module.parse_filter_params(
        view_filters_module.PLAYLIST_FILTERS, params
    )

    query = sa.select(playlist_models.Playlist).where(
        playlist_models.Playlist.user_id == user_id
    )

    query = filters_module.apply_filters(
        query, view_filters_module.PLAYLIST_FILTERS, params
    )

    query = (
        query.order_by(playlist_models.Playlist.created_at.desc())
        .offset(offset)
        .limit(common.PAGE_SIZE + 1)
    )

    result = await db.execute(query)
    playlists = list(result.scalars().all())

    has_next = len(playlists) > common.PAGE_SIZE
    playlists = playlists[: common.PAGE_SIZE]

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

    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.PLAYLIST_FILTERS
    )
    q_value = params.get("q", "").strip()
    if q_value:
        if filter_qs:
            filter_qs += f"&q={q_value}"
        else:
            filter_qs = f"q={q_value}"

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

    ctx = common.base_context(request)
    ctx.update(
        playlists=playlists,
        page=page,
        has_next=has_next,
        has_prev=page > 1,
        active_filters=template_active_filters,
        presets=presets,
        filters=view_filters_module.PLAYLIST_TEMPLATE_FILTERS,
        active_preset=active_preset,
        list_url="/playlists",
        list_target="#playlist-list",
        filter_qs=filter_qs,
    )

    return htmx.render_fragment(
        request,
        common.templates,
        partial_template="partials/playlist_list.html",
        full_template="playlists.html",
        context=ctx,
    )


# ---------------------------------------------------------------------------
# New playlist
# ---------------------------------------------------------------------------


async def _new_playlist_context(
    request: fastapi.Request,
    db: sa_async.AsyncSession,
    *,
    selected_event_id: str = "",
    selected_type: str = "",
    error: str | None = None,
) -> dict[str, object]:
    """Build the template context for the lineup builder (GET and error re-render).

    The builder fetches an event's artists and searches artists live via the API,
    so the page only needs the upcoming-events list for the "Add event" dropdown
    plus the parameter and generator-type registries.
    """
    import resonance.generators.parameters as params_module

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

    ctx = common.base_context(request)
    ctx.update(
        events=events,
        generator_types=params_module.GENERATOR_TYPE_CONFIG,
        parameters=params_module.PARAMETER_REGISTRY,
        selected_event_id=selected_event_id,
        selected_type=selected_type or "",
        error=error,
    )
    return ctx


async def _default_playlist_name(
    db: sa_async.AsyncSession, input_references: dict[str, object]
) -> str:
    """Derive a playlist name when the user leaves it blank.

    Names from the first event source's event (the common concert-prep case),
    otherwise falls back to a timestamped label.
    """
    sources = input_references.get("sources")
    first_event_id: str | None = None
    if isinstance(sources, list):
        for source in sources:
            if isinstance(source, dict) and source.get("kind") == "event":
                event_id = source.get("event_id")
                if event_id:
                    first_event_id = str(event_id)
                    break

    if first_event_id is not None:
        try:
            event = await db.get(concert_models.Event, uuid.UUID(first_event_id))
        except ValueError:
            event = None
        if event is not None:
            venue = (
                await db.get(concert_models.Venue, event.venue_id)
                if (event.venue_id)
                else None
            )
            venue_str = f" @ {venue.name}" if venue else ""
            return f"Concert Prep: {event.title}{venue_str}"

    now_str = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M")
    return f"Playlist {now_str}"


@router.get("/playlists/new", response_model=None)
async def new_playlist_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    event_id: str = "",
    type: str = "",
) -> fastapi.responses.HTMLResponse:
    """Render the lineup builder (New Playlist) page."""
    ctx = await _new_playlist_context(
        request, db, selected_event_id=event_id, selected_type=type
    )
    return common.templates.TemplateResponse(request, "playlists_new.html", ctx)


@router.post("/playlists/new", response_model=None)
async def create_playlist(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.Response:
    """Handle lineup builder submission.

    The builder serializes the lineup into a single ``input_references_json``
    hidden field carrying the layered source spec
    (``{"sources": [...], "exclude_artist_ids": [...]}``). This replaces the old
    single-event ``{"event_id": ...}`` shape (#128). Validation reuses the same
    ``validate_profile_inputs`` path as the JSON API so the UI and CLI/API agree
    on what a valid pool is.
    """
    form = await request.form()

    gen_type = str(form.get("generator_type", "") or "concert_prep")
    max_tracks = int(str(form.get("max_tracks", "30")) or "30")
    name = str(form.get("name", "")).strip()
    raw_json = str(form.get("input_references_json", "")).strip()

    param_values: dict[str, int] = {}
    for key, val in form.items():
        if key.startswith("param_"):
            param_values[key[6:]] = int(str(val))

    try:
        gen_type_enum = types_module.GeneratorType(gen_type)
    except ValueError:
        gen_type_enum = types_module.GeneratorType.CONCERT_PREP

    try:
        parsed = json.loads(raw_json) if raw_json else {}
    except json.JSONDecodeError:
        parsed = None
    input_references: dict[str, object] = parsed if isinstance(parsed, dict) else {}

    try:
        generators_api.validate_profile_inputs(input_references, gen_type_enum)
    except ValueError as exc:
        ctx = await _new_playlist_context(
            request, db, selected_type=gen_type, error=str(exc)
        )
        return common.templates.TemplateResponse(
            request, "playlists_new.html", ctx, status_code=400
        )

    if not name:
        name = await _default_playlist_name(db, input_references)

    profile = generator_models.GeneratorProfile(
        user_id=user_id,
        name=name,
        generator_type=gen_type_enum,
        input_references=input_references,
        parameter_values=param_values,
    )
    db.add(profile)
    await db.flush()

    task = task_models.Task(
        user_id=user_id,
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


# ---------------------------------------------------------------------------
# Generation status
# ---------------------------------------------------------------------------


@router.get("/playlists/generating/{task_id}", response_model=None)
async def generating_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    task_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse:
    """Render the playlist generation status page."""
    result = await db.execute(
        sa.select(task_models.Task).where(task_models.Task.id == task_id)
    )
    task = result.scalar_one_or_none()

    if task is None:
        raise fastapi.HTTPException(status_code=404, detail="Task not found")

    ctx = common.base_context(request)
    ctx.update(
        task_id=str(task_id),
        playlist_name=task.description or "New Playlist",
    )

    return common.templates.TemplateResponse(request, "playlists_generating.html", ctx)


@router.get("/partials/generating-status/{task_id}", response_model=None)
async def generating_status_partial(
    request: fastapi.Request,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    task_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse:
    """Polled partial for playlist generation progress."""
    result = await db.execute(
        sa.select(task_models.Task).where(task_models.Task.id == task_id)
    )
    task = result.scalar_one_or_none()

    if task is None:
        return fastapi.responses.HTMLResponse("<p>Task not found</p>")

    playlist_id = None
    if task.status == types_module.SyncStatus.COMPLETED:
        playlist_id = (task.result or {}).get("playlist_id")

    return common.templates.TemplateResponse(
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


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------


@router.post("/playlists/{playlist_id}/export", response_model=None)
async def export_playlist_submit(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    playlist_id: uuid.UUID,
) -> fastapi.responses.RedirectResponse:
    """Handle export form submission. Enqueue export tasks and redirect."""
    form = await request.form()
    connection_id = form.get("connection_id")

    playlist_result = await db.execute(
        sa.select(playlist_models.Playlist).where(
            playlist_models.Playlist.id == playlist_id,
            playlist_models.Playlist.user_id == user_id,
        )
    )
    playlist = playlist_result.scalar_one_or_none()
    if playlist is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

    if connection_id:
        conn_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id == uuid.UUID(str(connection_id)),
                user_models.ServiceConnection.user_id == user_id,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.SPOTIFY,
            )
        )
        connections = list(conn_result.scalars().all())
    else:
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
            status_code=400, detail="No Spotify connections found"
        )

    task_ids: list[str] = []
    for conn in connections:
        task = task_models.Task(
            id=uuid.uuid4(),
            user_id=user_id,
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    playlist_id: uuid.UUID,
    task_ids: str = "",
) -> fastapi.responses.HTMLResponse:
    """Render the playlist export status page."""
    result = await db.execute(
        sa.select(playlist_models.Playlist).where(
            playlist_models.Playlist.id == playlist_id
        )
    )
    playlist = result.scalar_one_or_none()

    playlist_name = playlist.name if playlist else "Playlist"

    ctx = common.base_context(request)
    ctx.update(
        playlist_id=str(playlist_id),
        playlist_name=playlist_name,
        task_ids=task_ids,
    )

    return common.templates.TemplateResponse(request, "playlists_exporting.html", ctx)


@router.get("/partials/export-status/{playlist_id}", response_model=None)
async def export_status_partial(
    request: fastapi.Request,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    playlist_id: uuid.UUID,
    task_ids: str = "",
) -> fastapi.responses.HTMLResponse:
    """Polled partial for playlist export progress."""
    task_id_list = [tid.strip() for tid in task_ids.split(",") if tid.strip()]

    task_results: list[dict[str, object]] = []
    all_completed = True
    any_failed = False

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
            task_info["spotify_playlist_id"] = task_result.get("spotify_playlist_id")
        elif task.status == types_module.SyncStatus.FAILED:
            task_info["error"] = task.error_message or "Unknown error"
            any_failed = True
            all_completed = False
        else:
            all_completed = False

        task_results.append(task_info)

    if not task_results:
        all_completed = False

    return common.templates.TemplateResponse(
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


# ---------------------------------------------------------------------------
# Playlist detail
# ---------------------------------------------------------------------------


@router.get("/playlists/{playlist_id}", response_model=None)
async def playlist_detail_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    playlist_id: uuid.UUID,
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Render playlist detail with tracks and generation metadata."""
    offset = common.page_offset(page)

    playlist_result = await db.execute(
        sa.select(playlist_models.Playlist).where(
            playlist_models.Playlist.id == playlist_id,
            playlist_models.Playlist.user_id == user_id,
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
        .limit(common.PAGE_SIZE + 1)
    )
    tracks = list(tracks_result.scalars().unique().all())

    has_next = len(tracks) > common.PAGE_SIZE
    tracks = tracks[: common.PAGE_SIZE]

    gen_result = await db.execute(
        sa.select(generator_models.GenerationRecord)
        .where(generator_models.GenerationRecord.playlist_id == playlist_id)
        .options(sa_orm.joinedload(generator_models.GenerationRecord.profile))
        .limit(1)
    )
    generation = gen_result.scalar_one_or_none()

    spotify_conn_result = await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_id,
            user_models.ServiceConnection.service_type
            == types_module.ServiceType.SPOTIFY,
        )
    )
    spotify_connections = list(spotify_conn_result.scalars().all())

    ctx = common.base_context(request)
    ctx.update(
        playlist=playlist,
        playlist_id=playlist_id,
        tracks=tracks,
        generation=generation,
        spotify_connections=spotify_connections,
        page=page,
        has_next=has_next,
        has_prev=page > 1,
    )

    return htmx.render_fragment(
        request,
        common.templates,
        partial_template="partials/playlist_detail_tracks.html",
        full_template="playlist_detail.html",
        context=ctx,
    )
