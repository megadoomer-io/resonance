"""Playlist routes: list, detail, new, generating, exporting."""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.api.v1.artists as artists_api
import resonance.api.v1.generators as generators_api
import resonance.dependencies as deps_module
import resonance.generators.pool as pool_module
import resonance.models.concert as concert_models
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.services.playlist_export as playlist_export_module
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


async def _resolve_event_artist_ids(
    db: sa_async.AsyncSession, event_id: uuid.UUID
) -> list[uuid.UUID]:
    """Resolve an event to its artist ids (confirmed + accepted), deduped.

    Mirrors ``worker.resolve_pool`` / the events lineup endpoint so the builder
    renders the same artists generation will use.
    """
    ea_result = await db.execute(
        sa.select(concert_models.EventArtist.artist_id).where(
            concert_models.EventArtist.event_id == event_id
        )
    )
    ordered: list[uuid.UUID] = list(ea_result.scalars().all())
    cand_result = await db.execute(
        sa.select(concert_models.EventArtistCandidate.matched_artist_id).where(
            concert_models.EventArtistCandidate.event_id == event_id,
            concert_models.EventArtistCandidate.status
            == types_module.CandidateStatus.ACCEPTED,
            concert_models.EventArtistCandidate.matched_artist_id.isnot(None),
        )
    )
    for cid in cand_result.scalars().all():
        if cid is not None:
            ordered.append(cid)
    seen: set[uuid.UUID] = set()
    unique: list[uuid.UUID] = []
    for aid in ordered:
        if aid not in seen:
            seen.add(aid)
            unique.append(aid)
    return unique


def _artist_row(summary: dict[str, object], *, included: bool) -> dict[str, object]:
    """Build a builder row from an artist summary."""
    bits = [
        str(summary[k])
        for k in ("disambiguation", "area", "begin_year")
        if summary.get(k)
    ]
    return {
        "id": str(summary["id"]),
        "name": summary.get("name", ""),
        "meta": " · ".join(bits),
        "included": included,
    }


async def _hydrate_lineup(
    db: sa_async.AsyncSession,
    profile: generator_models.GeneratorProfile,
) -> dict[str, object]:
    """Turn a profile's input_references into named, grouped builder rows (#133).

    Groups: one per event source, a manual "Added artists" group, and one
    "related" group per enrichment scope (``via_seed``). Excluded artists keep
    their row with ``included=False``. This is the initial state the client-side
    builder renders and then mutates.
    """
    refs = profile.input_references
    sources = pool_module.normalize_sources(refs)
    excludes = pool_module.extract_excludes(refs)

    event_sources = [
        s for s in sources if isinstance(s, pool_module.EventSource) and s.enabled
    ]
    manual = [
        s
        for s in sources
        if isinstance(s, pool_module.ArtistSource) and s.enabled and s.via_seed is None
    ]
    related: dict[str, list[pool_module.ArtistSource]] = {}
    for s in sources:
        if (
            isinstance(s, pool_module.ArtistSource)
            and s.enabled
            and s.via_seed is not None
        ):
            related.setdefault(s.via_seed, []).append(s)

    # Resolve event -> artist ids, and gather every artist id we must name.
    event_artist_ids: dict[uuid.UUID, list[uuid.UUID]] = {}
    for ev in event_sources:
        event_artist_ids[ev.event_id] = await _resolve_event_artist_ids(db, ev.event_id)

    wanted: set[uuid.UUID] = set()
    for ids in event_artist_ids.values():
        wanted.update(ids)
    wanted.update(s.artist_id for s in manual)
    for lst in related.values():
        wanted.update(s.artist_id for s in lst)
    seed_ids = {
        uuid.UUID(scope) for scope in related if scope != "lineup" and _is_uuid(scope)
    }
    wanted.update(seed_ids)

    summaries: dict[uuid.UUID, dict[str, object]] = {}
    if wanted:
        artist_result = await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id.in_(wanted))
        )
        for artist in artist_result.scalars().all():
            summaries[artist.id] = artists_api.format_artist_summary(artist)

    def row(aid: uuid.UUID) -> dict[str, object] | None:
        s = summaries.get(aid)
        if s is None:
            return None
        return _artist_row(s, included=aid not in excludes)

    groups: list[dict[str, object]] = []

    # Event groups (with date/venue subtitle).
    if event_sources:
        events_result = await db.execute(
            sa.select(concert_models.Event)
            .where(concert_models.Event.id.in_([ev.event_id for ev in event_sources]))
            .options(sa_orm.joinedload(concert_models.Event.venue))
        )
        events_by_id = {e.id: e for e in events_result.unique().scalars().all()}
        for ev in event_sources:
            event = events_by_id.get(ev.event_id)
            title = event.title if event else "Event"
            sub_bits = []
            if event:
                sub_bits.append(str(event.event_date))
                if event.venue:
                    sub_bits.append(event.venue.name)
            sub = "· " + " · ".join([*sub_bits, "live lineup"])
            rows = [
                r
                for aid in event_artist_ids.get(ev.event_id, [])
                if (r := row(aid)) is not None
            ]
            groups.append(
                {
                    "kind": "event",
                    "event_id": str(ev.event_id),
                    "title": title,
                    "sub": sub,
                    "artists": rows,
                }
            )

    if manual:
        groups.append(
            {
                "kind": "manual",
                "title": "Added artists",
                "artists": [r for s in manual if (r := row(s.artist_id)) is not None],
            }
        )

    for scope, lst in related.items():
        if scope == "lineup":
            title = "Related to your lineup"
        else:
            seed = summaries.get(uuid.UUID(scope)) if _is_uuid(scope) else None
            title = f"Related to {seed['name']}" if seed else "Related artists"
        groups.append(
            {
                "kind": "related",
                "scope": scope,
                "title": title,
                "artists": [r for s in lst if (r := row(s.artist_id)) is not None],
            }
        )

    return {
        "profile_id": str(profile.id),
        "version": profile.version,
        "name": profile.name,
        "generator_type": profile.generator_type.value,
        "parameter_values": profile.parameter_values,
        "groups": groups,
    }


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


@router.get("/playlists/new", response_model=None)
async def new_playlist_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    event_id: str = "",
    type: str = "",
) -> fastapi.responses.Response:
    """Eagerly create a draft profile and redirect to the server-backed editor.

    The builder is now a server-backed profile editor (#133): opening "new
    playlist" creates a draft immediately so every edit persists. A preseed
    ``?event_id=`` seeds the pool with that event.
    """
    try:
        gen_type_enum = types_module.GeneratorType(type or "concert_prep")
    except ValueError:
        gen_type_enum = types_module.GeneratorType.CONCERT_PREP

    sources: list[dict[str, object]] = []
    if event_id and _is_uuid(event_id):
        sources.append({"kind": "event", "event_id": event_id, "enabled": True})
    input_references: dict[str, object] = {
        "sources": sources,
        "exclude_artist_ids": [],
    }

    name = (
        await _default_playlist_name(db, input_references)
        if sources
        else "New Playlist"
    )
    profile = generator_models.GeneratorProfile(
        user_id=user_id,
        name=name,
        generator_type=gen_type_enum,
        status=types_module.ProfileStatus.DRAFT,
        input_references=input_references,
        parameter_values={},
    )
    db.add(profile)
    await db.commit()

    return fastapi.responses.RedirectResponse(
        url=f"/playlists/{profile.id}/edit", status_code=303
    )


@router.get("/playlists/{profile_id}/edit", response_model=None)
async def edit_playlist_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    profile_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse:
    """Render the server-backed lineup builder bound to a profile (#133)."""
    result = await db.execute(
        sa.select(generator_models.GeneratorProfile).where(
            generator_models.GeneratorProfile.id == profile_id,
            generator_models.GeneratorProfile.user_id == user_id,
        )
    )
    profile = result.scalar_one_or_none()
    if profile is None:
        raise fastapi.HTTPException(status_code=404, detail="Profile not found")

    ctx = await _new_playlist_context(request, db)
    lineup = await _hydrate_lineup(db, profile)
    # Whether a similar-artists service is connected (drives the enrich hint).
    conn_result = await db.execute(
        sa.select(user_models.ServiceConnection.id).where(
            user_models.ServiceConnection.user_id == user_id,
            user_models.ServiceConnection.service_type.in_(
                [types_module.ServiceType.LISTENBRAINZ, types_module.ServiceType.LASTFM]
            ),
        )
    )
    similar_available = conn_result.first() is not None
    ctx.update(
        profile_id=str(profile.id),
        # Pass the structured lineup and let the template serialize it with the
        # markupsafe-aware `tojson` filter (escapes <, >, & inside the <script>
        # block). Never json.dumps + | safe here — an artist/event name
        # containing </script> would break out (security review #141, #6).
        lineup=lineup,
        profile_name=profile.name,
        parameter_values=profile.parameter_values,
        similar_available=similar_available,
    )
    return common.templates.TemplateResponse(request, "playlists_new.html", ctx)


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


@router.get("/playlists/task-status/{task_id}", response_model=None)
async def task_status_json(
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    task_id: uuid.UUID,
) -> dict[str, object]:
    """User-scoped JSON task status, polled by the builder for enrich/generate."""
    result = await db.execute(
        sa.select(task_models.Task).where(
            task_models.Task.id == task_id,
            task_models.Task.user_id == user_id,
        )
    )
    task = result.scalar_one_or_none()
    if task is None:
        raise fastapi.HTTPException(status_code=404, detail="Task not found")
    return {
        "task_id": str(task.id),
        "status": task.status.value,
        "progress_current": task.progress_current,
        "progress_total": task.progress_total,
        "result": task.result or {},
        "error": task.error_message,
    }


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

    # Don't start a second export for a connection that's already exporting --
    # re-clicking while one is in flight is what produces duplicate Spotify
    # playlists. Land the user on the in-progress page for those instead.
    active_tasks = await playlist_export_module.in_progress_export_tasks(
        db, user_id, playlist_id
    )
    busy_connection_ids = playlist_export_module.export_connection_ids(active_tasks)
    new_connections = [c for c in connections if c.id not in busy_connection_ids]
    existing_task_ids = [str(t.id) for t in active_tasks]

    new_task_ids: list[str] = []
    for conn in new_connections:
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
        new_task_ids.append(str(task.id))
    await db.commit()

    arq_redis = request.app.state.arq_redis
    for tid in new_task_ids:
        await arq_redis.enqueue_job(
            "export_playlist",
            tid,
            _job_id=f"export_playlist:{tid}",
        )

    # Track both the newly-started and already-running tasks so the progress
    # page reflects everything in flight for this playlist.
    task_ids_param = ",".join(new_task_ids + existing_task_ids)
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
        .order_by(generator_models.GenerationRecord.created_at.desc())
        .options(sa_orm.joinedload(generator_models.GenerationRecord.profile))
        .limit(1)
    )
    generation = gen_result.scalar_one_or_none()

    # Refine context (#track-exclude, #spotify-sync-visibility): the profile is the
    # editable recipe; its exclude_track_ids drives which rows render struck-through.
    profile = generation.profile if generation is not None else None
    profile_id = profile.id if profile is not None else None
    excluded_track_ids: set[str] = (
        {str(t) for t in pool_module.extract_track_excludes(profile.input_references)}
        if profile is not None
        else set()
    )
    # How many of this playlist's tracks are confirmed on Spotify (drives the
    # "N of M synced" line and the exclude-unsynced affordance). Counts the whole
    # playlist, not just the current page.
    synced_count_result = await db.execute(
        sa.select(sa.func.count()).where(
            playlist_models.PlaylistTrack.playlist_id == playlist_id,
            playlist_models.PlaylistTrack.spotify_synced_at.isnot(None),
        )
    )
    synced_count = synced_count_result.scalar_one()

    spotify_conn_result = await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_id,
            user_models.ServiceConnection.service_type
            == types_module.ServiceType.SPOTIFY,
        )
    )
    spotify_connections = list(spotify_conn_result.scalars().all())

    # Surface any in-flight export so the page shows "in progress" instead of a
    # plain Export button (which invites a duplicate-creating re-click).
    active_export_tasks = await playlist_export_module.in_progress_export_tasks(
        db, user_id, playlist_id
    )
    export_task_ids = ",".join(str(t.id) for t in active_export_tasks)

    ctx = common.base_context(request)
    ctx.update(
        playlist=playlist,
        playlist_id=playlist_id,
        tracks=tracks,
        generation=generation,
        profile_id=profile_id,
        excluded_track_ids=excluded_track_ids,
        synced_count=synced_count,
        spotify_connections=spotify_connections,
        export_in_progress=bool(active_export_tasks),
        export_task_ids=export_task_ids,
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


async def _playlist_and_profile(
    db: sa_async.AsyncSession,
    user_id: uuid.UUID,
    playlist_id: uuid.UUID,
) -> tuple[playlist_models.Playlist, generator_models.GeneratorProfile]:
    """Load a user's playlist and the recipe (profile) it was generated from.

    Resolves the profile via the latest GenerationRecord. Raises 404 if the
    playlist is not the user's, or if it has no generating profile (e.g. a
    manually-built playlist that can't be refined/regenerated).
    """
    playlist = (
        await db.execute(
            sa.select(playlist_models.Playlist).where(
                playlist_models.Playlist.id == playlist_id,
                playlist_models.Playlist.user_id == user_id,
            )
        )
    ).scalar_one_or_none()
    if playlist is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

    generation = (
        await db.execute(
            sa.select(generator_models.GenerationRecord)
            .where(generator_models.GenerationRecord.playlist_id == playlist_id)
            .order_by(generator_models.GenerationRecord.created_at.desc())
            .options(sa_orm.joinedload(generator_models.GenerationRecord.profile))
            .limit(1)
        )
    ).scalar_one_or_none()
    if generation is None or generation.profile is None:
        raise fastapi.HTTPException(
            status_code=404, detail="This playlist has no editable recipe"
        )
    return playlist, generation.profile


async def _render_tracks_fragment(
    request: fastapi.Request,
    db: sa_async.AsyncSession,
    playlist_id: uuid.UUID,
    profile: generator_models.GeneratorProfile,
    page: int,
) -> fastapi.responses.HTMLResponse:
    """Re-render just the playlist tracks partial after a refine mutation."""
    offset = common.page_offset(page)
    tracks = list(
        (
            await db.execute(
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
        )
        .scalars()
        .unique()
        .all()
    )
    has_next = len(tracks) > common.PAGE_SIZE
    tracks = tracks[: common.PAGE_SIZE]
    excluded_track_ids = {
        str(t) for t in pool_module.extract_track_excludes(profile.input_references)
    }
    ctx = common.base_context(request)
    ctx.update(
        playlist_id=playlist_id,
        tracks=tracks,
        page=page,
        has_next=has_next,
        has_prev=page > 1,
        excluded_track_ids=excluded_track_ids,
        profile_id=profile.id,
    )
    return common.templates.TemplateResponse(
        request, "partials/playlist_detail_tracks.html", ctx
    )


@router.post("/playlists/{playlist_id}/tracks/{track_id}/toggle-exclude")
async def toggle_track_exclude(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    playlist_id: uuid.UUID,
    track_id: uuid.UUID,
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Toggle a track in the recipe's exclude_track_ids (mark/unmark for refill).

    Marking does NOT remove the track from the current playlist -- it is struck
    through until the user regenerates, which refills the freed slot
    (mark-then-regenerate, #track-exclude). Returns the re-rendered tracks partial.
    """
    _playlist, profile = await _playlist_and_profile(db, user_id, playlist_id)
    excluded = pool_module.extract_track_excludes(profile.input_references)
    if track_id in excluded:
        excluded.discard(track_id)
    else:
        excluded.add(track_id)
    profile.input_references = pool_module.with_track_excludes(
        profile.input_references, excluded
    )
    await db.commit()
    return await _render_tracks_fragment(request, db, playlist_id, profile, page)


@router.post("/playlists/{playlist_id}/regenerate", response_model=None)
async def regenerate_playlist(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    playlist_id: uuid.UUID,
    max_tracks: Annotated[int, fastapi.Form()] = 50,
    freshness_target: Annotated[str, fastapi.Form()] = "",
) -> fastapi.responses.RedirectResponse:
    """Regenerate this playlist in place from its recipe (refills excluded slots).

    Re-passes max_tracks/freshness_target (they live on the generation task, not
    the profile) and lands on the generation-progress page.
    """
    _playlist, profile = await _playlist_and_profile(db, user_id, playlist_id)
    fresh = int(freshness_target) if freshness_target.strip() else None
    task_id = await generators_api._trigger_profile_task(
        db=db,
        arq_redis=request.app.state.arq_redis,
        profile_id=profile.id,
        user_id=user_id,
        task_type=types_module.TaskType.PLAYLIST_GENERATION,
        params={
            "profile_id": str(profile.id),
            "freshness_target": fresh,
            "max_tracks": max_tracks,
        },
        job_name="generate_playlist",
    )
    return fastapi.responses.RedirectResponse(
        url=f"/playlists/generating/{task_id}", status_code=303
    )


@router.post(
    "/playlists/{playlist_id}/exclude-unsynced-and-regenerate", response_model=None
)
async def exclude_unsynced_and_regenerate(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    playlist_id: uuid.UUID,
    max_tracks: Annotated[int, fastapi.Form()] = 50,
    freshness_target: Annotated[str, fastapi.Form()] = "",
) -> fastapi.responses.RedirectResponse:
    """Exclude every not-yet-synced track from the recipe, then regenerate.

    The convergence affordance for partial Spotify exports: drop the tracks that
    failed to match, refill from the same pool, and land on the progress page so
    a re-export can sync the new selection. Convergence is best-effort, not
    monotone (a refill can pull in another unmatched track).
    """
    _playlist, profile = await _playlist_and_profile(db, user_id, playlist_id)
    unsynced = (
        (
            await db.execute(
                sa.select(playlist_models.PlaylistTrack.track_id).where(
                    playlist_models.PlaylistTrack.playlist_id == playlist_id,
                    playlist_models.PlaylistTrack.spotify_synced_at.is_(None),
                )
            )
        )
        .scalars()
        .all()
    )
    excluded = pool_module.extract_track_excludes(profile.input_references)
    excluded.update(unsynced)
    profile.input_references = pool_module.with_track_excludes(
        profile.input_references, excluded
    )
    await db.commit()
    fresh = int(freshness_target) if freshness_target.strip() else None
    task_id = await generators_api._trigger_profile_task(
        db=db,
        arq_redis=request.app.state.arq_redis,
        profile_id=profile.id,
        user_id=user_id,
        task_type=types_module.TaskType.PLAYLIST_GENERATION,
        params={
            "profile_id": str(profile.id),
            "freshness_target": fresh,
            "max_tracks": max_tracks,
        },
        job_name="generate_playlist",
    )
    return fastapi.responses.RedirectResponse(
        url=f"/playlists/generating/{task_id}", status_code=303
    )
