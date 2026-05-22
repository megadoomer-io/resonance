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

import resonance.dedup as dedup_module
import resonance.dependencies as deps_module
import resonance.merge as merge_module
import resonance.middleware.session as session_module
import resonance.models.concert as concert_models
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.models.taste as taste_models
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


@router.get("/artists", response_model=None)
async def artists_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated artists list with filtering, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * _PAGE_SIZE

    # Parse filter parameters
    params = dict(request.query_params)
    presets = view_filters_module.ARTIST_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    # Parse filters to get active_filters for template context
    applied = filters_module.parse_filter_params(
        view_filters_module.ARTIST_FILTERS, params
    )

    async with _get_db(request) as db:
        query = sa.select(music_models.Artist)

        # Apply registered filter fields (name, origin, has_events, has_tracks)
        query = filters_module.apply_filters(
            query, view_filters_module.ARTIST_FILTERS, params
        )

        query = (
            query.order_by(music_models.Artist.name)
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )

        result = await db.execute(query)
        artists = list(result.scalars().all())

    has_next = len(artists) > _PAGE_SIZE
    artists = artists[:_PAGE_SIZE]

    # Build filter query string for pagination links
    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.ARTIST_FILTERS
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
        elif isinstance(value, bool):
            template_active_filters[key] = value
        else:
            template_active_filters[key] = value
    if q_value:
        template_active_filters["q"] = q_value

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "artists": artists,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
        "active_filters": template_active_filters,
        "presets": presets,
        "filters": view_filters_module.ARTIST_TEMPLATE_FILTERS,
        "active_preset": active_preset,
        "list_url": "/artists",
        "list_target": "#artist-list",
        "filter_qs": filter_qs,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/artist_list.html", context)
    return templates.TemplateResponse(request, "artists.html", context)


@router.get("/artists/{artist_id}", response_model=None)
async def artist_detail_page(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    page: int = 1,
    section: str = "tracks",
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render artist detail page with tracks, events, candidates, and duplicates."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        artist_result = await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
        artist = artist_result.scalar_one_or_none()

        if artist is None:
            raise fastapi.HTTPException(status_code=404, detail="Artist not found")

        track_count = await _count(
            db, music_models.Track, music_models.Track.artist_id == artist_id
        )

        tracks_result = await db.execute(
            sa.select(music_models.Track)
            .where(music_models.Track.artist_id == artist_id)
            .order_by(music_models.Track.title)
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        tracks = list(tracks_result.scalars().all())
        tracks_has_next = len(tracks) > _PAGE_SIZE
        tracks = tracks[:_PAGE_SIZE]
        tracks_has_prev = page > 1

        events_result = await db.execute(
            sa.select(concert_models.EventArtist)
            .where(concert_models.EventArtist.artist_id == artist_id)
            .options(
                sa_orm.joinedload(concert_models.EventArtist.event).joinedload(
                    concert_models.Event.venue
                )
            )
            .order_by(concert_models.EventArtist.position)
        )
        event_artists = list(events_result.scalars().unique().all())

        candidates_result = await db.execute(
            sa.select(concert_models.EventArtistCandidate)
            .where(
                concert_models.EventArtistCandidate.matched_artist_id == artist_id,
                concert_models.EventArtistCandidate.status
                == types_module.CandidateStatus.PENDING,
            )
            .options(sa_orm.joinedload(concert_models.EventArtistCandidate.event))
        )
        candidates = list(candidates_result.scalars().unique().all())

        duplicates_result = await db.execute(
            sa.select(music_models.Artist).where(
                sa.func.lower(music_models.Artist.name) == sa.func.lower(artist.name),
                music_models.Artist.id != artist_id,
            )
        )
        duplicates = list(duplicates_result.scalars().all())

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "artist": artist,
        "tracks": tracks,
        "track_count": track_count,
        "tracks_has_next": tracks_has_next,
        "tracks_has_prev": tracks_has_prev,
        "event_artists": event_artists,
        "candidates": candidates,
        "duplicates": duplicates,
        "page": page,
        "section": section,
    }

    if request.headers.get("HX-Request") and section == "tracks":
        return templates.TemplateResponse(
            request, "partials/artist_tracks.html", context
        )
    return templates.TemplateResponse(request, "artist_detail.html", context)


@router.get("/tracks", response_model=None)
async def tracks_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated tracks list with artist names and filtering."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    offset = (page - 1) * _PAGE_SIZE

    # Parse filter parameters
    params = dict(request.query_params)
    presets = view_filters_module.TRACK_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    # Parse filters to get active_filters for template context
    applied = filters_module.parse_filter_params(
        view_filters_module.TRACK_FILTERS, params
    )

    async with _get_db(request) as db:
        # Build base query — join Artist for artist name filtering
        query = sa.select(music_models.Track).join(music_models.Artist)

        # Apply registered filter fields (title, artist, recently_played)
        query = filters_module.apply_filters(
            query, view_filters_module.TRACK_FILTERS, params
        )

        query = (
            query.order_by(music_models.Track.title)
            .options(sa_orm.joinedload(music_models.Track.artist))
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )

        result = await db.execute(query)
        tracks = list(result.scalars().unique().all())

    has_next = len(tracks) > _PAGE_SIZE
    tracks = tracks[:_PAGE_SIZE]

    # Build filter query string for pagination links
    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.TRACK_FILTERS
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
        elif isinstance(value, bool):
            template_active_filters[key] = value
        else:
            template_active_filters[key] = value
    if q_value:
        template_active_filters["q"] = q_value

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "tracks": tracks,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
        "active_filters": template_active_filters,
        "presets": presets,
        "filters": view_filters_module.TRACK_TEMPLATE_FILTERS,
        "active_preset": active_preset,
        "list_url": "/tracks",
        "list_target": "#track-list",
        "filter_qs": filter_qs,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/track_list.html", context)
    return templates.TemplateResponse(request, "tracks.html", context)


@router.get("/tracks/{track_id}", response_model=None)
async def track_detail_page(
    request: fastapi.Request,
    track_id: uuid.UUID,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render track detail page with listening history and duplicates."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        track_result = await db.execute(
            sa.select(music_models.Track)
            .where(music_models.Track.id == track_id)
            .options(sa_orm.joinedload(music_models.Track.artist))
        )
        track = track_result.scalar_one_or_none()

        if track is None:
            raise fastapi.HTTPException(status_code=404, detail="Track not found")

        history_result = await db.execute(
            sa.select(music_models.ListeningEvent)
            .where(
                music_models.ListeningEvent.track_id == track_id,
                music_models.ListeningEvent.user_id == user_uuid,
            )
            .order_by(music_models.ListeningEvent.listened_at.desc())
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        history = list(history_result.scalars().all())
        has_next = len(history) > _PAGE_SIZE
        history = history[:_PAGE_SIZE]
        has_prev = page > 1

        duplicates_result = await db.execute(
            sa.select(music_models.Track)
            .where(
                sa.func.lower(music_models.Track.title) == sa.func.lower(track.title),
                music_models.Track.artist_id == track.artist_id,
                music_models.Track.id != track_id,
            )
            .options(sa_orm.joinedload(music_models.Track.artist))
        )
        duplicates = list(duplicates_result.scalars().unique().all())

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "track": track,
        "history": history,
        "duplicates": duplicates,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/track_history.html", context
        )
    return templates.TemplateResponse(request, "track_detail.html", context)


@router.get("/artists/{artist_id}/compare/{other_id}", response_model=None)
async def artist_compare_page(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render side-by-side comparison of two artists with merge controls."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        artist_a_result = await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
        artist_a = artist_a_result.scalar_one_or_none()

        artist_b_result = await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == other_id)
        )
        artist_b = artist_b_result.scalar_one_or_none()

        if artist_a is None or artist_b is None:
            raise fastapi.HTTPException(status_code=404, detail="Artist not found")

        canonical, duplicate = dedup_module.pick_canonical(artist_a, artist_b)

        a_track_count = await _count(
            db, music_models.Track, music_models.Track.artist_id == artist_id
        )
        b_track_count = await _count(
            db, music_models.Track, music_models.Track.artist_id == other_id
        )
        a_event_count = await _count(
            db,
            concert_models.EventArtist,
            concert_models.EventArtist.artist_id == artist_id,
        )
        b_event_count = await _count(
            db,
            concert_models.EventArtist,
            concert_models.EventArtist.artist_id == other_id,
        )

    return templates.TemplateResponse(
        request,
        "artist_compare.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "artist_a": artist_a,
            "artist_b": artist_b,
            "canonical": canonical,
            "duplicate": duplicate,
            "a_track_count": a_track_count,
            "b_track_count": b_track_count,
            "a_event_count": a_event_count,
            "b_event_count": b_event_count,
        },
    )


@router.get("/tracks/{track_id}/compare/{other_id}", response_model=None)
async def track_compare_page(
    request: fastapi.Request,
    track_id: uuid.UUID,
    other_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render side-by-side comparison of two tracks with merge controls."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        track_a_result = await db.execute(
            sa.select(music_models.Track)
            .where(music_models.Track.id == track_id)
            .options(sa_orm.joinedload(music_models.Track.artist))
        )
        track_a = track_a_result.scalar_one_or_none()

        track_b_result = await db.execute(
            sa.select(music_models.Track)
            .where(music_models.Track.id == other_id)
            .options(sa_orm.joinedload(music_models.Track.artist))
        )
        track_b = track_b_result.scalar_one_or_none()

        if track_a is None or track_b is None:
            raise fastapi.HTTPException(status_code=404, detail="Track not found")

        canonical, duplicate = dedup_module.pick_canonical_track(track_a, track_b)

        a_listen_count = await _count(
            db,
            music_models.ListeningEvent,
            music_models.ListeningEvent.track_id == track_id,
            music_models.ListeningEvent.user_id == user_uuid,
        )
        b_listen_count = await _count(
            db,
            music_models.ListeningEvent,
            music_models.ListeningEvent.track_id == other_id,
            music_models.ListeningEvent.user_id == user_uuid,
        )

    return templates.TemplateResponse(
        request,
        "track_compare.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": _user_role(request),
            "track_a": track_a,
            "track_b": track_b,
            "canonical": canonical,
            "duplicate": duplicate,
            "a_listen_count": a_listen_count,
            "b_listen_count": b_listen_count,
        },
    )


@router.post("/tracks/{track_id}/merge-preview/{other_id}", response_model=None)
async def track_merge_preview(
    request: fastapi.Request,
    track_id: uuid.UUID,
    other_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Return track merge impact summary partial for HTMX."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        canonical_result = await db.execute(
            sa.select(music_models.Track).where(music_models.Track.id == track_id)
        )
        canonical = canonical_result.scalar_one_or_none()

        duplicate_result = await db.execute(
            sa.select(music_models.Track).where(music_models.Track.id == other_id)
        )
        duplicate = duplicate_result.scalar_one_or_none()

        if canonical is None or duplicate is None:
            raise fastapi.HTTPException(status_code=404, detail="Track not found")

        events_to_repoint = await _count(
            db,
            music_models.ListeningEvent,
            music_models.ListeningEvent.track_id == other_id,
        )
        relations_to_repoint = await _count(
            db,
            taste_models.UserTrackRelation,
            taste_models.UserTrackRelation.track_id == other_id,
        )
        playlist_appearances = await _count(
            db,
            playlist_models.PlaylistTrack,
            playlist_models.PlaylistTrack.track_id == other_id,
        )

        merged_links = dict(canonical.service_links or {})
        for k, v in (duplicate.service_links or {}).items():
            if v and k not in merged_links:
                merged_links[k] = v

        duration_backfill = None
        if not canonical.duration_ms and duplicate.duration_ms:
            mins = duplicate.duration_ms // 60000
            secs = (duplicate.duration_ms % 60000) // 1000
            duration_backfill = f"{mins}:{secs:02d}"

    return templates.TemplateResponse(
        request,
        "partials/track_merge_preview.html",
        {
            "canonical": canonical,
            "duplicate": duplicate,
            "events_to_repoint": events_to_repoint,
            "relations_to_repoint": relations_to_repoint,
            "playlist_appearances": playlist_appearances,
            "merged_links": merged_links,
            "duration_backfill": duration_backfill,
        },
    )


@router.post("/artists/{artist_id}/merge-preview/{other_id}", response_model=None)
async def artist_merge_preview(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Return merge impact summary partial for HTMX."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        canonical_result = await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
        canonical = canonical_result.scalar_one_or_none()

        duplicate_result = await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == other_id)
        )
        duplicate = duplicate_result.scalar_one_or_none()

        if canonical is None or duplicate is None:
            raise fastapi.HTTPException(status_code=404, detail="Artist not found")

        tracks_to_repoint = await _count(
            db, music_models.Track, music_models.Track.artist_id == other_id
        )
        events_to_repoint = await _count(
            db,
            concert_models.EventArtist,
            concert_models.EventArtist.artist_id == other_id,
        )

        merged_links = dict(canonical.service_links or {})
        for k, v in (duplicate.service_links or {}).items():
            if v and k not in merged_links:
                merged_links[k] = v

    return templates.TemplateResponse(
        request,
        "partials/merge_preview.html",
        {
            "canonical": canonical,
            "duplicate": duplicate,
            "tracks_to_repoint": tracks_to_repoint,
            "events_to_repoint": events_to_repoint,
            "merged_links": merged_links,
        },
    )


@router.get("/history", response_model=None)
async def history_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated listening history with filtering."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * _PAGE_SIZE

    # Parse filter parameters
    params = dict(request.query_params)
    multi_params = {
        "source": request.query_params.getlist("source"),
    }
    presets = view_filters_module.HISTORY_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    # Parse filters to get active_filters for template context
    applied = filters_module.parse_filter_params(
        view_filters_module.HISTORY_FILTERS, params
    )

    async with _get_db(request) as db:
        # Build base query with joins for track/artist filtering
        query = (
            sa.select(music_models.ListeningEvent)
            .join(
                music_models.Track,
                music_models.ListeningEvent.track_id == music_models.Track.id,
            )
            .join(
                music_models.Artist,
                music_models.Track.artist_id == music_models.Artist.id,
            )
            .where(music_models.ListeningEvent.user_id == user_uuid)
        )

        # Apply registered filter fields (track, artist, date)
        query = filters_module.apply_filters(
            query, view_filters_module.HISTORY_FILTERS, params
        )

        # Handle source filter manually (multiselect outside framework)
        source_values = multi_params.get("source", [])
        valid_sources = [
            v for v in source_values if v in ("SPOTIFY", "LISTENBRAINZ", "LASTFM")
        ]
        if valid_sources:
            query = query.where(
                music_models.ListeningEvent.source_service.in_(valid_sources)
            )

        query = (
            query.order_by(music_models.ListeningEvent.listened_at.desc())
            .options(
                sa_orm.joinedload(music_models.ListeningEvent.track).joinedload(
                    music_models.Track.artist
                )
            )
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )

        result = await db.execute(query)
        events = list(result.scalars().unique().all())

    has_next = len(events) > _PAGE_SIZE
    events = events[:_PAGE_SIZE]

    # Build filter query string for pagination links
    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.HISTORY_FILTERS
    )
    # Include source in filter_qs (handled outside the standard fields)
    if valid_sources:
        source_parts = [f"source={v}" for v in valid_sources]
        if filter_qs:
            filter_qs += "&" + "&".join(source_parts)
        else:
            filter_qs = "&".join(source_parts)
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
    if valid_sources:
        template_active_filters["source"] = valid_sources
    if q_value:
        template_active_filters["q"] = q_value

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "events": events,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
        "active_filters": template_active_filters,
        "presets": presets,
        "filters": view_filters_module.HISTORY_TEMPLATE_FILTERS,
        "active_preset": active_preset,
        "list_url": "/history",
        "list_target": "#history-list",
        "filter_qs": filter_qs,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/history_list.html", context
        )
    return templates.TemplateResponse(request, "history.html", context)


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
            sa.select(task_models.Task)
            .where(task_models.Task.parent_id.is_(None))
            .order_by(task_models.Task.created_at.desc())
            .options(
                sa_orm.joinedload(task_models.Task.service_connection).joinedload(
                    user_models.ServiceConnection.user
                )
            )
            .limit(20)
        )
        tasks: Sequence[task_models.Task] = tasks_result.scalars().unique().all()

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
                sa.select(task_models.Task).where(task_models.Task.id == task_id)
            )
        ).scalar_one_or_none()
        if original is None:
            raise fastapi.HTTPException(status_code=404)

        params = dict(original.params or {})
        if step_mode:
            params["step_mode"] = True

        cloned = task_models.Task(
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
                if original.task_type == types_module.TaskType.SYNC_JOB
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
                sa.select(task_models.Task).where(task_models.Task.id == task_id)
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
                    sa.select(task_models.Task)
                    .where(
                        task_models.Task.parent_id == parent_id,
                        task_models.Task.status == types_module.SyncStatus.PENDING,
                    )
                    .order_by(task_models.Task.created_at)
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


async def _enqueue_bulk_job(
    request: fastapi.Request,
    operation: str,
) -> dict[str, str]:
    """Create a BULK_JOB task and enqueue it to arq."""
    async with _get_db(request) as db:
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


@router.post("/admin/dedup-events", response_model=None)
async def dedup_listening_events(
    request: fastapi.Request,
) -> dict[str, str]:
    """Admin-only: enqueue cross-service event dedup as a bulk job."""
    deps_module.verify_admin_access(request)
    return await _enqueue_bulk_job(request, "dedup_events")


@router.post("/admin/dedup-artists", response_model=None)
async def dedup_artists(
    request: fastapi.Request,
) -> dict[str, str]:
    """Admin-only: enqueue artist dedup as a bulk job."""
    deps_module.verify_admin_access(request)
    return await _enqueue_bulk_job(request, "dedup_artists")


@router.post("/admin/dedup-tracks", response_model=None)
async def dedup_tracks(
    request: fastapi.Request,
) -> dict[str, str]:
    """Admin-only: enqueue track dedup as a bulk job."""
    deps_module.verify_admin_access(request)
    return await _enqueue_bulk_job(request, "dedup_tracks")


def _resolution_response(message: str) -> fastapi.responses.HTMLResponse:
    """Return an HTML response with a trigger to refresh the resolution list."""
    resp = fastapi.responses.HTMLResponse(f"<p><small>{message}</small></p>")
    resp.headers["HX-Trigger"] = "resolution-updated"
    return resp


_RESOLUTION_PRESETS: list[dict[str, str]] = [
    {"name": "pending", "label": "Pending", "params": "view=pending"},
    {"name": "multi_source", "label": "Multi-Source", "params": "view=multi_source"},
    {
        "name": "merge_suggestions",
        "label": "Merge Suggestions",
        "params": "view=merge_suggestions",
    },
    {"name": "orphaned", "label": "Orphaned", "params": "view=orphaned"},
]

_RESOLUTION_VIEWS = frozenset(p["name"] for p in _RESOLUTION_PRESETS)


@router.get("/admin/resolution", response_model=None)
async def admin_resolution(
    request: fastapi.Request,
    view: str = "pending",
    entity: str = "all",
    q: str = "",
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Admin page for entity resolution with preset views and search."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_role = _user_role(request)
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    if view not in _RESOLUTION_VIEWS:
        view = "pending"

    import resonance.normalize as normalize_module

    search_norm = normalize_module.normalize_name(q) if q else ""

    pending_venue_candidates: list[concert_models.VenueCandidate] = []
    pending_event_candidates: list[concert_models.EventCandidate] = []
    multi_cand_venues: list[
        tuple[concert_models.Venue, list[concert_models.VenueCandidate]]
    ] = []
    multi_cand_events: list[
        tuple[concert_models.Event, list[concert_models.EventCandidate]]
    ] = []
    venue_merge_suggestions: list[list[concert_models.Venue]] = []
    event_merge_suggestions: list[list[concert_models.Event]] = []
    orphaned_venues: list[concert_models.Venue] = []

    async with _get_db(request) as db:
        if view == "pending":
            if entity in ("all", "venues"):
                vc_stmt = sa.select(concert_models.VenueCandidate).where(
                    concert_models.VenueCandidate.status
                    == types_module.CandidateStatus.PENDING
                )
                if search_norm:
                    vc_stmt = vc_stmt.where(
                        sa.func.lower(concert_models.VenueCandidate.name).contains(
                            search_norm
                        )
                    )
                vc_stmt = vc_stmt.order_by(
                    concert_models.VenueCandidate.created_at.desc()
                ).limit(50)
                pending_venue_candidates = list(
                    (await db.execute(vc_stmt)).scalars().all()
                )

            if entity in ("all", "events"):
                ec_stmt = sa.select(concert_models.EventCandidate).where(
                    concert_models.EventCandidate.status
                    == types_module.CandidateStatus.PENDING
                )
                if search_norm:
                    ec_stmt = ec_stmt.where(
                        sa.func.lower(concert_models.EventCandidate.title).contains(
                            search_norm
                        )
                    )
                ec_stmt = ec_stmt.order_by(
                    concert_models.EventCandidate.created_at.desc()
                ).limit(50)
                pending_event_candidates = list(
                    (await db.execute(ec_stmt)).scalars().all()
                )

        elif view == "multi_source":
            if entity in ("all", "venues"):
                venues_result = await db.execute(
                    sa.select(concert_models.Venue).options(
                        sa_orm.selectinload(concert_models.Venue.candidates)
                    )
                )
                for venue in venues_result.scalars().all():
                    if len(venue.candidates) < 2:
                        continue
                    if all(
                        c.status == types_module.CandidateStatus.ACCEPTED
                        for c in venue.candidates
                    ):
                        continue
                    if search_norm and search_norm not in venue.name.lower():
                        continue
                    multi_cand_venues.append((venue, list(venue.candidates)))

            if entity in ("all", "events"):
                events_result = await db.execute(
                    sa.select(concert_models.Event).options(
                        sa_orm.selectinload(concert_models.Event.event_candidates)
                    )
                )
                for event in events_result.scalars().all():
                    if len(event.event_candidates) < 2:
                        continue
                    if all(
                        c.status == types_module.CandidateStatus.ACCEPTED
                        for c in event.event_candidates
                    ):
                        continue
                    if search_norm and search_norm not in event.title.lower():
                        continue
                    multi_cand_events.append((event, list(event.event_candidates)))

        elif view == "merge_suggestions":
            all_venues_result = await db.execute(
                sa.select(concert_models.Venue).options(
                    sa_orm.selectinload(concert_models.Venue.events),
                )
            )
            all_venues = list(all_venues_result.scalars().all())
            seen_ids: set[uuid.UUID] = set()
            groups: dict[tuple[str, ...], list[concert_models.Venue]] = {}
            for v in all_venues:
                key = (
                    normalize_module.normalize_name(v.name),
                    normalize_module.normalize_name(v.city or ""),
                )
                groups.setdefault(key, []).append(v)
            for group in groups.values():
                if len(group) > 1 and group[0].id not in seen_ids:
                    if search_norm:
                        names = " ".join(v.name.lower() for v in group)
                        if search_norm not in names:
                            continue
                    venue_merge_suggestions.append(group)
                    for v in group:
                        seen_ids.add(v.id)

            event_excl_result = await db.execute(
                sa.select(concert_models.EntityExclusion).where(
                    concert_models.EntityExclusion.entity_type == "event"
                )
            )
            excluded_event_pairs: set[frozenset[uuid.UUID]] = set()
            for ex in event_excl_result.scalars():
                excluded_event_pairs.add(frozenset([ex.entity_a_id, ex.entity_b_id]))

            all_events_result = await db.execute(
                sa.select(concert_models.Event).options(
                    sa_orm.selectinload(concert_models.Event.venue),
                    sa_orm.selectinload(concert_models.Event.event_candidates),
                    sa_orm.selectinload(concert_models.Event.artists),
                )
            )
            evt_groups: dict[
                tuple[datetime.date, uuid.UUID | None],
                list[concert_models.Event],
            ] = {}
            for evt in all_events_result.scalars().unique():
                evt_key = (evt.event_date, evt.venue_id)
                evt_groups.setdefault(evt_key, []).append(evt)

            seen_event_ids: set[uuid.UUID] = set()
            for evt_group in evt_groups.values():
                if len(evt_group) < 2:
                    continue
                if evt_group[0].id in seen_event_ids:
                    continue
                pair = frozenset(e.id for e in evt_group)
                if len(evt_group) == 2 and pair in excluded_event_pairs:
                    continue
                if search_norm:
                    titles = " ".join(e.title.lower() for e in evt_group)
                    if search_norm not in titles:
                        continue
                event_merge_suggestions.append(evt_group)
                for e in evt_group:
                    seen_event_ids.add(e.id)

        elif view == "orphaned":
            venues_with_rels = await db.execute(
                sa.select(concert_models.Venue).options(
                    sa_orm.selectinload(concert_models.Venue.candidates),
                    sa_orm.selectinload(concert_models.Venue.events),
                )
            )
            for v in venues_with_rels.scalars().all():
                if not v.candidates and not v.events:
                    if search_norm and search_norm not in v.name.lower():
                        continue
                    orphaned_venues.append(v)

    active_filters: dict[str, object] = {}
    if q:
        active_filters["q"] = q
    if entity != "all":
        active_filters["entity"] = [entity]

    context: dict[str, object] = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": user_role,
        "view": view,
        "entity": entity,
        "pending_venue_candidates": pending_venue_candidates,
        "pending_event_candidates": pending_event_candidates,
        "multi_cand_venues": multi_cand_venues,
        "multi_cand_events": multi_cand_events,
        "venue_merge_suggestions": venue_merge_suggestions,
        "event_merge_suggestions": event_merge_suggestions,
        "orphaned_venues": orphaned_venues,
        "presets": _RESOLUTION_PRESETS,
        "active_preset": view,
        "active_filters": active_filters,
        "list_url": "/admin/resolution",
        "list_target": "#resolution-list",
        "filters": [
            {
                "name": "entity",
                "label": "Entity Type",
                "type": "multiselect",
                "options": [
                    {"value": "venues", "label": "Venues"},
                    {"value": "events", "label": "Events"},
                ],
            },
        ],
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/resolution_list.html", context
        )
    return templates.TemplateResponse(request, "admin_resolution.html", context)


@router.post("/admin/resolution/unlink-venue-candidate/{candidate_id}")
async def unlink_venue_candidate(
    candidate_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Unlink a VenueCandidate from its resolved Venue."""
    deps_module.verify_admin_access(request)
    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(concert_models.VenueCandidate).where(
                concert_models.VenueCandidate.id == candidate_id
            )
        )
        candidate = result.scalar_one_or_none()
        if candidate is None:
            raise fastapi.HTTPException(status_code=404)
        candidate.resolved_venue_id = None
        candidate.status = types_module.CandidateStatus.PENDING
        candidate.confidence_score = 0
        await db.commit()
    return _resolution_response("Candidate unlinked.")


@router.post("/admin/resolution/unlink-event-candidate/{candidate_id}")
async def unlink_event_candidate(
    candidate_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Unlink an EventCandidate from its resolved Event."""
    deps_module.verify_admin_access(request)
    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(concert_models.EventCandidate).where(
                concert_models.EventCandidate.id == candidate_id
            )
        )
        candidate = result.scalar_one_or_none()
        if candidate is None:
            raise fastapi.HTTPException(status_code=404)
        candidate.resolved_event_id = None
        candidate.status = types_module.CandidateStatus.PENDING
        candidate.confidence_score = 0
        await db.commit()
    return _resolution_response("Candidate unlinked.")


@router.post("/admin/resolution/delete-orphan-venue/{venue_id}")
async def delete_orphan_venue(
    venue_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Delete a venue with no candidates and no events."""
    deps_module.verify_admin_access(request)
    async with _get_db(request) as db:
        venue_result = await db.execute(
            sa.select(concert_models.Venue)
            .options(
                sa_orm.selectinload(concert_models.Venue.candidates),
                sa_orm.selectinload(concert_models.Venue.events),
            )
            .where(concert_models.Venue.id == venue_id)
        )
        venue = venue_result.scalar_one_or_none()
        if venue is None:
            raise fastapi.HTTPException(status_code=404)
        if venue.candidates or venue.events:
            raise fastapi.HTTPException(
                status_code=409, detail="Venue still has candidates or events"
            )
        await db.delete(venue)
        await db.commit()
    return _resolution_response("Venue deleted.")


@router.post("/admin/resolution/merge-venues")
async def merge_venue_group(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Non-destructively merge a group of venues by moving candidates.

    Picks the venue with the most events as canonical, re-points all
    candidates from the other venues to it, and re-points their events.
    The now-empty duplicate venues become orphans (deletable separately).
    """
    deps_module.verify_admin_access(request)
    content_type = request.headers.get("content-type", "")
    if "json" in content_type:
        body = await request.json()
        raw_ids = body.get("venue_ids", [])
    else:
        form = await request.form()
        raw_ids = form.getlist("venue_ids")
    venue_ids = [uuid.UUID(v) for v in raw_ids]
    if len(venue_ids) < 2:
        raise fastapi.HTTPException(status_code=400, detail="Need at least 2 venue IDs")

    async with _get_db(request) as db:
        venues_result = await db.execute(
            sa.select(concert_models.Venue)
            .options(
                sa_orm.selectinload(concert_models.Venue.candidates),
                sa_orm.selectinload(concert_models.Venue.events),
            )
            .where(concert_models.Venue.id.in_(venue_ids))
        )
        venues = list(venues_result.scalars().all())
        if len(venues) < 2:
            raise fastapi.HTTPException(status_code=404)

        venues.sort(key=lambda v: len(v.events), reverse=True)
        canonical = venues[0]
        merged_count = 0

        for dup in venues[1:]:
            for vc in dup.candidates:
                vc.resolved_venue_id = canonical.id
            await db.execute(
                sa.update(concert_models.Event)
                .where(concert_models.Event.venue_id == dup.id)
                .values(venue_id=canonical.id)
            )
            merged_count += 1

        await db.commit()

    return _resolution_response(
        f"Merged {merged_count} venue(s) into {canonical.name}."
    )


@router.post(
    "/admin/resolution/confirm-venue/{venue_id}",
)
async def confirm_venue_resolution(
    venue_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Mark all candidates for a venue as human-accepted."""
    deps_module.verify_admin_access(request)
    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(concert_models.VenueCandidate).where(
                concert_models.VenueCandidate.resolved_venue_id == venue_id,
            )
        )
        candidates = result.scalars().all()
        for c in candidates:
            c.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()
    return _resolution_response("All candidates confirmed.")


@router.post(
    "/admin/resolution/confirm-event/{event_id}",
)
async def confirm_event_resolution(
    event_id: uuid.UUID,
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Mark all candidates for an event as human-accepted."""
    deps_module.verify_admin_access(request)
    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(concert_models.EventCandidate).where(
                concert_models.EventCandidate.resolved_event_id == event_id,
            )
        )
        candidates = result.scalars().all()
        for c in candidates:
            c.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()
    return _resolution_response("All candidates confirmed.")


@router.post("/admin/resolution/exclude-venues")
async def exclude_venue_group(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Create exclusions between all venue pairs."""
    deps_module.verify_admin_access(request)
    form = await request.form()
    raw_ids = form.getlist("venue_ids")
    venue_ids = [uuid.UUID(str(vid)) for vid in raw_ids]

    if len(venue_ids) < 2:
        return _resolution_response("Need at least 2 venues to exclude.")

    async with _get_db(request) as db:
        created = 0
        for i, a_id in enumerate(venue_ids):
            for b_id in venue_ids[i + 1 :]:
                lo, hi = sorted([a_id, b_id])
                existing = (
                    await db.execute(
                        sa.select(concert_models.EntityExclusion).where(
                            concert_models.EntityExclusion.entity_type == "venue",
                            concert_models.EntityExclusion.entity_a_id == lo,
                            concert_models.EntityExclusion.entity_b_id == hi,
                        )
                    )
                ).scalar_one_or_none()
                if not existing:
                    db.add(
                        concert_models.EntityExclusion(
                            entity_type="venue",
                            entity_a_id=lo,
                            entity_b_id=hi,
                        )
                    )
                    created += 1
        await db.commit()

    return _resolution_response(f"Excluded — {created} exclusion(s) created.")


@router.post("/admin/resolution/merge-events")
async def merge_event_group(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Merge events by moving candidates and artists to the canonical one."""
    deps_module.verify_admin_access(request)
    form = await request.form()
    raw_ids = form.getlist("event_ids")
    event_ids = [uuid.UUID(str(eid)) for eid in raw_ids]
    if len(event_ids) < 2:
        return _resolution_response("Need at least 2 events.")

    async with _get_db(request) as db:
        events_result = await db.execute(
            sa.select(concert_models.Event)
            .options(
                sa_orm.selectinload(concert_models.Event.event_candidates),
                sa_orm.selectinload(concert_models.Event.artists),
                sa_orm.selectinload(concert_models.Event.artist_candidates),
            )
            .where(concert_models.Event.id.in_(event_ids))
        )
        events = list(events_result.scalars().unique())
        if len(events) < 2:
            return _resolution_response("Events not found.")

        events.sort(
            key=lambda e: len(e.event_candidates) + len(e.artists),
            reverse=True,
        )
        canonical = events[0]
        canonical_artist_ids = {ea.artist_id for ea in canonical.artists}
        canonical_candidate_names = {
            eac.raw_name for eac in canonical.artist_candidates
        }
        merged_count = 0

        for dup in events[1:]:
            for ec in dup.event_candidates:
                ec.resolved_event_id = canonical.id
            for eac in dup.artist_candidates:
                if eac.raw_name in canonical_candidate_names:
                    await db.delete(eac)
                else:
                    eac.event_id = canonical.id
                    canonical_candidate_names.add(eac.raw_name)
            for confirmed_ea in dup.artists:
                if confirmed_ea.artist_id in canonical_artist_ids:
                    await db.delete(confirmed_ea)
                else:
                    confirmed_ea.event_id = canonical.id
                    canonical_artist_ids.add(confirmed_ea.artist_id)
            existing_att = {
                row[0]
                for row in (
                    await db.execute(
                        sa.select(concert_models.UserEventAttendance.user_id).where(
                            concert_models.UserEventAttendance.event_id == canonical.id
                        )
                    )
                ).all()
            }
            dup_att = (
                (
                    await db.execute(
                        sa.select(concert_models.UserEventAttendance).where(
                            concert_models.UserEventAttendance.event_id == dup.id
                        )
                    )
                )
                .scalars()
                .all()
            )
            for att in dup_att:
                if att.user_id in existing_att:
                    await db.delete(att)
                else:
                    att.event_id = canonical.id
                    existing_att.add(att.user_id)
            await db.flush()
            await db.delete(dup)
            merged_count += 1

        await db.commit()

    return _resolution_response(
        f'Merged {merged_count} event(s) into "{canonical.title}".'
    )


@router.post("/admin/resolution/exclude-events")
async def exclude_event_group(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse:
    """Create exclusions between all event pairs."""
    deps_module.verify_admin_access(request)
    form = await request.form()
    raw_ids = form.getlist("event_ids")
    event_ids = [uuid.UUID(str(eid)) for eid in raw_ids]

    if len(event_ids) < 2:
        return _resolution_response("Need at least 2 events to exclude.")

    async with _get_db(request) as db:
        created = 0
        for i, a_id in enumerate(event_ids):
            for b_id in event_ids[i + 1 :]:
                lo, hi = sorted([a_id, b_id])
                existing = (
                    await db.execute(
                        sa.select(concert_models.EntityExclusion).where(
                            concert_models.EntityExclusion.entity_type == "event",
                            concert_models.EntityExclusion.entity_a_id == lo,
                            concert_models.EntityExclusion.entity_b_id == hi,
                        )
                    )
                ).scalar_one_or_none()
                if not existing:
                    db.add(
                        concert_models.EntityExclusion(
                            entity_type="event",
                            entity_a_id=lo,
                            entity_b_id=hi,
                        )
                    )
                    created += 1
        await db.commit()

    return _resolution_response(f"Excluded — {created} exclusion(s) created.")


@router.get("/admin/tasks/{task_id}", response_model=None)
async def admin_task_status(
    task_id: uuid.UUID,
    request: fastapi.Request,
) -> dict[str, object]:
    """Admin-only: get status of a bulk/admin task."""
    deps_module.verify_admin_access(request)

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(task_models.Task).where(task_models.Task.id == task_id)
        )
        task = result.scalar_one_or_none()

    if task is None:
        raise fastapi.HTTPException(status_code=404, detail="Task not found")

    return {
        "task_id": str(task.id),
        "status": task.status.value,
        "operation": task.params.get("operation"),
        "progress_current": task.progress_current,
        "progress_total": task.progress_total,
        "result": task.result if task.result else None,
        "error": task.error_message,
        "started_at": (task.started_at.isoformat() if task.started_at else None),
        "completed_at": (task.completed_at.isoformat() if task.completed_at else None),
    }


@router.get("/admin/status", response_model=None)
async def admin_status(
    request: fastapi.Request,
) -> dict[str, object]:
    """Admin-only: overview of recent sync tasks."""
    deps_module.verify_admin_access(request)

    async with _get_db(request) as db:
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


# ---------------------------------------------------------------------------
# Admin: Venue management
# ---------------------------------------------------------------------------

_VENUE_PAGE_SIZE = 50


def _entity_action_response(
    message: str,
    *,
    error: bool = False,
) -> fastapi.responses.HTMLResponse:
    """Return an HTML response with trigger to refresh entity detail."""
    if error:
        html = (
            f'<p><mark style="background: var(--pico-del-color);">{message}</mark></p>'
        )
    else:
        html = f"<p><small>{message}</small></p>"
    resp = fastapi.responses.HTMLResponse(html)
    resp.headers["HX-Trigger"] = "entity-updated"
    return resp


@router.get("/admin/venues", response_model=None)
async def admin_venues(
    request: fastapi.Request,
    q: str = "",
    page: int = 1,
    filter: str = "",
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Admin venue list with search and filtering."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    user_role = _user_role(request)
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    offset = (page - 1) * _VENUE_PAGE_SIZE
    user_tz = _user_tz(request)

    async with _get_db(request) as db:
        stmt = (
            sa.select(concert_models.Venue)
            .options(
                sa_orm.selectinload(concert_models.Venue.candidates),
                sa_orm.selectinload(concert_models.Venue.events),
            )
            .order_by(concert_models.Venue.name)
            .offset(offset)
            .limit(_VENUE_PAGE_SIZE + 1)
        )

        if q:
            escaped = _escape_ilike(q)
            pattern = f"%{escaped}%"
            stmt = stmt.where(
                sa.or_(
                    concert_models.Venue.name.ilike(pattern),
                    concert_models.Venue.city.ilike(pattern),
                )
            )

        if filter == "multi_candidates":
            sub = (
                sa.select(concert_models.VenueCandidate.resolved_venue_id)
                .group_by(concert_models.VenueCandidate.resolved_venue_id)
                .having(sa.func.count() > 1)
            )
            stmt = stmt.where(concert_models.Venue.id.in_(sub))
        elif filter == "unresolved":
            sub = sa.select(concert_models.VenueCandidate.resolved_venue_id).where(
                concert_models.VenueCandidate.status
                == types_module.CandidateStatus.PENDING,
                concert_models.VenueCandidate.resolved_venue_id.isnot(None),
            )
            stmt = stmt.where(concert_models.Venue.id.in_(sub))
        elif filter == "has_exclusions":
            excl_sub = sa.union(
                sa.select(concert_models.EntityExclusion.entity_a_id).where(
                    concert_models.EntityExclusion.entity_type == "venue"
                ),
                sa.select(concert_models.EntityExclusion.entity_b_id).where(
                    concert_models.EntityExclusion.entity_type == "venue"
                ),
            )
            stmt = stmt.where(concert_models.Venue.id.in_(excl_sub))

        result = await db.execute(stmt)
        venues = list(result.scalars().unique())

    has_next = len(venues) > _VENUE_PAGE_SIZE
    has_prev = page > 1
    venues = venues[:_VENUE_PAGE_SIZE]

    ctx: dict[str, object] = {
        "request": request,
        "venues": venues,
        "q": q,
        "filter": filter,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
        "user_tz": user_tz,
        "user_role": user_role,
        "list_url": "/admin/venues",
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/admin_venue_list.html", ctx
        )

    return templates.TemplateResponse(request, "admin_venues.html", ctx)


@router.get("/admin/venues/{venue_id}", response_model=None)
async def admin_venue_detail(
    request: fastapi.Request,
    venue_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Admin venue detail page with candidate history."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    user_role = _user_role(request)
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    user_tz = _user_tz(request)

    async with _get_db(request) as db:
        stmt = (
            sa.select(concert_models.Venue)
            .options(
                sa_orm.selectinload(concert_models.Venue.candidates),
                sa_orm.selectinload(concert_models.Venue.events),
            )
            .where(concert_models.Venue.id == venue_id)
        )
        venue = (await db.execute(stmt)).scalar_one_or_none()
        if not venue:
            raise fastapi.HTTPException(status_code=404, detail="Venue not found")

        exclusions_stmt = sa.select(concert_models.EntityExclusion).where(
            concert_models.EntityExclusion.entity_type == "venue",
            sa.or_(
                concert_models.EntityExclusion.entity_a_id == venue_id,
                concert_models.EntityExclusion.entity_b_id == venue_id,
            ),
        )
        exclusions = list((await db.execute(exclusions_stmt)).scalars())

        other_venue_ids = []
        for ex in exclusions:
            other_id = ex.entity_b_id if ex.entity_a_id == venue_id else ex.entity_a_id
            other_venue_ids.append(other_id)

        other_venues: dict[uuid.UUID, concert_models.Venue] = {}
        if other_venue_ids:
            ov_stmt = sa.select(concert_models.Venue).where(
                concert_models.Venue.id.in_(other_venue_ids)
            )
            for ov in (await db.execute(ov_stmt)).scalars():
                other_venues[ov.id] = ov

        import resonance.normalize as normalize_module

        norm_name = normalize_module.normalize_name(venue.name)
        orphan_stmt = (
            sa.select(concert_models.VenueCandidate)
            .where(
                concert_models.VenueCandidate.resolved_venue_id.is_(None),
                concert_models.VenueCandidate.status
                == types_module.CandidateStatus.PENDING,
            )
            .order_by(concert_models.VenueCandidate.name)
            .limit(20)
        )
        all_orphans = list((await db.execute(orphan_stmt)).scalars())
        orphan_candidates = [
            vc
            for vc in all_orphans
            if normalize_module.normalize_name(vc.name) == norm_name
        ]

    ctx: dict[str, object] = {
        "request": request,
        "venue": venue,
        "exclusions": exclusions,
        "other_venues": other_venues,
        "orphan_candidates": orphan_candidates,
        "user_tz": user_tz,
        "user_role": user_role,
    }

    return templates.TemplateResponse(request, "admin_venue_detail.html", ctx)


@router.post(
    "/admin/venues/{venue_id}/candidates/{candidate_id}/accept",
    response_model=None,
)
async def admin_accept_venue_candidate(
    request: fastapi.Request,
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Accept a venue candidate (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        vc = await db.get(concert_models.VenueCandidate, candidate_id)
        if not vc or vc.resolved_venue_id != venue_id:
            return _entity_action_response("Candidate not found.", error=True)
        vc.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()

    return _entity_action_response("Candidate accepted.")


@router.post(
    "/admin/venues/{venue_id}/candidates/{candidate_id}/reject",
    response_model=None,
)
async def admin_reject_venue_candidate(
    request: fastapi.Request,
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Reject a venue candidate (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        vc = await db.get(concert_models.VenueCandidate, candidate_id)
        if not vc or vc.resolved_venue_id != venue_id:
            return _entity_action_response("Candidate not found.", error=True)
        vc.status = types_module.CandidateStatus.REJECTED
        await db.commit()

    return _entity_action_response("Candidate rejected.")


@router.post(
    "/admin/venues/{venue_id}/candidates/{candidate_id}/unlink",
    response_model=None,
)
async def admin_unlink_venue_candidate_detail(
    request: fastapi.Request,
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Unlink a venue candidate back to pending (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        vc = await db.get(concert_models.VenueCandidate, candidate_id)
        if not vc or vc.resolved_venue_id != venue_id:
            return _entity_action_response("Candidate not found.", error=True)
        vc.resolved_venue_id = None
        vc.status = types_module.CandidateStatus.PENDING
        vc.confidence_score = 0
        await db.commit()

    return _entity_action_response("Candidate unlinked.")


@router.post(
    "/admin/venues/{venue_id}/claim/{candidate_id}",
    response_model=None,
)
async def admin_claim_venue_candidate(
    request: fastapi.Request,
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Claim an orphaned pending candidate into this venue."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        vc = await db.get(concert_models.VenueCandidate, candidate_id)
        if not vc:
            return _entity_action_response("Candidate not found.", error=True)
        if vc.resolved_venue_id is not None:
            return _entity_action_response("Candidate already assigned.", error=True)
        vc.resolved_venue_id = venue_id
        vc.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()

    return _entity_action_response("Candidate claimed.")


@router.post("/admin/venues/{venue_id}/split", response_model=None)
async def admin_split_venue(
    request: fastapi.Request,
    venue_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Split selected candidates into a new venue (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    form = await request.form()
    raw_ids = form.getlist("candidate_ids")
    candidate_ids = [uuid.UUID(str(cid)) for cid in raw_ids]

    if not candidate_ids:
        return _entity_action_response("Select at least one candidate.", error=True)

    async with _get_db(request) as db:
        stmt = (
            sa.select(concert_models.Venue)
            .options(sa_orm.selectinload(concert_models.Venue.candidates))
            .where(concert_models.Venue.id == venue_id)
        )
        venue = (await db.execute(stmt)).scalar_one_or_none()
        if not venue:
            return _entity_action_response("Venue not found.", error=True)

        to_move = [vc for vc in venue.candidates if vc.id in candidate_ids]
        if not to_move:
            return _entity_action_response("No matching candidates.", error=True)
        if len(to_move) == len(venue.candidates):
            return _entity_action_response("Cannot split all candidates.", error=True)

        first = to_move[0]
        new_venue = concert_models.Venue(
            name=first.name,
            city=first.city,
            state=first.state,
            country=first.country,
            address=first.address,
            postal_code=first.postal_code,
        )
        db.add(new_venue)
        await db.flush()

        for vc in to_move:
            vc.resolved_venue_id = new_venue.id
            vc.status = types_module.CandidateStatus.ACCEPTED

        exclusion = concert_models.EntityExclusion(
            entity_type="venue",
            entity_a_id=venue_id,
            entity_b_id=new_venue.id,
        )
        db.add(exclusion)
        await db.commit()

    return _entity_action_response(f"Split {len(to_move)} candidate(s) to new venue.")


@router.post(
    "/admin/venues/exclusions/{exclusion_id}/delete",
    response_model=None,
)
async def admin_delete_venue_exclusion(
    request: fastapi.Request,
    exclusion_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Remove a venue exclusion (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        exclusion = await db.get(concert_models.EntityExclusion, exclusion_id)
        if not exclusion:
            return _entity_action_response("Exclusion not found.", error=True)
        await db.delete(exclusion)
        await db.commit()

    return _entity_action_response("Exclusion removed.")


# ---------------------------------------------------------------------------
# Admin: Event management
# ---------------------------------------------------------------------------

_EVENT_ADMIN_PAGE_SIZE = 50


@router.get("/admin/events", response_model=None)
async def admin_events(
    request: fastapi.Request,
    q: str = "",
    page: int = 1,
    filter: str = "",
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Admin event list with search and filtering."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    user_role = _user_role(request)
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    offset = (page - 1) * _EVENT_ADMIN_PAGE_SIZE
    user_tz = _user_tz(request)

    async with _get_db(request) as db:
        stmt = (
            sa.select(concert_models.Event)
            .options(
                sa_orm.selectinload(concert_models.Event.venue),
                sa_orm.selectinload(concert_models.Event.event_candidates),
                sa_orm.selectinload(concert_models.Event.artists),
            )
            .order_by(concert_models.Event.event_date.desc())
            .offset(offset)
            .limit(_EVENT_ADMIN_PAGE_SIZE + 1)
        )

        if q:
            escaped = _escape_ilike(q)
            pattern = f"%{escaped}%"
            stmt = stmt.where(
                sa.or_(
                    concert_models.Event.title.ilike(pattern),
                    concert_models.Event.venue.has(
                        concert_models.Venue.name.ilike(pattern)
                    ),
                )
            )

        if filter == "multi_candidates":
            sub = (
                sa.select(concert_models.EventCandidate.resolved_event_id)
                .group_by(concert_models.EventCandidate.resolved_event_id)
                .having(sa.func.count() > 1)
            )
            stmt = stmt.where(concert_models.Event.id.in_(sub))
        elif filter == "unresolved":
            sub = sa.select(concert_models.EventCandidate.resolved_event_id).where(
                concert_models.EventCandidate.status
                == types_module.CandidateStatus.PENDING,
                concert_models.EventCandidate.resolved_event_id.isnot(None),
            )
            stmt = stmt.where(concert_models.Event.id.in_(sub))
        elif filter == "has_exclusions":
            excl_sub = sa.union(
                sa.select(concert_models.EntityExclusion.entity_a_id).where(
                    concert_models.EntityExclusion.entity_type == "event"
                ),
                sa.select(concert_models.EntityExclusion.entity_b_id).where(
                    concert_models.EntityExclusion.entity_type == "event"
                ),
            )
            stmt = stmt.where(concert_models.Event.id.in_(excl_sub))

        result = await db.execute(stmt)
        events = list(result.scalars().unique())

    has_next = len(events) > _EVENT_ADMIN_PAGE_SIZE
    has_prev = page > 1
    events = events[:_EVENT_ADMIN_PAGE_SIZE]

    ctx: dict[str, object] = {
        "request": request,
        "events": events,
        "q": q,
        "filter": filter,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
        "user_tz": user_tz,
        "user_role": user_role,
        "list_url": "/admin/events",
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/admin_event_list.html", ctx
        )

    return templates.TemplateResponse(request, "admin_events.html", ctx)


@router.get("/admin/events/{event_id}/manage", response_model=None)
async def admin_event_detail(
    request: fastapi.Request,
    event_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Admin event detail page with candidate history."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    user_role = _user_role(request)
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    user_tz = _user_tz(request)

    async with _get_db(request) as db:
        stmt = (
            sa.select(concert_models.Event)
            .options(
                sa_orm.selectinload(concert_models.Event.venue),
                sa_orm.selectinload(concert_models.Event.event_candidates),
                sa_orm.selectinload(concert_models.Event.artists),
                sa_orm.selectinload(concert_models.Event.artist_candidates),
            )
            .where(concert_models.Event.id == event_id)
        )
        event = (await db.execute(stmt)).scalar_one_or_none()
        if not event:
            raise fastapi.HTTPException(status_code=404, detail="Event not found")

        exclusions_stmt = sa.select(concert_models.EntityExclusion).where(
            concert_models.EntityExclusion.entity_type == "event",
            sa.or_(
                concert_models.EntityExclusion.entity_a_id == event_id,
                concert_models.EntityExclusion.entity_b_id == event_id,
            ),
        )
        exclusions = list((await db.execute(exclusions_stmt)).scalars())

        other_event_ids = []
        for ex in exclusions:
            other_id = ex.entity_b_id if ex.entity_a_id == event_id else ex.entity_a_id
            other_event_ids.append(other_id)

        other_events: dict[uuid.UUID, concert_models.Event] = {}
        if other_event_ids:
            oe_stmt = sa.select(concert_models.Event).where(
                concert_models.Event.id.in_(other_event_ids)
            )
            for oe in (await db.execute(oe_stmt)).scalars():
                other_events[oe.id] = oe

        import resonance.normalize as normalize_module

        norm_title = normalize_module.normalize_name(event.title)
        orphan_ec_stmt = (
            sa.select(concert_models.EventCandidate)
            .where(
                concert_models.EventCandidate.resolved_event_id.is_(None),
                concert_models.EventCandidate.status
                == types_module.CandidateStatus.PENDING,
                concert_models.EventCandidate.event_date == event.event_date,
            )
            .order_by(concert_models.EventCandidate.title)
            .limit(20)
        )
        all_orphan_ec = list((await db.execute(orphan_ec_stmt)).scalars())
        orphan_event_candidates = [
            ec
            for ec in all_orphan_ec
            if normalize_module.normalize_name(ec.title) == norm_title
        ]

    ctx: dict[str, object] = {
        "request": request,
        "event": event,
        "exclusions": exclusions,
        "other_events": other_events,
        "orphan_event_candidates": orphan_event_candidates,
        "user_tz": user_tz,
        "user_role": user_role,
    }

    return templates.TemplateResponse(request, "admin_event_detail.html", ctx)


@router.post(
    "/admin/events/{event_id}/candidates/{candidate_id}/accept",
    response_model=None,
)
async def admin_accept_event_candidate(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Accept an event candidate (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        ec = await db.get(concert_models.EventCandidate, candidate_id)
        if not ec or ec.resolved_event_id != event_id:
            return _entity_action_response("Candidate not found.", error=True)
        ec.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()

    return _entity_action_response("Candidate accepted.")


@router.post(
    "/admin/events/{event_id}/candidates/{candidate_id}/reject",
    response_model=None,
)
async def admin_reject_event_candidate(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Reject an event candidate (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        ec = await db.get(concert_models.EventCandidate, candidate_id)
        if not ec or ec.resolved_event_id != event_id:
            return _entity_action_response("Candidate not found.", error=True)
        ec.status = types_module.CandidateStatus.REJECTED
        await db.commit()

    return _entity_action_response("Candidate rejected.")


@router.post(
    "/admin/events/{event_id}/candidates/{candidate_id}/unlink",
    response_model=None,
)
async def admin_unlink_event_candidate_detail(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Unlink an event candidate back to pending (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        ec = await db.get(concert_models.EventCandidate, candidate_id)
        if not ec or ec.resolved_event_id != event_id:
            return _entity_action_response("Candidate not found.", error=True)
        ec.resolved_event_id = None
        ec.status = types_module.CandidateStatus.PENDING
        ec.confidence_score = 0
        await db.commit()

    return _entity_action_response("Candidate unlinked.")


@router.post(
    "/admin/events/{event_id}/claim/{candidate_id}",
    response_model=None,
)
async def admin_claim_event_candidate(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Claim an orphaned pending candidate into this event."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        ec = await db.get(concert_models.EventCandidate, candidate_id)
        if not ec:
            return _entity_action_response("Candidate not found.", error=True)
        if ec.resolved_event_id is not None:
            return _entity_action_response("Candidate already assigned.", error=True)
        ec.resolved_event_id = event_id
        ec.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()

    return _entity_action_response("Candidate claimed.")


@router.post("/admin/events/{event_id}/split", response_model=None)
async def admin_split_event(
    request: fastapi.Request,
    event_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Split selected candidates into a new event (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    form = await request.form()
    raw_ids = form.getlist("candidate_ids")
    candidate_ids = [uuid.UUID(str(cid)) for cid in raw_ids]

    if not candidate_ids:
        return _entity_action_response("Select at least one candidate.", error=True)

    async with _get_db(request) as db:
        stmt = (
            sa.select(concert_models.Event)
            .options(sa_orm.selectinload(concert_models.Event.event_candidates))
            .where(concert_models.Event.id == event_id)
        )
        event = (await db.execute(stmt)).scalar_one_or_none()
        if not event:
            return _entity_action_response("Event not found.", error=True)

        to_move = [ec for ec in event.event_candidates if ec.id in candidate_ids]
        if not to_move:
            return _entity_action_response("No matching candidates.", error=True)
        if len(to_move) == len(event.event_candidates):
            return _entity_action_response("Cannot split all candidates.", error=True)

        first = to_move[0]
        new_event = concert_models.Event(
            title=first.title,
            event_date=first.event_date,
            source_service=first.source_service,
            external_id=f"split-{uuid.uuid4().hex[:8]}",
            external_url=first.external_url,
            venue_id=event.venue_id,
        )
        db.add(new_event)
        await db.flush()

        for ec in to_move:
            ec.resolved_event_id = new_event.id
            ec.status = types_module.CandidateStatus.ACCEPTED

        exclusion = concert_models.EntityExclusion(
            entity_type="event",
            entity_a_id=event_id,
            entity_b_id=new_event.id,
        )
        db.add(exclusion)
        await db.commit()

    return _entity_action_response(f"Split {len(to_move)} candidate(s) to new event.")


@router.post(
    "/admin/events/exclusions/{exclusion_id}/delete",
    response_model=None,
)
async def admin_delete_event_exclusion(
    request: fastapi.Request,
    exclusion_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Remove an event exclusion (UI action)."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)
    if _user_role(request) not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        exclusion = await db.get(concert_models.EntityExclusion, exclusion_id)
        if not exclusion:
            return _entity_action_response("Exclusion not found.", error=True)
        await db.delete(exclusion)
        await db.commit()

    return _entity_action_response("Exclusion removed.")
