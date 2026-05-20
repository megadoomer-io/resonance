from __future__ import annotations

import datetime
import pathlib
import uuid
import zoneinfo
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated, Any

import fastapi
import fastapi.requests
import fastapi.responses
import fastapi.templating
import httpx
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.concerts.sync as concert_sync
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
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
import resonance.services.artist_utils as artist_utils
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


@router.get("/events", response_model=None)
async def events_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated events list with venue and artist info, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * _PAGE_SIZE

    # Parse filter parameters
    params = dict(request.query_params)
    multi_params = {
        "attendance": request.query_params.getlist("attendance"),
        "source_service": request.query_params.getlist("source_service"),
    }

    # Build presets (today's date resolved dynamically)
    presets = view_filters_module.build_event_presets()
    _event_filter_keys = {
        "q",
        "title",
        "venue",
        "artist",
        "date_from",
        "date_to",
        "attendance",
        "source_service",
        "has_pending",
        "include_not_going",
    }
    active_preset = view_filters_module.detect_active_preset(
        params,
        presets,
        filter_keys=_event_filter_keys,
        default_preset="upcoming",
    )

    # If "upcoming" is the default preset and no date_from was explicitly set,
    # inject today's date so the filter is applied.
    if active_preset == "upcoming" and "date_from" not in params:
        params["date_from"] = view_filters_module._today_iso()

    # Parse filters to get active_filters for template context
    applied = filters_module.parse_filter_params(
        view_filters_module.EVENT_FILTERS, params, multi_params=multi_params
    )

    async with _get_db(request) as db:
        # Build base query with joins for cross-entity filtering
        query = (
            sa.select(concert_models.Event)
            .outerjoin(
                concert_models.Venue,
                concert_models.Event.venue_id == concert_models.Venue.id,
            )
            .outerjoin(
                concert_models.EventArtist,
                concert_models.Event.id == concert_models.EventArtist.event_id,
            )
            .outerjoin(
                music_models.Artist,
                concert_models.EventArtist.artist_id == music_models.Artist.id,
            )
        )

        # Apply registered filter fields (title, venue, artist, date, has_pending)
        query = filters_module.apply_filters(
            query,
            view_filters_module.EVENT_FILTERS,
            params,
            multi_params=multi_params,
        )

        # Handle attendance filter manually (requires user_id context)
        attendance_values = multi_params.get("attendance", [])
        _valid_set = {"GOING", "INTERESTED", "NOT_GOING", "UNSET"}
        valid_attendance = [v for v in attendance_values if v in _valid_set]
        if valid_attendance:
            status_values = [v for v in valid_attendance if v != "UNSET"]
            include_unset = "UNSET" in valid_attendance
            conditions: list[sa.ColumnElement[bool]] = []
            if status_values:
                attendance_subquery = sa.select(
                    concert_models.UserEventAttendance.event_id
                ).where(
                    concert_models.UserEventAttendance.user_id == user_uuid,
                    concert_models.UserEventAttendance.status.in_(status_values),
                )
                conditions.append(concert_models.Event.id.in_(attendance_subquery))
            if include_unset:
                has_attendance = sa.select(
                    concert_models.UserEventAttendance.event_id
                ).where(
                    concert_models.UserEventAttendance.user_id == user_uuid,
                )
                conditions.append(concert_models.Event.id.not_in(has_attendance))
            query = query.where(sa.or_(*conditions))
        elif params.get("include_not_going") != "true":
            # Default: exclude NOT_GOING events
            not_going_subquery = sa.select(
                concert_models.UserEventAttendance.event_id
            ).where(
                concert_models.UserEventAttendance.user_id == user_uuid,
                concert_models.UserEventAttendance.status == "NOT_GOING",
            )
            query = query.where(concert_models.Event.id.not_in(not_going_subquery))

        # Deduplicate rows from outer joins, order, and paginate
        query = (
            query.group_by(concert_models.Event.id)
            .order_by(concert_models.Event.event_date.desc())
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )

        # Wrap in a subquery so eager loads work correctly with group_by
        event_ids_subquery = query.with_only_columns(concert_models.Event.id)
        final_query = (
            sa.select(concert_models.Event)
            .where(concert_models.Event.id.in_(event_ids_subquery))
            .options(
                sa_orm.joinedload(concert_models.Event.venue),
                sa_orm.joinedload(concert_models.Event.artists),
                sa_orm.joinedload(concert_models.Event.artist_candidates),
            )
            .order_by(concert_models.Event.event_date.desc())
        )

        result = await db.execute(final_query)
        events = list(result.unique().scalars().all())

        has_next = len(events) > _PAGE_SIZE
        events = events[:_PAGE_SIZE]

        event_ids = [e.id for e in events]
        attendance_map = await _get_attendance_map(db, user_uuid, event_ids)

    # Build filter query string for pagination links
    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.EVENT_FILTERS
    )
    # Include attendance in filter_qs (handled outside the standard fields)
    if valid_attendance:
        attendance_parts = [f"attendance={v}" for v in valid_attendance]
        if filter_qs:
            filter_qs += "&" + "&".join(attendance_parts)
        else:
            filter_qs = "&".join(attendance_parts)
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
            # DateRangeField stores {date_from: ..., date_to: ...}
            for dk, dv in value.items():
                if dv is not None:
                    template_active_filters[dk] = str(dv)
        else:
            template_active_filters[key] = value
    if valid_attendance:
        template_active_filters["attendance"] = valid_attendance
    if q_value:
        template_active_filters["q"] = q_value

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "events": events,
        "attendance_map": attendance_map,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
        "active_filters": template_active_filters,
        "presets": presets,
        "filters": view_filters_module.EVENT_TEMPLATE_FILTERS,
        "active_preset": active_preset,
        "list_url": "/events",
        "list_target": "#event-list",
        "filter_qs": filter_qs,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, "partials/event_list.html", context)
    return templates.TemplateResponse(request, "events.html", context)


@router.get("/events/{event_id}", response_model=None)
async def event_detail_page(
    request: fastapi.Request,
    event_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render event detail page with artists, candidates, and add-artist search."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        event_result = await db.execute(
            sa.select(concert_models.Event)
            .where(concert_models.Event.id == event_id)
            .options(
                sa_orm.joinedload(concert_models.Event.venue),
                sa_orm.joinedload(concert_models.Event.artists),
                sa_orm.joinedload(concert_models.Event.artist_candidates),
            )
        )
        event = event_result.unique().scalar_one_or_none()

        if event is None:
            raise fastapi.HTTPException(status_code=404, detail="Event not found")

        # Re-match any unmatched candidates against the current artist catalog
        has_unmatched = any(
            c.matched_artist_id is None
            for c in event.artist_candidates
            if c.status == types_module.CandidateStatus.PENDING
        )
        if has_unmatched:
            matched = await concert_sync.match_candidates_to_artists(db, event)
            if matched > 0:
                await db.commit()
                await db.refresh(event, ["artist_candidates"])

        # Build dict of matched_artist_id -> Artist for candidates with matches
        matched_ids = [
            c.matched_artist_id
            for c in event.artist_candidates
            if c.matched_artist_id is not None
        ]
        matched_artists: dict[uuid.UUID, music_models.Artist] = {}
        if matched_ids:
            artists_result = await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id.in_(matched_ids)
                )
            )
            for a in artists_result.scalars().all():
                matched_artists[a.id] = a

        attendance = (
            await db.execute(
                sa.select(concert_models.UserEventAttendance).where(
                    concert_models.UserEventAttendance.user_id == uuid.UUID(user_id),
                    concert_models.UserEventAttendance.event_id == event_id,
                )
            )
        ).scalar_one_or_none()

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "event": event,
        "matched_artists": matched_artists,
        "attendance": attendance,
    }

    return templates.TemplateResponse(request, "event_detail.html", context)


def _normalize_positions(
    artists: list[concert_models.EventArtist],
) -> list[concert_models.EventArtist]:
    """Assign sequential positions (0, 1, 2, ...) based on current sort order."""
    sorted_artists = sorted(artists, key=lambda ea: ea.position)
    for i, ea in enumerate(sorted_artists):
        ea.position = i
    return sorted_artists


@router.post("/events/{event_id}/artists/{ea_id}/move-up", response_model=None)
async def move_artist_up(
    request: fastapi.Request,
    event_id: uuid.UUID,
    ea_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Swap an event artist with the one above it and return updated list."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        event = await _load_event_with_artists(db, event_id)
        sorted_artists = _normalize_positions(list(event.artists))
        target_idx = next(
            (i for i, ea in enumerate(sorted_artists) if ea.id == ea_id), None
        )
        if target_idx is not None and target_idx > 0:
            sorted_artists[target_idx - 1].position = target_idx
            sorted_artists[target_idx].position = target_idx - 1
        await db.commit()

    return templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


@router.post("/events/{event_id}/artists/{ea_id}/move-down", response_model=None)
async def move_artist_down(
    request: fastapi.Request,
    event_id: uuid.UUID,
    ea_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Swap an event artist with the one below it and return updated list."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        event = await _load_event_with_artists(db, event_id)
        sorted_artists = _normalize_positions(list(event.artists))
        target_idx = next(
            (i for i, ea in enumerate(sorted_artists) if ea.id == ea_id), None
        )
        if target_idx is not None and target_idx < len(sorted_artists) - 1:
            sorted_artists[target_idx].position = target_idx + 1
            sorted_artists[target_idx + 1].position = target_idx
        await db.commit()

    return templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


@router.post("/events/{event_id}/artists/{ea_id}/remove", response_model=None)
async def remove_artist_from_event(
    request: fastapi.Request,
    event_id: uuid.UUID,
    ea_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Remove a confirmed artist from an event and return updated list."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        ea = (
            await db.execute(
                sa.select(concert_models.EventArtist).where(
                    concert_models.EventArtist.id == ea_id,
                    concert_models.EventArtist.event_id == event_id,
                )
            )
        ).scalar_one_or_none()
        if ea is not None:
            await db.delete(ea)
            await db.commit()

        event = await _load_event_with_artists(db, event_id)
        _normalize_positions(list(event.artists))
        await db.commit()

    return templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


@router.get("/events/{event_id}/artists", response_model=None)
async def event_artists_partial(
    request: fastapi.Request,
    event_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Return confirmed artists partial for HTMX refresh."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        event = await _load_event_with_artists(db, event_id)

    return templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


async def _load_event_with_artists(
    db: sa_async.AsyncSession,
    event_id: uuid.UUID,
) -> concert_models.Event:
    result = await db.execute(
        sa.select(concert_models.Event)
        .where(concert_models.Event.id == event_id)
        .options(sa_orm.joinedload(concert_models.Event.artists))
    )
    event = result.unique().scalar_one_or_none()
    if event is None:
        raise fastapi.HTTPException(status_code=404, detail="Event not found")
    return event


async def _get_attendance_map(
    db: sa_async.AsyncSession,
    user_id: uuid.UUID,
    event_ids: list[uuid.UUID],
) -> dict[uuid.UUID, concert_models.UserEventAttendance]:
    if not event_ids:
        return {}
    result = await db.execute(
        sa.select(concert_models.UserEventAttendance).where(
            concert_models.UserEventAttendance.user_id == user_id,
            concert_models.UserEventAttendance.event_id.in_(event_ids),
        )
    )
    return {a.event_id: a for a in result.scalars().all()}


@router.post("/events/{event_id}/attendance", response_model=None)
async def set_attendance(
    request: fastapi.Request,
    event_id: uuid.UUID,
    status: Annotated[str, fastapi.Form()],
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Set user's attendance status for an event, returns updated partial."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    new_status = types_module.AttendanceStatus(status)

    async with _get_db(request) as db:
        existing = (
            await db.execute(
                sa.select(concert_models.UserEventAttendance).where(
                    concert_models.UserEventAttendance.user_id == user_uuid,
                    concert_models.UserEventAttendance.event_id == event_id,
                )
            )
        ).scalar_one_or_none()

        if existing is not None:
            existing.status = new_status
            existing.source_service = types_module.ServiceType.MANUAL
            attendance = existing
        else:
            attendance = concert_models.UserEventAttendance(
                user_id=user_uuid,
                event_id=event_id,
                status=new_status,
                source_service=types_module.ServiceType.MANUAL,
            )
            db.add(attendance)

        await db.commit()

    return templates.TemplateResponse(
        request,
        "partials/attendance_status.html",
        {"attendance": attendance, "event_id": event_id},
    )


@router.get("/events/{event_id}/candidates", response_model=None)
async def event_candidates_partial(
    request: fastapi.Request,
    event_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Return candidates partial for HTMX refresh."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        event = (
            (
                await db.execute(
                    sa.select(concert_models.Event)
                    .where(concert_models.Event.id == event_id)
                    .options(
                        sa_orm.joinedload(concert_models.Event.artist_candidates),
                    )
                )
            )
            .unique()
            .scalar_one()
        )
        matched_artist_ids = [
            c.matched_artist_id
            for c in event.artist_candidates
            if c.matched_artist_id is not None
        ]
        matched_artists: dict[uuid.UUID, music_models.Artist] = {}
        if matched_artist_ids:
            ma_result = await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id.in_(matched_artist_ids)
                )
            )
            for a in ma_result.scalars().all():
                matched_artists[a.id] = a

    return templates.TemplateResponse(
        request,
        "partials/event_candidates.html",
        {"event": event, "matched_artists": matched_artists},
    )


@router.post("/events/{event_id}/candidates/{candidate_id}/accept", response_model=None)
async def accept_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Accept an artist candidate and create a confirmed EventArtist."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        candidate = (
            await db.execute(
                sa.select(concert_models.EventArtistCandidate).where(
                    concert_models.EventArtistCandidate.id == candidate_id,
                    concert_models.EventArtistCandidate.event_id == event_id,
                )
            )
        ).scalar_one_or_none()

        if candidate is None:
            raise fastapi.HTTPException(status_code=404, detail="Candidate not found")

        if candidate.matched_artist_id is None:
            raise fastapi.HTTPException(
                status_code=400, detail="Candidate has no matched artist"
            )

        event_artist = concert_models.EventArtist(
            event_id=event_id,
            artist_id=candidate.matched_artist_id,
            position=candidate.position,
            raw_name=candidate.raw_name,
        )
        db.add(event_artist)
        candidate.status = types_module.CandidateStatus.ACCEPTED
        await db.commit()

    response = templates.TemplateResponse(
        request, "partials/candidate_accepted.html", {"candidate": candidate}
    )
    response.headers["HX-Trigger"] = "artistsChanged"
    return response


@router.post("/events/{event_id}/candidates/{candidate_id}/reject", response_model=None)
async def reject_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Reject an artist candidate."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        candidate = (
            await db.execute(
                sa.select(concert_models.EventArtistCandidate).where(
                    concert_models.EventArtistCandidate.id == candidate_id,
                    concert_models.EventArtistCandidate.event_id == event_id,
                )
            )
        ).scalar_one_or_none()

        if candidate is None:
            raise fastapi.HTTPException(status_code=404, detail="Candidate not found")

        candidate.status = types_module.CandidateStatus.REJECTED
        await db.commit()

    return templates.TemplateResponse(
        request, "partials/candidate_rejected.html", {"candidate": candidate}
    )


@router.post("/events/{event_id}/add-artist", response_model=None)
async def add_artist_to_event_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    artist_id: Annotated[uuid.UUID, fastapi.Form()],
    candidate_id: Annotated[uuid.UUID | None, fastapi.Form()] = None,
) -> fastapi.responses.HTMLResponse:
    """Create a candidate from artist search and return feedback partial."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.HTMLResponse("Unauthorized", status_code=401)

    async with _get_db(request) as db:
        artist = (
            await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id == artist_id
                )
            )
        ).scalar_one_or_none()
        if artist is None:
            return fastapi.responses.HTMLResponse(
                '<small style="color: var(--pico-del-color);">Artist not found</small>'
            )

        # Check if already confirmed as EventArtist
        already_confirmed = (
            await db.execute(
                sa.select(concert_models.EventArtist).where(
                    concert_models.EventArtist.event_id == event_id,
                    concert_models.EventArtist.artist_id == artist_id,
                )
            )
        ).scalar_one_or_none()
        # Resolve the specific candidate if provided
        if candidate_id is not None:
            target_candidate = (
                await db.execute(
                    sa.select(concert_models.EventArtistCandidate).where(
                        concert_models.EventArtistCandidate.id == candidate_id,
                        concert_models.EventArtistCandidate.event_id == event_id,
                    )
                )
            ).scalar_one_or_none()
            if target_candidate is not None:
                target_candidate.matched_artist_id = artist.id
                target_candidate.confidence_score = 100
                target_candidate.status = types_module.CandidateStatus.ACCEPTED

        if already_confirmed is not None:
            if candidate_id is not None and target_candidate is not None:
                await db.commit()
            # Reload and return updated candidates
            event = (
                (
                    await db.execute(
                        sa.select(concert_models.Event)
                        .where(concert_models.Event.id == event_id)
                        .options(
                            sa_orm.joinedload(concert_models.Event.artist_candidates),
                        )
                    )
                )
                .unique()
                .scalar_one()
            )
            matched_artist_ids = [
                c.matched_artist_id
                for c in event.artist_candidates
                if c.matched_artist_id is not None
            ]
            early_matched: dict[uuid.UUID, music_models.Artist] = {}
            if matched_artist_ids:
                ma_result = await db.execute(
                    sa.select(music_models.Artist).where(
                        music_models.Artist.id.in_(matched_artist_ids)
                    )
                )
                for a in ma_result.scalars().all():
                    early_matched[a.id] = a
            resp = templates.TemplateResponse(
                request,
                "partials/event_candidates.html",
                {"event": event, "matched_artists": early_matched},
            )
            resp.headers["HX-Trigger"] = "artistsChanged"
            return resp

        # Check for any existing candidate with this raw_name
        existing = (
            await db.execute(
                sa.select(concert_models.EventArtistCandidate).where(
                    concert_models.EventArtistCandidate.event_id == event_id,
                    concert_models.EventArtistCandidate.raw_name == artist.name,
                )
            )
        ).scalar_one_or_none()

        if existing is not None:
            existing.matched_artist_id = artist.id
            existing.confidence_score = 100
            existing.status = types_module.CandidateStatus.ACCEPTED
            candidate = existing
        else:
            candidate = concert_models.EventArtistCandidate(
                event_id=event_id,
                raw_name=artist.name,
                matched_artist_id=artist.id,
                status=types_module.CandidateStatus.ACCEPTED,
                confidence_score=100,
            )
            db.add(candidate)

        event_artist = concert_models.EventArtist(
            event_id=event_id,
            artist_id=artist.id,
            position=candidate.position or 0,
            raw_name=artist.name,
        )
        db.add(event_artist)
        await db.commit()

        # Reload event with candidates for refreshing the candidates section
        event = (
            (
                await db.execute(
                    sa.select(concert_models.Event)
                    .where(concert_models.Event.id == event_id)
                    .options(
                        sa_orm.joinedload(concert_models.Event.artist_candidates),
                    )
                )
            )
            .unique()
            .scalar_one()
        )

        matched_artist_ids = [
            c.matched_artist_id
            for c in event.artist_candidates
            if c.matched_artist_id is not None
        ]
        matched_artists: dict[uuid.UUID, music_models.Artist] = {}
        if matched_artist_ids:
            ma_result = await db.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id.in_(matched_artist_ids)
                )
            )
            for a in ma_result.scalars().all():
                matched_artists[a.id] = a

    response = templates.TemplateResponse(
        request,
        "partials/event_candidates.html",
        {"event": event, "matched_artists": matched_artists},
    )
    response.headers["HX-Trigger"] = "artistsChanged"
    return response


@router.get("/partials/artist-search", response_model=None)
async def artist_search_partial(
    request: fastapi.Request,
    q: str = "",
    event_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Search artists by name and return results partial for HTMX."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    if len(q.strip()) < 2:
        return fastapi.responses.HTMLResponse("")

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(music_models.Artist)
            .where(music_models.Artist.name.ilike(f"%{_escape_ilike(q.strip())}%"))
            .order_by(music_models.Artist.name)
            .limit(10)
        )
        artists = list(result.scalars().all())

    return templates.TemplateResponse(
        request,
        "partials/artist_search_results.html",
        {
            "artists": artists,
            "event_id": event_id,
            "query": q.strip(),
        },
    )


@router.get("/partials/artist-search-modal", response_model=None)
async def artist_search_modal_partial(
    request: fastapi.Request,
    q: str = "",
    event_id: uuid.UUID | None = None,
    candidate_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Serve the external search modal dialog."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    connected_services: list[str] = []
    async with _get_db(request) as db:
        stmt = sa.select(user_models.ServiceConnection.service_type).where(
            user_models.ServiceConnection.user_id == uuid.UUID(user_id),
            user_models.ServiceConnection.service_type
            == types_module.ServiceType.SPOTIFY,
        )
        result = await db.execute(stmt)
        if result.scalar_one_or_none() is not None:
            connected_services.append("spotify")

    return templates.TemplateResponse(
        request,
        "partials/artist_search_modal.html",
        {
            "query": q,
            "event_id": event_id,
            "candidate_id": candidate_id,
            "connected_services": connected_services,
        },
    )


@router.get("/partials/artist-search-external", response_model=None)
async def artist_search_external_partial(
    request: fastapi.Request,
    q: str = "",
    event_id: uuid.UUID | None = None,
    candidate_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Search external services and return results partial for HTMX."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    registry = request.app.state.connector_registry
    lb = registry.get_base_connector(types_module.ServiceType.LISTENBRAINZ)
    query = q.strip()

    results: list[dict[str, Any]] = []
    detected_service: str | None = None

    # Check if query is a recognized service URL
    url_connector = None
    url_id: str | None = None
    if query.startswith(("http://", "https://")):
        for connector in registry.all_base_connectors():
            parsed = connector.parse_url(query)
            if parsed is not None:
                url_connector = connector
                url_id = parsed
                break

    if url_connector is not None and url_id is not None:
        detected_service = url_connector.service_type.value
        if isinstance(url_connector, listenbrainz_module.ListenBrainzConnector):
            detected_service = "musicbrainz"
            mb_artist = await url_connector.get_artist_by_mbid(url_id)
            if mb_artist:
                results = [mb_artist]
        elif isinstance(url_connector, spotify_module.SpotifyConnector):
            async with _get_db(request) as db:
                conn_result = await db.execute(
                    sa.select(user_models.ServiceConnection).where(
                        user_models.ServiceConnection.user_id == uuid.UUID(user_id),
                        user_models.ServiceConnection.service_type
                        == types_module.ServiceType.SPOTIFY,
                    )
                )
                spotify_conn = conn_result.scalar_one_or_none()
                if spotify_conn and spotify_conn.encrypted_access_token:
                    settings = request.app.state.settings
                    token = crypto_module.decrypt_token(
                        spotify_conn.encrypted_access_token,
                        settings.token_encryption_key,
                    )
                    if (
                        spotify_conn.token_expires_at is not None
                        and spotify_conn.token_expires_at
                        <= datetime.datetime.now(datetime.UTC)
                        and spotify_conn.encrypted_refresh_token is not None
                    ):
                        refresh = crypto_module.decrypt_token(
                            spotify_conn.encrypted_refresh_token,
                            settings.token_encryption_key,
                        )
                        tok_resp = await url_connector.refresh_access_token(refresh)
                        token = tok_resp.access_token
                        spotify_conn.encrypted_access_token = (
                            crypto_module.encrypt_token(
                                token, settings.token_encryption_key
                            )
                        )
                        if tok_resp.expires_in is not None:
                            spotify_conn.token_expires_at = datetime.datetime.now(
                                datetime.UTC
                            ) + datetime.timedelta(seconds=tok_resp.expires_in)
                        await db.commit()
                    artist_data = await url_connector.get_artist_by_id(token, url_id)
                    if artist_data:
                        results.append(
                            {
                                "mbid": "",
                                "name": artist_data["name"],
                                "disambiguation": "",
                                "artist_type": "",
                                "area": "",
                                "begin_year": None,
                                "end_year": None,
                                "source": "spotify",
                                "spotify_id": artist_data["spotify_id"],
                            }
                        )
    elif len(query) < 2:
        return fastapi.responses.HTMLResponse("")
    elif lb:
        mb_results = await lb.search_artists(query, limit=10)
        results = mb_results
        async with _get_db(request) as db:
            for r in mb_results:
                stmt = sa.select(music_models.Artist).where(
                    sa.or_(
                        music_models.Artist.service_links["musicbrainz"][
                            "id"
                        ].as_string()
                        == r["mbid"],
                        music_models.Artist.service_links["listenbrainz"].as_string()
                        == r["mbid"],
                    )
                )
                result = await db.execute(stmt)
                existing = result.scalar_one_or_none()
                r["already_imported"] = existing is not None
                r["local_artist_id"] = str(existing.id) if existing else None
                r["already_on_event"] = False
                if existing is not None and event_id is not None:
                    on_event = await db.execute(
                        sa.select(concert_models.EventArtist.id).where(
                            concert_models.EventArtist.event_id == event_id,
                            concert_models.EventArtist.artist_id == existing.id,
                        )
                    )
                    r["already_on_event"] = on_event.scalar_one_or_none() is not None
            results = mb_results

    return templates.TemplateResponse(
        request,
        "partials/artist_external_results.html",
        {
            "external_artists": results,
            "event_id": event_id,
            "candidate_id": candidate_id,
            "detected_service": detected_service,
        },
    )


@router.post("/partials/artist-import", response_model=None)
async def artist_import_partial(
    request: fastapi.Request,
    mbid: Annotated[str, fastapi.Form()] = "",
    spotify_id: Annotated[str, fastapi.Form()] = "",
    name: Annotated[str, fastapi.Form()] = "",
    disambiguation: Annotated[str, fastapi.Form()] = "",
    artist_type: Annotated[str, fastapi.Form()] = "",
    area: Annotated[str, fastapi.Form()] = "",
    begin_year: Annotated[str, fastapi.Form()] = "",
    end_year: Annotated[str, fastapi.Form()] = "",
    event_id: Annotated[uuid.UUID | None, fastapi.Form()] = None,
    candidate_id: Annotated[uuid.UUID | None, fastapi.Form()] = None,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Import an artist from external search and return a local result row."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        artist: music_models.Artist | None = None

        # Check for existing artist by MBID or Spotify ID
        dedup_conditions: list[sa.ColumnElement[bool]] = []
        if mbid:
            dedup_conditions.append(
                music_models.Artist.service_links["musicbrainz"]["id"].as_string()
                == mbid
            )
            dedup_conditions.append(
                music_models.Artist.service_links["listenbrainz"].as_string() == mbid
            )
        if spotify_id:
            dedup_conditions.append(
                music_models.Artist.service_links["spotify"]["id"].as_string()
                == spotify_id
            )
        if dedup_conditions:
            result = await db.execute(
                sa.select(music_models.Artist).where(sa.or_(*dedup_conditions))
            )
            artist = result.scalar_one_or_none()

        if artist is None:
            links: dict[str, Any] = {}
            if mbid:
                links["musicbrainz"] = {"id": mbid}
            if spotify_id:
                links["spotify"] = {"id": spotify_id}
            artist = music_models.Artist(
                name=name,
                disambiguation=disambiguation or None,
                artist_type=artist_type or None,
                area=area or None,
                begin_year=int(begin_year) if begin_year else None,
                end_year=int(end_year) if end_year else None,
                service_links=links,
            )
            db.add(artist)
            await db.flush()

        if event_id:
            # Check if already on event
            already = (
                await db.execute(
                    sa.select(concert_models.EventArtist.id).where(
                        concert_models.EventArtist.event_id == event_id,
                        concert_models.EventArtist.artist_id == artist.id,
                    )
                )
            ).scalar_one_or_none()

            if already is None:
                # Resolve specific candidate if provided
                if candidate_id:
                    candidate = (
                        await db.execute(
                            sa.select(concert_models.EventArtistCandidate).where(
                                concert_models.EventArtistCandidate.id == candidate_id,
                                concert_models.EventArtistCandidate.event_id
                                == event_id,
                            )
                        )
                    ).scalar_one_or_none()
                    if candidate is not None:
                        candidate.matched_artist_id = artist.id
                        candidate.confidence_score = 100
                        candidate.status = types_module.CandidateStatus.ACCEPTED

                # Also check for unresolved candidate by name
                if not candidate_id:
                    existing_candidate = (
                        await db.execute(
                            sa.select(concert_models.EventArtistCandidate).where(
                                concert_models.EventArtistCandidate.event_id
                                == event_id,
                                concert_models.EventArtistCandidate.raw_name
                                == artist.name,
                                concert_models.EventArtistCandidate.status
                                == types_module.CandidateStatus.PENDING,
                            )
                        )
                    ).scalar_one_or_none()
                    if existing_candidate is not None:
                        existing_candidate.matched_artist_id = artist.id
                        existing_candidate.confidence_score = 100
                        existing_candidate.status = (
                            types_module.CandidateStatus.ACCEPTED
                        )

                db.add(
                    concert_models.EventArtist(
                        event_id=event_id,
                        artist_id=artist.id,
                        position=0,
                        raw_name=artist.name,
                    )
                )

        await db.commit()

    response = fastapi.responses.HTMLResponse("")
    response.headers["HX-Trigger"] = '{"artist-imported":"", "artistsChanged":""}'
    return response


_ENRICHMENT_STALENESS_SECONDS = 180


@router.get("/partials/artist-enrich/{artist_id}", response_model=None)
async def artist_enrich_partial(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    event_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Lazily enrich an artist with MusicBrainz metadata."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
        artist = result.scalar_one_or_none()
        if artist is None:
            return fastapi.responses.HTMLResponse("")

        mbid = artist_utils.get_mbid(artist.service_links)

        # Already enriched or no MBID — return as-is
        if not mbid or artist.disambiguation is not None:
            return templates.TemplateResponse(
                request,
                "partials/artist_row.html",
                {"artist": artist, "event_id": event_id},
            )

        # Check enrichment_requested_at timestamp
        mb_data = (artist.service_links or {}).get("musicbrainz", {})
        requested_at_str = (
            mb_data.get("enrichment_requested_at")
            if isinstance(mb_data, dict)
            else None
        )
        if requested_at_str:
            requested_at = datetime.datetime.fromisoformat(requested_at_str)
            elapsed = (
                datetime.datetime.now(datetime.UTC) - requested_at
            ).total_seconds()
            if elapsed < _ENRICHMENT_STALENESS_SECONDS:
                # Recent request, skip
                return templates.TemplateResponse(
                    request,
                    "partials/artist_row.html",
                    {"artist": artist, "event_id": event_id},
                )

        # Mark enrichment requested
        links = dict(artist.service_links or {})
        mb: dict[str, Any] = (
            dict(links.get("musicbrainz", {}))
            if isinstance(links.get("musicbrainz"), dict)
            else {}
        )
        mb["enrichment_requested_at"] = datetime.datetime.now(datetime.UTC).isoformat()
        if mbid and "id" not in mb:
            mb["id"] = mbid
        links["musicbrainz"] = mb
        artist.service_links = links
        await db.commit()

        # Fetch from MusicBrainz
        registry = request.app.state.connector_registry
        lb = registry.get_base_connector(types_module.ServiceType.LISTENBRAINZ)
        if lb:
            mb_artist = await lb.get_artist_by_mbid(mbid)
            if mb_artist:
                artist.disambiguation = mb_artist.get("disambiguation") or ""
                artist.artist_type = mb_artist.get("artist_type") or None
                artist.area = mb_artist.get("area") or None
                artist.begin_year = mb_artist.get("begin_year")
                artist.end_year = mb_artist.get("end_year")
            else:
                artist.disambiguation = ""
            await db.commit()

    return templates.TemplateResponse(
        request,
        "partials/artist_row.html",
        {"artist": artist, "event_id": event_id},
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


@router.get("/admin/resolution", response_model=None)
async def admin_resolution(
    request: fastapi.Request,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Admin page for entity resolution: candidates, duplicates, orphans."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_role = _user_role(request)
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    async with _get_db(request) as db:
        import resonance.normalize as normalize_module

        # Pending venue candidates
        pending_vc_result = await db.execute(
            sa.select(concert_models.VenueCandidate)
            .where(
                concert_models.VenueCandidate.status
                == types_module.CandidateStatus.PENDING
            )
            .order_by(concert_models.VenueCandidate.created_at.desc())
            .limit(50)
        )
        pending_venue_candidates = list(pending_vc_result.scalars().all())

        # Pending event candidates
        pending_ec_result = await db.execute(
            sa.select(concert_models.EventCandidate)
            .where(
                concert_models.EventCandidate.status
                == types_module.CandidateStatus.PENDING
            )
            .order_by(concert_models.EventCandidate.created_at.desc())
            .limit(50)
        )
        pending_event_candidates = list(pending_ec_result.scalars().all())

        # Venues with multiple candidates (potential duplicates)
        multi_cand_venues: list[
            tuple[concert_models.Venue, list[concert_models.VenueCandidate]]
        ] = []
        venues_result = await db.execute(
            sa.select(concert_models.Venue).options(
                sa_orm.selectinload(concert_models.Venue.candidates)
            )
        )
        for venue in venues_result.scalars().all():
            if len(venue.candidates) > 1:
                multi_cand_venues.append((venue, list(venue.candidates)))

        # Events with multiple candidates (cross-source)
        multi_cand_events: list[
            tuple[concert_models.Event, list[concert_models.EventCandidate]]
        ] = []
        events_result = await db.execute(
            sa.select(concert_models.Event).options(
                sa_orm.selectinload(concert_models.Event.event_candidates)
            )
        )
        for event in events_result.scalars().all():
            if len(event.event_candidates) > 1:
                multi_cand_events.append((event, list(event.event_candidates)))

        # Suggest venue merges via normalized name matching
        all_venues_result = await db.execute(sa.select(concert_models.Venue))
        all_venues = list(all_venues_result.scalars().all())
        venue_merge_suggestions: list[list[concert_models.Venue]] = []
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
                venue_merge_suggestions.append(group)
                for v in group:
                    seen_ids.add(v.id)

        # Orphaned venues (no candidates, no events)
        orphaned_venues: list[concert_models.Venue] = []
        venues_with_events = await db.execute(
            sa.select(concert_models.Venue).options(
                sa_orm.selectinload(concert_models.Venue.candidates),
                sa_orm.selectinload(concert_models.Venue.events),
            )
        )
        for v in venues_with_events.scalars().all():
            if not v.candidates and not v.events:
                orphaned_venues.append(v)

    return templates.TemplateResponse(
        request,
        "admin_resolution.html",
        {
            "user_id": user_id,
            "user_tz": _user_tz(request),
            "user_role": user_role,
            "pending_venue_candidates": pending_venue_candidates,
            "pending_event_candidates": pending_event_candidates,
            "multi_cand_venues": multi_cand_venues,
            "multi_cand_events": multi_cand_events,
            "venue_merge_suggestions": venue_merge_suggestions,
            "orphaned_venues": orphaned_venues,
        },
    )


@router.post("/admin/resolution/unlink-venue-candidate/{candidate_id}")
async def unlink_venue_candidate(
    candidate_id: uuid.UUID,
    request: fastapi.Request,
) -> dict[str, str]:
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
    return {"status": "unlinked", "candidate_id": str(candidate_id)}


@router.post("/admin/resolution/unlink-event-candidate/{candidate_id}")
async def unlink_event_candidate(
    candidate_id: uuid.UUID,
    request: fastapi.Request,
) -> dict[str, str]:
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
    return {"status": "unlinked", "candidate_id": str(candidate_id)}


@router.post("/admin/resolution/delete-orphan-venue/{venue_id}")
async def delete_orphan_venue(
    venue_id: uuid.UUID,
    request: fastapi.Request,
) -> dict[str, str]:
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
    return {"status": "deleted", "venue_id": str(venue_id)}


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
