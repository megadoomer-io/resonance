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
            sa.select(task_models.Task)
            .where(
                task_models.Task.user_id == user_uuid,
                task_models.Task.task_type == types_module.TaskType.SYNC_JOB,
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

        canonical, duplicate = dedup_module._pick_canonical(artist_a, artist_b)

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

        canonical, duplicate = dedup_module._pick_canonical_track(track_a, track_b)

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

    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(concert_models.Event)
            .options(
                sa_orm.joinedload(concert_models.Event.venue),
                sa_orm.joinedload(concert_models.Event.artists),
                sa_orm.joinedload(concert_models.Event.artist_candidates),
            )
            .order_by(concert_models.Event.event_date.desc())
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
        events = list(result.unique().scalars().all())

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

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "event": event,
        "matched_artists": matched_artists,
    }

    return templates.TemplateResponse(request, "event_detail.html", context)


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

    return templates.TemplateResponse(
        request, "partials/candidate_accepted.html", {"candidate": candidate}
    )


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
            .where(music_models.Artist.name.ilike(f"%{q.strip()}%"))
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
        },
    )


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


@router.get("/playlists", response_model=None)
async def playlists_page(
    request: fastapi.Request,
    page: int = 1,
) -> fastapi.responses.HTMLResponse | fastapi.responses.RedirectResponse:
    """Render paginated playlists list, or redirect to login."""
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_uuid = uuid.UUID(user_id)
    offset = (page - 1) * _PAGE_SIZE

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(playlist_models.Playlist)
            .where(playlist_models.Playlist.user_id == user_uuid)
            .order_by(playlist_models.Playlist.created_at.desc())
            .offset(offset)
            .limit(_PAGE_SIZE + 1)
        )
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

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "playlists": playlists,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/playlist_list.html", context
        )
    return templates.TemplateResponse(request, "playlists.html", context)


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

    context = {
        "user_id": user_id,
        "user_tz": _user_tz(request),
        "user_role": _user_role(request),
        "playlist": playlist,
        "playlist_id": playlist_id,
        "tracks": tracks,
        "generation": generation,
        "page": page,
        "has_next": has_next,
        "has_prev": page > 1,
    }

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(
            request, "partials/playlist_detail_tracks.html", context
        )
    return templates.TemplateResponse(request, "playlist_detail.html", context)


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
