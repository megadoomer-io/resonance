"""Event routes: list, detail, artist management, candidates, attendance."""

from __future__ import annotations

import datetime
import uuid
from typing import Annotated, Any

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.concerts.sync as concert_sync
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.music as music_models
import resonance.models.user as user_models
import resonance.services.artist_utils as artist_utils
import resonance.types as types_module
import resonance.ui.common as common
import resonance.ui.filters as filters_module
import resonance.ui.htmx as htmx
import resonance.ui.view_filters as view_filters_module

router = fastapi.APIRouter(tags=["ui"])

_ENRICHMENT_STALENESS_SECONDS = 180


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _normalize_positions(
    artists: list[concert_models.EventArtist],
) -> list[concert_models.EventArtist]:
    """Assign sequential positions (0, 1, 2, ...) based on current sort order."""
    sorted_artists = sorted(artists, key=lambda ea: ea.position)
    for i, ea in enumerate(sorted_artists):
        ea.position = i
    return sorted_artists


# ---------------------------------------------------------------------------
# Event list
# ---------------------------------------------------------------------------


@router.get("/events", response_model=None)
async def events_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Render paginated events list with venue and artist info."""
    offset = common.page_offset(page)

    params = dict(request.query_params)
    multi_params = {
        "attendance": request.query_params.getlist("attendance"),
        "source_service": request.query_params.getlist("source_service"),
    }

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

    if active_preset == "upcoming" and "date_from" not in params:
        params["date_from"] = view_filters_module._today_iso()

    applied = filters_module.parse_filter_params(
        view_filters_module.EVENT_FILTERS, params, multi_params=multi_params
    )

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

    query = filters_module.apply_filters(
        query,
        view_filters_module.EVENT_FILTERS,
        params,
        multi_params=multi_params,
    )

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
                concert_models.UserEventAttendance.user_id == user_id,
                concert_models.UserEventAttendance.status.in_(status_values),
            )
            conditions.append(concert_models.Event.id.in_(attendance_subquery))
        if include_unset:
            has_attendance = sa.select(
                concert_models.UserEventAttendance.event_id
            ).where(
                concert_models.UserEventAttendance.user_id == user_id,
            )
            conditions.append(concert_models.Event.id.not_in(has_attendance))
        query = query.where(sa.or_(*conditions))
    elif params.get("include_not_going") != "true":
        not_going_subquery = sa.select(
            concert_models.UserEventAttendance.event_id
        ).where(
            concert_models.UserEventAttendance.user_id == user_id,
            concert_models.UserEventAttendance.status == "NOT_GOING",
        )
        query = query.where(concert_models.Event.id.not_in(not_going_subquery))

    query = (
        query.group_by(concert_models.Event.id)
        .order_by(concert_models.Event.event_date.desc())
        .offset(offset)
        .limit(common.PAGE_SIZE + 1)
    )

    total_event_count = await common.count_rows(db, concert_models.Event)

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

    has_next = len(events) > common.PAGE_SIZE
    events = events[: common.PAGE_SIZE]

    event_ids = [e.id for e in events]
    attendance_map = await _get_attendance_map(db, user_id, event_ids)

    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.EVENT_FILTERS
    )
    if valid_attendance:
        attendance_parts = [f"attendance={v}" for v in valid_attendance]
        if filter_qs:
            filter_qs += "&" + "&".join(attendance_parts)
        else:
            filter_qs = "&".join(attendance_parts)
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
    if valid_attendance:
        template_active_filters["attendance"] = valid_attendance
    if q_value:
        template_active_filters["q"] = q_value

    ctx = {
        **common.base_context(request),
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
        "total_event_count": total_event_count,
    }

    return htmx.render_fragment(
        request,
        common.templates,
        partial_template="partials/event_list.html",
        full_template="events.html",
        context=ctx,
    )


# ---------------------------------------------------------------------------
# Event detail
# ---------------------------------------------------------------------------


@router.get("/events/{event_id}", response_model=None)
async def event_detail_page(
    request: fastapi.Request,
    event_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Render event detail page with artists, candidates, and add-artist search."""
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
                concert_models.UserEventAttendance.user_id == user_id,
                concert_models.UserEventAttendance.event_id == event_id,
            )
        )
    ).scalar_one_or_none()

    ctx = {
        **common.base_context(request),
        "event": event,
        "matched_artists": matched_artists,
        "attendance": attendance,
    }

    return common.templates.TemplateResponse(request, "event_detail.html", ctx)


# ---------------------------------------------------------------------------
# Artist ordering (move up / down / remove)
# ---------------------------------------------------------------------------


@router.post("/events/{event_id}/artists/{ea_id}/move-up", response_model=None)
async def move_artist_up(
    request: fastapi.Request,
    event_id: uuid.UUID,
    ea_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Swap an event artist with the one above it and return updated list."""
    event = await _load_event_with_artists(db, event_id)
    sorted_artists = _normalize_positions(list(event.artists))
    target_idx = next(
        (i for i, ea in enumerate(sorted_artists) if ea.id == ea_id), None
    )
    if target_idx is not None and target_idx > 0:
        sorted_artists[target_idx - 1].position = target_idx
        sorted_artists[target_idx].position = target_idx - 1
    await db.commit()

    return common.templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


@router.post("/events/{event_id}/artists/{ea_id}/move-down", response_model=None)
async def move_artist_down(
    request: fastapi.Request,
    event_id: uuid.UUID,
    ea_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Swap an event artist with the one below it and return updated list."""
    event = await _load_event_with_artists(db, event_id)
    sorted_artists = _normalize_positions(list(event.artists))
    target_idx = next(
        (i for i, ea in enumerate(sorted_artists) if ea.id == ea_id), None
    )
    if target_idx is not None and target_idx < len(sorted_artists) - 1:
        sorted_artists[target_idx].position = target_idx + 1
        sorted_artists[target_idx + 1].position = target_idx
    await db.commit()

    return common.templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


@router.post("/events/{event_id}/artists/{ea_id}/remove", response_model=None)
async def remove_artist_from_event(
    request: fastapi.Request,
    event_id: uuid.UUID,
    ea_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Remove a confirmed artist from an event and return updated list."""
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

    return common.templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


@router.get("/events/{event_id}/artists", response_model=None)
async def event_artists_partial(
    request: fastapi.Request,
    event_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Return confirmed artists partial for HTMX refresh."""
    event = await _load_event_with_artists(db, event_id)

    return common.templates.TemplateResponse(
        request,
        "partials/event_confirmed_artists.html",
        {"event": event},
    )


# ---------------------------------------------------------------------------
# Attendance
# ---------------------------------------------------------------------------


@router.post("/events/{event_id}/attendance", response_model=None)
async def set_attendance(
    request: fastapi.Request,
    event_id: uuid.UUID,
    status: Annotated[str, fastapi.Form()],
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Set user's attendance status for an event, returns updated partial."""
    new_status = types_module.AttendanceStatus(status)

    existing = (
        await db.execute(
            sa.select(concert_models.UserEventAttendance).where(
                concert_models.UserEventAttendance.user_id == user_id,
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
            user_id=user_id,
            event_id=event_id,
            status=new_status,
            source_service=types_module.ServiceType.MANUAL,
        )
        db.add(attendance)

    await db.commit()

    return common.templates.TemplateResponse(
        request,
        "partials/attendance_status.html",
        {"attendance": attendance, "event_id": event_id},
    )


# ---------------------------------------------------------------------------
# Candidates (accept / reject / list)
# ---------------------------------------------------------------------------


@router.get("/events/{event_id}/candidates", response_model=None)
async def event_candidates_partial(
    request: fastapi.Request,
    event_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Return candidates partial for HTMX refresh."""
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

    return common.templates.TemplateResponse(
        request,
        "partials/event_candidates.html",
        {"event": event, "matched_artists": matched_artists},
    )


@router.post("/events/{event_id}/candidates/{candidate_id}/accept", response_model=None)
async def accept_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Accept an artist candidate and create a confirmed EventArtist."""
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
    await db.flush()

    # Normalize positions to ensure sequential ordering after insert
    event = await _load_event_with_artists(db, event_id)
    _normalize_positions(list(event.artists))
    await db.commit()

    response = common.templates.TemplateResponse(
        request, "partials/candidate_accepted.html", {"candidate": candidate}
    )
    return htmx.trigger_event(response, "artistsChanged")


@router.post("/events/{event_id}/candidates/{candidate_id}/reject", response_model=None)
async def reject_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Reject an artist candidate."""
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

    return common.templates.TemplateResponse(
        request, "partials/candidate_rejected.html", {"candidate": candidate}
    )


@router.post(
    "/events/{event_id}/candidates/{candidate_id}/unreject", response_model=None
)
async def unreject_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Move a rejected candidate back to pending status."""
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

    if candidate.status != types_module.CandidateStatus.REJECTED:
        raise fastapi.HTTPException(
            status_code=400, detail="Only rejected candidates can be unrejected"
        )

    candidate.status = types_module.CandidateStatus.PENDING
    await db.commit()

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

    return common.templates.TemplateResponse(
        request,
        "partials/event_candidates.html",
        {"event": event, "matched_artists": matched_artists},
    )


@router.post(
    "/events/{event_id}/candidates/{candidate_id}/unaccept", response_model=None
)
async def unaccept_candidate_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Revert an accepted candidate: remove the EventArtist and return to pending."""
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

    accepted_statuses = {
        types_module.CandidateStatus.ACCEPTED,
        types_module.CandidateStatus.AUTO_ACCEPTED,
    }
    if candidate.status not in accepted_statuses:
        raise fastapi.HTTPException(
            status_code=400, detail="Only accepted candidates can be unaccepted"
        )

    # Remove the corresponding EventArtist if one exists
    if candidate.matched_artist_id is not None:
        ea = (
            await db.execute(
                sa.select(concert_models.EventArtist).where(
                    concert_models.EventArtist.event_id == event_id,
                    concert_models.EventArtist.artist_id == candidate.matched_artist_id,
                )
            )
        ).scalar_one_or_none()
        if ea is not None:
            await db.delete(ea)

    candidate.status = types_module.CandidateStatus.PENDING
    await db.flush()

    # Normalize positions on remaining artists
    event = await _load_event_with_artists(db, event_id)
    _normalize_positions(list(event.artists))
    await db.commit()

    # Reload with candidates for the response
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

    response = common.templates.TemplateResponse(
        request,
        "partials/event_candidates.html",
        {"event": event, "matched_artists": matched_artists},
    )
    return htmx.trigger_event(response, "artistsChanged")


# ---------------------------------------------------------------------------
# Add artist to event (from search)
# ---------------------------------------------------------------------------


@router.post("/events/{event_id}/add-artist", response_model=None)
async def add_artist_to_event_ui(
    request: fastapi.Request,
    event_id: uuid.UUID,
    artist_id: Annotated[uuid.UUID, fastapi.Form()],
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    candidate_id: Annotated[uuid.UUID | None, fastapi.Form()] = None,
) -> fastapi.responses.HTMLResponse:
    """Create a candidate from artist search and return feedback partial."""
    artist = (
        await db.execute(
            sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
        )
    ).scalar_one_or_none()
    if artist is None:
        return fastapi.responses.HTMLResponse(
            '<small style="color: var(--pico-del-color);">Artist not found</small>'
        )

    already_confirmed = (
        await db.execute(
            sa.select(concert_models.EventArtist).where(
                concert_models.EventArtist.event_id == event_id,
                concert_models.EventArtist.artist_id == artist_id,
            )
        )
    ).scalar_one_or_none()
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
        resp = common.templates.TemplateResponse(
            request,
            "partials/event_candidates.html",
            {"event": event, "matched_artists": early_matched},
        )
        return htmx.trigger_event(resp, "artistsChanged")

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
        candidate_obj = existing
    else:
        candidate_obj = concert_models.EventArtistCandidate(
            event_id=event_id,
            raw_name=artist.name,
            matched_artist_id=artist.id,
            status=types_module.CandidateStatus.ACCEPTED,
            confidence_score=100,
        )
        db.add(candidate_obj)

    event_artist = concert_models.EventArtist(
        event_id=event_id,
        artist_id=artist.id,
        position=candidate_obj.position or 0,
        raw_name=artist.name,
    )
    db.add(event_artist)
    await db.flush()

    # Normalize positions to ensure sequential ordering after insert
    add_event = await _load_event_with_artists(db, event_id)
    _normalize_positions(list(add_event.artists))
    await db.commit()

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

    response = common.templates.TemplateResponse(
        request,
        "partials/event_candidates.html",
        {"event": event, "matched_artists": matched_artists},
    )
    return htmx.trigger_event(response, "artistsChanged")


# ---------------------------------------------------------------------------
# Artist search partials (used from event detail add-artist flow)
# ---------------------------------------------------------------------------


@router.get("/partials/artist-search", response_model=None)
async def artist_search_partial(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str = "",
    event_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse:
    """Search artists by name and return results partial for HTMX."""
    if len(q.strip()) < 2:
        return fastapi.responses.HTMLResponse("")

    result = await db.execute(
        sa.select(music_models.Artist)
        .where(music_models.Artist.name.ilike(f"%{common.escape_ilike(q.strip())}%"))
        .order_by(music_models.Artist.name)
        .limit(10)
    )
    artists = list(result.scalars().all())

    return common.templates.TemplateResponse(
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str = "",
    event_id: uuid.UUID | None = None,
    candidate_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse:
    """Serve the external search modal dialog."""
    connected_services: list[str] = []
    stmt = sa.select(user_models.ServiceConnection.service_type).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type == types_module.ServiceType.SPOTIFY,
    )
    result = await db.execute(stmt)
    if result.scalar_one_or_none() is not None:
        connected_services.append("spotify")

    return common.templates.TemplateResponse(
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str = "",
    event_id: uuid.UUID | None = None,
    candidate_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse:
    """Search external services and return results partial for HTMX."""
    registry = request.app.state.connector_registry
    lb = registry.get_base_connector(types_module.ServiceType.LISTENBRAINZ)
    query = q.strip()

    results: list[dict[str, Any]] = []
    detected_service: str | None = None

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
            conn_result = await db.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.user_id == user_id,
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
                    spotify_conn.encrypted_access_token = crypto_module.encrypt_token(
                        token, settings.token_encryption_key
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
        for r in mb_results:
            stmt = sa.select(music_models.Artist).where(
                sa.or_(
                    music_models.Artist.service_links["musicbrainz"]["id"].as_string()
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

    return common.templates.TemplateResponse(
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
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
) -> fastapi.responses.HTMLResponse:
    """Import an artist from external search and return a local result row."""
    artist: music_models.Artist | None = None

    dedup_conditions: list[sa.ColumnElement[bool]] = []
    if mbid:
        dedup_conditions.append(
            music_models.Artist.service_links["musicbrainz"]["id"].as_string() == mbid
        )
        dedup_conditions.append(
            music_models.Artist.service_links["listenbrainz"].as_string() == mbid
        )
    if spotify_id:
        dedup_conditions.append(
            music_models.Artist.service_links["spotify"]["id"].as_string() == spotify_id
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
        already = (
            await db.execute(
                sa.select(concert_models.EventArtist.id).where(
                    concert_models.EventArtist.event_id == event_id,
                    concert_models.EventArtist.artist_id == artist.id,
                )
            )
        ).scalar_one_or_none()

        if already is None:
            if candidate_id:
                candidate = (
                    await db.execute(
                        sa.select(concert_models.EventArtistCandidate).where(
                            concert_models.EventArtistCandidate.id == candidate_id,
                            concert_models.EventArtistCandidate.event_id == event_id,
                        )
                    )
                ).scalar_one_or_none()
                if candidate is not None:
                    candidate.matched_artist_id = artist.id
                    candidate.confidence_score = 100
                    candidate.status = types_module.CandidateStatus.ACCEPTED

            if not candidate_id:
                existing_candidate = (
                    await db.execute(
                        sa.select(concert_models.EventArtistCandidate).where(
                            concert_models.EventArtistCandidate.event_id == event_id,
                            concert_models.EventArtistCandidate.raw_name == artist.name,
                            concert_models.EventArtistCandidate.status
                            == types_module.CandidateStatus.PENDING,
                        )
                    )
                ).scalar_one_or_none()
                if existing_candidate is not None:
                    existing_candidate.matched_artist_id = artist.id
                    existing_candidate.confidence_score = 100
                    existing_candidate.status = types_module.CandidateStatus.ACCEPTED

            db.add(
                concert_models.EventArtist(
                    event_id=event_id,
                    artist_id=artist.id,
                    position=0,
                    raw_name=artist.name,
                )
            )
            await db.flush()

            # Normalize positions to ensure sequential ordering after insert
            import_event = await _load_event_with_artists(db, event_id)
            _normalize_positions(list(import_event.artists))

    await db.commit()

    response = fastapi.responses.HTMLResponse("")
    response.headers["HX-Trigger"] = '{"artist-imported":"", "artistsChanged":""}'
    return response


@router.get("/partials/artist-enrich/{artist_id}", response_model=None)
async def artist_enrich_partial(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    event_id: uuid.UUID | None = None,
) -> fastapi.responses.HTMLResponse:
    """Lazily enrich an artist with MusicBrainz metadata."""
    result = await db.execute(
        sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
    )
    artist = result.scalar_one_or_none()
    if artist is None:
        return fastapi.responses.HTMLResponse("")

    mbid = artist_utils.get_mbid(artist.service_links)

    if not mbid or artist.disambiguation is not None:
        return common.templates.TemplateResponse(
            request,
            "partials/artist_row.html",
            {"artist": artist, "event_id": event_id},
        )

    mb_data = (artist.service_links or {}).get("musicbrainz", {})
    requested_at_str = (
        mb_data.get("enrichment_requested_at") if isinstance(mb_data, dict) else None
    )
    if requested_at_str:
        requested_at = datetime.datetime.fromisoformat(requested_at_str)
        elapsed = (datetime.datetime.now(datetime.UTC) - requested_at).total_seconds()
        if elapsed < _ENRICHMENT_STALENESS_SECONDS:
            return common.templates.TemplateResponse(
                request,
                "partials/artist_row.html",
                {"artist": artist, "event_id": event_id},
            )

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

    return common.templates.TemplateResponse(
        request,
        "partials/artist_row.html",
        {"artist": artist, "event_id": event_id},
    )
