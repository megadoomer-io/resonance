"""Track routes: list, detail, compare, merge preview, listening history."""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.dedup as dedup_module
import resonance.dependencies as deps_module
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models
import resonance.models.taste as taste_models
import resonance.ui.common as common
import resonance.ui.filters as filters_module
import resonance.ui.htmx as htmx
import resonance.ui.view_filters as view_filters_module

router = fastapi.APIRouter(tags=["ui"])


# ---------------------------------------------------------------------------
# Track list
# ---------------------------------------------------------------------------


@router.get("/tracks", response_model=None)
async def tracks_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Render paginated tracks list with artist names and filtering."""
    offset = common.page_offset(page)

    params = dict(request.query_params)
    presets = view_filters_module.TRACK_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    applied = filters_module.parse_filter_params(
        view_filters_module.TRACK_FILTERS, params
    )

    query = sa.select(music_models.Track).join(music_models.Artist)
    query = filters_module.apply_filters(
        query, view_filters_module.TRACK_FILTERS, params
    )
    query = (
        query.order_by(music_models.Track.title)
        .options(sa_orm.joinedload(music_models.Track.artist))
        .offset(offset)
        .limit(common.PAGE_SIZE + 1)
    )

    result = await db.execute(query)
    tracks = list(result.scalars().unique().all())

    has_next = len(tracks) > common.PAGE_SIZE
    tracks = tracks[: common.PAGE_SIZE]

    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.TRACK_FILTERS
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
        elif isinstance(value, bool):
            template_active_filters[key] = value
        else:
            template_active_filters[key] = value
    if q_value:
        template_active_filters["q"] = q_value

    ctx = {
        **common.base_context(request),
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

    return htmx.render_fragment(
        request,
        common.templates,
        partial_template="partials/track_list.html",
        full_template="tracks.html",
        context=ctx,
    )


# ---------------------------------------------------------------------------
# Track detail
# ---------------------------------------------------------------------------


@router.get("/tracks/{track_id}", response_model=None)
async def track_detail_page(
    request: fastapi.Request,
    track_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Render track detail page with listening history and duplicates."""
    offset = common.page_offset(page)

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
            music_models.ListeningEvent.user_id == user_id,
        )
        .order_by(music_models.ListeningEvent.listened_at.desc())
        .offset(offset)
        .limit(common.PAGE_SIZE + 1)
    )
    history = list(history_result.scalars().all())
    has_next = len(history) > common.PAGE_SIZE
    history = history[: common.PAGE_SIZE]
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

    ctx = {
        **common.base_context(request),
        "track": track,
        "history": history,
        "duplicates": duplicates,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
    }

    if request.headers.get("HX-Request"):
        return common.templates.TemplateResponse(
            request, "partials/track_history.html", ctx
        )
    return common.templates.TemplateResponse(request, "track_detail.html", ctx)


# ---------------------------------------------------------------------------
# Track compare
# ---------------------------------------------------------------------------


@router.get("/tracks/{track_id}/compare/{other_id}", response_model=None)
async def track_compare_page(
    request: fastapi.Request,
    track_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Render side-by-side comparison of two tracks with merge controls."""
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

    a_listen_count = await common.count_rows(
        db,
        music_models.ListeningEvent,
        music_models.ListeningEvent.track_id == track_id,
        music_models.ListeningEvent.user_id == user_id,
    )
    b_listen_count = await common.count_rows(
        db,
        music_models.ListeningEvent,
        music_models.ListeningEvent.track_id == other_id,
        music_models.ListeningEvent.user_id == user_id,
    )

    return common.templates.TemplateResponse(
        request,
        "track_compare.html",
        {
            **common.base_context(request),
            "track_a": track_a,
            "track_b": track_b,
            "canonical": canonical,
            "duplicate": duplicate,
            "a_listen_count": a_listen_count,
            "b_listen_count": b_listen_count,
        },
    )


# ---------------------------------------------------------------------------
# Track merge preview
# ---------------------------------------------------------------------------


@router.post("/tracks/{track_id}/merge-preview/{other_id}", response_model=None)
async def track_merge_preview(
    request: fastapi.Request,
    track_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Return track merge impact summary partial for HTMX."""
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

    events_to_repoint = await common.count_rows(
        db,
        music_models.ListeningEvent,
        music_models.ListeningEvent.track_id == other_id,
    )
    relations_to_repoint = await common.count_rows(
        db,
        taste_models.UserTrackRelation,
        taste_models.UserTrackRelation.track_id == other_id,
    )
    playlist_appearances = await common.count_rows(
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

    return common.templates.TemplateResponse(
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


# ---------------------------------------------------------------------------
# Listening history
# ---------------------------------------------------------------------------


@router.get("/history", response_model=None)
async def history_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Render paginated listening history with filtering."""
    offset = common.page_offset(page)

    params = dict(request.query_params)
    multi_params = {
        "source": request.query_params.getlist("source"),
    }
    presets = view_filters_module.HISTORY_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    applied = filters_module.parse_filter_params(
        view_filters_module.HISTORY_FILTERS, params
    )

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
        .where(music_models.ListeningEvent.user_id == user_id)
    )

    query = filters_module.apply_filters(
        query, view_filters_module.HISTORY_FILTERS, params
    )

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
        .limit(common.PAGE_SIZE + 1)
    )

    result = await db.execute(query)
    events = list(result.scalars().unique().all())

    has_next = len(events) > common.PAGE_SIZE
    events = events[: common.PAGE_SIZE]

    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.HISTORY_FILTERS
    )
    if valid_sources:
        source_parts = [f"source={v}" for v in valid_sources]
        if filter_qs:
            filter_qs += "&" + "&".join(source_parts)
        else:
            filter_qs = "&".join(source_parts)
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
    if valid_sources:
        template_active_filters["source"] = valid_sources
    if q_value:
        template_active_filters["q"] = q_value

    ctx = {
        **common.base_context(request),
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

    return htmx.render_fragment(
        request,
        common.templates,
        partial_template="partials/history_list.html",
        full_template="history.html",
        context=ctx,
    )
