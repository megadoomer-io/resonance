"""Artist routes: list, detail, compare, merge preview."""

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
import resonance.models.concert as concert_models
import resonance.models.music as music_models
import resonance.types as types_module
import resonance.ui.common as common
import resonance.ui.filters as filters_module
import resonance.ui.htmx as htmx
import resonance.ui.view_filters as view_filters_module

router = fastapi.APIRouter(tags=["ui"])


# ---------------------------------------------------------------------------
# Artist list
# ---------------------------------------------------------------------------


@router.get("/artists", response_model=None)
async def artists_page(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
) -> fastapi.responses.HTMLResponse:
    """Render paginated artists list with filtering."""
    offset = common.page_offset(page)

    params = dict(request.query_params)
    # Repeated params (e.g. ?genre_mbid=a&genre_mbid=b) collapse under dict(); pull
    # the multi-value genre filter out of the raw query params so all selections
    # apply (OR-match), not just the last one.
    multi_params = {"genre_mbid": request.query_params.getlist("genre_mbid")}
    presets = view_filters_module.ARTIST_PRESETS
    active_preset = view_filters_module.detect_active_preset(params, presets)

    applied = filters_module.parse_filter_params(
        view_filters_module.ARTIST_FILTERS, params, multi_params=multi_params
    )

    query = sa.select(music_models.Artist)
    query = filters_module.apply_filters(
        query, view_filters_module.ARTIST_FILTERS, params, multi_params=multi_params
    )
    query = (
        query.order_by(music_models.Artist.name)
        .offset(offset)
        .limit(common.PAGE_SIZE + 1)
    )

    result = await db.execute(query)
    artists = list(result.scalars().all())

    has_next = len(artists) > common.PAGE_SIZE
    artists = artists[: common.PAGE_SIZE]

    filter_qs = view_filters_module.build_filter_query_string(
        applied.active_filters, view_filters_module.ARTIST_FILTERS
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

    return htmx.render_fragment(
        request,
        common.templates,
        partial_template="partials/artist_list.html",
        full_template="artists.html",
        context=ctx,
    )


# ---------------------------------------------------------------------------
# Artist detail
# ---------------------------------------------------------------------------


@router.get("/artists/{artist_id}", response_model=None)
async def artist_detail_page(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
    section: str = "tracks",
) -> fastapi.responses.HTMLResponse:
    """Render artist detail page with tracks, events, candidates, and duplicates."""
    offset = common.page_offset(page)

    artist_result = await db.execute(
        sa.select(music_models.Artist).where(music_models.Artist.id == artist_id)
    )
    artist = artist_result.scalar_one_or_none()
    if artist is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    track_count = await common.count_rows(
        db, music_models.Track, music_models.Track.artist_id == artist_id
    )

    tracks_result = await db.execute(
        sa.select(music_models.Track)
        .where(music_models.Track.artist_id == artist_id)
        .order_by(music_models.Track.title)
        .offset(offset)
        .limit(common.PAGE_SIZE + 1)
    )
    tracks = list(tracks_result.scalars().all())
    tracks_has_next = len(tracks) > common.PAGE_SIZE
    tracks = tracks[: common.PAGE_SIZE]
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

    ctx = {
        **common.base_context(request),
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
        return common.templates.TemplateResponse(
            request, "partials/artist_tracks.html", ctx
        )
    return common.templates.TemplateResponse(request, "artist_detail.html", ctx)


# ---------------------------------------------------------------------------
# Artist compare
# ---------------------------------------------------------------------------


@router.get("/artists/{artist_id}/compare/{other_id}", response_model=None)
async def artist_compare_page(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Render side-by-side comparison of two artists with merge controls."""
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

    a_track_count = await common.count_rows(
        db, music_models.Track, music_models.Track.artist_id == artist_id
    )
    b_track_count = await common.count_rows(
        db, music_models.Track, music_models.Track.artist_id == other_id
    )
    a_event_count = await common.count_rows(
        db,
        concert_models.EventArtist,
        concert_models.EventArtist.artist_id == artist_id,
    )
    b_event_count = await common.count_rows(
        db,
        concert_models.EventArtist,
        concert_models.EventArtist.artist_id == other_id,
    )

    return common.templates.TemplateResponse(
        request,
        "artist_compare.html",
        {
            **common.base_context(request),
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


# ---------------------------------------------------------------------------
# Artist merge preview
# ---------------------------------------------------------------------------


@router.post("/artists/{artist_id}/merge-preview/{other_id}", response_model=None)
async def artist_merge_preview(
    request: fastapi.Request,
    artist_id: uuid.UUID,
    other_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_user)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Return merge impact summary partial for HTMX."""
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

    tracks_to_repoint = await common.count_rows(
        db, music_models.Track, music_models.Track.artist_id == other_id
    )
    events_to_repoint = await common.count_rows(
        db,
        concert_models.EventArtist,
        concert_models.EventArtist.artist_id == other_id,
    )

    merged_links = dict(canonical.service_links or {})
    for k, v in (duplicate.service_links or {}).items():
        if v and k not in merged_links:
            merged_links[k] = v

    return common.templates.TemplateResponse(
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
