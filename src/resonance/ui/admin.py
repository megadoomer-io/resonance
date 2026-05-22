"""Admin routes: dashboard, user/task management, resolution,
venue/event management, dedup, and stats."""

from __future__ import annotations

import datetime
import time
import uuid
from typing import TYPE_CHECKING, Annotated

import fastapi
import fastapi.responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.music as music_models
import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.normalize as normalize_module
import resonance.types as types_module
import resonance.ui.common as common

if TYPE_CHECKING:
    from collections.abc import Sequence

router = fastapi.APIRouter(tags=["ui"])


# ---------------------------------------------------------------------------
# Module-local constants and helpers
# ---------------------------------------------------------------------------

_VENUE_PAGE_SIZE = 50
_EVENT_ADMIN_PAGE_SIZE = 50

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


def _resolution_response(message: str) -> fastapi.responses.HTMLResponse:
    """Return an HTML response with a trigger to refresh the resolution list."""
    resp = fastapi.responses.HTMLResponse(f"<p><small>{message}</small></p>")
    resp.headers["HX-Trigger"] = "resolution-updated"
    return resp


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


async def _enqueue_bulk_job(
    request: fastapi.Request,
    db: sa_async.AsyncSession,
    operation: str,
) -> dict[str, str]:
    """Create a BULK_JOB task and enqueue it to arq."""
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


# ---------------------------------------------------------------------------
# Admin dashboard
# ---------------------------------------------------------------------------


@router.get("/admin", response_model=None)
async def admin_dashboard(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Render admin dashboard with user management controls."""
    user_count = await common.count_rows(db, user_models.User)
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

    return common.templates.TemplateResponse(
        request,
        "admin.html",
        {
            **common.base_context(request),
            "user_count": user_count,
            "users": users,
            "tasks": tasks,
        },
    )


# ---------------------------------------------------------------------------
# User management
# ---------------------------------------------------------------------------


@router.post("/admin/users/{target_user_id}/role", response_model=None)
async def change_user_role(
    target_user_id: uuid.UUID,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.RedirectResponse:
    """Change a user's role (admin/owner only)."""
    form = await request.form()
    new_role_str = form.get("role", "user")

    user_role = request.state.session.get("user_role", "user")
    if user_role != "owner" and new_role_str == "owner":
        raise fastapi.HTTPException(
            status_code=403, detail="Only owner can promote to owner"
        )

    if target_user_id == user_id:
        raise fastapi.HTTPException(
            status_code=400, detail="Cannot change your own role"
        )

    result = await db.execute(
        sa.select(user_models.User).where(user_models.User.id == target_user_id)
    )
    target_user = result.scalar_one_or_none()
    if target_user is None:
        raise fastapi.HTTPException(status_code=404)

    target_user.role = types_module.UserRole(str(new_role_str))
    await db.commit()

    return fastapi.responses.RedirectResponse(url="/admin", status_code=303)


# ---------------------------------------------------------------------------
# Task management
# ---------------------------------------------------------------------------


@router.post("/admin/tasks/{task_id}/clone", response_model=None)
async def clone_task(
    task_id: uuid.UUID,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.RedirectResponse:
    """Clone a sync task, optionally enabling step-through mode."""
    form = await request.form()
    step_mode = form.get("step_mode") == "true"

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
        user_id=user_id,
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.RedirectResponse:
    """Resume a deferred step-mode task."""
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
            # No more pending tasks -- complete the parent
            if task.parent_id is None:
                task.status = types_module.SyncStatus.COMPLETED
                task.completed_at = datetime.datetime.now(datetime.UTC)
                await db.commit()
            return fastapi.responses.RedirectResponse(url="/admin", status_code=303)
        if arq_redis:
            # Use a unique job ID to avoid arq dedup with previous runs
            job_id = f"sync_range:{next_task.id}:{int(time.time())}"
            await arq_redis.enqueue_job(
                "sync_range",
                str(next_task.id),
                _job_id=job_id,
            )

    return fastapi.responses.RedirectResponse(url="/admin", status_code=303)


# ---------------------------------------------------------------------------
# Dedup operations
# ---------------------------------------------------------------------------


@router.post("/admin/dedup-events", response_model=None)
async def dedup_listening_events(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Admin-only: enqueue cross-service event dedup as a bulk job."""
    return await _enqueue_bulk_job(request, db, "dedup_events")


@router.post("/admin/dedup-artists", response_model=None)
async def dedup_artists(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Admin-only: enqueue artist dedup as a bulk job."""
    return await _enqueue_bulk_job(request, db, "dedup_artists")


@router.post("/admin/dedup-tracks", response_model=None)
async def dedup_tracks(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Admin-only: enqueue track dedup as a bulk job."""
    return await _enqueue_bulk_job(request, db, "dedup_tracks")


# ---------------------------------------------------------------------------
# Entity resolution
# ---------------------------------------------------------------------------


@router.get("/admin/resolution", response_model=None)
async def admin_resolution(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    view: str = "pending",
    entity: str = "all",
    q: str = "",
) -> fastapi.responses.HTMLResponse:
    """Admin page for entity resolution with preset views and search."""
    if view not in _RESOLUTION_VIEWS:
        view = "pending"

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
            pending_venue_candidates = list((await db.execute(vc_stmt)).scalars().all())

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
            pending_event_candidates = list((await db.execute(ec_stmt)).scalars().all())

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
        **common.base_context(request),
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
        return common.templates.TemplateResponse(
            request, "partials/resolution_list.html", context
        )
    return common.templates.TemplateResponse(request, "admin_resolution.html", context)


@router.post("/admin/resolution/unlink-venue-candidate/{candidate_id}")
async def unlink_venue_candidate(
    candidate_id: uuid.UUID,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Unlink a VenueCandidate from its resolved Venue."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Unlink an EventCandidate from its resolved Event."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Delete a venue with no candidates and no events."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Non-destructively merge a group of venues by moving candidates.

    Picks the venue with the most events as canonical, re-points all
    candidates from the other venues to it, and re-points their events.
    The now-empty duplicate venues become orphans (deletable separately).
    """
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Mark all candidates for a venue as human-accepted."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Mark all candidates for an event as human-accepted."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Create exclusions between all venue pairs."""
    form = await request.form()
    raw_ids = form.getlist("venue_ids")
    venue_ids = [uuid.UUID(str(vid)) for vid in raw_ids]

    if len(venue_ids) < 2:
        return _resolution_response("Need at least 2 venues to exclude.")

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

    return _resolution_response(f"Excluded -- {created} exclusion(s) created.")


@router.post("/admin/resolution/merge-events")
async def merge_event_group(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Merge events by moving candidates and artists to the canonical one."""
    form = await request.form()
    raw_ids = form.getlist("event_ids")
    event_ids = [uuid.UUID(str(eid)) for eid in raw_ids]
    if len(event_ids) < 2:
        return _resolution_response("Need at least 2 events.")

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
    canonical_candidate_names = {eac.raw_name for eac in canonical.artist_candidates}
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Create exclusions between all event pairs."""
    form = await request.form()
    raw_ids = form.getlist("event_ids")
    event_ids = [uuid.UUID(str(eid)) for eid in raw_ids]

    if len(event_ids) < 2:
        return _resolution_response("Need at least 2 events to exclude.")

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

    return _resolution_response(f"Excluded -- {created} exclusion(s) created.")


# ---------------------------------------------------------------------------
# Task status and admin API
# ---------------------------------------------------------------------------


@router.get("/admin/tasks/{task_id}", response_model=None)
async def admin_task_status(
    task_id: uuid.UUID,
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    """Admin-only: get status of a bulk/admin task."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    """Admin-only: overview of recent sync tasks."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, object]:
    """Admin-only: database statistics overview."""
    artists = await common.count_rows(db, music_models.Artist)
    tracks_total = await common.count_rows(db, music_models.Track)
    events_total = await common.count_rows(db, music_models.ListeningEvent)

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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str = "",
) -> dict[str, object]:
    """Admin-only: search tracks by title (fuzzy match)."""
    if not q.strip():
        return {"error": "Query parameter 'q' is required."}

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


@router.get("/admin/venues", response_model=None)
async def admin_venues(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str = "",
    page: int = 1,
    filter: str = "",
) -> fastapi.responses.HTMLResponse:
    """Admin venue list with search and filtering."""
    offset = (page - 1) * _VENUE_PAGE_SIZE

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
        escaped = common.escape_ilike(q)
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
        **common.base_context(request),
        "venues": venues,
        "q": q,
        "filter": filter,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
        "list_url": "/admin/venues",
    }

    if request.headers.get("HX-Request"):
        return common.templates.TemplateResponse(
            request, "partials/admin_venue_list.html", ctx
        )

    return common.templates.TemplateResponse(request, "admin_venues.html", ctx)


@router.get("/admin/venues/{venue_id}", response_model=None)
async def admin_venue_detail(
    request: fastapi.Request,
    venue_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Admin venue detail page with candidate history."""
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
        **common.base_context(request),
        "venue": venue,
        "exclusions": exclusions,
        "other_venues": other_venues,
        "orphan_candidates": orphan_candidates,
    }

    return common.templates.TemplateResponse(request, "admin_venue_detail.html", ctx)


@router.post(
    "/admin/venues/{venue_id}/candidates/{candidate_id}/accept",
    response_model=None,
)
async def admin_accept_venue_candidate(
    request: fastapi.Request,
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Accept a venue candidate (UI action)."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Reject a venue candidate (UI action)."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Unlink a venue candidate back to pending (UI action)."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Claim an orphaned pending candidate into this venue."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Split selected candidates into a new venue (UI action)."""
    form = await request.form()
    raw_ids = form.getlist("candidate_ids")
    candidate_ids = [uuid.UUID(str(cid)) for cid in raw_ids]

    if not candidate_ids:
        return _entity_action_response("Select at least one candidate.", error=True)

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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Remove a venue exclusion (UI action)."""
    exclusion = await db.get(concert_models.EntityExclusion, exclusion_id)
    if not exclusion:
        return _entity_action_response("Exclusion not found.", error=True)
    await db.delete(exclusion)
    await db.commit()

    return _entity_action_response("Exclusion removed.")


# ---------------------------------------------------------------------------
# Admin: Event management
# ---------------------------------------------------------------------------


@router.get("/admin/events", response_model=None)
async def admin_events(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    q: str = "",
    page: int = 1,
    filter: str = "",
) -> fastapi.responses.HTMLResponse:
    """Admin event list with search and filtering."""
    offset = (page - 1) * _EVENT_ADMIN_PAGE_SIZE

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
        escaped = common.escape_ilike(q)
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
        **common.base_context(request),
        "events": events,
        "q": q,
        "filter": filter,
        "page": page,
        "has_next": has_next,
        "has_prev": has_prev,
        "list_url": "/admin/events",
    }

    if request.headers.get("HX-Request"):
        return common.templates.TemplateResponse(
            request, "partials/admin_event_list.html", ctx
        )

    return common.templates.TemplateResponse(request, "admin_events.html", ctx)


@router.get("/admin/events/{event_id}/manage", response_model=None)
async def admin_event_detail(
    request: fastapi.Request,
    event_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Admin event detail page with candidate history."""
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
        **common.base_context(request),
        "event": event,
        "exclusions": exclusions,
        "other_events": other_events,
        "orphan_event_candidates": orphan_event_candidates,
    }

    return common.templates.TemplateResponse(request, "admin_event_detail.html", ctx)


@router.post(
    "/admin/events/{event_id}/candidates/{candidate_id}/accept",
    response_model=None,
)
async def admin_accept_event_candidate(
    request: fastapi.Request,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Accept an event candidate (UI action)."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Reject an event candidate (UI action)."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Unlink an event candidate back to pending (UI action)."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Claim an orphaned pending candidate into this event."""
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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Split selected candidates into a new event (UI action)."""
    form = await request.form()
    raw_ids = form.getlist("candidate_ids")
    candidate_ids = [uuid.UUID(str(cid)) for cid in raw_ids]

    if not candidate_ids:
        return _entity_action_response("Select at least one candidate.", error=True)

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
    user_id: Annotated[uuid.UUID, fastapi.Depends(common.require_admin)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi.responses.HTMLResponse:
    """Remove an event exclusion (UI action)."""
    exclusion = await db.get(concert_models.EntityExclusion, exclusion_id)
    if not exclusion:
        return _entity_action_response("Exclusion not found.", error=True)
    await db.delete(exclusion)
    await db.commit()

    return _entity_action_response("Exclusion removed.")
