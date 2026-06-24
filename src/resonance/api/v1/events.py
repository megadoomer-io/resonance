"""Concert event API routes — list and detail endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm
import structlog

import resonance.api.v1.artists as artists_api
import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.music as music_models
import resonance.normalize as normalize_module
import resonance.types as types_module

logger = structlog.get_logger()

_PAGE_SIZE = 50

router = fastapi.APIRouter(prefix="/events", tags=["events"])


def _format_venue(venue: concert_models.Venue | Any) -> dict[str, Any]:
    return {
        "id": str(venue.id),
        "name": venue.name,
        "city": venue.city,
        "state": venue.state,
        "country": venue.country,
    }


def _format_event_summary(event: concert_models.Event | Any) -> dict[str, Any]:
    return {
        "id": str(event.id),
        "title": event.title,
        "event_date": str(event.event_date),
        "source_service": str(event.source_service),
        "external_url": event.external_url,
        "venue": _format_venue(event.venue) if event.venue else None,
        "artist_count": len(event.artists) if event.artists else 0,
        "candidate_count": len(event.artist_candidates)
        if event.artist_candidates
        else 0,
    }


@router.get(
    "",
    summary="List concert events",
    description="Paginated list of concert events, newest first.",
)
async def list_events(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
    upcoming: bool = False,
) -> dict[str, Any]:
    offset = (page - 1) * _PAGE_SIZE

    stmt = (
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

    if upcoming:
        import datetime

        stmt = stmt.where(concert_models.Event.event_date >= datetime.date.today())

    result = await db.execute(stmt)
    events = list(result.unique().scalars().all())

    has_next = len(events) > _PAGE_SIZE
    events = events[:_PAGE_SIZE]

    return {
        "items": [_format_event_summary(e) for e in events],
        "page": page,
        "page_size": _PAGE_SIZE,
        "has_next": has_next,
    }


@router.get(
    "/{event_id}/lineup",
    summary="Resolve an event's lineup to artists",
    description=(
        "Return the artists an event resolves to (confirmed lineup plus accepted"
        " candidate matches), with disambiguation metadata and an in-library flag."
        " Powers the playlist lineup builder's 'Add event' action so each artist"
        " can be individually included or excluded."
    ),
)
async def event_lineup(
    event_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    """Resolve an event's lineup to displayable artists for the lineup builder.

    Mirrors the event-resolution that ``worker.resolve_pool`` performs (confirmed
    ``EventArtist`` rows plus accepted ``EventArtistCandidate`` matches), but
    returns full artist summaries so the builder can list each artist with an
    include/exclude toggle.

    Args:
        event_id: The event to resolve.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with the event id and an ordered, deduplicated ``artists`` list.

    Raises:
        HTTPException: 404 if the event does not exist.
    """
    event = await db.get(concert_models.Event, event_id)
    if event is None:
        raise fastapi.HTTPException(status_code=404, detail="Event not found")

    # Confirmed lineup first, then accepted candidate matches (mirrors resolve_pool).
    ea_result = await db.execute(
        sa.select(concert_models.EventArtist.artist_id).where(
            concert_models.EventArtist.event_id == event_id
        )
    )
    ordered_ids: list[uuid.UUID] = list(ea_result.scalars().all())

    cand_result = await db.execute(
        sa.select(concert_models.EventArtistCandidate.matched_artist_id).where(
            concert_models.EventArtistCandidate.event_id == event_id,
            concert_models.EventArtistCandidate.status
            == types_module.CandidateStatus.ACCEPTED,
            concert_models.EventArtistCandidate.matched_artist_id.isnot(None),
        )
    )
    for cand_id in cand_result.scalars().all():
        if cand_id is not None:
            ordered_ids.append(cand_id)

    # Dedup preserving first-seen order.
    seen: set[uuid.UUID] = set()
    unique_ids: list[uuid.UUID] = []
    for aid in ordered_ids:
        if aid not in seen:
            seen.add(aid)
            unique_ids.append(aid)

    if not unique_ids:
        return {"event_id": str(event_id), "artists": []}

    artist_result = await db.execute(
        sa.select(music_models.Artist).where(music_models.Artist.id.in_(unique_ids))
    )
    artist_map = {a.id: a for a in artist_result.scalars().all()}
    in_library = await artists_api.artists_in_library(db, unique_ids)

    artists: list[dict[str, Any]] = []
    for aid in unique_ids:
        artist = artist_map.get(aid)
        if artist is None:
            continue
        summary = artists_api._format_artist_summary(artist)
        summary["in_library"] = aid in in_library
        artists.append(summary)

    return {"event_id": str(event_id), "artists": artists}


@router.get(
    "/{event_id}",
    summary="Get event detail",
    description="Get a concert event with venue, artists, and candidates.",
)
async def get_event(
    event_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    stmt = (
        sa.select(concert_models.Event)
        .where(concert_models.Event.id == event_id)
        .options(
            sa_orm.joinedload(concert_models.Event.venue),
            sa_orm.joinedload(concert_models.Event.artists),
            sa_orm.joinedload(concert_models.Event.artist_candidates),
        )
    )
    result = await db.execute(stmt)
    event = result.unique().scalar_one_or_none()

    if event is None:
        raise fastapi.HTTPException(status_code=404, detail="Event not found")

    detail = _format_event_summary(event)
    detail["service_links"] = event.service_links
    detail["external_id"] = event.external_id
    detail["artists"] = [
        {
            "id": str(a.id),
            "artist_id": str(a.artist_id),
            "raw_name": a.raw_name,
            "position": a.position,
        }
        for a in (event.artists or [])
    ]
    detail["artist_candidates"] = [
        {
            "id": str(c.id),
            "raw_name": c.raw_name,
            "matched_artist_id": str(c.matched_artist_id)
            if c.matched_artist_id
            else None,
            "status": str(c.status),
            "confidence_score": c.confidence_score,
            "position": c.position,
        }
        for c in (event.artist_candidates or [])
    ]
    detail["created_at"] = event.created_at.isoformat()

    return detail


# --- Candidate management ---


async def _get_candidate(
    db: sa_async.AsyncSession,
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
) -> concert_models.EventArtistCandidate:
    stmt = sa.select(concert_models.EventArtistCandidate).where(
        concert_models.EventArtistCandidate.id == candidate_id,
        concert_models.EventArtistCandidate.event_id == event_id,
    )
    candidate = (await db.execute(stmt)).scalar_one_or_none()
    if candidate is None:
        raise fastapi.HTTPException(status_code=404, detail="Candidate not found")
    return candidate


class _CreateCandidateBody(pydantic.BaseModel):
    artist_id: uuid.UUID


@router.post(
    "/{event_id}/candidates/{candidate_id}/accept",
    summary="Accept a candidate",
    description="Create an EventArtist from a candidate and mark it accepted.",
)
async def accept_candidate(
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    candidate = await _get_candidate(db, event_id, candidate_id)

    if candidate.matched_artist_id is None:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Candidate must have a matched_artist_id before accepting",
        )

    event_artist = concert_models.EventArtist(
        event_id=candidate.event_id,
        artist_id=candidate.matched_artist_id,
        position=candidate.position,
        raw_name=candidate.raw_name,
    )
    db.add(event_artist)

    candidate.status = types_module.CandidateStatus.ACCEPTED
    await db.commit()

    return {
        "id": str(candidate.id),
        "status": str(candidate.status),
        "event_artist_id": str(event_artist.id),
    }


@router.post(
    "/{event_id}/candidates/{candidate_id}/reject",
    summary="Reject a candidate",
    description="Mark a candidate as rejected.",
)
async def reject_candidate(
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    candidate = await _get_candidate(db, event_id, candidate_id)
    candidate.status = types_module.CandidateStatus.REJECTED
    await db.commit()

    return {
        "id": str(candidate.id),
        "status": str(candidate.status),
    }


class _PatchCandidateBody(pydantic.BaseModel):
    status: types_module.CandidateStatus | None = None
    matched_artist_id: uuid.UUID | None = None
    confidence_score: int | None = None


@router.patch(
    "/{event_id}/candidates/{candidate_id}",
    summary="Update an event artist candidate",
    description="Update a candidate's status, matched artist, or confidence score. "
    "Status transitions trigger side effects: accepting creates an EventArtist, "
    "un-accepting removes it.",
)
async def patch_candidate(
    event_id: uuid.UUID,
    candidate_id: uuid.UUID,
    body: _PatchCandidateBody,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    candidate = await _get_candidate(db, event_id, candidate_id)
    old_status = candidate.status
    old_matched_artist_id = candidate.matched_artist_id

    new_status = body.status if body.status is not None else old_status

    if body.matched_artist_id is not None:
        artist_check = (
            await db.execute(
                sa.select(music_models.Artist.id).where(
                    music_models.Artist.id == body.matched_artist_id
                )
            )
        ).scalar_one_or_none()
        if artist_check is None:
            raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    effective_artist_id = (
        body.matched_artist_id
        if body.matched_artist_id is not None
        else candidate.matched_artist_id
    )
    if (
        new_status == types_module.CandidateStatus.ACCEPTED
        and effective_artist_id is None
    ):
        raise fastapi.HTTPException(
            status_code=400,
            detail="Cannot accept candidate without matched_artist_id",
        )

    if body.matched_artist_id is not None:
        candidate.matched_artist_id = body.matched_artist_id
    if body.confidence_score is not None:
        candidate.confidence_score = body.confidence_score
    if body.status is not None:
        candidate.status = body.status

    event_artist_id: str | None = None

    if new_status == types_module.CandidateStatus.ACCEPTED:
        if old_status == types_module.CandidateStatus.ACCEPTED and (
            body.matched_artist_id is not None
            and body.matched_artist_id != old_matched_artist_id
        ):
            old_ea = (
                await db.execute(
                    sa.select(concert_models.EventArtist).where(
                        concert_models.EventArtist.event_id == event_id,
                        concert_models.EventArtist.artist_id == old_matched_artist_id,
                    )
                )
            ).scalar_one_or_none()
            if old_ea is not None:
                await db.delete(old_ea)

        existing_ea = (
            await db.execute(
                sa.select(concert_models.EventArtist).where(
                    concert_models.EventArtist.event_id == event_id,
                    concert_models.EventArtist.artist_id == effective_artist_id,
                )
            )
        ).scalar_one_or_none()
        if existing_ea is None:
            ea = concert_models.EventArtist(
                event_id=event_id,
                artist_id=effective_artist_id,
                position=candidate.position,
                raw_name=candidate.raw_name,
            )
            db.add(ea)
            event_artist_id = str(ea.id)
        else:
            event_artist_id = str(existing_ea.id)

    elif old_status == types_module.CandidateStatus.ACCEPTED:
        ea_to_remove = (
            await db.execute(
                sa.select(concert_models.EventArtist).where(
                    concert_models.EventArtist.event_id == event_id,
                    concert_models.EventArtist.artist_id == old_matched_artist_id,
                )
            )
        ).scalar_one_or_none()
        if ea_to_remove is not None:
            await db.delete(ea_to_remove)

    await db.commit()

    logger.info(
        "candidate_patched",
        candidate_id=str(candidate_id),
        event_id=str(event_id),
        old_status=str(old_status),
        new_status=str(new_status),
    )

    return {
        "status": "updated",
        "id": str(candidate.id),
        "event_id": str(candidate.event_id),
        "raw_name": candidate.raw_name,
        "matched_artist_id": str(candidate.matched_artist_id)
        if candidate.matched_artist_id
        else None,
        "candidate_status": str(candidate.status),
        "confidence_score": candidate.confidence_score,
        "event_artist_id": event_artist_id,
    }


@router.post(
    "/{event_id}/candidates",
    summary="Create a candidate from artist search",
    description="Create a new EventArtistCandidate linked to an existing artist.",
)
async def create_candidate(
    event_id: uuid.UUID,
    body: _CreateCandidateBody,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    # Verify event exists
    event_stmt = sa.select(concert_models.Event).where(
        concert_models.Event.id == event_id
    )
    event = (await db.execute(event_stmt)).scalar_one_or_none()
    if event is None:
        raise fastapi.HTTPException(status_code=404, detail="Event not found")

    # Look up the artist
    artist_stmt = sa.select(music_models.Artist).where(
        music_models.Artist.id == body.artist_id
    )
    artist = (await db.execute(artist_stmt)).scalar_one_or_none()
    if artist is None:
        raise fastapi.HTTPException(status_code=404, detail="Artist not found")

    normalized = normalize_module.normalize_name(artist.name)
    dup_stmt = sa.select(concert_models.EventArtistCandidate).where(
        concert_models.EventArtistCandidate.event_id == event_id,
        concert_models.EventArtistCandidate.normalized_raw_name == normalized,
    )
    existing = (await db.execute(dup_stmt)).scalar_one_or_none()
    if existing is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="Candidate with this name already exists for this event",
        )

    candidate = concert_models.EventArtistCandidate(
        event_id=event_id,
        raw_name=artist.name,
        matched_artist_id=artist.id,
        status=types_module.CandidateStatus.PENDING,
        confidence_score=100,
    )
    db.add(candidate)
    await db.commit()

    return {
        "id": str(candidate.id),
        "event_id": str(candidate.event_id),
        "raw_name": candidate.raw_name,
        "matched_artist_id": str(candidate.matched_artist_id),
        "status": str(candidate.status),
    }
