"""Concert event API routes — list and detail endpoints."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.models.music as music_models
import resonance.types as types_module

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

    # Check for duplicate candidate with same raw_name
    dup_stmt = sa.select(concert_models.EventArtistCandidate).where(
        concert_models.EventArtistCandidate.event_id == event_id,
        concert_models.EventArtistCandidate.raw_name == artist.name,
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
