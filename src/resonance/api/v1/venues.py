"""Venue API routes — list, detail, candidate management, and split."""

from __future__ import annotations

import uuid
from typing import Annotated, Any

import fastapi
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm
import structlog

import resonance.dependencies as deps_module
import resonance.models.concert as concert_models
import resonance.types as types_module

logger = structlog.get_logger()

_PAGE_SIZE = 50

router = fastapi.APIRouter(
    prefix="/venues",
    tags=["venues"],
    dependencies=[fastapi.Depends(deps_module.verify_admin_access)],
)


def _format_venue(venue: concert_models.Venue) -> dict[str, Any]:
    return {
        "id": str(venue.id),
        "name": venue.name,
        "city": venue.city,
        "state": venue.state,
        "country": venue.country,
        "address": venue.address,
        "postal_code": venue.postal_code,
    }


def _format_candidate(vc: concert_models.VenueCandidate) -> dict[str, Any]:
    return {
        "id": str(vc.id),
        "source_service": str(vc.source_service.value),
        "external_id": vc.external_id,
        "name": vc.name,
        "city": vc.city,
        "state": vc.state,
        "country": vc.country,
        "address": vc.address,
        "postal_code": vc.postal_code,
        "status": vc.status.value,
        "confidence_score": vc.confidence_score,
    }


@router.get(
    "",
    summary="List venues",
    description="Paginated list of venues with optional search.",
)
async def list_venues(
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    page: int = 1,
    q: str | None = None,
) -> dict[str, Any]:
    offset = (page - 1) * _PAGE_SIZE

    stmt = (
        sa.select(concert_models.Venue)
        .options(
            sa_orm.selectinload(concert_models.Venue.candidates),
            sa_orm.selectinload(concert_models.Venue.events),
        )
        .order_by(concert_models.Venue.name)
        .offset(offset)
        .limit(_PAGE_SIZE + 1)
    )

    if q:
        pattern = f"%{q}%"
        stmt = stmt.where(
            sa.or_(
                concert_models.Venue.name.ilike(pattern),
                concert_models.Venue.city.ilike(pattern),
            )
        )

    result = await db.execute(stmt)
    venues = list(result.scalars().unique())
    has_next = len(venues) > _PAGE_SIZE
    venues = venues[:_PAGE_SIZE]

    return {
        "venues": [
            {
                **_format_venue(v),
                "candidate_count": len(v.candidates),
                "event_count": len(v.events),
            }
            for v in venues
        ],
        "page": page,
        "has_next": has_next,
    }


@router.get(
    "/{venue_id}",
    summary="Venue detail",
    description="Venue with all candidates, event count, and exclusions.",
)
async def get_venue(
    venue_id: uuid.UUID,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
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

    return {
        **_format_venue(venue),
        "candidates": [_format_candidate(vc) for vc in venue.candidates],
        "event_count": len(venue.events),
        "exclusions": [
            {
                "id": str(e.id),
                "other_id": str(
                    e.entity_b_id if e.entity_a_id == venue_id else e.entity_a_id
                ),
            }
            for e in exclusions
        ],
    }


@router.post(
    "/{venue_id}/candidates/{candidate_id}/accept",
    summary="Accept a venue candidate",
)
async def accept_venue_candidate(
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    vc = await db.get(concert_models.VenueCandidate, candidate_id)
    if not vc or vc.resolved_venue_id != venue_id:
        raise fastapi.HTTPException(status_code=404, detail="Candidate not found")

    vc.status = types_module.CandidateStatus.ACCEPTED
    await db.commit()
    return {"status": "accepted"}


@router.post(
    "/{venue_id}/candidates/{candidate_id}/reject",
    summary="Reject a venue candidate",
)
async def reject_venue_candidate(
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    vc = await db.get(concert_models.VenueCandidate, candidate_id)
    if not vc or vc.resolved_venue_id != venue_id:
        raise fastapi.HTTPException(status_code=404, detail="Candidate not found")

    vc.status = types_module.CandidateStatus.REJECTED
    await db.commit()
    return {"status": "rejected"}


@router.post(
    "/{venue_id}/candidates/{candidate_id}/unlink",
    summary="Unlink a venue candidate (return to pending)",
)
async def unlink_venue_candidate(
    venue_id: uuid.UUID,
    candidate_id: uuid.UUID,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    vc = await db.get(concert_models.VenueCandidate, candidate_id)
    if not vc or vc.resolved_venue_id != venue_id:
        raise fastapi.HTTPException(status_code=404, detail="Candidate not found")

    vc.resolved_venue_id = None
    vc.status = types_module.CandidateStatus.PENDING
    vc.confidence_score = 0
    await db.commit()
    return {"status": "unlinked"}


class SplitRequest(pydantic.BaseModel):
    candidate_ids: list[uuid.UUID]


@router.post(
    "/{venue_id}/split",
    summary="Split candidates into a new venue",
    description=(
        "Create a new venue from selected candidates, move them to it,"
        " and create an EntityExclusion between old and new."
    ),
)
async def split_venue(
    venue_id: uuid.UUID,
    body: SplitRequest,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, Any]:
    if not body.candidate_ids:
        raise fastapi.HTTPException(
            status_code=422, detail="At least one candidate required"
        )

    stmt = (
        sa.select(concert_models.Venue)
        .options(sa_orm.selectinload(concert_models.Venue.candidates))
        .where(concert_models.Venue.id == venue_id)
    )
    venue = (await db.execute(stmt)).scalar_one_or_none()
    if not venue:
        raise fastapi.HTTPException(status_code=404, detail="Venue not found")

    candidates_to_move = [vc for vc in venue.candidates if vc.id in body.candidate_ids]
    if len(candidates_to_move) != len(body.candidate_ids):
        raise fastapi.HTTPException(
            status_code=422, detail="Some candidate IDs not found on this venue"
        )

    if len(candidates_to_move) == len(venue.candidates):
        raise fastapi.HTTPException(
            status_code=422,
            detail="Cannot split all candidates — at least one must remain",
        )

    first = candidates_to_move[0]
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

    for vc in candidates_to_move:
        vc.resolved_venue_id = new_venue.id
        vc.status = types_module.CandidateStatus.ACCEPTED

    exclusion = concert_models.EntityExclusion(
        entity_type="venue",
        entity_a_id=venue_id,
        entity_b_id=new_venue.id,
    )
    db.add(exclusion)
    await db.commit()

    logger.info(
        "venue_split",
        original_venue_id=str(venue_id),
        new_venue_id=str(new_venue.id),
        candidates_moved=len(candidates_to_move),
    )

    return {
        "status": "split",
        "new_venue_id": str(new_venue.id),
        "candidates_moved": len(candidates_to_move),
    }


@router.delete(
    "/exclusions/{exclusion_id}",
    summary="Remove an entity exclusion",
)
async def delete_exclusion(
    exclusion_id: uuid.UUID,
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    exclusion = await db.get(concert_models.EntityExclusion, exclusion_id)
    if not exclusion:
        raise fastapi.HTTPException(status_code=404, detail="Exclusion not found")

    await db.delete(exclusion)
    await db.commit()
    return {"status": "deleted"}
