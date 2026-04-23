"""Calendar feed management API routes.

These endpoints manage Songkick and iCal connections via the unified
ServiceConnection model.  The ``/songkick/lookup`` endpoint validates
a Songkick username without creating a connection.
"""

from __future__ import annotations

import uuid
from typing import Annotated

import fastapi
import httpx
import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.connectors.songkick as songkick_module
import resonance.dependencies as deps_module
import resonance.models.user as user_models
import resonance.types as types_module

logger = structlog.get_logger()

router = fastapi.APIRouter(prefix="/calendar-feeds", tags=["calendar-feeds"])


# ---------------------------------------------------------------------------
# Pydantic request/response models
# ---------------------------------------------------------------------------


class SongkickFeedRequest(pydantic.BaseModel):
    """Request body for creating a Songkick connection."""

    username: str


class GenericFeedRequest(pydantic.BaseModel):
    """Request body for creating a generic iCal connection."""

    url: str
    label: str | None = None


class ConnectionResponse(pydantic.BaseModel):
    """Response model for a connection."""

    id: str
    service_type: str
    external_user_id: str | None
    url: str | None
    label: str | None
    enabled: bool
    last_synced_at: str | None


class SongkickLookupResponse(pydantic.BaseModel):
    """Response model for Songkick username lookup validation."""

    username: str
    plans_count: int
    tracked_artist_count: int


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _connection_to_response(conn: user_models.ServiceConnection) -> ConnectionResponse:
    """Convert a ServiceConnection to a ConnectionResponse.

    Args:
        conn: The ORM model instance.

    Returns:
        A serialisable ConnectionResponse.
    """
    return ConnectionResponse(
        id=str(conn.id),
        service_type=str(conn.service_type),
        external_user_id=conn.external_user_id,
        url=conn.url,
        label=conn.label,
        enabled=conn.enabled if conn.enabled is not None else True,
        last_synced_at=(
            conn.last_synced_at.isoformat() if conn.last_synced_at is not None else None
        ),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "/songkick",
    summary="Add Songkick connection",
    description=(
        "Creates a ServiceConnection for the given Songkick username. "
        "Requires session authentication."
    ),
)
async def add_songkick_connection(
    body: SongkickFeedRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> ConnectionResponse:
    """Create a Songkick ServiceConnection.

    Args:
        body: Request containing the Songkick username.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        The created ConnectionResponse.

    Raises:
        HTTPException: 409 if a connection already exists for this username.
    """
    # Check for duplicate
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type == types_module.ServiceType.SONGKICK,
        user_models.ServiceConnection.external_user_id == body.username,
    )
    result = await db.execute(stmt)
    if result.scalar_one_or_none() is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="Songkick connection already exists for this username",
        )

    conn = user_models.ServiceConnection(
        user_id=user_id,
        service_type=types_module.ServiceType.SONGKICK,
        external_user_id=body.username,
        enabled=True,
    )
    db.add(conn)
    await db.commit()

    return _connection_to_response(conn)


@router.post(
    "/songkick/lookup",
    summary="Validate Songkick username",
    description=(
        "Fetches attendance and tracked-artist iCal feeds from Songkick "
        "to verify a username exists and returns event counts."
    ),
)
async def lookup_songkick_user(
    body: SongkickFeedRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
) -> SongkickLookupResponse:
    """Look up a Songkick username and return event counts.

    Fetches both the attendance and tracked-artist iCal feeds from
    Songkick to verify the username exists, then counts VEVENT entries.

    Args:
        body: Request containing the Songkick username.
        user_id: The authenticated user's ID.

    Returns:
        A SongkickLookupResponse with plans and tracked artist counts.

    Raises:
        HTTPException: 404 if Songkick user not found, 502 if Songkick
            is unreachable.
    """
    urls = songkick_module.derive_songkick_urls(body.username)
    async with httpx.AsyncClient() as client:
        try:
            att_resp = await client.get(urls[0])
            att_resp.raise_for_status()
            trk_resp = await client.get(urls[1])
            trk_resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise fastapi.HTTPException(404, "Songkick user not found") from exc
            raise fastapi.HTTPException(502, "Songkick unavailable") from exc
        except httpx.ConnectError as exc:
            raise fastapi.HTTPException(502, "Songkick unavailable") from exc

    return SongkickLookupResponse(
        username=body.username,
        plans_count=att_resp.text.count("BEGIN:VEVENT"),
        tracked_artist_count=trk_resp.text.count("BEGIN:VEVENT"),
    )


@router.post(
    "/ical",
    summary="Add generic iCal connection",
    description=(
        "Creates a generic iCal ServiceConnection from a URL. "
        "Requires session authentication."
    ),
)
async def add_generic_connection(
    body: GenericFeedRequest,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> ConnectionResponse:
    """Create a generic iCal ServiceConnection.

    Args:
        body: Request containing the feed URL and optional label.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        The created ConnectionResponse.

    Raises:
        HTTPException: 409 if a connection already exists for this URL.
    """
    # Check for duplicate
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type == types_module.ServiceType.ICAL,
        user_models.ServiceConnection.url == body.url,
    )
    result = await db.execute(stmt)
    if result.scalar_one_or_none() is not None:
        raise fastapi.HTTPException(
            status_code=409,
            detail="A connection already exists for this URL",
        )

    conn = user_models.ServiceConnection(
        user_id=user_id,
        service_type=types_module.ServiceType.ICAL,
        url=body.url,
        label=body.label,
        enabled=True,
    )
    db.add(conn)
    await db.commit()

    return _connection_to_response(conn)


@router.get(
    "",
    summary="List calendar connections",
    description=(
        "Returns all calendar-related connections (Songkick, iCal) "
        "for the authenticated user. Requires session authentication."
    ),
)
async def list_connections(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> list[ConnectionResponse]:
    """List calendar connections for the authenticated user.

    Args:
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A list of ConnectionResponse objects.
    """
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type.in_(
            [types_module.ServiceType.SONGKICK, types_module.ServiceType.ICAL]
        ),
    )
    result = await db.execute(stmt)
    connections = result.scalars().all()
    return [_connection_to_response(c) for c in connections]


@router.delete(
    "/songkick/{username}",
    summary="Delete Songkick connection by username",
    description=(
        "Deletes the Songkick connection for the given username. "
        "Returns 404 if no connection is found. "
        "Requires session authentication."
    ),
)
async def delete_songkick_connection(
    username: str,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Delete a Songkick connection by username.

    Args:
        username: The Songkick username whose connection to delete.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with status "deleted".

    Raises:
        HTTPException: 404 if no connection found for this username.
    """
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type == types_module.ServiceType.SONGKICK,
        user_models.ServiceConnection.external_user_id == username,
    )
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()
    if conn is None:
        raise fastapi.HTTPException(404, "No Songkick connection for this username")
    await db.delete(conn)
    await db.commit()
    return {"status": "deleted"}


@router.delete(
    "/{connection_id}",
    summary="Delete calendar connection",
    description=(
        "Deletes a calendar connection. Returns 404 if not found "
        "or not owned by the authenticated user. "
        "Requires session authentication."
    ),
)
async def delete_connection(
    connection_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Delete a calendar connection owned by the authenticated user.

    Args:
        connection_id: The UUID of the connection to delete.
        user_id: The authenticated user's ID.
        db: The async database session.

    Returns:
        A dict with status "deleted".

    Raises:
        HTTPException: 404 if connection not found or not owned by user.
    """
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.id == connection_id,
        user_models.ServiceConnection.user_id == user_id,
        user_models.ServiceConnection.service_type.in_(
            [types_module.ServiceType.SONGKICK, types_module.ServiceType.ICAL]
        ),
    )
    result = await db.execute(stmt)
    conn = result.scalar_one_or_none()

    if conn is None:
        raise fastapi.HTTPException(status_code=404, detail="Connection not found")

    await db.delete(conn)
    await db.commit()

    return {"status": "deleted"}
