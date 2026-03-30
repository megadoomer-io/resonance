"""FastAPI dependency functions for session, database, and auth."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Annotated

import fastapi

import resonance.middleware.session as session_module  # noqa: TC001 - runtime import required for FastAPI dependency resolution

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import sqlalchemy.ext.asyncio as sa_async


def get_session(request: fastapi.Request) -> session_module.SessionData:
    """Return the current request's session data."""
    session: session_module.SessionData = request.state.session
    return session


async def get_db(request: fastapi.Request) -> AsyncIterator[sa_async.AsyncSession]:
    """Yield an async database session from the app's session factory."""
    factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = (
        request.app.state.session_factory
    )
    async with factory() as db:
        yield db


def get_current_user_id(
    session: Annotated[session_module.SessionData, fastapi.Depends(get_session)],
) -> uuid.UUID:
    """Extract the authenticated user ID from the session.

    Raises:
        HTTPException: 401 if no user_id is present in the session.
    """
    user_id = session.get("user_id")
    if user_id is None:
        raise fastapi.HTTPException(
            status_code=401,
            detail="Not authenticated",
        )
    return uuid.UUID(user_id)
