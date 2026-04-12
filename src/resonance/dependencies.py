"""FastAPI dependency functions for session, database, and auth."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Annotated

import fastapi

import resonance.middleware.session as session_module
import resonance.types as types_module

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


async def get_current_user_id(
    request: fastapi.Request,
    session: Annotated[session_module.SessionData, fastapi.Depends(get_session)],
) -> uuid.UUID:
    """Extract the authenticated user ID from the session or bearer token.

    Checks session first. If no session, checks for a valid admin API
    token and resolves to the owner user.

    Raises:
        HTTPException: 401 if not authenticated.
    """
    # 1. Try session auth
    user_id = session.get("user_id")
    if user_id is not None:
        return uuid.UUID(user_id)

    # 2. Try bearer token → resolve to owner user
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        settings = request.app.state.settings
        if settings.admin_api_token and token == settings.admin_api_token:
            # Look up the owner user
            import sqlalchemy as sa

            import resonance.models.user as user_models

            factory = request.app.state.session_factory
            async with factory() as db:
                result = await db.execute(
                    sa.select(user_models.User.id)
                    .where(user_models.User.role == types_module.UserRole.OWNER)
                    .limit(1)
                )
                owner_id = result.scalar_one_or_none()
                if owner_id is not None:
                    return uuid.UUID(str(owner_id))
            raise fastapi.HTTPException(status_code=500, detail="No owner user found")
        raise fastapi.HTTPException(status_code=403, detail="Invalid API token")

    raise fastapi.HTTPException(
        status_code=401,
        detail="Not authenticated",
    )


def verify_admin_access(request: fastapi.Request) -> None:
    """Verify admin access via session role OR bearer token.

    Checks in order:
    1. Session-based: user_role in session is admin/owner
    2. Token-based: Authorization header matches ADMIN_API_TOKEN

    Raises:
        HTTPException: 401 if not authenticated, 403 if not admin.
    """
    # Check bearer token first (for CLI/programmatic access)
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        settings = request.app.state.settings
        if settings.admin_api_token and token == settings.admin_api_token:
            return
        raise fastapi.HTTPException(status_code=403, detail="Invalid API token")

    # Fall back to session-based auth
    session = request.state.session
    user_role = session.get("user_role", "")
    if user_role in ("admin", "owner"):
        return

    user_id = session.get("user_id")
    if not user_id:
        raise fastapi.HTTPException(status_code=401, detail="Not authenticated")

    raise fastapi.HTTPException(status_code=403, detail="Admin access required")


def require_admin(role: types_module.UserRole) -> None:
    """Raise 403 if user is not admin or owner.

    Args:
        role: The user's role to validate.

    Raises:
        HTTPException: 403 if role is not ADMIN or OWNER.
    """
    if role not in (types_module.UserRole.ADMIN, types_module.UserRole.OWNER):
        raise fastapi.HTTPException(status_code=403, detail="Admin access required")


def require_owner(role: types_module.UserRole) -> None:
    """Raise 403 if user is not owner.

    Args:
        role: The user's role to validate.

    Raises:
        HTTPException: 403 if role is not OWNER.
    """
    if role != types_module.UserRole.OWNER:
        raise fastapi.HTTPException(status_code=403, detail="Owner access required")
