"""FastAPI dependency functions for session, database, and auth."""

from __future__ import annotations

import secrets
import uuid
from typing import TYPE_CHECKING, Annotated

import fastapi
import sqlalchemy as sa
import structlog

import resonance.middleware.session as session_module
import resonance.models.user as user_models
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()


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


def _assume_user_selector(request: fastapi.Request) -> str | None:
    """Return the requested assume-user identity, if any.

    Honors the ``X-Assume-User`` header first, then the ``?as_user=`` query
    param. Returns None when neither is present.
    """
    header = request.headers.get("x-assume-user")
    if header:
        return header
    query = request.query_params.get("as_user")
    return query or None


async def _resolve_assumed_user(request: fastapi.Request, selector: str) -> uuid.UUID:
    """Resolve an admin-token assume-user selector to a real user ID.

    Args:
        request: The incoming request (for settings + DB factory + path).
        selector: The requested user identity (a UUID string).

    Returns:
        The assumed user's UUID.

    Raises:
        HTTPException: 403 if assume-user is disabled, 400 if the selector is
            not a UUID, 404 if no such user exists.
    """
    settings = request.app.state.settings
    if not settings.admin_assume_user_enabled:
        raise fastapi.HTTPException(status_code=403, detail="Assume-user is disabled")
    try:
        assumed_id = uuid.UUID(selector)
    except ValueError as exc:
        raise fastapi.HTTPException(
            status_code=400, detail="Invalid assume-user identity"
        ) from exc

    factory = request.app.state.session_factory
    async with factory() as db:
        result = await db.execute(
            sa.select(user_models.User.id).where(user_models.User.id == assumed_id)
        )
        found = result.scalar_one_or_none()
    if found is None:
        raise fastapi.HTTPException(status_code=404, detail="Assumed user not found")

    logger.info(
        "admin_assume_user",
        assumed_user_id=str(assumed_id),
        path=request.url.path,
    )
    return assumed_id


async def get_current_user_id(
    request: fastapi.Request,
    session: Annotated[session_module.SessionData, fastapi.Depends(get_session)],
) -> uuid.UUID:
    """Extract the authenticated user ID from the session or bearer token.

    Checks session first. If no session, checks for a valid admin API token.
    A valid admin token resolves to the owner user, unless the request carries
    an assume-user selector (``X-Assume-User`` header or ``?as_user=`` query
    param), in which case it resolves to that user instead (#135). Assume-user
    is gated by ``admin_assume_user_enabled`` and audit-logged.

    Raises:
        HTTPException: 401 if not authenticated; 403 if assume-user is disabled
            or the token is invalid; 400 for a malformed selector; 404 if the
            assumed user does not exist.
    """
    # 1. Try session auth
    user_id = session.get("user_id")
    if user_id is not None:
        return uuid.UUID(user_id)

    # 2. Try the admin bearer token (assumed user or owner).
    resolved = await resolve_bearer_user(request)
    if resolved is not None:
        return resolved

    raise fastapi.HTTPException(
        status_code=401,
        detail="Not authenticated",
    )


async def resolve_bearer_user(request: fastapi.Request) -> uuid.UUID | None:
    """Resolve a request's admin bearer token to a user id, else None.

    Returns ``None`` when there is no ``Authorization: Bearer`` header (the caller
    decides what unauthenticated means — 401 for the API, a login redirect for the
    UI). A valid admin token resolves to the assume-user selector (#135) when
    present, otherwise the owner. An invalid token raises 403 — a wrong token is an
    explicit auth failure, not "unauthenticated", so it never falls through to a
    session/login path.

    Shared by ``get_current_user_id`` (API) and ``ui.common.require_user`` (UI) so
    agents can drive the UI with the admin token exactly as they drive the API.
    """
    auth_header = request.headers.get("authorization", "")
    if not auth_header.startswith("Bearer "):
        return None

    token = auth_header[7:]
    settings = request.app.state.settings
    # Constant-time compare to avoid a timing side channel on the admin token
    # (#141, finding #7). compare_digest short-circuits safely on empty config.
    if not settings.admin_api_token or not secrets.compare_digest(
        token, settings.admin_api_token
    ):
        raise fastapi.HTTPException(status_code=403, detail="Invalid API token")

    selector = _assume_user_selector(request)
    if selector is not None:
        return await _resolve_assumed_user(request, selector)

    # Default: resolve to the owner user.
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
        # Constant-time compare (#141, finding #7).
        if settings.admin_api_token and secrets.compare_digest(
            token, settings.admin_api_token
        ):
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
