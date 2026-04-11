"""Auth API routes — OAuth initiate, callback, and logout."""

from __future__ import annotations

import datetime
import secrets
import uuid
from typing import Annotated

import fastapi
import fastapi.responses as fastapi_responses
import httpx
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.crypto as crypto_module
import resonance.dependencies as deps_module
import resonance.middleware.session as session_module
import resonance.models.user as user_models
import resonance.types as types_module

logger = structlog.get_logger()

router = fastapi.APIRouter(prefix="/auth", tags=["auth"])


def _parse_service_type(service: str) -> types_module.ServiceType:
    """Parse a service string to ServiceType enum, raising 404 if unknown."""
    try:
        return types_module.ServiceType(service)
    except ValueError as exc:
        raise fastapi.HTTPException(
            status_code=404, detail=f"Unknown service: {service}"
        ) from exc


def _get_connector(
    request: fastapi.Request,
    service_type: types_module.ServiceType,
) -> base_module.BaseConnector:
    """Get a connector from the registry, raising 404 if not found."""
    registry: registry_module.ConnectorRegistry = request.app.state.connector_registry
    connector = registry.get(service_type)
    if connector is None:
        raise fastapi.HTTPException(
            status_code=404,
            detail=f"No connector registered for {service_type.value}",
        )
    return connector


@router.get("/{service}")
async def auth_initiate(
    service: str,
    request: fastapi.Request,
    session: Annotated[
        session_module.SessionData, fastapi.Depends(deps_module.get_session)
    ],
) -> fastapi_responses.RedirectResponse:
    """Initiate OAuth flow for an external service."""
    service_type = _parse_service_type(service)
    connector = _get_connector(request, service_type)

    if not connector.has_capability(base_module.ConnectorCapability.AUTHENTICATION):
        raise fastapi.HTTPException(
            status_code=400,
            detail=f"Service {service} does not support authentication",
        )

    state = secrets.token_urlsafe(32)
    session["oauth_state"] = state
    session["oauth_service"] = service_type.value

    auth_url: str = connector.get_auth_url(state=state)
    return fastapi_responses.RedirectResponse(url=auth_url, status_code=307)


@router.get("/{service}/callback")
async def auth_callback(
    service: str,
    request: fastapi.Request,
    session: Annotated[
        session_module.SessionData, fastapi.Depends(deps_module.get_session)
    ],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    code: str | None = None,
    token: str | None = None,
    state: str = "",
) -> fastapi_responses.RedirectResponse:
    """Handle OAuth callback — exchange code, create/update user and connection.

    Accepts either ``code`` (standard OAuth2) or ``token`` (Last.fm) as the
    authorization credential.  The ``state`` parameter is optional because
    Last.fm does not echo it back in the callback.
    """
    service_type = _parse_service_type(service)
    connector = _get_connector(request, service_type)

    auth_code = code or token
    if auth_code is None:
        raise fastapi.HTTPException(
            status_code=400, detail="Missing code or token parameter"
        )

    # Verify state matches.  When state is non-empty (standard OAuth2) we
    # require it to match the stored value.  When state is empty the service
    # does not echo it back (e.g. Last.fm), so we skip the check.
    stored_state = session.get("oauth_state")
    if state and (stored_state is None or stored_state != state):
        raise fastapi.HTTPException(
            status_code=400, detail="Invalid or missing OAuth state"
        )

    # Exchange code for tokens
    try:
        tokens: base_module.TokenResponse = await connector.exchange_code(
            code=auth_code
        )
    except httpx.HTTPStatusError as exc:
        logger.exception("Token exchange failed for %s", service)
        detail = (
            f"Failed to exchange authorization code with "
            f"{service}: {exc.response.status_code}"
        )
        raise fastapi.HTTPException(status_code=502, detail=detail) from exc

    # Encrypt tokens
    settings = request.app.state.settings
    encrypted_access = crypto_module.encrypt_token(
        tokens.access_token, settings.token_encryption_key
    )
    encrypted_refresh: str | None = None
    if tokens.refresh_token is not None:
        encrypted_refresh = crypto_module.encrypt_token(
            tokens.refresh_token, settings.token_encryption_key
        )

    token_expires_at: datetime.datetime | None = None
    if tokens.expires_in is not None:
        token_expires_at = datetime.datetime.now(tz=datetime.UTC) + datetime.timedelta(
            seconds=tokens.expires_in
        )

    # Try to get user profile from service
    external_user_id: str | None = None
    display_name: str = ""
    try:
        user_profile: dict[str, str] = await connector.get_current_user(
            access_token=tokens.access_token
        )
        external_user_id = user_profile["id"]
        display_name = user_profile.get("display_name", external_user_id)
    except httpx.HTTPStatusError, base_module.RateLimitExceededError:
        logger.warning(
            "Could not fetch %s user profile (rate limited?), "
            "falling back to session lookup",
            service,
        )

    if external_user_id is not None:
        # Normal path: look up by external user ID
        stmt = sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.service_type == service_type,
            user_models.ServiceConnection.external_user_id == external_user_id,
        )
        result = await db.execute(stmt)
        existing_connection = result.scalar_one_or_none()
    else:
        # Fallback: look up by session user ID (returning user, rate limited)
        session_user_id = session.get("user_id")
        if session_user_id is not None:
            stmt = sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == uuid.UUID(session_user_id),
                user_models.ServiceConnection.service_type == service_type,
            )
            result = await db.execute(stmt)
            existing_connection = result.scalar_one_or_none()
        else:
            # New user but can't get profile — can't proceed
            detail = (
                f"{service.capitalize()} is rate limiting "
                f"requests. Please try again later."
            )
            raise fastapi.HTTPException(status_code=503, detail=detail)

    if existing_connection is not None:
        current_user_id = session.get("user_id")

        # CONFLICT: connection belongs to a different user than the logged-in user
        if current_user_id is not None and existing_connection.user_id != uuid.UUID(
            current_user_id
        ):
            # Store merge details for the merge page
            session["merge_source_user_id"] = str(existing_connection.user_id)
            session["merge_service_type"] = service_type.value
            session["merge_connection_id"] = str(existing_connection.id)
            # Update tokens (they're fresh from the OAuth flow)
            existing_connection.encrypted_access_token = encrypted_access
            existing_connection.encrypted_refresh_token = encrypted_refresh
            existing_connection.token_expires_at = token_expires_at
            existing_connection.scopes = tokens.scope
            await db.commit()
            session["oauth_state"] = None
            session["oauth_service"] = None
            return fastapi_responses.RedirectResponse(url="/merge", status_code=307)

        # Returning user — update tokens
        existing_connection.encrypted_access_token = encrypted_access
        existing_connection.encrypted_refresh_token = encrypted_refresh
        existing_connection.token_expires_at = token_expires_at
        existing_connection.scopes = tokens.scope
        user_id = existing_connection.user_id
    else:
        if external_user_id is None:
            # Can't create a new connection without the external ID
            detail = (
                f"{service.capitalize()} is rate limiting "
                f"requests. Please try again later."
            )
            raise fastapi.HTTPException(status_code=503, detail=detail)

        # Check if the user is already logged in (connecting a new service)
        session_user_id = session.get("user_id")
        if session_user_id is not None:
            user_id = uuid.UUID(session_user_id)
        else:
            # New user — create User
            # Check if this is the first user in the system
            count_result = await db.execute(
                sa.select(sa.func.count()).select_from(user_models.User)
            )
            is_first_user = count_result.scalar_one() == 0

            new_user = user_models.User(
                display_name=display_name,
                role=(
                    types_module.UserRole.OWNER
                    if is_first_user
                    else types_module.UserRole.USER
                ),
            )
            db.add(new_user)
            await db.flush()
            user_id = new_user.id

        # Create new ServiceConnection
        connection = user_models.ServiceConnection(
            user_id=user_id,
            service_type=service_type,
            external_user_id=external_user_id,
            encrypted_access_token=encrypted_access,
            encrypted_refresh_token=encrypted_refresh,
            token_expires_at=token_expires_at,
            scopes=tokens.scope,
        )
        db.add(connection)

    await db.commit()

    # Set session
    session["user_id"] = str(user_id)

    # Load and cache the user's role in the session for template access.
    role_result = await db.execute(
        sa.select(user_models.User.role).where(user_models.User.id == user_id)
    )
    user_role = role_result.scalar_one()
    session["user_role"] = user_role.value

    # Load timezone preference into session for templates.
    tz_result = await db.execute(
        sa.select(user_models.User.timezone).where(user_models.User.id == user_id)
    )
    user_tz = tz_result.scalar_one_or_none()
    if user_tz:
        session["user_tz"] = user_tz

    # Clear OAuth state
    session["oauth_state"] = None
    session["oauth_service"] = None

    return fastapi_responses.RedirectResponse(url="/", status_code=307)


@router.post("/logout")
async def logout(
    session: Annotated[
        session_module.SessionData, fastapi.Depends(deps_module.get_session)
    ],
) -> fastapi_responses.RedirectResponse:
    """Clear the session and log the user out."""
    session.clear()
    # 303 See Other — browser follows redirect with GET (not POST)
    return fastapi_responses.RedirectResponse(url="/login", status_code=303)
