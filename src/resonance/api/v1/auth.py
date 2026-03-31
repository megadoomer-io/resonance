"""Auth API routes — OAuth initiate, callback, and logout."""

from __future__ import annotations

import datetime
import secrets
import uuid
from typing import Annotated

import fastapi
import fastapi.responses as fastapi_responses
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.crypto as crypto_module
import resonance.dependencies as deps_module
import resonance.middleware.session as session_module
import resonance.models.user as user_models
import resonance.types as types_module

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

    auth_url: str = connector.get_auth_url(state=state)  # type: ignore[attr-defined]
    return fastapi_responses.RedirectResponse(url=auth_url, status_code=307)


@router.get("/{service}/callback")
async def auth_callback(
    service: str,
    code: str,
    state: str,
    request: fastapi.Request,
    session: Annotated[
        session_module.SessionData, fastapi.Depends(deps_module.get_session)
    ],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> fastapi_responses.RedirectResponse:
    """Handle OAuth callback — exchange code, create/update user and connection."""
    service_type = _parse_service_type(service)
    connector = _get_connector(request, service_type)

    # Verify state matches
    stored_state = session.get("oauth_state")
    if stored_state is None or stored_state != state:
        raise fastapi.HTTPException(
            status_code=400, detail="Invalid or missing OAuth state"
        )

    # Exchange code for tokens
    tokens: base_module.TokenResponse = await connector.exchange_code(code=code)  # type: ignore[attr-defined]

    # Get user profile from service
    user_profile: dict[str, str] = await connector.get_current_user(  # type: ignore[attr-defined]
        access_token=tokens.access_token
    )
    external_user_id = user_profile["id"]
    display_name = user_profile.get("display_name", external_user_id)

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

    # Look for existing connection by (service_type, external_user_id)
    stmt = sa.select(user_models.ServiceConnection).where(
        user_models.ServiceConnection.service_type == service_type,
        user_models.ServiceConnection.external_user_id == external_user_id,
    )
    result = await db.execute(stmt)
    existing_connection = result.scalar_one_or_none()

    if existing_connection is not None:
        # Returning user — update tokens
        existing_connection.encrypted_access_token = encrypted_access
        existing_connection.encrypted_refresh_token = encrypted_refresh
        existing_connection.token_expires_at = token_expires_at
        existing_connection.scopes = tokens.scope
        user_id = existing_connection.user_id
    else:
        # Check if the user is already logged in (connecting a new service)
        session_user_id = session.get("user_id")
        if session_user_id is not None:
            user_id = uuid.UUID(session_user_id)
        else:
            # New user — create User
            new_user = user_models.User(display_name=display_name)
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
    return fastapi_responses.RedirectResponse(url="/login", status_code=307)
