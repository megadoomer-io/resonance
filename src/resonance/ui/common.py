"""Shared UI infrastructure: auth dependencies, pagination, context, templates."""

from __future__ import annotations

import dataclasses
import pathlib
import uuid
import zoneinfo
from typing import TYPE_CHECKING, Any

import fastapi
import fastapi.templating

import resonance.connectors.registry as registry_module
import resonance.types as types_module

if TYPE_CHECKING:
    import datetime
    from collections.abc import Sequence

    import sqlalchemy as sa
    import sqlalchemy.ext.asyncio as sa_async
    import sqlalchemy.orm as sa_orm

PAGE_SIZE = 50

# ---------------------------------------------------------------------------
# Templates singleton
# ---------------------------------------------------------------------------

_TEMPLATE_DIR = pathlib.Path(__file__).resolve().parent.parent / "templates"
templates = fastapi.templating.Jinja2Templates(directory=str(_TEMPLATE_DIR))


def _localtime(
    value: datetime.datetime | None,
    tz_name: str | None,
) -> datetime.datetime | None:
    if value is None:
        return None
    if tz_name is None:
        return value
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except KeyError, zoneinfo.ZoneInfoNotFoundError:
        return value
    return value.astimezone(tz)


templates.env.filters["localtime"] = _localtime

# ---------------------------------------------------------------------------
# Service display metadata filters (backed by connector registry)
# ---------------------------------------------------------------------------

_registry: registry_module.ConnectorRegistry | None = None


def set_connector_registry(registry: registry_module.ConnectorRegistry) -> None:
    """Set the connector registry for template filter lookups."""
    global _registry
    _registry = registry


def _service_name(value: str) -> str:
    """Return the human-friendly display name for a service type value."""
    if _registry is not None:
        try:
            return _registry.display_name(types_module.ServiceType(value))
        except ValueError:
            pass
    return value.replace("_", " ").title()


def _service_icon(value: str) -> str:
    """Return the Lucide icon name for a service type value."""
    if _registry is not None:
        try:
            return _registry.icon(types_module.ServiceType(value))
        except ValueError:
            pass
    return "link"


def _service_color(value: str) -> str:
    """Return the CSS color value for a service, or empty string if none."""
    if _registry is not None:
        try:
            return _registry.color(types_module.ServiceType(value))
        except ValueError:
            pass
    return ""


templates.env.filters["service_name"] = _service_name
templates.env.filters["service_icon"] = _service_icon
templates.env.filters["service_color"] = _service_color


# ---------------------------------------------------------------------------
# Auth dependencies
# ---------------------------------------------------------------------------


async def require_user(request: fastapi.Request) -> uuid.UUID:
    """Return authenticated user_id or redirect to /login.

    Use as a FastAPI dependency::

        user_id: Annotated[uuid.UUID, Depends(require_user)]
    """
    user_id = request.state.session.get("user_id")
    if not user_id:
        raise fastapi.HTTPException(status_code=307, headers={"Location": "/login"})
    return uuid.UUID(user_id)


async def require_admin(request: fastapi.Request) -> uuid.UUID:
    """Return admin/owner user_id, or redirect/forbid.

    Checks effective role (view-as aware): if view_as is set in the
    session, uses that instead of the actual role.

    Use as a FastAPI dependency::

        user_id: Annotated[uuid.UUID, Depends(require_admin)]
    """
    user_id = request.state.session.get("user_id")
    if not user_id:
        raise fastapi.HTTPException(status_code=307, headers={"Location": "/login"})
    effective = _effective_role(request.state.session)
    if effective not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403, detail="Admin access required")
    return uuid.UUID(user_id)


_ROLE_HIERARCHY = {"user": 0, "admin": 1, "owner": 2}


def _effective_role(session: Any) -> str:
    """Return the effective role, accounting for view-as."""
    actual: str = session.get("user_role", "user")
    view_as: str | None = session.get("view_as")
    if not view_as:
        return actual
    if _ROLE_HIERARCHY.get(view_as, 0) < _ROLE_HIERARCHY.get(actual, 0):
        return view_as
    return actual


# ---------------------------------------------------------------------------
# Template context builder
# ---------------------------------------------------------------------------


def base_context(request: fastapi.Request) -> dict[str, Any]:
    """Build common template context with auth and timezone info."""
    session = request.state.session
    actual_role = session.get("user_role", "user")
    return {
        "request": request,
        "user_id": session.get("user_id"),
        "user_tz": session.get("user_tz"),
        "user_role": _effective_role(session),
        "actual_role": actual_role,
        "viewing_as": session.get("view_as"),
    }


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PaginationResult[T]:
    """Result of paginating a sequence of items."""

    items: list[T]
    has_next: bool
    has_prev: bool
    page: int


def paginate[T](
    items: Sequence[T], page: int, page_size: int = PAGE_SIZE
) -> PaginationResult[T]:
    """Paginate a pre-fetched sequence (queried with LIMIT + 1)."""
    has_next = len(items) > page_size
    return PaginationResult(
        items=list(items[:page_size]),
        has_next=has_next,
        has_prev=page > 1,
        page=page,
    )


def page_offset(page: int, page_size: int = PAGE_SIZE) -> int:
    """Compute the SQL OFFSET for a given page number."""
    return (page - 1) * page_size


# ---------------------------------------------------------------------------
# Shared query helpers
# ---------------------------------------------------------------------------


def escape_ilike(q: str) -> str:
    """Escape ``%`` and ``_`` for safe use in ILIKE patterns."""
    return q.replace("%", r"\%").replace("_", r"\_")


async def count_rows(
    db: sa_async.AsyncSession,
    model: type[sa_orm.DeclarativeBase],
    *filters: sa.ColumnElement[bool],
) -> int:
    """Return the row count for *model*, optionally filtered."""
    import sqlalchemy as sa

    stmt = sa.select(sa.func.count()).select_from(model)
    for f in filters:
        stmt = stmt.where(f)
    result = await db.execute(stmt)
    return int(result.scalar_one())
