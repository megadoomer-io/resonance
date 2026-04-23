"""User and service connection models."""

from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class User(base_module.TimestampMixin, base_module.Base):
    """A user of the Resonance platform."""

    __tablename__ = "users"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    display_name: orm.Mapped[str] = orm.mapped_column(sa.String(255))
    email: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(255), nullable=True, default=None
    )
    timezone: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(63), nullable=True, default=None
    )
    role: orm.Mapped[types_module.UserRole] = orm.mapped_column(
        sa.Enum(types_module.UserRole, native_enum=False),
        nullable=False,
        server_default="USER",
        insert_default=types_module.UserRole.USER,
    )

    connections: orm.Mapped[list[ServiceConnection]] = orm.relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


_USER_DEFAULTS: dict[str, object] = {
    "role": types_module.UserRole.USER,
}


@sa.event.listens_for(User, "init")
def _set_user_defaults(
    target: User,
    _args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    """Apply Python-side defaults for User fields at construction."""
    for attr, default in _USER_DEFAULTS.items():
        if attr not in kwargs:
            value = default() if callable(default) else default
            setattr(target, attr, value)


class ServiceConnection(base_module.TimestampMixin, base_module.Base):
    """A connection to an external service (OAuth, username-based, or URL-based)."""

    __tablename__ = "service_connections"
    __table_args__ = (
        sa.UniqueConstraint(
            "user_id",
            "service_type",
            "external_user_id",
            name="uq_service_connections_user_service_ext",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    service_type: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
    external_user_id: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(255), nullable=True, default=None
    )
    encrypted_access_token: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    encrypted_refresh_token: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    token_expires_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    scopes: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    url: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(2048), nullable=True, default=None
    )
    label: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    enabled: orm.Mapped[bool] = orm.mapped_column(
        sa.Boolean, nullable=False, default=True
    )
    connected_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )
    last_synced_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    sync_watermark: orm.Mapped[dict[str, dict[str, object]]] = orm.mapped_column(
        sa.JSON, nullable=False, server_default="{}", insert_default=dict
    )

    user: orm.Mapped[User] = orm.relationship(back_populates="connections")


# Python-side defaults for mutable fields applied via init event.
_SERVICE_CONNECTION_DEFAULTS: dict[str, object] = {
    "enabled": True,
    "sync_watermark": dict,
}


@sa.event.listens_for(ServiceConnection, "init")
def _set_python_defaults(
    target: ServiceConnection,
    _args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    """Apply Python-side defaults for fields that need them at construction."""
    for attr, default in _SERVICE_CONNECTION_DEFAULTS.items():
        if attr not in kwargs:
            value = default() if callable(default) else default
            setattr(target, attr, value)
