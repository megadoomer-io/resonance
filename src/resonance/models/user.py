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

    connections: orm.Mapped[list[ServiceConnection]] = orm.relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class ServiceConnection(base_module.TimestampMixin, base_module.Base):
    """An OAuth connection between a user and an external service."""

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
    external_user_id: orm.Mapped[str] = orm.mapped_column(
        sa.String(255), nullable=False
    )
    encrypted_access_token: orm.Mapped[str] = orm.mapped_column(sa.Text, nullable=False)
    encrypted_refresh_token: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    token_expires_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    scopes: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    connected_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )
    last_used_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )

    user: orm.Mapped[User] = orm.relationship(back_populates="connections")
