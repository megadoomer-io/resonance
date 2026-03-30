"""Sync job model for tracking data synchronization with external services."""

from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class SyncJob(base_module.Base):
    """A data synchronization job for a user's service connection."""

    __tablename__ = "sync_jobs"
    __table_args__ = (sa.Index("ix_sync_jobs_user_status", "user_id", "status"),)

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    service_connection_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE"), nullable=False
    )
    sync_type: orm.Mapped[types_module.SyncType] = orm.mapped_column(
        sa.Enum(types_module.SyncType, native_enum=False), nullable=False
    )
    status: orm.Mapped[types_module.SyncStatus] = orm.mapped_column(
        sa.Enum(types_module.SyncStatus, native_enum=False),
        nullable=False,
        default=types_module.SyncStatus.PENDING,
    )
    progress_current: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    progress_total: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True, default=None
    )
    error_message: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    items_created: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    items_updated: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    started_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    completed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )
