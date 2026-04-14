"""Task model for tracking sync jobs, bulk operations, and other async work.

Note: The underlying table is ``sync_tasks`` (historical name).
"""

from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.models.user as user_models
import resonance.types as types_module

# Python-side defaults for mutable/computed fields applied via init event.
_PYTHON_DEFAULTS: dict[str, object] = {
    "params": dict,
    "result": dict,
    "progress_current": 0,
}


class Task(base_module.Base):
    """A task: sync jobs, bulk operations, or other async work.

    Sync tasks use parent/children for hierarchy (sync_job -> time_range).
    Bulk tasks (e.g., dedup) are standalone with no parent or service connection.
    """

    __tablename__ = "sync_tasks"
    __table_args__ = (
        sa.Index("ix_sync_tasks_user_status", "user_id", "status"),
        sa.Index("ix_sync_tasks_parent_status", "parent_id", "status"),
        sa.Index(
            "ix_sync_tasks_connection_type_status",
            "service_connection_id",
            "task_type",
            "status",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=True
    )
    service_connection_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE"), nullable=True
    )
    service_connection: orm.Mapped[user_models.ServiceConnection | None] = (
        orm.relationship()
    )
    parent_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("sync_tasks.id", ondelete="CASCADE"), nullable=True, default=None
    )
    parent: orm.Mapped[Task | None] = orm.relationship(
        back_populates="children", remote_side=[id]
    )
    children: orm.Mapped[list[Task]] = orm.relationship(
        back_populates="parent", cascade="all, delete-orphan"
    )
    task_type: orm.Mapped[types_module.TaskType] = orm.mapped_column(
        sa.Enum(types_module.TaskType, native_enum=False), nullable=False
    )
    status: orm.Mapped[types_module.SyncStatus] = orm.mapped_column(
        sa.Enum(types_module.SyncStatus, native_enum=False),
        nullable=False,
        default=types_module.SyncStatus.PENDING,
    )
    params: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, insert_default=dict
    )
    result: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, insert_default=dict
    )
    error_message: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    progress_current: orm.Mapped[int] = orm.mapped_column(
        sa.BigInteger, nullable=False, insert_default=0
    )
    progress_total: orm.Mapped[int | None] = orm.mapped_column(
        sa.BigInteger, nullable=True, default=None
    )
    started_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    completed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    description: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    deferred_until: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )


@sa.event.listens_for(Task, "init")
def _set_python_defaults(
    target: Task,
    _args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    """Apply Python-side defaults for fields that need them at construction."""
    for attr, default in _PYTHON_DEFAULTS.items():
        if attr not in kwargs:
            value = default() if callable(default) else default
            setattr(target, attr, value)
