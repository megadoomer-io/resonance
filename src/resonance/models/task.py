"""Task model for tracking sync jobs, bulk operations, and other async work.

Note: The underlying table is ``sync_tasks`` (historical name).
"""

from __future__ import annotations

import contextvars
import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.ext.compiler as sa_compiler
import sqlalchemy.orm as orm
import sqlalchemy.sql.expression as sa_expression

import resonance.models.base as base_module
import resonance.models.user as user_models
import resonance.types as types_module

# Python-side defaults applied via init event. ``id`` is assigned at
# construction so callers can pass ``str(task.id)`` as a job arg before flush.
_PYTHON_DEFAULTS: dict[str, object] = {
    "id": uuid.uuid4,
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
        # Serves the reaper and startup-recovery scans; built CONCURRENTLY by
        # its migration.
        sa.Index(
            "ix_sync_tasks_reaper_candidates",
            "status",
            postgresql_where=sa.text("status IN ('PENDING', 'DEFERRED', 'RUNNING')"),
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
    # NULL means no delivery was ever handed to arq (e.g. a sequential sibling
    # waiting its turn); the reaper never auto-delivers those rows.
    enqueued_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    arq_job_id: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(255), nullable=True, default=None
    )
    # Bumped by every claim (lifecycle.claim); writes from a delivery whose
    # claim is no longer the latest are refused (see _fence_claimed_writes).
    attempt: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0, server_default="0"
    )
    # Set by the claim and renewed while the job runs (DB clock); NULL when no
    # delivery holds the row. The reaper recovers RUNNING rows past it.
    lease_expires_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        nullable=False,
    )


class db_clock_offset(sa_expression.FunctionElement[datetime.datetime]):  # noqa: N801
    """The database's clock plus *offset*, so leases never trust a worker clock."""

    type = sa.DateTime(timezone=True)
    inherit_cache = True

    def __init__(self, offset: datetime.timedelta) -> None:
        self.offset = offset
        super().__init__()


@sa_compiler.compiles(db_clock_offset)
def _db_clock_offset_pg(element: db_clock_offset, compiler: object, **_: object) -> str:
    seconds = element.offset.total_seconds()
    return f"(now() + interval '{seconds} seconds')"


@sa_compiler.compiles(db_clock_offset, "sqlite")
def _db_clock_offset_sqlite(
    element: db_clock_offset, compiler: object, **_: object
) -> str:
    seconds = element.offset.total_seconds()
    return f"datetime('now', '{seconds:+} seconds')"


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


class TaskFenced(BaseException):
    """A delivery tried to write a task row another delivery has since claimed.

    A BaseException so the handlers' ``except Exception`` blocks can't catch it
    and record a failure; ``lifecycle.task_job`` stops the job instead.
    """

    def __init__(self, task_id: uuid.UUID, mine: int, current: int | None) -> None:
        super().__init__(f"task {task_id}: attempt {mine} fenced by {current}")
        self.task_id = task_id
        self.mine = mine
        self.current = current


# task id -> attempt this job claimed. Each arq job runs in its own asyncio
# task, so the claims are per job.
claimed_attempts: contextvars.ContextVar[dict[uuid.UUID, int] | None] = (
    contextvars.ContextVar("claimed_attempts", default=None)
)


@sa.event.listens_for(orm.Session, "before_flush")
def _fence_claimed_writes(
    session: orm.Session,
    _flush_context: object,
    _instances: object,
) -> None:
    """Refuse writes to a row this job claimed once a newer claim exists.

    FOR UPDATE holds the row until this transaction ends, so a claim can't land
    between the check and the write.
    """
    claims = claimed_attempts.get()
    if not claims:
        return
    changed = [
        obj
        for obj in session.dirty
        if isinstance(obj, Task) and obj.id in claims and session.is_modified(obj)
    ]
    if not changed:
        return
    current = dict(
        session.connection()
        .execute(
            sa.select(Task.id, Task.attempt)
            .where(Task.id.in_([obj.id for obj in changed]))
            # NO KEY UPDATE: doesn't block child-row inserts taking KEY SHARE.
            .with_for_update(key_share=True)
        )
        .tuples()
        .all()
    )
    for obj in changed:
        if current.get(obj.id) != claims[obj.id]:
            raise TaskFenced(obj.id, claims[obj.id], current.get(obj.id))


@sa.event.listens_for(orm.Session, "before_flush")
def _clear_lease_off_running(
    session: orm.Session,
    _flush_context: object,
    _instances: object,
) -> None:
    """A row that leaves RUNNING has no holder, so it keeps no lease."""
    for obj in session.dirty:
        if (
            isinstance(obj, Task)
            and obj.status != types_module.SyncStatus.RUNNING
            and sa.inspect(obj).attrs.status.history.has_changes()
        ):
            obj.lease_expires_at = None
            # The loaded value may already be a stale None; write it anyway.
            orm.attributes.flag_modified(obj, "lease_expires_at")


@sa.event.listens_for(orm.Session, "before_flush")
def _keep_cancelled(
    session: orm.Session,
    _flush_context: object,
    _instances: object,
) -> None:
    """A cancel wins over a run that was already in flight.

    Cancel commits CANCELLED from another session while a job may still hold
    the row in memory (the abort hasn't landed yet, or missed). When that job
    flushes a move out of the active statuses, the stored CANCELLED (and its
    reason) is kept.
    """
    changed = [
        obj
        for obj in session.dirty
        if isinstance(obj, Task)
        and obj.status not in types_module.ACTIVE_STATUSES
        and obj.status != types_module.SyncStatus.CANCELLED
        and sa.inspect(obj).attrs.status.history.has_changes()
    ]
    if not changed:
        return
    cancelled = {
        row.id: row.error_message
        for row in session.connection().execute(
            sa.select(Task.id, Task.error_message).where(
                Task.id.in_([obj.id for obj in changed]),
                Task.status == types_module.SyncStatus.CANCELLED,
            )
        )
    }
    for obj in changed:
        if obj.id in cancelled:
            obj.status = types_module.SyncStatus.CANCELLED
            obj.error_message = cancelled[obj.id]
