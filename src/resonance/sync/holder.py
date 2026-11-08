"""Find and terminate the Postgres backend holding a task row's lock.

A holder that froze (SIGSTOP, a hung process) mid-transaction stays ``idle in
transaction`` with its row locked, so the reaper's lock attempt fails on every
tick. These helpers identify that backend from the row's ``xmax`` and, only
when it is unmistakably a frozen worker, terminate it so Postgres rolls back
and releases the row. Everything here works as a non-superuser role for
backends of the same role.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as pg
import structlog

import resonance.models.task as task_models

if TYPE_CHECKING:
    import datetime
    import uuid

    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

WORKER_APPLICATION_PREFIX = "resonance-worker:"
FROZEN_STATES = frozenset({"idle in transaction", "idle in transaction (aborted)"})

_INSUFFICIENT_PRIVILEGE = "42501"

_HOLDERS = sa.text(
    """
    SELECT DISTINCT l.transactionid::text AS xid, a.pid, a.state,
           a.application_name,
           extract(epoch FROM clock_timestamp() - a.state_change)::float8
               AS idle_seconds,
           a.pid = pg_backend_pid() AS own
    FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'transactionid' AND l.mode = 'ExclusiveLock'
      AND l.granted
      AND l.transactionid = ANY(CAST(CAST(:xids AS text[]) AS xid[]))
    """
).bindparams(sa.bindparam("xids", type_=pg.ARRAY(sa.Text)))

_MULTIXACT_MEMBERS = sa.text(
    "SELECT xid::text FROM pg_get_multixact_members(CAST(CAST(:xmax AS text) AS xid))"
)

# Re-checks every safety condition in the statement that terminates, in a
# fresh transaction (pg_stat_activity is snapshotted per transaction), so a
# holder that moved on since the lookup is left alone.
_TERMINATE = sa.text(
    """
    SELECT pg_terminate_backend(a.pid, :timeout_ms)
    FROM pg_stat_activity a
    WHERE a.pid = :pid
      AND a.pid <> pg_backend_pid()
      AND a.state IN ('idle in transaction', 'idle in transaction (aborted)')
      AND a.application_name LIKE :app_prefix
      AND clock_timestamp() - a.state_change > make_interval(secs => :ttl_s)
      AND EXISTS (
          SELECT 1 FROM pg_locks l
          WHERE l.pid = a.pid AND l.locktype = 'transactionid'
            AND l.mode = 'ExclusiveLock' AND l.granted
            AND l.transactionid = CAST(CAST(:xid AS text) AS xid)
      )
    """
)


@dataclasses.dataclass(frozen=True)
class Holder:
    """The backend holding a row lock, as Postgres reports it."""

    pid: int
    xid: str
    state: str | None
    application_name: str | None
    idle_seconds: float | None
    own: bool


@dataclasses.dataclass(frozen=True)
class Lookup:
    """The holder of a row's lock, or why none could be singled out."""

    holder: Holder | None
    reason: str | None = None
    multixact: bool = False


async def find_holder(session: sa_async.AsyncSession, task_id: uuid.UUID) -> Lookup:
    """Identify the one backend holding ``task_id``'s row lock.

    ``xmax`` is either a transaction id or, when several transactions lock the
    row (e.g. FK key-share locks from child inserts), a multixact id; Postgres
    gives SQL no way to tell which, so both readings are tried and a row where
    both match is left alone.
    """
    xmax = (
        await session.execute(
            sa.text(
                f"SELECT xmax::text FROM {task_models.Task.__tablename__} "
                "WHERE id = :id"
            ),
            {"id": task_id},
        )
    ).scalar_one_or_none()
    if xmax is None or xmax == "0":
        return Lookup(None, "no_holder")

    direct = await _holders(session, [xmax])
    members = await _multixact_members(session, xmax)
    via_multixact = await _holders(session, members) if members else []
    if direct and via_multixact:
        return Lookup(None, "ambiguous_xmax")
    holders = direct or via_multixact
    multixact = not direct and bool(via_multixact)
    pids = {h.pid for h in holders}
    if not pids:
        return Lookup(None, "no_holder")
    if len(pids) > 1:
        return Lookup(None, "multiple_holders", multixact)
    return Lookup(holders[0], None, multixact)


async def _holders(session: sa_async.AsyncSession, xids: list[str]) -> list[Holder]:
    rows = (await session.execute(_HOLDERS, {"xids": xids})).mappings().all()
    return [Holder(**row) for row in rows]


async def _multixact_members(session: sa_async.AsyncSession, xmax: str) -> list[str]:
    try:
        async with session.begin_nested():
            return list(
                (await session.execute(_MULTIXACT_MEMBERS, {"xmax": xmax})).scalars()
            )
    except sa.exc.DBAPIError:
        # Not a live multixact id (the common case for a plain xid).
        return []


def unsafe_reason(holder: Holder, ttl: datetime.timedelta) -> str | None:
    """Why ``holder`` must not be terminated, or None when every check holds."""
    if holder.own:
        return "own_backend"
    if not (holder.application_name or "").startswith(WORKER_APPLICATION_PREFIX):
        return "not_worker"
    if holder.state not in FROZEN_STATES:
        return "not_idle_in_transaction"
    if holder.idle_seconds is None or holder.idle_seconds <= ttl.total_seconds():
        return "idle_under_ttl"
    return None


class TerminateError(Exception):
    """``pg_terminate_backend`` was refused or the backend didn't exit."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


async def terminate(
    session: sa_async.AsyncSession,
    holder: Holder,
    ttl: datetime.timedelta,
    timeout: datetime.timedelta,
) -> bool:
    """Terminate ``holder`` if it still passes every safety check.

    Returns:
        True once the backend has exited, False if it no longer qualified.

    Raises:
        TerminateError: refused (insufficient privilege) or not exited within
            ``timeout``.
    """
    try:
        result = (
            await session.execute(
                _TERMINATE,
                {
                    "pid": holder.pid,
                    "xid": holder.xid,
                    "timeout_ms": int(timeout.total_seconds() * 1000),
                    "app_prefix": f"{WORKER_APPLICATION_PREFIX}%",
                    "ttl_s": ttl.total_seconds(),
                },
            )
        ).scalar_one_or_none()
    except sa.exc.DBAPIError as exc:
        await session.rollback()
        if getattr(exc.orig, "sqlstate", None) == _INSUFFICIENT_PRIVILEGE:
            raise TerminateError("insufficient_privilege") from exc
        raise TerminateError("error") from exc
    await session.rollback()
    if result is None:
        return False
    if not result:
        raise TerminateError("timeout")
    return True
