"""Re-deliver PENDING and DEFERRED tasks whose arq job was lost, and recover
RUNNING tasks whose lease expired.

The task row is the truth and arq is the transport. A row the reaper may act
on records its current delivery (``enqueued_at`` + ``arq_job_id``); the
reaper re-delivers it only when arq reports that job as not found or
complete. Rows never handed to arq (``enqueued_at IS NULL``: sequential
siblings, step-mode children) are left alone, as are rows with children
(re-planning would duplicate them) and children of a terminal parent.

A RUNNING row is recovered only once its lease (renewed by the job holding it,
see ``lifecycle.renew_lease``) is past ``LEASE_GRACE``. A NULL lease means
pre-lease code claimed it, or its job ended leaving it RUNNING (a parent
waiting on children); startup recovery covers those. ``LeaseRecovery.LOG``
records the decision without acting.

An expired row that is still locked belongs to a holder frozen mid-transaction
(see ``holder``); in active mode the reaper terminates that backend so the next
tick can recover the row.
"""

from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import enum
import json
from typing import TYPE_CHECKING

import arq.jobs as arq_jobs
import sqlalchemy as sa
import sqlalchemy.orm as sa_orm
import structlog

import resonance.models.task as task_models
import resonance.sync.dispatch as dispatch_module
import resonance.sync.holder as holder_module
import resonance.sync.lifecycle as lifecycle_module
import resonance.sync.resume as resume_module
import resonance.types as types_module

if TYPE_CHECKING:
    import uuid

    import arq
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()

# One indexed query plus one pipelined Redis read per candidate (normally a
# handful), so a short interval is cheap and bounds recovery latency.
INTERVAL = datetime.timedelta(minutes=1)
# Covers the window between a submitter's commit and its enqueue.
DELIVERY_GRACE = datetime.timedelta(seconds=60)
MAX_CANDIDATES_PER_TICK = 100
# On top of the lease TTL: a renewal delayed by a DB blip or a long GC pause
# shouldn't cost a live run its row.
LEASE_GRACE = datetime.timedelta(seconds=30)

LAST_TICK_KEY = "resonance:reaper:last_tick"
DECISIONS_KEY = "resonance:reaper:decisions"
# One key per (action, task, attempt) already recorded, so a stuck row is
# recorded once instead of every tick.
SEEN_PREFIX = "resonance:reaper:"
LEASE_EXPIRED_SEEN_PREFIX = f"{SEEN_PREFIX}lease_expired:"
LEASE_EXPIRED_SEEN_TTL = datetime.timedelta(days=7)
MAX_RECORDED_DECISIONS = 50
TERMINATE_TIMEOUT = datetime.timedelta(seconds=5)

ARQ_HOLDS_JOB: frozenset[arq_jobs.JobStatus] = frozenset(
    {
        arq_jobs.JobStatus.queued,
        arq_jobs.JobStatus.deferred,
        arq_jobs.JobStatus.in_progress,
    }
)

_ONCE_PER_ATTEMPT = frozenset(
    {"lease_expired", "holder_locked", "holder_frozen", "terminate_failed"}
)

_ARQ_WILL_RUN: frozenset[str] = frozenset(
    {arq_jobs.JobStatus.queued.value, arq_jobs.JobStatus.deferred.value}
)

_REAPABLE_STATUSES = (types_module.SyncStatus.PENDING, types_module.SyncStatus.DEFERRED)


class LeaseRecovery(enum.StrEnum):
    """What the reaper does with a RUNNING row whose lease expired."""

    LOG = "log"
    ACTIVE = "active"


class Action(enum.StrEnum):
    """What the reaper did with one candidate row."""

    REDELIVERED = "redelivered"
    ENQUEUE_FAILED = "enqueue_failed"
    ARQ_HOLDS_JOB = "arq_holds_job"
    ROW_CHANGED = "row_changed"
    STATUS_ERROR = "status_error"
    NO_DISPATCH = "no_dispatch"
    LEASE_EXPIRED = "lease_expired"
    LEASE_RECOVERED = "lease_recovered"
    HOLDER_ALIVE = "holder_alive"
    LIVENESS_ERROR = "liveness_error"
    HOLDER_LOCKED = "holder_locked"
    HOLDER_FROZEN = "holder_frozen"
    HOLDER_TERMINATED = "holder_terminated"
    TERMINATE_FAILED = "terminate_failed"
    HOLDER_CHECK_ERROR = "holder_check_error"


# Recorded for the health endpoint; ARQ_HOLDS_JOB repeats every tick for a
# queued backlog, so it is only logged.
_RECORDED_ACTIONS = frozenset(Action) - {Action.ARQ_HOLDS_JOB, Action.HOLDER_ALIVE}


@dataclasses.dataclass(frozen=True)
class Decision:
    """One reaper decision, as logged and recorded for health."""

    at: datetime.datetime
    task_id: str
    task_type: str
    action: Action
    arq_status: str | None
    job_id: str | None
    attempt: int | None = None
    detail: dict[str, object] | None = None

    def to_json(self) -> str:
        return json.dumps({**dataclasses.asdict(self), "at": self.at.isoformat()})


async def job_status(arq_redis: arq.ArqRedis, job_id: str) -> arq_jobs.JobStatus:
    """arq's view of one job on the default queue."""
    job = arq_jobs.Job(job_id, arq_redis, _queue_name=arq_redis.default_queue_name)
    return await job.status()


def _candidate_query(
    now: datetime.datetime,
) -> sa.Select[tuple[uuid.UUID, types_module.TaskType, str | None]]:
    task = task_models.Task
    parent = sa_orm.aliased(task_models.Task)
    child = sa_orm.aliased(task_models.Task)
    return (
        sa.select(task.id, task.task_type, task.arq_job_id)
        .outerjoin(parent, task.parent_id == parent.id)
        .where(
            task.enqueued_at.is_not(None),
            task.enqueued_at <= now - DELIVERY_GRACE,
            sa.or_(
                task.status == types_module.SyncStatus.PENDING,
                sa.and_(
                    task.status == types_module.SyncStatus.DEFERRED,
                    sa.or_(task.deferred_until.is_(None), task.deferred_until <= now),
                ),
            ),
            sa.or_(
                task.parent_id.is_(None),
                parent.status.notin_(types_module.TERMINAL_STATUSES),
            ),
            ~sa.exists().where(child.parent_id == task.id),
        )
        .order_by(task.enqueued_at)
        .limit(MAX_CANDIDATES_PER_TICK)
    )


def _lease_expired(task: type[task_models.Task]) -> sa.ColumnElement[bool]:
    return sa.and_(
        task.status == types_module.SyncStatus.RUNNING,
        task.lease_expires_at.is_not(None),
        task.lease_expires_at < task_models.db_clock_offset(-LEASE_GRACE),
    )


def _expired_lease_query() -> sa.Select[
    tuple[uuid.UUID, types_module.TaskType, str | None, int]
]:
    task = task_models.Task
    parent = sa_orm.aliased(task_models.Task)
    child = sa_orm.aliased(task_models.Task)
    return (
        sa.select(task.id, task.task_type, task.arq_job_id, task.attempt)
        .outerjoin(parent, task.parent_id == parent.id)
        .where(
            _lease_expired(task),
            sa.or_(
                task.parent_id.is_(None),
                parent.status.notin_(types_module.TERMINAL_STATUSES),
            ),
            ~sa.exists().where(child.parent_id == task.id),
        )
        .order_by(task.lease_expires_at)
        .limit(MAX_CANDIDATES_PER_TICK)
    )


async def _still_reapable(
    session: sa_async.AsyncSession,
    task: task_models.Task,
    seen_job_id: str | None,
    now: datetime.datetime,
) -> bool:
    """Re-check a locked row: it may have moved on since the scan."""
    if task.status not in _REAPABLE_STATUSES or task.arq_job_id != seen_job_id:
        return False
    if (
        task.status == types_module.SyncStatus.DEFERRED
        and task.deferred_until is not None
        and _aware(task.deferred_until) > now
    ):
        return False
    if task.parent_id is not None:
        parent_status = (
            await session.execute(
                sa.select(task_models.Task.status).where(
                    task_models.Task.id == task.parent_id
                )
            )
        ).scalar_one_or_none()
        if parent_status in types_module.TERMINAL_STATUSES:
            return False
    has_children = (
        await session.execute(
            sa.select(sa.exists().where(task_models.Task.parent_id == task.id))
        )
    ).scalar_one()
    return not has_children


def _aware(value: datetime.datetime) -> datetime.datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=datetime.UTC)


async def _redeliver(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
    task_id: uuid.UUID,
    seen_job_id: str | None,
    arq_status: str,
    now: datetime.datetime,
) -> Decision | None:
    async with session_factory() as session:
        task = (
            await session.execute(
                sa.select(task_models.Task)
                .where(task_models.Task.id == task_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if task is None:
            return None
        # Read before any rollback, which expires the loaded row.
        task_type = task.task_type.value

        def decide(action: Action, job_id: str | None) -> Decision:
            return Decision(
                at=now,
                task_id=str(task_id),
                task_type=task_type,
                action=action,
                arq_status=arq_status,
                job_id=job_id,
            )

        if not await _still_reapable(session, task, seen_job_id, now):
            await session.rollback()
            return decide(Action.ROW_CHANGED, seen_job_id)
        job = dispatch_module.job_for(task)
        if job is None:
            await session.rollback()
            return decide(Action.NO_DISPATCH, seen_job_id)

        if not await lifecycle_module.release_for_redelivery(session, task):
            await session.rollback()
            return decide(Action.ROW_CHANGED, seen_job_id)
        delivery = lifecycle_module.stamp_delivery(task, job[0], *job[1])
        await session.commit()
        delivered = await lifecycle_module.deliver(arq_redis, delivery)
        return decide(
            Action.REDELIVERED if delivered else Action.ENQUEUE_FAILED,
            delivery.job_id,
        )


async def _recover_expired_lease(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
    task_id: uuid.UUID,
    seen_attempt: int,
    decide: DecideFn,
) -> Decision:
    """Fence the stalled holder and redeliver with a fresh job id."""
    async with session_factory() as session:
        try:
            task = await _lock_expired(session, task_id, seen_attempt)
        except _RowLockedError:
            return await _handle_locked_row(
                session_factory, task_id, seen_attempt, LeaseRecovery.ACTIVE, decide
            )
        if task is None:
            await session.rollback()
            return decide(Action.ROW_CHANGED, None)
        seen_job_id = task.arq_job_id
        job = dispatch_module.job_for(task)
        if job is None:
            await session.rollback()
            return decide(Action.NO_DISPATCH, seen_job_id)
        if not await lifecycle_module.release_for_redelivery(session, task):
            await session.rollback()
            return decide(Action.ROW_CHANGED, seen_job_id)
        await resume_module.resume_recovered(session, task)
        delivery = lifecycle_module.stamp_delivery(task, job[0], *job[1])
        await session.commit()
    delivered = await lifecycle_module.deliver(arq_redis, delivery)
    return decide(
        Action.LEASE_RECOVERED if delivered else Action.ENQUEUE_FAILED,
        delivery.job_id,
    )


DecideFn = collections.abc.Callable[..., Decision]


class _RowLockedError(Exception):
    pass


async def _lock_expired(
    session: sa_async.AsyncSession, task_id: uuid.UUID, seen_attempt: int
) -> task_models.Task | None:
    """Lock the row if it is still expired under ``seen_attempt``.

    Raises:
        _RowLockedError: another transaction holds the row (the session is rolled
            back).
    """
    try:
        return (
            await session.execute(
                sa.select(task_models.Task)
                .where(
                    task_models.Task.id == task_id,
                    task_models.Task.attempt == seen_attempt,
                    _lease_expired(task_models.Task),
                )
                # Never queue behind the holder: a locked row goes to the
                # holder check instead.
                .with_for_update(nowait=True)
            )
        ).scalar_one_or_none()
    except sa.exc.DBAPIError as exc:
        if getattr(exc.orig, "sqlstate", None) != lifecycle_module.LOCK_NOT_AVAILABLE:
            raise
        await session.rollback()
        raise _RowLockedError from exc


async def _row_locked(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    task_id: uuid.UUID,
    seen_attempt: int,
) -> bool:
    async with session_factory() as session:
        try:
            await _lock_expired(session, task_id, seen_attempt)
        except _RowLockedError:
            return True
        await session.rollback()
        return False


async def _handle_locked_row(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    task_id: uuid.UUID,
    seen_attempt: int,
    lease_recovery: LeaseRecovery,
    decide: DecideFn,
) -> Decision:
    """Deal with the backend holding an expired row's lock.

    Terminates only an unmistakably frozen worker, and only in active mode.
    Never recovers in the same tick: the dead backend's transaction rolls back
    and the next tick's normal path recovers (and fences) the row.
    """
    ttl = lifecycle_module.LEASE_TTL
    async with session_factory() as session:
        try:
            lookup = await holder_module.find_holder(session, task_id)
        except sa.exc.DBAPIError:
            logger.exception("reaper_holder_check_failed", task_id=str(task_id))
            await session.rollback()
            return decide(Action.HOLDER_CHECK_ERROR, None)
        await session.rollback()
        holder = lookup.holder
        if holder is None:
            return decide(
                Action.HOLDER_LOCKED,
                None,
                detail={"reason": lookup.reason, "multixact": lookup.multixact},
            )
        detail: dict[str, object] = {
            "pid": holder.pid,
            "state": holder.state,
            "application_name": holder.application_name,
            "idle_seconds": (
                round(holder.idle_seconds, 1)
                if holder.idle_seconds is not None
                else None
            ),
            "multixact": lookup.multixact,
        }
        reason = holder_module.unsafe_reason(holder, ttl)
        if reason is not None:
            return decide(
                Action.HOLDER_LOCKED, None, detail={**detail, "reason": reason}
            )
        if lease_recovery == LeaseRecovery.LOG:
            return decide(Action.HOLDER_FROZEN, None, detail=detail)
        if lookup.xmax is None:
            return decide(
                Action.HOLDER_LOCKED, None, detail={**detail, "reason": "no_xmax"}
            )
        try:
            terminated = await holder_module.terminate(
                session,
                holder,
                holder_module.RowState(task_id, seen_attempt, lookup.xmax),
                ttl=ttl,
                grace=LEASE_GRACE,
                timeout=TERMINATE_TIMEOUT,
            )
        except holder_module.TerminateError as exc:
            logger.warning(
                "reaper_terminate_failed",
                task_id=str(task_id),
                reason=exc.reason,
                **detail,
            )
            return decide(
                Action.TERMINATE_FAILED, None, detail={**detail, "reason": exc.reason}
            )
    if not terminated:
        return decide(
            Action.HOLDER_LOCKED, None, detail={**detail, "reason": "holder_moved_on"}
        )
    logger.warning("reaper_holder_terminated", task_id=str(task_id), **detail)
    return decide(Action.HOLDER_TERMINATED, None, detail=detail)


async def _reap_expired_leases(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
    lease_recovery: LeaseRecovery,
    now: datetime.datetime,
) -> list[Decision]:
    async with session_factory() as session:
        expired = (await session.execute(_expired_lease_query())).all()

    decisions: list[Decision] = []
    for task_id, task_type, seen_job_id, seen_attempt in expired:
        try:
            arq_status: str | None = (
                (await job_status(arq_redis, seen_job_id)).value
                if seen_job_id
                else arq_jobs.JobStatus.not_found.value
            )
        except Exception:
            arq_status = None

        def decide(
            action: Action,
            job_id: str | None,
            *,
            _task_id: uuid.UUID = task_id,
            _task_type: types_module.TaskType = task_type,
            _arq_status: str | None = arq_status,
            _attempt: int = seen_attempt,
            detail: dict[str, object] | None = None,
        ) -> Decision:
            return Decision(
                now,
                str(_task_id),
                _task_type.value,
                action,
                _arq_status,
                job_id,
                _attempt,
                detail,
            )

        # An expired DB lease alone isn't death: the holder's own transaction
        # can block its renewal. Its Redis liveness key says it's still running.
        try:
            alive = bool(
                await arq_redis.exists(
                    lifecycle_module.alive_key(task_id, seen_attempt)
                )
            )
        except Exception:
            logger.exception("reaper_liveness_check_failed", task_id=str(task_id))
            decisions.append(decide(Action.LIVENESS_ERROR, seen_job_id))
            continue
        if alive:
            decisions.append(decide(Action.HOLDER_ALIVE, seen_job_id))
            continue
        if lease_recovery == LeaseRecovery.LOG:
            decisions.append(decide(Action.LEASE_EXPIRED, seen_job_id))
            if await _row_locked(session_factory, task_id, seen_attempt):
                decisions.append(
                    await _handle_locked_row(
                        session_factory,
                        task_id,
                        seen_attempt,
                        LeaseRecovery.LOG,
                        decide,
                    )
                )
            continue
        if arq_status is None:
            decisions.append(decide(Action.STATUS_ERROR, seen_job_id))
            continue
        # A queued or deferred job (arq's SIGTERM retry) will reclaim the row
        # itself. An in_progress one with an expired lease is the stalled
        # worker the lease exists for, so it is recovered.
        if arq_status in _ARQ_WILL_RUN:
            decisions.append(decide(Action.ARQ_HOLDS_JOB, seen_job_id))
            continue
        decisions.append(
            await _recover_expired_lease(
                session_factory, arq_redis, task_id, seen_attempt, decide
            )
        )
    return decisions


async def reap(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
    *,
    lease_recovery: LeaseRecovery = LeaseRecovery.LOG,
    now: datetime.datetime | None = None,
) -> list[Decision]:
    """Re-deliver every PENDING/DEFERRED row whose arq job is gone, and handle
    RUNNING rows whose lease expired.

    Args:
        session_factory: Session factory for the task database.
        arq_redis: arq pool used for job status and re-delivery.
        lease_recovery: Log expired leases only, or recover them.
        now: Clock override for tests (lease expiry always uses the DB clock).

    Returns:
        Every decision made this tick, including rows left to arq.
    """
    now = now or datetime.datetime.now(datetime.UTC)
    async with session_factory() as session:
        candidates = (await session.execute(_candidate_query(now))).all()

    decisions: list[Decision] = []
    for task_id, task_type, seen_job_id in candidates:
        try:
            status = (
                await job_status(arq_redis, seen_job_id)
                if seen_job_id
                else arq_jobs.JobStatus.not_found
            )
        except Exception:
            logger.exception("reaper_status_check_failed", task_id=str(task_id))
            decisions.append(
                Decision(
                    now,
                    str(task_id),
                    task_type.value,
                    Action.STATUS_ERROR,
                    None,
                    seen_job_id,
                )
            )
            continue

        if status in ARQ_HOLDS_JOB:
            decisions.append(
                Decision(
                    now,
                    str(task_id),
                    task_type.value,
                    Action.ARQ_HOLDS_JOB,
                    status.value,
                    seen_job_id,
                )
            )
            continue

        decision = await _redeliver(
            session_factory, arq_redis, task_id, seen_job_id, status.value, now
        )
        if decision is not None:
            decisions.append(decision)

    decisions += await _reap_expired_leases(
        session_factory, arq_redis, lease_recovery, now
    )
    recorded = await _to_record(arq_redis, decisions)
    for decision in decisions:
        quiet = decision.action in _QUIET_ACTIONS or (
            decision.action in _ONCE_PER_ATTEMPT and decision not in recorded
        )
        (logger.debug if quiet else logger.info)(
            "reaper_decision",
            task_id=decision.task_id,
            task_type=decision.task_type,
            action=decision.action.value,
            arq_status=decision.arq_status,
            job_id=decision.job_id,
            **(decision.detail or {}),
        )
    await _record_tick(arq_redis, now, len(candidates), decisions, recorded)
    return decisions


async def _record_tick(
    arq_redis: arq.ArqRedis,
    now: datetime.datetime,
    candidates: int,
    decisions: list[Decision],
    recorded: list[Decision],
) -> None:
    tick = {
        "at": now.isoformat(),
        "candidates": candidates,
        "redelivered": sum(d.action == Action.REDELIVERED for d in decisions),
        "lease_expired": sum(d.action == Action.LEASE_EXPIRED for d in decisions),
        "lease_recovered": sum(d.action == Action.LEASE_RECOVERED for d in decisions),
        "holder_frozen": sum(d.action == Action.HOLDER_FROZEN for d in decisions),
        "holder_terminated": sum(
            d.action == Action.HOLDER_TERMINATED for d in decisions
        ),
    }
    try:
        async with arq_redis.pipeline(transaction=True) as pipe:
            pipe.set(LAST_TICK_KEY, json.dumps(tick))
            if recorded:
                pipe.lpush(DECISIONS_KEY, *[d.to_json() for d in recorded])
                pipe.ltrim(DECISIONS_KEY, 0, MAX_RECORDED_DECISIONS - 1)
            await pipe.execute()
    except Exception:
        logger.exception("reaper_state_record_failed")


_QUIET_ACTIONS = frozenset({Action.HOLDER_ALIVE})


async def _to_record(
    arq_redis: arq.ArqRedis, decisions: list[Decision]
) -> list[Decision]:
    return [
        d
        for d in decisions
        if d.action in _RECORDED_ACTIONS
        and (d.action not in _ONCE_PER_ATTEMPT or await _first_sighting(arq_redis, d))
    ]


async def _first_sighting(arq_redis: arq.ArqRedis, decision: Decision) -> bool:
    key = f"{SEEN_PREFIX}{decision.action}:{decision.task_id}:{decision.attempt}"
    try:
        return bool(
            await arq_redis.set(
                key, b"1", nx=True, ex=int(LEASE_EXPIRED_SEEN_TTL.total_seconds())
            )
        )
    except Exception:
        logger.exception("reaper_lease_seen_check_failed")
        return True


@dataclasses.dataclass(frozen=True)
class State:
    """Reaper state as recorded in Redis."""

    last_tick: dict[str, object] | None
    decisions: list[dict[str, object]]


async def read_state(redis: arq.ArqRedis) -> State:
    """The last tick and recent recorded decisions, newest first."""
    async with redis.pipeline(transaction=False) as pipe:
        pipe.get(LAST_TICK_KEY)
        pipe.lrange(DECISIONS_KEY, 0, MAX_RECORDED_DECISIONS - 1)
        raw_tick, raw_decisions = await pipe.execute()
    return State(
        last_tick=json.loads(raw_tick) if raw_tick else None,
        decisions=[json.loads(d) for d in raw_decisions],
    )
