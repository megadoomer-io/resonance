"""Re-deliver PENDING and DEFERRED tasks whose arq job was lost.

The task row is the truth and arq is the transport. A row the reaper may act
on records its current delivery (``enqueued_at`` + ``arq_job_id``); the
reaper re-delivers it only when arq reports that job as not found or
complete. Rows never handed to arq (``enqueued_at IS NULL``: sequential
siblings, step-mode children) are left alone, as are rows with children
(re-planning would duplicate them) and children of a terminal parent.
"""

from __future__ import annotations

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
import resonance.sync.lifecycle as lifecycle_module
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

LAST_TICK_KEY = "resonance:reaper:last_tick"
DECISIONS_KEY = "resonance:reaper:decisions"
MAX_RECORDED_DECISIONS = 50

ARQ_HOLDS_JOB: frozenset[arq_jobs.JobStatus] = frozenset(
    {
        arq_jobs.JobStatus.queued,
        arq_jobs.JobStatus.deferred,
        arq_jobs.JobStatus.in_progress,
    }
)

_REAPABLE_STATUSES = (types_module.SyncStatus.PENDING, types_module.SyncStatus.DEFERRED)


class Action(enum.StrEnum):
    """What the reaper did with one candidate row."""

    REDELIVERED = "redelivered"
    ENQUEUE_FAILED = "enqueue_failed"
    ARQ_HOLDS_JOB = "arq_holds_job"
    ROW_CHANGED = "row_changed"
    STATUS_ERROR = "status_error"
    NO_DISPATCH = "no_dispatch"


# Recorded for the health endpoint; ARQ_HOLDS_JOB repeats every tick for a
# queued backlog, so it is only logged.
_RECORDED_ACTIONS = frozenset(Action) - {Action.ARQ_HOLDS_JOB}


@dataclasses.dataclass(frozen=True)
class Decision:
    """One reaper decision, as logged and recorded for health."""

    at: datetime.datetime
    task_id: str
    task_type: str
    action: Action
    arq_status: str | None
    job_id: str | None

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


async def reap(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
    *,
    now: datetime.datetime | None = None,
) -> list[Decision]:
    """Re-deliver every PENDING/DEFERRED row whose arq job is gone.

    Args:
        session_factory: Session factory for the task database.
        arq_redis: arq pool used for job status and re-delivery.
        now: Clock override for tests.

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

    for decision in decisions:
        logger.info(
            "reaper_decision",
            task_id=decision.task_id,
            task_type=decision.task_type,
            action=decision.action.value,
            arq_status=decision.arq_status,
            job_id=decision.job_id,
        )
    await _record_tick(arq_redis, now, len(candidates), decisions)
    return decisions


async def _record_tick(
    arq_redis: arq.ArqRedis,
    now: datetime.datetime,
    candidates: int,
    decisions: list[Decision],
) -> None:
    recorded = [d for d in decisions if d.action in _RECORDED_ACTIONS]
    tick = {
        "at": now.isoformat(),
        "candidates": candidates,
        "redelivered": sum(d.action == Action.REDELIVERED for d in decisions),
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
