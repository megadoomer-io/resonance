"""Reaper and startup recovery against arq's real job state in Redis."""

from __future__ import annotations

import uuid

import arq
import arq.constants as arq_constants
import pytest

import resonance.sync.reaper as reaper_module
import resonance.types as types_module
import resonance.worker as worker_module
from tests.conftest import TaskDB, minutes_ago

pytestmark = pytest.mark.integration


async def _queued_job_ids(arq_redis: arq.ArqRedis) -> list[str]:
    return [
        jid.decode()
        for jid in await arq_redis.zrange(arq_redis.default_queue_name, 0, -1)
    ]


async def _clear_reaper_state(arq_redis: arq.ArqRedis) -> None:
    await arq_redis.delete(reaper_module.LAST_TICK_KEY, reaper_module.DECISIONS_KEY)


async def test_reaper_redelivers_pending_row_whose_job_is_gone(
    arq_redis: arq.ArqRedis, task_db: TaskDB
) -> None:
    lost_job_id = f"sync_range:{uuid.uuid4()}:lost"
    task = await task_db.add(enqueued_at=minutes_ago(5), arq_job_id=lost_job_id)
    try:
        decisions = await reaper_module.reap(task_db.factory, arq_redis)

        assert [d.action for d in decisions] == [reaper_module.Action.REDELIVERED]
        row = await task_db.get(task.id)
        assert row.arq_job_id is not None
        assert row.arq_job_id != lost_job_id
        assert await _queued_job_ids(arq_redis) == [row.arq_job_id]
        state = await reaper_module.read_state(arq_redis)
        assert state.last_tick is not None
        assert state.decisions[0]["task_id"] == str(task.id)
    finally:
        await _clear_reaper_state(arq_redis)


async def test_reaper_leaves_row_whose_job_is_queued(
    arq_redis: arq.ArqRedis, task_db: TaskDB
) -> None:
    job_id = f"sync_range:{uuid.uuid4()}:live"
    task = await task_db.add(enqueued_at=minutes_ago(5), arq_job_id=job_id)
    assert await arq_redis.enqueue_job("sync_range", str(task.id), _job_id=job_id)
    try:
        decisions = await reaper_module.reap(task_db.factory, arq_redis)

        assert [d.action for d in decisions] == [reaper_module.Action.ARQ_HOLDS_JOB]
        assert decisions[0].arq_status == "queued"
        assert await _queued_job_ids(arq_redis) == [job_id]
        assert (await task_db.get(task.id)).arq_job_id == job_id
    finally:
        await _clear_reaper_state(arq_redis)


@pytest.mark.parametrize("held", ["queued", "in_progress"])
async def test_startup_leaves_running_row_whose_job_arq_holds(
    arq_redis: arq.ArqRedis, task_db: TaskDB, held: str
) -> None:
    job_id = f"sync_range:{uuid.uuid4()}:running"
    task = await task_db.add(
        status=types_module.SyncStatus.RUNNING,
        enqueued_at=minutes_ago(5),
        arq_job_id=job_id,
    )
    # A SIGTERM-interrupted job stays queued; a live one holds the in-progress key.
    assert await arq_redis.enqueue_job("sync_range", str(task.id), _job_id=job_id)
    in_progress_key = arq_constants.in_progress_key_prefix + job_id
    if held == "in_progress":
        await arq_redis.set(in_progress_key, b"1", ex=60)
    try:
        await worker_module._reenqueue_orphaned_tasks(task_db.factory, arq_redis)

        row = await task_db.get(task.id)
        assert row.status == types_module.SyncStatus.RUNNING
        assert row.arq_job_id == job_id
        assert await _queued_job_ids(arq_redis) == [job_id]
    finally:
        await arq_redis.delete(in_progress_key)
        await _clear_reaper_state(arq_redis)


async def test_startup_resets_running_row_whose_job_is_gone(
    arq_redis: arq.ArqRedis, task_db: TaskDB
) -> None:
    task = await task_db.add(
        status=types_module.SyncStatus.RUNNING,
        enqueued_at=minutes_ago(5),
        arq_job_id=f"sync_range:{uuid.uuid4()}:gone",
    )
    try:
        await worker_module._reenqueue_orphaned_tasks(task_db.factory, arq_redis)

        row = await task_db.get(task.id)
        assert row.status == types_module.SyncStatus.PENDING
        assert await _queued_job_ids(arq_redis) == [row.arq_job_id]
    finally:
        await _clear_reaper_state(arq_redis)
