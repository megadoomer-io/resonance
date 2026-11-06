"""Hard cancel: a blocked running job is interrupted by arq abort (real Redis)."""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import arq
import arq.worker as arq_worker
import pytest

import resonance.sync.lifecycle as lifecycle_module

pytestmark = pytest.mark.integration


async def test_abort_interrupts_blocked_job_within_timeout(
    arq_redis: arq.ArqRedis,
) -> None:
    started = asyncio.Event()
    outcome: dict[str, Any] = {}

    async def blocked_export(ctx: dict[str, Any]) -> None:
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            outcome["cancelled"] = True
            raise
        outcome["finished"] = True

    worker = arq_worker.Worker(
        functions=[arq.func(blocked_export, name="blocked_export")],
        redis_pool=arq_redis,
        queue_name=arq_redis.default_queue_name,
        allow_abort_jobs=True,
        poll_delay=0.05,
        handle_signals=False,
    )
    job_id = f"blocked_export:{uuid.uuid4()}:1"
    await arq_redis.enqueue_job("blocked_export", _job_id=job_id)
    run = asyncio.create_task(worker.async_run())
    try:
        await asyncio.wait_for(started.wait(), timeout=5)

        loop = asyncio.get_running_loop()
        before = loop.time()
        confirmed = await lifecycle_module.abort_job(arq_redis, uuid.uuid4(), job_id)
        elapsed = loop.time() - before

        assert confirmed is True
        assert outcome == {"cancelled": True}
        assert elapsed < lifecycle_module.ABORT_TIMEOUT_SECONDS + 1
    finally:
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        await worker.close()
        await arq_redis.delete(f"arq:result:{job_id}")
