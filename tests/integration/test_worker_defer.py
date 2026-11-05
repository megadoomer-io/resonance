"""In-job deferral re-enqueues must survive arq's job-id dedup (real Redis)."""

import datetime
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import arq
import arq.constants as arq_constants
import pytest

import resonance.models.task as task_module
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.types as types_module
import resonance.worker as worker_module

pytestmark = pytest.mark.integration


def _session_factory(session: AsyncMock) -> MagicMock:
    ctx_manager = AsyncMock()
    ctx_manager.__aenter__.return_value = session
    ctx_manager.__aexit__.return_value = False
    return MagicMock(return_value=ctx_manager)


def _result(**attrs: Any) -> MagicMock:
    result = MagicMock()
    for name, value in attrs.items():
        getattr(result, name).return_value = value
    return result


async def test_sync_range_deferral_is_queued_while_running_job_key_exists(
    arq_redis: arq.ArqRedis,
) -> None:
    task_id = uuid.uuid4()
    parent_id = uuid.uuid4()
    running_job_id = f"sync_range:{task_id}"

    # Stand in for the running job: arq keeps this key for the job's lifetime.
    running_key = arq_constants.job_key_prefix + running_job_id
    await arq_redis.set(running_key, b"running", ex=60)
    try:
        dropped = await arq_redis.enqueue_job(
            "sync_range", str(task_id), _job_id=running_job_id
        )
        assert dropped is None, "precondition: arq drops a reused live job id"

        task = task_module.Task(
            id=task_id,
            user_id=uuid.uuid4(),
            service_connection_id=uuid.uuid4(),
            parent_id=parent_id,
            task_type=types_module.TaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": "saved_tracks"},
        )
        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        parent = task_module.Task(
            id=parent_id,
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.RUNNING,
        )

        session = AsyncMock()
        session.execute.side_effect = [
            _result(scalar_one_or_none=task),
            _result(scalar_one_or_none=parent),
            _result(scalar_one=connection),
            _result(scalar_one=1),
            _result(scalar_one_or_none=None),
        ]
        strategy = AsyncMock(spec=sync_base.SyncStrategy)
        strategy.concurrency = "sequential"
        strategy.execute.side_effect = sync_base.DeferRequest(
            retry_after=120.0, resume_params={"offset": 50}
        )

        ctx: dict[str, Any] = {
            "session_factory": _session_factory(session),
            "connector_registry": MagicMock(),
            "strategies": {types_module.ServiceType.SPOTIFY: strategy},
            "redis": arq_redis,
        }

        before = datetime.datetime.now(datetime.UTC)
        await worker_module.sync_range(ctx, str(task_id))

        assert task.status == types_module.SyncStatus.DEFERRED
        queued = await arq_redis.zrange(
            arq_redis.default_queue_name, 0, -1, withscores=True
        )
        assert len(queued) == 1
        job_id, score = queued[0][0].decode(), queued[0][1]
        assert job_id.startswith(f"sync_range:{task_id}:")
        assert await arq_redis.exists(arq_constants.job_key_prefix + job_id)
        runs_at = datetime.datetime.fromtimestamp(score / 1000, datetime.UTC)
        assert runs_at >= before + datetime.timedelta(seconds=119)
    finally:
        await arq_redis.delete(running_key)
