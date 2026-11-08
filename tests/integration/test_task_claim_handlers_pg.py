"""Claims and fencing through real handler paths (sync_range), on Postgres."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.models.task as task_models
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module
import resonance.worker as worker_module

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration

S = types_module.SyncStatus


async def _seed_time_range(db: TaskDB) -> task_models.Task:
    user = user_models.User(id=uuid.uuid4(), display_name="t")
    connection = user_models.ServiceConnection(
        id=uuid.uuid4(), user_id=user.id, service_type=types_module.ServiceType.TEST
    )
    async with db.factory() as session:
        session.add_all([user, connection])
        await session.commit()
    return await db.add(
        task_type=types_module.TaskType.TIME_RANGE,
        status=S.PENDING,
        user_id=user.id,
        service_connection_id=connection.id,
        params={},
    )


def _ctx(
    db: TaskDB,
    job_id: str,
    execute: Callable[..., Awaitable[dict[str, object]]],
) -> dict[str, Any]:
    strategy = MagicMock(spec=sync_base.SyncStrategy)
    strategy.execute = execute
    registry = MagicMock()
    registry.get_base_connector.return_value = MagicMock()
    redis = AsyncMock()
    return {
        "session_factory": db.factory,
        "redis": redis,
        "job_id": job_id,
        "connector_registry": registry,
        "strategies": {types_module.ServiceType.TEST: strategy},
    }


def _sync_range() -> Callable[..., Awaitable[None]]:
    return lifecycle_module.task_job(worker_module.sync_range)


async def _stalled_run(
    db: TaskDB, task_id: uuid.UUID, outcome: BaseException | dict[str, object]
) -> tuple[asyncio.Task[None], asyncio.Event, dict[str, Any]]:
    """Start sync_range as delivery "a"; it stalls inside execute until resumed."""
    started, resume = asyncio.Event(), asyncio.Event()

    async def execute(session: Any, task: Any, *_: Any) -> dict[str, object]:
        task.progress_current = 7
        started.set()
        await resume.wait()
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    ctx = _ctx(db, "a", execute)
    run = asyncio.create_task(_sync_range()(ctx, str(task_id)))
    await started.wait()
    return run, resume, ctx


async def test_cancel_fences_a_run_that_then_defers(pg_task_db: TaskDB) -> None:
    """A missed abort must not let the old run's DEFERRED revive the task."""
    task = await _seed_time_range(pg_task_db)
    run, resume, ctx = await _stalled_run(
        pg_task_db, task.id, sync_base.DeferRequest(30, {"page": 2})
    )
    async with pg_task_db.factory() as session:
        result = await lifecycle_module.request_cancel(session, task.id, task.user_id)
    assert result.outcome == lifecycle_module.CancelOutcome.CANCELLED
    resume.set()
    await run

    row = await pg_task_db.get(task.id)
    assert row.status == S.CANCELLED
    assert row.progress_current == 0
    assert "page" not in row.params
    ctx["redis"].enqueue_job.assert_not_called()


async def test_cancelled_parent_fences_running_child(pg_task_db: TaskDB) -> None:
    task = await _seed_time_range(pg_task_db)
    parent = await pg_task_db.add(
        task_type=types_module.TaskType.SYNC_JOB,
        status=S.RUNNING,
        user_id=task.user_id,
        service_connection_id=task.service_connection_id,
    )
    await pg_task_db.update(task.id, parent_id=parent.id)
    run, resume, _ = await _stalled_run(
        pg_task_db, task.id, sync_base.DeferRequest(30, {})
    )
    async with pg_task_db.factory() as session:
        await lifecycle_module.request_cancel(session, parent.id, task.user_id)
    resume.set()
    await run

    assert (await pg_task_db.get(task.id)).status == S.CANCELLED


async def test_defer_then_new_delivery_reclaims(pg_task_db: TaskDB) -> None:
    task = await _seed_time_range(pg_task_db)

    async def defer(*_: Any) -> dict[str, object]:
        raise sync_base.DeferRequest(30, {"page": 2})

    await _sync_range()(_ctx(pg_task_db, "a", defer), str(task.id))
    deferred = await pg_task_db.get(task.id)
    assert (deferred.status, deferred.attempt) == (S.DEFERRED, 1)
    assert deferred.params["page"] == 2
    new_job_id = deferred.arq_job_id
    assert new_job_id is not None and new_job_id != "a"

    async def finish(*_: Any) -> dict[str, object]:
        return {"items_created": 1, "items_updated": 0}

    await _sync_range()(_ctx(pg_task_db, new_job_id, finish), str(task.id))

    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.COMPLETED, 2)
    assert row.result["items_created"] == 1


async def test_reclaimed_run_is_fenced_and_rolled_back(pg_task_db: TaskDB) -> None:
    """The stale run's completion, progress and watermark all roll back."""
    task = await _seed_time_range(pg_task_db)
    run, resume, _ = await _stalled_run(
        pg_task_db,
        task.id,
        {"items_created": 9, "items_updated": 0, "watermark": {"stale": 1}},
    )
    async with pg_task_db.factory() as session:
        loaded = await session.get(task_models.Task, task.id)
        assert loaded is not None
        assert await lifecycle_module.release_for_redelivery(session, loaded)
        await session.commit()
    resume.set()
    await run

    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt, row.progress_current) == (S.PENDING, 2, 0)
    assert row.result == {}
    async with pg_task_db.factory() as session:
        connection = await session.get(
            user_models.ServiceConnection, task.service_connection_id
        )
        assert connection is not None
        assert connection.sync_watermark == {}


async def test_sigterm_mid_sync_retry_resumes_from_watermark(
    pg_task_db: TaskDB,
) -> None:
    """arq cancels a job on SIGTERM and retries the same job id; the retry
    reclaims the RUNNING row and resumes from the committed watermark."""
    user = user_models.User(id=uuid.uuid4(), display_name="t")
    connection = user_models.ServiceConnection(
        id=uuid.uuid4(),
        user_id=user.id,
        service_type=types_module.ServiceType.LISTENBRAINZ,
        external_user_id="lb",
    )
    async with pg_task_db.factory() as session:
        session.add_all([user, connection])
        await session.commit()
    task = await pg_task_db.add(
        task_type=types_module.TaskType.TIME_RANGE,
        user_id=user.id,
        service_connection_id=connection.id,
        params={"username": "lb", "min_ts": 1700000000},
    )
    page_committed = asyncio.Event()

    async def first_run(
        session: Any, running: Any, _c: Any, conn: Any
    ) -> dict[str, object]:
        running.progress_current = 500
        conn.sync_watermark = {"listens": {"oldest_synced_at": 1700400000}}
        await session.commit()
        page_committed.set()
        await asyncio.Event().wait()
        return {}

    seen: dict[str, object] = {}

    async def retry(_s: Any, running: Any, *_: Any) -> dict[str, object]:
        seen.update(running.params)
        return {"items_created": 0, "items_updated": 0}

    def ctx(execute: Any) -> dict[str, Any]:
        c = _ctx(pg_task_db, "sync_range:same", execute)
        c["strategies"] = {
            types_module.ServiceType.LISTENBRAINZ: c["strategies"][
                types_module.ServiceType.TEST
            ]
        }
        return c

    run = asyncio.create_task(_sync_range()(ctx(first_run), str(task.id)))
    await page_committed.wait()
    run.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run
    assert (await pg_task_db.get(task.id)).status == S.RUNNING

    await _sync_range()(ctx(retry), str(task.id))

    assert (seen["max_ts"], seen["items_so_far"]) == (1700400000, 500)
    row = await pg_task_db.get(task.id)
    assert (row.status, row.attempt) == (S.COMPLETED, 2)
