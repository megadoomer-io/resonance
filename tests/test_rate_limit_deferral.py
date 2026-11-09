"""An exhausted 429 budget defers the job instead of failing it (#200)."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import httpx

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.ratelimit as ratelimit_module
import resonance.models.user as user_models
import resonance.sync.listenbrainz as lb_sync
import resonance.types as types_module
import resonance.worker as worker_module

if TYPE_CHECKING:
    import pytest

    from tests.conftest import TaskDB


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def _always_429(retry_after: str) -> AsyncMock:
    client = AsyncMock(spec=httpx.AsyncClient)

    async def request(method: str, url: str, **_: Any) -> httpx.Response:
        return httpx.Response(
            429,
            headers={"Retry-After": retry_after},
            request=httpx.Request(method, url),
        )

    client.request.side_effect = request
    return client


async def test_sync_range_defers_when_429_budget_runs_out(
    task_db: TaskDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock()
    monkeypatch.setattr(base_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(base_module.asyncio, "sleep", clock.sleep)

    connector = listenbrainz_module.ListenBrainzConnector(config_module.Settings())
    connector._budget = ratelimit_module.RateLimitBudget(default_interval=0.0)
    connector._http_client = _always_429("60")

    user = user_models.User(id=uuid.uuid4(), display_name="t")
    connection = user_models.ServiceConnection(
        id=uuid.uuid4(),
        user_id=user.id,
        service_type=types_module.ServiceType.LISTENBRAINZ,
        external_user_id="someone",
    )
    async with task_db.factory() as session:
        session.add_all([user, connection])
        await session.commit()
    task = await task_db.add(
        task_type=types_module.TaskType.TIME_RANGE,
        user_id=user.id,
        service_connection_id=connection.id,
        params={"username": "someone"},
    )

    registry = MagicMock()
    registry.get_base_connector.return_value = connector
    redis = AsyncMock()
    ctx: dict[str, Any] = {
        "session_factory": task_db.factory,
        "redis": redis,
        "job_id": "a",
        "connector_registry": registry,
        "strategies": {
            types_module.ServiceType.LISTENBRAINZ: lb_sync.ListenBrainzSyncStrategy()
        },
    }

    await worker_module.sync_range(ctx, str(task.id))

    row = await task_db.get(task.id)
    assert row.status == types_module.SyncStatus.DEFERRED
    assert row.deferred_until is not None
    assert clock.sleeps == [60.0] * 5
    assert redis.enqueue_job.await_count == 1
    assert redis.enqueue_job.await_args.kwargs["_defer_by"].total_seconds() == 60.0
