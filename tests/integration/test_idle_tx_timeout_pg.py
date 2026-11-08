"""A worker session left idle in a transaction is ended by Postgres (#197)."""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa

import resonance.config as config_module
import resonance.database as database_module
import resonance.models as models
import resonance.models.user as user_models
import resonance.sync.base as sync_base
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module
import resonance.worker as worker_module
from tests.conftest import TaskDB

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    import sqlalchemy.ext.asyncio as sa_async

pytestmark = pytest.mark.integration

S = types_module.SyncStatus


@pytest.fixture
async def worker_engine(pg_task_db: TaskDB) -> AsyncIterator[sa_async.AsyncEngine]:
    """The worker's engine, built the way startup builds it, with a 1s timeout."""
    schema = (await _search_path(pg_task_db)).split(",")[0].strip('" ')
    settings = config_module.Settings(worker_idle_tx_timeout_seconds=1)
    engine = database_module.create_async_engine(
        settings,
        server_settings={
            **worker_module.worker_server_settings(settings),
            "search_path": schema,
        },
    )
    try:
        yield engine
    finally:
        await engine.dispose()


async def _search_path(db: TaskDB) -> str:
    async with db.factory() as session:
        return str((await session.execute(sa.text("SHOW search_path"))).scalar_one())


async def test_worker_connection_settings(worker_engine: sa_async.AsyncEngine) -> None:
    async with worker_engine.connect() as conn:
        timeout = (
            await conn.execute(sa.text("SHOW idle_in_transaction_session_timeout"))
        ).scalar_one()
        name = (await conn.execute(sa.text("SHOW application_name"))).scalar_one()
    assert timeout == "1s"
    assert name.startswith("resonance-worker:")


async def test_idle_session_is_killed_and_pool_recovers(
    worker_engine: sa_async.AsyncEngine,
) -> None:
    factory = database_module.create_session_factory(worker_engine)
    async with factory() as session:
        await session.execute(sa.text("SELECT 1"))
        await asyncio.sleep(2)
        with pytest.raises(sa.exc.DBAPIError):
            await session.execute(sa.text("SELECT 1"))

    async with factory() as session:
        assert (await session.execute(sa.text("SELECT 1"))).scalar_one() == 1


async def test_killed_sync_range_fails_cleanly(
    pg_task_db: TaskDB, worker_engine: sa_async.AsyncEngine
) -> None:
    async with worker_engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)
    db = TaskDB(database_module.create_session_factory(worker_engine))

    user = user_models.User(id=uuid.uuid4(), display_name="t")
    connection = user_models.ServiceConnection(
        id=uuid.uuid4(), user_id=user.id, service_type=types_module.ServiceType.TEST
    )
    async with db.factory() as session:
        session.add_all([user, connection])
        await session.commit()
    task = await db.add(
        task_type=types_module.TaskType.TIME_RANGE,
        user_id=user.id,
        service_connection_id=connection.id,
        params={},
    )

    async def execute(session: Any, task: Any, *_: Any) -> dict[str, object]:
        task.progress_current = 7
        await session.flush()  # holds the row lock, like a mid-page write
        await asyncio.sleep(2)  # an external call past the timeout
        return {"items_created": 1}

    strategy = MagicMock(spec=sync_base.SyncStrategy)
    strategy.execute = execute
    registry = MagicMock()
    registry.get_base_connector.return_value = MagicMock()
    ctx: dict[str, Any] = {
        "session_factory": db.factory,
        "redis": AsyncMock(),
        "job_id": "a",
        "connector_registry": registry,
        "strategies": {types_module.ServiceType.TEST: strategy},
    }

    await lifecycle_module.task_job(worker_module.sync_range)(ctx, str(task.id))

    row = await db.get(task.id)
    assert row.status == S.FAILED
    assert row.progress_current == 0  # the killed transaction rolled back
    assert row.lease_expires_at is None
    # The server's FATAL arrives while the client is idle; the next statement
    # sees only a closed connection.
    assert "connection is closed" in (row.error_message or "")
