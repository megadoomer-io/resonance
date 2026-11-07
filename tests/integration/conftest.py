"""Fixtures for tests that need real backing services (see the integration marker)."""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import arq
import arq.connections as arq_connections
import pytest
import redis.exceptions
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.config as config_module
import resonance.database as database_module
import resonance.models as models
from tests.conftest import TaskDB

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def _unavailable(reason: str) -> None:
    # CI provides the services, so a missing one there is a failure, not a skip.
    if os.environ.get("CI"):
        pytest.fail(reason)
    pytest.skip(reason)


@pytest.fixture
async def arq_redis() -> AsyncIterator[arq.ArqRedis]:
    """A real arq pool on an isolated, per-test default queue."""
    settings = config_module.Settings()
    redis_settings = arq_connections.RedisSettings(
        host=settings.redis_host,
        port=settings.redis_port,
        password=settings.redis_password or None,
        conn_retries=0,
    )
    queue_name = f"arq:queue:test:{uuid.uuid4().hex}"
    try:
        pool = await arq.create_pool(redis_settings, default_queue_name=queue_name)
    except (OSError, redis.exceptions.ConnectionError) as exc:
        _unavailable(f"Redis unavailable at {settings.redis_host}: {exc}")
        raise

    try:
        yield pool
    finally:
        job_ids = await pool.zrange(queue_name, 0, -1)
        keys = [f"arq:job:{jid.decode()}" for jid in job_ids]
        await pool.delete(queue_name, *keys)
        await pool.aclose()


@pytest.fixture
async def pg_task_db() -> AsyncIterator[TaskDB]:
    """A TaskDB on real Postgres, in a throwaway schema dropped afterwards."""
    settings = config_module.Settings()
    schema = f"test_{uuid.uuid4().hex}"
    admin = sa_async.create_async_engine(settings.database_url)
    try:
        async with admin.begin() as conn:
            await conn.execute(sa.text(f'CREATE SCHEMA "{schema}"'))
    except OSError as exc:
        await admin.dispose()
        _unavailable(f"PostgreSQL unavailable at {settings.pghost}: {exc}")
        raise
    engine = sa_async.create_async_engine(
        settings.database_url,
        connect_args={"server_settings": {"search_path": schema}},
    )
    try:
        async with engine.begin() as conn:
            await conn.run_sync(models.Base.metadata.create_all)
        yield TaskDB(database_module.create_session_factory(engine))
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(sa.text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()
