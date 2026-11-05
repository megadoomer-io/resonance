"""Fixtures for tests that need real backing services (see the integration marker)."""

from __future__ import annotations

import os
import uuid
from typing import TYPE_CHECKING

import arq
import arq.connections as arq_connections
import pytest
import redis.exceptions

import resonance.config as config_module

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
