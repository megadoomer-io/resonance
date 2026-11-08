"""Fixtures for tests that need real backing services (see the integration marker)."""

from __future__ import annotations

import contextlib
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


class AppRole:
    """A non-superuser login role on ``pg_task_db``'s schema, as the app runs
    in production."""

    def __init__(self, name: str, url: str, schema: str) -> None:
        self.name = name
        self._url = url
        self._schema = schema
        self._engines: list[sa_async.AsyncEngine] = []
        self.factory = database_module.create_session_factory(self.engine())

    def engine(self, application_name: str | None = None) -> sa_async.AsyncEngine:
        server_settings = {"search_path": self._schema}
        if application_name is not None:
            server_settings["application_name"] = application_name
        engine = sa_async.create_async_engine(
            self._url, connect_args={"server_settings": server_settings}
        )
        self._engines.append(engine)
        return engine

    async def dispose(self) -> None:
        for engine in self._engines:
            await engine.dispose()


@contextlib.asynccontextmanager
async def _app_role(db: TaskDB) -> AsyncIterator[AppRole]:
    settings = config_module.Settings()
    async with db.factory() as session:
        schema = (
            await session.execute(sa.text("SELECT current_schema()"))
        ).scalar_one()
    name = f"app_{uuid.uuid4().hex[:12]}"
    password = uuid.uuid4().hex
    admin = sa_async.create_async_engine(settings.database_url)
    async with admin.begin() as conn:
        await conn.execute(
            sa.text(f"CREATE ROLE {name} LOGIN NOSUPERUSER PASSWORD '{password}'")
        )
        await conn.execute(sa.text(f'GRANT USAGE ON SCHEMA "{schema}" TO {name}'))
        await conn.execute(
            sa.text(
                "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
                f'"{schema}" TO {name}'
            )
        )
    url = sa.make_url(settings.database_url).set(username=name, password=password)
    role = AppRole(name, url.render_as_string(hide_password=False), schema)
    try:
        yield role
    finally:
        await role.dispose()
        async with admin.begin() as conn:
            await conn.execute(sa.text(f"DROP OWNED BY {name}"))
            await conn.execute(sa.text(f"DROP ROLE {name}"))
        await admin.dispose()


@pytest.fixture
async def pg_app_role(pg_task_db: TaskDB) -> AsyncIterator[AppRole]:
    """A non-superuser role with DML on the test schema."""
    async with _app_role(pg_task_db) as role:
        yield role


@pytest.fixture
async def pg_other_role(pg_task_db: TaskDB) -> AsyncIterator[AppRole]:
    """A second non-superuser role, distinct from ``pg_app_role``."""
    async with _app_role(pg_task_db) as role:
        yield role
