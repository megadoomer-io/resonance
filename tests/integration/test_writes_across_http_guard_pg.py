"""Self-tests for the ``no_writes_across_http`` guardrail, on Postgres."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import sqlalchemy as sa

import resonance.models.music as music_models

if TYPE_CHECKING:
    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration


async def _ok(request: httpx.Request) -> httpx.Response:
    return httpx.Response(200)


async def _send() -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(_ok)) as client:
        await client.get("https://example.test/")


async def _seed(db: TaskDB) -> uuid.UUID:
    artist = music_models.Artist(id=uuid.uuid4(), name="A")
    async with db.factory() as session:
        session.add(artist)
        await session.commit()
    return artist.id


_HELD = [
    pytest.param(
        lambda aid: (
            sa.select(music_models.Artist)
            .where(music_models.Artist.id == aid)
            .with_for_update()
        ),
        id="select-for-update",
    ),
    pytest.param(
        lambda aid: sa.text("UPDATE artists SET name = 'B' WHERE id = :id").bindparams(
            id=aid
        ),
        id="text-dml",
    ),
    pytest.param(
        lambda aid: (
            sa.update(music_models.Artist)
            .where(music_models.Artist.id == aid)
            .values(name="C")
        ),
        id="core-update",
    ),
]


@pytest.mark.parametrize("stmt", _HELD)
async def test_flags_http_while_lock_or_write_is_held(
    pg_task_db: TaskDB, no_writes_across_http: Any, stmt: Any
) -> None:
    aid = await _seed(pg_task_db)
    async with pg_task_db.factory() as session:
        await session.execute(stmt(aid))
        await _send()
        await session.rollback()

    assert no_writes_across_http.violations == ["GET https://example.test/"]
    no_writes_across_http.violations.clear()  # the fixture would fail the test


async def test_flags_write_through_session_connection(
    pg_task_db: TaskDB, no_writes_across_http: Any
) -> None:
    aid = await _seed(pg_task_db)
    async with pg_task_db.factory() as session:
        conn = await session.connection()
        await conn.execute(
            sa.text("DELETE FROM artists WHERE id = :id").bindparams(id=aid)
        )
        await _send()
        await session.rollback()

    assert no_writes_across_http.violations == ["GET https://example.test/"]
    no_writes_across_http.violations.clear()


async def test_ignores_plain_reads_and_committed_writes(
    pg_task_db: TaskDB, no_writes_across_http: Any
) -> None:
    aid = await _seed(pg_task_db)
    async with pg_task_db.factory() as session:
        await session.execute(
            sa.update(music_models.Artist)
            .where(music_models.Artist.id == aid)
            .values(name="D")
        )
        await session.commit()
        await session.execute(sa.select(music_models.Artist))
        await _send()

    assert no_writes_across_http.violations == []
