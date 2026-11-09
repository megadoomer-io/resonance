"""Transaction-scoped Postgres advisory locks for wholesale-replace writes.

A delete-then-insert replace of a key's child rows races under READ COMMITTED:
two writers both delete, then both insert, and the second hits a unique
constraint or leaves a union of both batches. Taking an advisory lock on the key
first serializes them into clean replaces. The lock ends with the transaction,
so it is never held across an external call when the caller commits before one.
"""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async


async def lock_key(session: Any, *parts: object) -> None:
    """Take ``pg_advisory_xact_lock`` on a key built from ``parts``.

    No-op for anything but a Postgres ``AsyncSession`` (SQLite and fake test
    sessions). Callers locking several keys in one transaction must take them in
    a stable order (e.g. sorted ids) so two writers cannot deadlock.
    """
    if not isinstance(session, sa_async.AsyncSession):
        return
    if session.get_bind().dialect.name != "postgresql":
        return
    key = ":".join(str(p) for p in parts)
    await session.execute(
        sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": key},
    )
