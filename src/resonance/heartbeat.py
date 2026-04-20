"""Worker heartbeat system for arq job lock renewal and worker registry."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import os
import socket
from typing import Any

import structlog

logger = structlog.get_logger()

_LOCK_KEY_PREFIX = b"arq:in-progress:"
_WORKER_KEY_PREFIX = "arq:worker:"

_worker_id: str | None = None


def get_worker_id() -> str:
    """Return a stable worker identity string: ``worker:<hostname>:<pid>``.

    The value is cached after the first call.
    """
    global _worker_id
    if _worker_id is None:
        _worker_id = f"worker:{socket.gethostname()}:{os.getpid()}"
    return _worker_id


def with_heartbeat(
    fn: Any = None,
    *,
    interval: float = 30.0,
    ttl: float = 60.0,
) -> Any:
    """Decorator that refreshes arq job locks and worker registry while a task runs.

    Supports both bare ``@with_heartbeat`` and parameterized
    ``@with_heartbeat(interval=15, ttl=30)`` usage.

    On entry a background :class:`asyncio.Task` is spawned that every *interval*
    seconds refreshes:

    - ``arq:in-progress:<job_id>`` — the per-job lock
    - ``arq:worker:<worker_id>`` — the worker registry entry

    The heartbeat task is cancelled in a ``finally`` block so it stops even when
    the wrapped function raises.
    """

    def decorator(func: Any) -> Any:
        @functools.wraps(func)
        async def wrapper(ctx: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
            redis: Any = ctx["redis"]
            job_id: str = ctx.get("job_id", "")
            worker_id = get_worker_id()
            worker_id_bytes = worker_id.encode()
            ttl_ms = int(ttl * 1000)
            lock_key = _LOCK_KEY_PREFIX + job_id.encode()
            worker_key = f"{_WORKER_KEY_PREFIX}{worker_id}"

            async def _heartbeat_loop() -> None:
                # Immediately overwrite arq's b'1' lock with our worker ID
                # and short TTL, then continue refreshing on the interval.
                while True:
                    try:
                        await redis.psetex(lock_key, ttl_ms, worker_id_bytes)
                        await redis.psetex(worker_key, ttl_ms, b"1")
                    except Exception:
                        logger.warning(
                            "heartbeat_refresh_failed",
                            job_id=job_id,
                            worker_id=worker_id,
                        )
                    await asyncio.sleep(interval)

            heartbeat_task = asyncio.create_task(_heartbeat_loop())
            try:
                return await func(ctx, *args, **kwargs)
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

        return wrapper

    if fn is not None:
        return decorator(fn)
    return decorator


async def register_worker(redis: Any, *, ttl: float = 60.0) -> None:
    """Write a worker registry key with the given TTL (seconds)."""
    worker_id = get_worker_id()
    worker_key = f"{_WORKER_KEY_PREFIX}{worker_id}"
    ttl_ms = int(ttl * 1000)
    await redis.psetex(worker_key, ttl_ms, b"1")
    logger.info("worker_registered", worker_id=worker_id, ttl_ms=ttl_ms)


async def unregister_worker(redis: Any) -> None:
    """Delete the worker registry key."""
    worker_id = get_worker_id()
    worker_key = f"{_WORKER_KEY_PREFIX}{worker_id}"
    await redis.delete(worker_key)
    logger.info("worker_unregistered", worker_id=worker_id)


def start_idle_heartbeat(
    redis: Any,
    *,
    interval: float = 30.0,
    ttl: float = 60.0,
) -> asyncio.Task[None]:
    """Start a background task that refreshes the worker registry key between jobs."""
    worker_id = get_worker_id()
    worker_key = f"{_WORKER_KEY_PREFIX}{worker_id}"
    ttl_ms = int(ttl * 1000)

    async def _idle_loop() -> None:
        while True:
            await asyncio.sleep(interval)
            try:
                await redis.psetex(worker_key, ttl_ms, b"1")
            except Exception:
                logger.warning(
                    "idle_heartbeat_refresh_failed",
                    worker_id=worker_id,
                )

    return asyncio.create_task(_idle_loop())


async def cleanup_stale_locks(redis: Any) -> int:
    """Scan arq in-progress locks and delete those held by dead workers.

    Locks whose value is a worker ID (``worker:…``) are checked against the
    worker registry — if the corresponding ``arq:worker:*`` key is missing the
    lock is deleted.  Legacy locks (value ``b'1'``, from before the heartbeat
    system) are always deleted.

    Returns:
        Number of locks deleted.
    """
    keys: list[bytes] = await redis.keys(_LOCK_KEY_PREFIX + b"*")
    deleted = 0

    for key in keys:
        value: bytes | None = await redis.get(key)
        if value is None:
            # Key expired between scan and read.
            continue

        value_str = value.decode()

        if not value_str.startswith("worker:"):
            # Legacy lock (pre-heartbeat, typically b"1") — treat as stale.
            await redis.delete(key)
            deleted += 1
            logger.info("stale_lock_deleted", key=key.decode(), reason="legacy_lock")
            continue

        # Check whether the owning worker is still alive.
        worker_key = f"{_WORKER_KEY_PREFIX}{value_str}"
        alive: bool = await redis.exists(worker_key)
        if not alive:
            await redis.delete(key)
            deleted += 1
            logger.info(
                "stale_lock_deleted",
                key=key.decode(),
                worker_id=value_str,
                reason="dead_worker",
            )

    logger.info("stale_lock_cleanup_complete", deleted=deleted, scanned=len(keys))
    return deleted
