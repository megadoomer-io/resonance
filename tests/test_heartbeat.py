"""Tests for the heartbeat module."""

from __future__ import annotations

import asyncio
import contextlib
from unittest.mock import AsyncMock

import pytest

import resonance.heartbeat as heartbeat_module


class TestWorkerIdentity:
    """Tests for get_worker_id()."""

    def test_returns_string_with_hostname_and_pid(self) -> None:
        worker_id = heartbeat_module.get_worker_id()
        parts = worker_id.split(":")
        assert parts[0] == "worker"
        assert len(parts) == 3
        assert parts[2].isdigit()

    def test_is_stable_across_calls(self) -> None:
        first = heartbeat_module.get_worker_id()
        second = heartbeat_module.get_worker_id()
        assert first == second


class TestWithHeartbeat:
    """Tests for the @with_heartbeat decorator."""

    @pytest.mark.asyncio
    async def test_decorated_function_runs_normally(self) -> None:
        @heartbeat_module.with_heartbeat
        async def my_task(ctx: dict[str, object]) -> str:
            return "ok"

        redis = AsyncMock()
        ctx: dict[str, object] = {"redis": redis, "job_id": "j1"}
        result = await my_task(ctx)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_refreshes_lock_during_execution(self) -> None:
        @heartbeat_module.with_heartbeat(interval=0.05, ttl=0.12)
        async def slow_task(ctx: dict[str, object]) -> str:
            await asyncio.sleep(0.15)
            return "done"

        redis = AsyncMock()
        ctx: dict[str, object] = {"redis": redis, "job_id": "j2"}
        await slow_task(ctx)

        lock_key = heartbeat_module._LOCK_KEY_PREFIX + b"j2"
        psetex_calls = redis.psetex.call_args_list
        lock_calls = [c for c in psetex_calls if c.args[0] == lock_key]
        assert len(lock_calls) >= 1

    @pytest.mark.asyncio
    async def test_heartbeat_cancelled_on_error(self) -> None:
        @heartbeat_module.with_heartbeat(interval=0.05, ttl=0.12)
        async def failing_task(ctx: dict[str, object]) -> None:
            await asyncio.sleep(0.08)
            msg = "boom"
            raise RuntimeError(msg)

        redis = AsyncMock()
        ctx: dict[str, object] = {"redis": redis, "job_id": "j3"}

        with pytest.raises(RuntimeError, match="boom"):
            await failing_task(ctx)

        # Give event loop a tick to ensure cancelled task is cleaned up.
        await asyncio.sleep(0.02)

        # After the error, no more psetex calls should appear.
        call_count = redis.psetex.call_count
        await asyncio.sleep(0.1)
        assert redis.psetex.call_count == call_count

    @pytest.mark.asyncio
    async def test_stores_worker_id_in_lock_value(self) -> None:
        @heartbeat_module.with_heartbeat(interval=0.05, ttl=0.12)
        async def task(ctx: dict[str, object]) -> None:
            await asyncio.sleep(0.08)

        redis = AsyncMock()
        ctx: dict[str, object] = {"redis": redis, "job_id": "j4"}
        await task(ctx)

        lock_key = heartbeat_module._LOCK_KEY_PREFIX + b"j4"
        psetex_calls = redis.psetex.call_args_list
        lock_calls = [c for c in psetex_calls if c.args[0] == lock_key]
        assert len(lock_calls) >= 1
        lock_value = lock_calls[0].args[2]
        assert lock_value.startswith(b"worker:")
        assert lock_value != b"1"


class TestWorkerRegistry:
    """Tests for register_worker, unregister_worker, and start_idle_heartbeat."""

    @pytest.mark.asyncio
    async def test_register_writes_worker_key(self) -> None:
        redis = AsyncMock()
        await heartbeat_module.register_worker(redis, ttl=60.0)

        worker_id = heartbeat_module.get_worker_id()
        expected_key = f"{heartbeat_module._WORKER_KEY_PREFIX}{worker_id}"
        redis.psetex.assert_called_once_with(expected_key, 60_000, b"1")

    @pytest.mark.asyncio
    async def test_unregister_deletes_worker_key(self) -> None:
        redis = AsyncMock()
        await heartbeat_module.unregister_worker(redis)

        worker_id = heartbeat_module.get_worker_id()
        expected_key = f"{heartbeat_module._WORKER_KEY_PREFIX}{worker_id}"
        redis.delete.assert_called_once_with(expected_key)

    @pytest.mark.asyncio
    async def test_idle_heartbeat_refreshes_worker_key(self) -> None:
        redis = AsyncMock()
        task = heartbeat_module.start_idle_heartbeat(redis, interval=0.05, ttl=0.12)
        await asyncio.sleep(0.12)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        worker_id = heartbeat_module.get_worker_id()
        expected_key = f"{heartbeat_module._WORKER_KEY_PREFIX}{worker_id}"
        worker_calls = [
            c for c in redis.psetex.call_args_list if c.args[0] == expected_key
        ]
        assert len(worker_calls) >= 1
