"""GET /api/v1/admin/tasks/health: reaper liveness and queue state."""

from __future__ import annotations

import datetime
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

import httpx

import resonance.api.v1.admin as admin_module
import resonance.sync.reaper as reaper_module
import resonance.types as types_module
from tests.test_api_admin import FakeAsyncSession, _make_bearer_app

if TYPE_CHECKING:
    from tests.conftest import TaskDB

_NOW = datetime.datetime(2026, 11, 5, 12, 0, tzinfo=datetime.UTC)


class _FakeRedis:
    def __init__(self, tick: dict[str, Any] | None, decisions: list[Any]) -> None:
        self._tick = json.dumps(tick) if tick else None
        self._decisions = [json.dumps(d) for d in decisions]

    def pipeline(self, transaction: bool = True) -> _FakeRedis:
        self._ops: list[Any] = []
        return self

    async def __aenter__(self) -> _FakeRedis:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def get(self, _key: str) -> None:
        self._ops.append(self._tick)

    def lrange(self, _key: str, _start: int, _end: int) -> None:
        self._ops.append(self._decisions)

    async def execute(self) -> list[Any]:
        return self._ops


def _ago(**delta: float) -> datetime.datetime:
    return _NOW - datetime.timedelta(**delta)


class TestGetTasksHealth:
    async def test_full_report(self, task_db: TaskDB) -> None:
        await task_db.add(
            status=types_module.SyncStatus.PENDING, created_at=_ago(minutes=30)
        )
        oldest = await task_db.add(
            status=types_module.SyncStatus.DEFERRED,
            task_type=types_module.TaskType.TRACK_DISCOVERY,
            created_at=_ago(hours=2),
        )
        running = await task_db.add(
            status=types_module.SyncStatus.RUNNING,
            task_type=types_module.TaskType.PLAYLIST_EXPORT,
            started_at=_ago(minutes=5),
            progress_current=40,
            progress_total=100,
        )
        await task_db.add(status=types_module.SyncStatus.COMPLETED)
        decision = {"task_id": "t", "action": "redelivered"}
        redis = _FakeRedis(
            {
                "at": _ago(seconds=30).isoformat(),
                "candidates": 2,
                "redelivered": 1,
                "lease_expired": 3,
                "lease_recovered": 0,
            },
            [decision],
        )

        async with task_db.factory() as session:
            health = await admin_module.get_tasks_health(
                session,
                redis,  # type: ignore[arg-type]
                now=_NOW,
            )

        assert health["reaper"] == {
            "last_tick_at": _ago(seconds=30).isoformat(),
            "age_seconds": 30,
            "stale": False,
            "stale_after_seconds": 120,
            "last_candidates": 2,
            "last_redelivered": 1,
            "last_lease_expired": 3,
            "last_lease_recovered": 0,
        }
        assert health["oldest_pending"] == {
            "task_id": str(oldest.id),
            "task_type": "track_discovery",
            "status": "deferred",
            "created_at": _ago(hours=2).isoformat(),
            "age_seconds": 7200,
        }
        assert health["counts"] == {
            "pending": 1,
            "running": 1,
            "completed": 1,
            "failed": 0,
            "deferred": 1,
            "cancelled": 0,
        }
        assert health["running"] == [
            {
                "task_id": str(running.id),
                "task_type": "playlist_export",
                "started_at": _ago(minutes=5).isoformat(),
                "age_seconds": 300,
                "progress_current": 40,
                "progress_total": 100,
            }
        ]
        assert health["recent_decisions"] == [decision]

    async def test_empty_and_never_ticked(self, task_db: TaskDB) -> None:
        async with task_db.factory() as session:
            health = await admin_module.get_tasks_health(
                session,
                _FakeRedis(None, []),  # type: ignore[arg-type]
                now=_NOW,
            )

        reaper = health["reaper"]
        assert isinstance(reaper, dict)
        assert reaper["last_tick_at"] is None
        assert reaper["stale"] is True
        assert health["oldest_pending"] is None
        assert health["running"] == []
        assert health["recent_decisions"] == []
        assert health["counts"] == {s.value: 0 for s in types_module.SyncStatus}

    async def test_stale_past_twice_the_interval(self, task_db: TaskDB) -> None:
        stale_after = 2 * reaper_module.INTERVAL
        tick = {"at": (_NOW - stale_after - datetime.timedelta(seconds=1)).isoformat()}
        async with task_db.factory() as session:
            health = await admin_module.get_tasks_health(
                session,
                _FakeRedis(tick, []),  # type: ignore[arg-type]
                now=_NOW,
            )

        reaper = health["reaper"]
        assert isinstance(reaper, dict)
        assert reaper["stale"] is True


class TestTasksHealthRoute:
    async def test_admin_gets_report_not_task_lookup(self) -> None:
        application = _make_bearer_app(db_session=FakeAsyncSession())
        application.state.arq_redis = object()
        report = {"reaper": {}, "counts": {}}
        with patch.object(
            admin_module, "get_tasks_health", AsyncMock(return_value=report)
        ) as health:
            transport = httpx.ASGITransport(app=application)
            async with httpx.AsyncClient(
                transport=transport, base_url="http://test"
            ) as c:
                response = await c.get(
                    "/api/v1/admin/tasks/health",
                    headers={"Authorization": "Bearer test-admin-token"},
                )

        assert response.status_code == 200
        assert response.json() == report
        assert health.await_args is not None
        assert health.await_args.args[1] is application.state.arq_redis

    async def test_wrong_token_is_forbidden(self) -> None:
        application = _make_bearer_app()
        transport = httpx.ASGITransport(app=application)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            response = await c.get(
                "/api/v1/admin/tasks/health",
                headers={"Authorization": "Bearer wrong"},
            )
        assert response.status_code == 403
