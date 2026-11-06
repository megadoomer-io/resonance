"""Export progress and retry through the JSON API (#173)."""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import fastapi
import httpx
import pytest

import resonance.api.v1.playlists as playlists_api
import resonance.dependencies as deps_module
import resonance.models.task as task_models
import resonance.services.playlist_export as playlist_export_module
import resonance.types as types_module


class _Result:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None

    def scalars(self) -> _Result:
        return self

    def all(self) -> list[Any]:
        return list(self._items)


class _DB:
    """Matches selects on bound ``id``/``user_id``; unfiltered selects get all."""

    def __init__(self, rows: list[task_models.Task]) -> None:
        self.rows = rows

    async def execute(self, stmt: Any, *args: Any, **kwargs: Any) -> _Result:
        params = stmt.compile().params
        rows = [
            r
            for r in self.rows
            if ("id_1" not in params or r.id == params["id_1"])
            and ("user_id_1" not in params or r.user_id == params["user_id_1"])
        ]
        return _Result(rows)


def _task(
    user_id: uuid.UUID,
    playlist_id: uuid.UUID,
    *,
    status: types_module.SyncStatus = types_module.SyncStatus.RUNNING,
    result: dict[str, object] | None = None,
    error: str | None = None,
) -> task_models.Task:
    task = task_models.Task(
        id=uuid.uuid4(),
        user_id=user_id,
        task_type=types_module.TaskType.PLAYLIST_EXPORT,
        status=status,
        params={"playlist_id": str(playlist_id), "connection_id": str(uuid.uuid4())},
        description="Export to Spotify (me)",
    )
    task.result = result or {}
    task.error_message = error
    return task


async def _call(db: _DB, user_id: uuid.UUID, method: str, url: str) -> httpx.Response:
    app = fastapi.FastAPI()
    app.state.arq_redis = None
    app.include_router(playlists_api.router, prefix="/api/v1")

    async def _db() -> Any:
        yield db

    app.dependency_overrides[deps_module.get_db] = _db
    app.dependency_overrides[deps_module.get_current_user_id] = lambda: user_id
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.request(method, url)


_NOW_ISO = datetime.datetime.now(datetime.UTC).isoformat()


class TestExportStatusEndpoint:
    async def test_running_export_reports_progress(self) -> None:
        user_id, playlist_id = uuid.uuid4(), uuid.uuid4()
        task = _task(
            user_id,
            playlist_id,
            result={
                "phase": "matching_search",
                "matched": 5,
                "skipped": 1,
                "progress_updated_at": _NOW_ISO,
            },
        )
        task.progress_current = 6
        task.progress_total = 10
        resp = await _call(
            _DB([task]),
            user_id,
            "GET",
            f"/api/v1/playlists/{playlist_id}/exports/{task.id}",
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["task_id"] == str(task.id)
        assert body["status"] == "running"
        assert body["phase"] == "matching_search"
        assert body["phase_label"] == "Matching tracks (Spotify search)"
        assert body["progress_current"] == 6
        assert body["progress_total"] == 10
        assert body["matched"] == 5
        assert body["skipped"] == 1
        assert body["stalled"] is False
        assert body["retryable"] is False
        assert body["last_progress_at"] == _NOW_ISO

    async def test_failed_export_reports_error_and_retryable(self) -> None:
        user_id, playlist_id = uuid.uuid4(), uuid.uuid4()
        task = _task(
            user_id, playlist_id, status=types_module.SyncStatus.FAILED, error="nope"
        )
        resp = await _call(
            _DB([task]),
            user_id,
            "GET",
            f"/api/v1/playlists/{playlist_id}/exports/{task.id}",
        )
        body = resp.json()
        assert body["error"] == "nope"
        assert body["retryable"] is True

    async def test_other_users_or_playlists_task_is_404(self) -> None:
        user_id, playlist_id = uuid.uuid4(), uuid.uuid4()
        theirs = _task(uuid.uuid4(), playlist_id)
        resp = await _call(
            _DB([theirs]),
            user_id,
            "GET",
            f"/api/v1/playlists/{playlist_id}/exports/{theirs.id}",
        )
        assert resp.status_code == 404
        mine = _task(user_id, uuid.uuid4())
        resp = await _call(
            _DB([mine]),
            user_id,
            "GET",
            f"/api/v1/playlists/{playlist_id}/exports/{mine.id}",
        )
        assert resp.status_code == 404

    async def test_list_returns_in_flight_exports(self) -> None:
        user_id, playlist_id = uuid.uuid4(), uuid.uuid4()
        running = _task(user_id, playlist_id)
        resp = await _call(
            _DB([running]), user_id, "GET", f"/api/v1/playlists/{playlist_id}/exports"
        )
        assert resp.status_code == 200
        body = resp.json()
        assert [e["task_id"] for e in body] == [str(running.id)]
        assert "phase" in body[0]


class _ExportDB:
    """Records statements, inserts and commits for the export submit path."""

    def __init__(self, playlist: Any, connections: list[Any]) -> None:
        self.playlist = playlist
        self.connections = connections
        self.log: list[str] = []
        self.added: list[Any] = []

    async def execute(self, stmt: Any, *args: Any, **kwargs: Any) -> _Result:
        sql = str(stmt.compile())
        self.log.append("lock" if "FOR UPDATE" in sql else "select")
        if "FROM service_connections" in sql:
            return _Result(self.connections)
        if "FROM sync_tasks" in sql:
            return _Result([])
        return _Result([self.playlist])

    def add(self, obj: Any) -> None:
        self.log.append("add")
        self.added.append(obj)

    async def commit(self) -> None:
        self.log.append("commit")


class TestExportSubmitLocking:
    async def test_locks_playlist_then_inserts_all_then_commits_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import resonance.models.playlist as playlist_models
        import resonance.models.user as user_models
        import resonance.sync.lifecycle as lifecycle_module

        delivered: list[Any] = []

        async def fake_deliver(arq_redis: Any, delivery: Any) -> bool:
            delivered.append(delivery)
            db.log.append("deliver")
            return True

        monkeypatch.setattr(lifecycle_module, "deliver", fake_deliver)
        user_id = uuid.uuid4()
        playlist = playlist_models.Playlist(id=uuid.uuid4(), user_id=user_id, name="P")
        conns = [
            user_models.ServiceConnection(
                id=uuid.uuid4(),
                user_id=user_id,
                service_type=types_module.ServiceType.SPOTIFY,
            )
            for _ in range(2)
        ]
        db = _ExportDB(playlist, conns)
        resp = await _call(
            db,  # type: ignore[arg-type]
            user_id,
            "POST",
            f"/api/v1/playlists/{playlist.id}/export",
        )
        assert resp.status_code == 202
        lock_at = db.log.index("lock")
        # Lock precedes the in-flight check and every insert; one commit holds
        # it until all tasks exist, and delivery happens after the commit.
        assert db.log.index("add") > lock_at
        assert db.log.count("commit") == 1
        commit_at = db.log.index("commit")
        assert all(i < commit_at for i, e in enumerate(db.log) if e == "add")
        assert all(i > commit_at for i, e in enumerate(db.log) if e == "deliver")
        assert len(delivered) == 2
        assert all(t.enqueued_at is not None for t in db.added)

    async def test_job_arg_and_response_carry_the_task_id(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import resonance.models.playlist as playlist_models
        import resonance.models.user as user_models
        import resonance.sync.lifecycle as lifecycle_module

        delivered: list[Any] = []

        async def fake_deliver(arq_redis: Any, delivery: Any) -> bool:
            delivered.append(delivery)
            return True

        monkeypatch.setattr(lifecycle_module, "deliver", fake_deliver)
        user_id = uuid.uuid4()
        playlist = playlist_models.Playlist(id=uuid.uuid4(), user_id=user_id, name="P")
        conn = user_models.ServiceConnection(
            id=uuid.uuid4(),
            user_id=user_id,
            service_type=types_module.ServiceType.SPOTIFY,
        )
        db = _ExportDB(playlist, [conn])
        resp = await _call(
            db,  # type: ignore[arg-type]
            user_id,
            "POST",
            f"/api/v1/playlists/{playlist.id}/export",
        )
        task = db.added[0]
        assert delivered[0].args == (str(task.id),)
        assert resp.json()["tasks"][0]["task_id"] == str(task.id)


class TestExportRetryEndpoint:
    async def test_started(self, monkeypatch: pytest.MonkeyPatch) -> None:
        user_id, playlist_id = uuid.uuid4(), uuid.uuid4()
        fresh = _task(user_id, playlist_id, status=types_module.SyncStatus.PENDING)

        async def fake_retry(*args: Any, **kwargs: Any) -> Any:
            return playlist_export_module.RetryOutcome(
                playlist_export_module.RetryStatus.STARTED, fresh
            )

        monkeypatch.setattr(playlist_export_module, "retry_export", fake_retry)
        resp = await _call(
            _DB([]),
            user_id,
            "POST",
            f"/api/v1/playlists/{playlist_id}/exports/{uuid.uuid4()}/retry",
        )
        assert resp.status_code == 202
        assert resp.json() == {"status": "started", "task_id": str(fresh.id)}

    @pytest.mark.parametrize(
        ("outcome", "code"),
        [
            (playlist_export_module.RetryStatus.NOT_FOUND, 404),
            (playlist_export_module.RetryStatus.NOT_RETRYABLE, 409),
            (playlist_export_module.RetryStatus.ALREADY_RUNNING, 409),
        ],
    )
    async def test_refusals(
        self,
        monkeypatch: pytest.MonkeyPatch,
        outcome: playlist_export_module.RetryStatus,
        code: int,
    ) -> None:
        async def fake_retry(*args: Any, **kwargs: Any) -> Any:
            return playlist_export_module.RetryOutcome(outcome)

        monkeypatch.setattr(playlist_export_module, "retry_export", fake_retry)
        resp = await _call(
            _DB([]),
            uuid.uuid4(),
            "POST",
            f"/api/v1/playlists/{uuid.uuid4()}/exports/{uuid.uuid4()}/retry",
        )
        assert resp.status_code == code
