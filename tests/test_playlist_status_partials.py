"""Authorization for the HTMX status partials (generating + export).

A task belonging to another user must look exactly like a missing task, and a
malformed task id must never 500.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import fastapi
import httpx
import pytest

import resonance.config as config_module
import resonance.middleware.session as session_middleware
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.types as types_module
import resonance.ui.playlists as playlists_ui

# --- self-contained fakes (mirrors test_lineup_builder) ---


class FakeResult:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


class TaskStoreSession:
    """Resolves selects by the bound ``id`` and (if present) ``user_id``.

    Rows are matched against the selected entity type. A query without a
    ``user_id`` filter matches any owner, so an unscoped lookup leaks other
    users' rows just as it would against a real database.
    """

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows

    async def execute(self, stmt: Any, *args: Any, **kwargs: Any) -> FakeResult:
        entity = stmt.column_descriptions[0]["entity"]
        params = stmt.compile().params
        row_id = params.get("id_1")
        has_user_filter = "user_id_1" in params
        matches = [
            r
            for r in self._rows
            if isinstance(r, entity)
            and r.id == row_id
            and (not has_user_filter or r.user_id == params["user_id_1"])
        ]
        return FakeResult(matches)

    async def __aenter__(self) -> TaskStoreSession:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


class FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def aclose(self) -> None:
        pass


def _session_cookie(secret_key: str) -> str:
    import itsdangerous

    return itsdangerous.TimestampSigner(secret_key).sign("test-session-id").decode()


def _app(db: TaskStoreSession, user_id: uuid.UUID | None) -> tuple[Any, dict[str, str]]:
    settings = config_module.Settings(
        spotify_client_id="test-id",
        spotify_client_secret="test-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
    )
    redis = FakeRedis()
    cookies: dict[str, str] = {}
    if user_id is not None:
        redis._store["session:test-session-id"] = json.dumps(
            {"user_id": str(user_id), "user_role": "owner"}
        )
        cookies["session_id"] = _session_cookie(settings.session_secret_key)

    app = fastapi.FastAPI(title="test", lifespan=None)
    app.state.settings = settings
    app.state.session_factory = lambda: db
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(playlists_ui.router)
    return app, cookies


def _task(
    owner: uuid.UUID,
    *,
    task_type: types_module.TaskType,
    status: types_module.SyncStatus = types_module.SyncStatus.RUNNING,
    description: str = "Doing the thing",
) -> task_models.Task:
    return task_models.Task(
        id=uuid.uuid4(),
        user_id=owner,
        task_type=task_type,
        status=status,
        params={},
        description=description,
    )


@pytest.fixture
def user_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def other_user_id() -> uuid.UUID:
    return uuid.uuid4()


async def _get(
    db: TaskStoreSession, user_id: uuid.UUID | None, url: str
) -> httpx.Response:
    app, cookies = _app(db, user_id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        cookies=cookies,
        follow_redirects=False,
    ) as client:
        return await client.get(url)


class TestGeneratingStatusPartial:
    async def test_unauthenticated_redirects_to_login(self, user_id: uuid.UUID) -> None:
        task = _task(user_id, task_type=types_module.TaskType.PLAYLIST_GENERATION)
        resp = await _get(
            TaskStoreSession([task]), None, f"/partials/generating-status/{task.id}"
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == "/login"

    async def test_own_task_renders_progress(self, user_id: uuid.UUID) -> None:
        task = _task(
            user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            description="Building My Mix",
        )
        resp = await _get(
            TaskStoreSession([task]), user_id, f"/partials/generating-status/{task.id}"
        )
        assert resp.status_code == 200
        assert "Building My Mix" in resp.text

    async def test_other_users_task_looks_missing(
        self, user_id: uuid.UUID, other_user_id: uuid.UUID
    ) -> None:
        task = _task(
            other_user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            description="Secret Mix",
        )
        db = TaskStoreSession([task])
        theirs = await _get(db, user_id, f"/partials/generating-status/{task.id}")
        missing = await _get(db, user_id, f"/partials/generating-status/{uuid.uuid4()}")
        assert theirs.status_code == missing.status_code == 200
        assert theirs.text == missing.text
        assert "Secret Mix" not in theirs.text

    async def test_malformed_task_id_does_not_500(self, user_id: uuid.UUID) -> None:
        resp = await _get(
            TaskStoreSession([]), user_id, "/partials/generating-status/not-a-uuid"
        )
        assert resp.status_code < 500


class TestExportStatusPartial:
    async def test_unauthenticated_redirects_to_login(self, user_id: uuid.UUID) -> None:
        task = _task(user_id, task_type=types_module.TaskType.PLAYLIST_EXPORT)
        resp = await _get(
            TaskStoreSession([task]),
            None,
            f"/partials/export-status/{uuid.uuid4()}?task_ids={task.id}",
        )
        assert resp.status_code == 307
        assert resp.headers["location"] == "/login"

    async def test_own_task_renders(self, user_id: uuid.UUID) -> None:
        task = _task(
            user_id,
            task_type=types_module.TaskType.PLAYLIST_EXPORT,
            status=types_module.SyncStatus.FAILED,
            description="Export to Spotify (me)",
        )
        task.error_message = "boom"
        resp = await _get(
            TaskStoreSession([task]),
            user_id,
            f"/partials/export-status/{uuid.uuid4()}?task_ids={task.id}",
        )
        assert resp.status_code == 200
        assert "Export to Spotify (me): boom" in resp.text

    async def test_other_users_task_looks_missing(
        self, user_id: uuid.UUID, other_user_id: uuid.UUID
    ) -> None:
        task = _task(
            other_user_id,
            task_type=types_module.TaskType.PLAYLIST_EXPORT,
            status=types_module.SyncStatus.FAILED,
            description="Export to Spotify (them)",
        )
        task.error_message = "their secret"
        db = TaskStoreSession([task])
        playlist_id = uuid.uuid4()
        theirs = await _get(
            db, user_id, f"/partials/export-status/{playlist_id}?task_ids={task.id}"
        )
        bogus = uuid.uuid4()
        missing = await _get(
            db, user_id, f"/partials/export-status/{playlist_id}?task_ids={bogus}"
        )
        assert theirs.status_code == missing.status_code == 200
        assert "their secret" not in theirs.text
        assert "Export to Spotify (them)" not in theirs.text
        assert theirs.text.replace(str(task.id), "X") == missing.text.replace(
            str(bogus), "X"
        )

    async def test_malformed_task_id_is_skipped(self, user_id: uuid.UUID) -> None:
        task = _task(
            user_id,
            task_type=types_module.TaskType.PLAYLIST_EXPORT,
            status=types_module.SyncStatus.FAILED,
            description="Export to Spotify (me)",
        )
        task.error_message = "boom"
        resp = await _get(
            TaskStoreSession([task]),
            user_id,
            f"/partials/export-status/{uuid.uuid4()}?task_ids=not-a-uuid,{task.id}",
        )
        assert resp.status_code == 200
        assert "Export to Spotify (me): boom" in resp.text

    async def test_only_malformed_ids_keeps_polling(self, user_id: uuid.UUID) -> None:
        resp = await _get(
            TaskStoreSession([]),
            user_id,
            f"/partials/export-status/{uuid.uuid4()}?task_ids=garbage",
        )
        assert resp.status_code == 200
        assert 'id="export-status"' in resp.text


def _playlist(owner: uuid.UUID, name: str) -> playlist_models.Playlist:
    return playlist_models.Playlist(id=uuid.uuid4(), user_id=owner, name=name)


class TestGeneratingPage:
    async def test_own_task_renders(self, user_id: uuid.UUID) -> None:
        task = _task(
            user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            description="My Mix",
        )
        resp = await _get(
            TaskStoreSession([task]), user_id, f"/playlists/generating/{task.id}"
        )
        assert resp.status_code == 200
        assert "My Mix" in resp.text

    async def test_other_users_task_is_404(
        self, user_id: uuid.UUID, other_user_id: uuid.UUID
    ) -> None:
        task = _task(
            other_user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            description="Secret Mix",
        )
        db = TaskStoreSession([task])
        theirs = await _get(db, user_id, f"/playlists/generating/{task.id}")
        missing = await _get(db, user_id, f"/playlists/generating/{uuid.uuid4()}")
        assert theirs.status_code == missing.status_code == 404
        assert theirs.text == missing.text
        assert "Secret Mix" not in theirs.text


class TestExportStatusPage:
    async def test_own_playlist_renders_name(self, user_id: uuid.UUID) -> None:
        playlist = _playlist(user_id, "My Playlist")
        resp = await _get(
            TaskStoreSession([playlist]),
            user_id,
            f"/playlists/exporting/{playlist.id}?task_ids=",
        )
        assert resp.status_code == 200
        assert "My Playlist" in resp.text

    async def test_other_users_playlist_is_404(
        self, user_id: uuid.UUID, other_user_id: uuid.UUID
    ) -> None:
        playlist = _playlist(other_user_id, "Secret Playlist")
        db = TaskStoreSession([playlist])
        theirs = await _get(db, user_id, f"/playlists/exporting/{playlist.id}")
        missing = await _get(db, user_id, f"/playlists/exporting/{uuid.uuid4()}")
        assert theirs.status_code == missing.status_code == 404
        assert theirs.text == missing.text
        assert "Secret Playlist" not in theirs.text
