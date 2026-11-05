"""Authorization for the HTMX status partials (generating + export).

A task belonging to another user must look exactly like a missing task, and a
malformed task id must never 500.
"""

from __future__ import annotations

import json
import uuid
from typing import Any
from unittest import mock

import fastapi
import httpx
import pytest

import resonance.config as config_module
import resonance.middleware.session as session_middleware
import resonance.models.playlist as playlist_models
import resonance.models.task as task_models
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module
import resonance.ui.common as common_ui
import resonance.ui.playlists as playlists_ui

# --- self-contained fakes (mirrors test_lineup_builder) ---


class FakeResult:
    def __init__(self, items: list[Any]) -> None:
        self._items = items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return list(self._items)


class TaskStoreSession:
    """Resolves selects by the bound ``id`` and (if present) ``user_id``.

    Rows are matched against the selected entity type. A query without a
    ``user_id`` filter matches any owner, so an unscoped lookup leaks other
    users' rows just as it would against a real database.
    """

    def __init__(self, rows: list[Any]) -> None:
        self._rows = rows
        self.commits = 0

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

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        pass

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
    params: dict[str, Any] | None = None,
) -> task_models.Task:
    return task_models.Task(
        id=uuid.uuid4(),
        user_id=owner,
        task_type=task_type,
        status=status,
        params=params or {},
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


def _export_task(
    owner: uuid.UUID,
    playlist_id: uuid.UUID,
    *,
    status: types_module.SyncStatus = types_module.SyncStatus.RUNNING,
    description: str = "Export to Spotify (me)",
    error: str | None = None,
    result: dict[str, Any] | None = None,
) -> task_models.Task:
    task = _task(
        owner,
        task_type=types_module.TaskType.PLAYLIST_EXPORT,
        status=status,
        description=description,
        params={"playlist_id": str(playlist_id), "connection_id": str(uuid.uuid4())},
    )
    task.error_message = error
    task.result = result
    return task


def _poll_url(playlist_id: uuid.UUID, *tasks: task_models.Task | str) -> str:
    ids = ",".join(str(t.id) if isinstance(t, task_models.Task) else t for t in tasks)
    return f"/partials/export-status/{playlist_id}?task_ids={ids}"


def _keeps_polling(html: str) -> bool:
    return 'hx-trigger="every 3s"' in html


class TestExportStatusPartial:
    async def test_unauthenticated_redirects_to_login(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(user_id, playlist_id)
        resp = await _get(TaskStoreSession([task]), None, _poll_url(playlist_id, task))
        assert resp.status_code == 307
        assert resp.headers["location"] == "/login"

    async def test_failed_row_renders_error_in_mark(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.FAILED, error="boom"
        )
        resp = await _get(
            TaskStoreSession([task]), user_id, _poll_url(playlist_id, task)
        )
        assert resp.status_code == 200
        assert "<mark>Failed: boom</mark>" in resp.text
        assert "Export finished with errors" in resp.text
        assert not _keeps_polling(resp.text)

    async def test_other_users_task_looks_missing(
        self, user_id: uuid.UUID, other_user_id: uuid.UUID
    ) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            other_user_id,
            playlist_id,
            status=types_module.SyncStatus.FAILED,
            description="Export to Spotify (them)",
            error="their secret",
        )
        db = TaskStoreSession([task])
        theirs = await _get(db, user_id, _poll_url(playlist_id, task))
        bogus = str(uuid.uuid4())
        missing = await _get(db, user_id, _poll_url(playlist_id, bogus))
        assert theirs.status_code == missing.status_code == 200
        assert "their secret" not in theirs.text
        assert "Export to Spotify (them)" not in theirs.text
        assert theirs.text.replace(str(task.id), "X") == missing.text.replace(
            bogus, "X"
        )

    async def test_task_for_another_playlist_looks_missing(
        self, user_id: uuid.UUID
    ) -> None:
        task = _export_task(
            user_id, uuid.uuid4(), status=types_module.SyncStatus.FAILED, error="boom"
        )
        resp = await _get(
            TaskStoreSession([task]), user_id, _poll_url(uuid.uuid4(), task)
        )
        assert "boom" not in resp.text
        assert "Export not found" in resp.text

    async def test_malformed_task_id_is_skipped(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.FAILED, error="boom"
        )
        resp = await _get(
            TaskStoreSession([task]),
            user_id,
            _poll_url(playlist_id, "not-a-uuid", task),
        )
        assert resp.status_code == 200
        assert "Failed: boom" in resp.text

    async def test_no_tasks_is_not_found_and_stops_polling(
        self, user_id: uuid.UUID
    ) -> None:
        resp = await _get(
            TaskStoreSession([]), user_id, _poll_url(uuid.uuid4(), "garbage")
        )
        assert resp.status_code == 200
        assert "Export not found" in resp.text
        assert not _keeps_polling(resp.text)

    async def test_in_progress_keeps_polling_and_updates_oob(
        self, user_id: uuid.UUID
    ) -> None:
        playlist_id = uuid.uuid4()
        pending = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.PENDING
        )
        running = _export_task(user_id, playlist_id)
        done = _export_task(
            user_id,
            playlist_id,
            status=types_module.SyncStatus.COMPLETED,
            result={"exported": 3, "skipped": 0, "spotify_playlist_id": "sp1"},
        )
        resp = await _get(
            TaskStoreSession([pending, running, done]),
            user_id,
            _poll_url(playlist_id, pending, running, done),
        )
        assert _keeps_polling(resp.text)
        assert f'id="export-status-{running.id}" hx-swap-oob="innerHTML"' in resp.text
        assert "Exporting…" in resp.text
        # Rows that can no longer be stopped lose their actions; a still-stoppable
        # row's Stop is left alone and never re-rendered by the poll.
        assert f'id="export-actions-{done.id}" hx-swap-oob' in resp.text
        assert f'id="export-actions-{running.id}" hx-swap-oob' in resp.text
        assert f'id="export-actions-{pending.id}"' not in resp.text
        assert "Stop" not in resp.text

    async def test_completed_row_copy(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id,
            playlist_id,
            status=types_module.SyncStatus.COMPLETED,
            result={"exported": 12, "skipped": 2, "spotify_playlist_id": "sp1"},
        )
        resp = await _get(
            TaskStoreSession([task]), user_id, _poll_url(playlist_id, task)
        )
        assert "Export complete" in resp.text
        assert "Exported &middot; 12 tracks" in resp.text
        assert "(2 not found on Spotify)" in resp.text
        assert "https://open.spotify.com/playlist/sp1" in resp.text
        assert not _keeps_polling(resp.text)

    async def test_skipped_note_hidden_when_zero(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id,
            playlist_id,
            status=types_module.SyncStatus.COMPLETED,
            result={"exported": 12, "skipped": 0},
        )
        resp = await _get(
            TaskStoreSession([task]), user_id, _poll_url(playlist_id, task)
        )
        assert "not found on Spotify" not in resp.text

    async def test_stopped_row_is_muted_not_marked(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.CANCELLED
        )
        resp = await _get(
            TaskStoreSession([task]), user_id, _poll_url(playlist_id, task)
        )
        assert '<span class="text-muted">Stopped</span>' in resp.text
        assert "<mark>" not in resp.text
        assert "Export stopped" in resp.text
        assert not _keeps_polling(resp.text)

    async def test_headline_failed_beats_stopped(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        stopped = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.CANCELLED
        )
        failed = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.FAILED, error="x"
        )
        resp = await _get(
            TaskStoreSession([stopped, failed]),
            user_id,
            _poll_url(playlist_id, stopped, failed),
        )
        assert "Export finished with errors" in resp.text

    async def test_headline_stopped_beats_complete(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        stopped = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.CANCELLED
        )
        done = _export_task(
            user_id,
            playlist_id,
            status=types_module.SyncStatus.COMPLETED,
            result={"exported": 1},
        )
        resp = await _get(
            TaskStoreSession([stopped, done]),
            user_id,
            _poll_url(playlist_id, stopped, done),
        )
        assert "Export stopped" in resp.text

    async def test_one_active_task_keeps_polling(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        failed = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.FAILED, error="x"
        )
        pending = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.PENDING
        )
        resp = await _get(
            TaskStoreSession([failed, pending]),
            user_id,
            _poll_url(playlist_id, failed, pending),
        )
        assert _keeps_polling(resp.text)
        assert "Exporting to Spotify…" in resp.text


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


class TestExportingPageFrame:
    async def test_stable_frame_with_rows_and_back_link(
        self, user_id: uuid.UUID
    ) -> None:
        playlist = _playlist(user_id, "My Playlist")
        pending = _export_task(
            user_id, playlist.id, status=types_module.SyncStatus.PENDING
        )
        running = _export_task(user_id, playlist.id)
        resp = await _get(
            TaskStoreSession([playlist, pending, running]),
            user_id,
            f"/playlists/exporting/{playlist.id}?task_ids={pending.id},{running.id}",
        )
        html = resp.text
        assert 'aria-live="polite" aria-atomic="false"' in html
        assert f'id="export-row-{pending.id}"' in html
        assert f'id="export-row-{running.id}"' in html
        assert "Back to playlist" in html
        assert _keeps_polling(html)
        # Stop only for PENDING/DEFERRED; RUNNING explains why it can't stop.
        assert html.count(">Stop</button>") == 1
        assert f"/export/{pending.id}/cancel" in html
        assert "Can't be stopped once it has started." in html

    async def test_all_terminal_renders_without_poller(
        self, user_id: uuid.UUID
    ) -> None:
        playlist = _playlist(user_id, "My Playlist")
        done = _export_task(
            user_id,
            playlist.id,
            status=types_module.SyncStatus.COMPLETED,
            result={"exported": 4},
        )
        resp = await _get(
            TaskStoreSession([playlist, done]),
            user_id,
            f"/playlists/exporting/{playlist.id}?task_ids={done.id}",
        )
        assert "Export complete" in resp.text
        assert not _keeps_polling(resp.text)

    async def test_no_tasks_is_export_not_found(self, user_id: uuid.UUID) -> None:
        playlist = _playlist(user_id, "My Playlist")
        resp = await _get(
            TaskStoreSession([playlist]),
            user_id,
            f"/playlists/exporting/{playlist.id}?task_ids=",
        )
        assert "Export not found" in resp.text
        assert "Back to playlist" in resp.text
        assert not _keeps_polling(resp.text)


async def _post(
    db: TaskStoreSession, user_id: uuid.UUID | None, url: str
) -> httpx.Response:
    app, cookies = _app(db, user_id)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        cookies=cookies,
        follow_redirects=False,
    ) as client:
        return await client.post(url, headers={"HX-Request": "true"})


def _cancel_url(playlist_id: uuid.UUID, task_id: uuid.UUID) -> str:
    return f"/playlists/{playlist_id}/export/{task_id}/cancel"


_CANCEL_ERROR = "Couldn&#39;t stop the export. Try again."


class TestCancelExportRoute:
    async def test_unauthenticated_redirects_to_login(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.PENDING
        )
        resp = await _post(
            TaskStoreSession([task]), None, _cancel_url(playlist_id, task.id)
        )
        assert resp.status_code == 307

    async def test_pending_export_is_stopped(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.PENDING
        )
        db = TaskStoreSession([task])
        resp = await _post(db, user_id, _cancel_url(playlist_id, task.id))
        assert resp.status_code == 200
        assert task.status == types_module.SyncStatus.CANCELLED
        assert db.commits == 1
        assert f'id="export-row-{task.id}"' in resp.text
        assert '<span class="text-muted">Stopped</span>' in resp.text
        assert ">Stop</button>" not in resp.text
        assert "HX-Retarget" not in resp.headers
        # Focus moves to the swapped row's status element.
        assert f'id="export-status-{task.id}" tabindex="-1" autofocus' in resp.text

    async def test_already_finished_shows_real_state_and_notice(
        self, user_id: uuid.UUID
    ) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id,
            playlist_id,
            status=types_module.SyncStatus.COMPLETED,
            result={"exported": 9, "spotify_playlist_id": "sp9"},
        )
        db = TaskStoreSession([task])
        resp = await _post(db, user_id, _cancel_url(playlist_id, task.id))
        assert task.status == types_module.SyncStatus.COMPLETED
        assert db.commits == 0
        assert "Exported &middot; 9 tracks" in resp.text
        assert "This export had already finished." in resp.text
        assert ">Stop</button>" not in resp.text

    async def test_other_users_task_is_error_and_untouched(
        self, user_id: uuid.UUID, other_user_id: uuid.UUID
    ) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            other_user_id, playlist_id, status=types_module.SyncStatus.PENDING
        )
        resp = await _post(
            TaskStoreSession([task]), user_id, _cancel_url(playlist_id, task.id)
        )
        assert task.status == types_module.SyncStatus.PENDING
        assert _CANCEL_ERROR in resp.text
        assert resp.headers["HX-Retarget"] == f"#export-error-{task.id}"
        assert resp.headers["HX-Reswap"] == "innerHTML"

    async def test_task_on_another_playlist_is_error_and_untouched(
        self, user_id: uuid.UUID
    ) -> None:
        task = _export_task(
            user_id, uuid.uuid4(), status=types_module.SyncStatus.PENDING
        )
        resp = await _post(
            TaskStoreSession([task]), user_id, _cancel_url(uuid.uuid4(), task.id)
        )
        assert task.status == types_module.SyncStatus.PENDING
        assert _CANCEL_ERROR in resp.text

    async def test_non_export_task_is_error_and_untouched(
        self, user_id: uuid.UUID
    ) -> None:
        playlist_id = uuid.uuid4()
        task = _task(
            user_id,
            task_type=types_module.TaskType.PLAYLIST_GENERATION,
            status=types_module.SyncStatus.PENDING,
            params={"playlist_id": str(playlist_id)},
        )
        resp = await _post(
            TaskStoreSession([task]), user_id, _cancel_url(playlist_id, task.id)
        )
        assert task.status == types_module.SyncStatus.PENDING
        assert _CANCEL_ERROR in resp.text

    async def test_cancel_error_keeps_button_and_shows_inline_error(
        self, user_id: uuid.UUID, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        async def boom(*args: Any, **kwargs: Any) -> Any:
            raise RuntimeError("db down")

        monkeypatch.setattr(lifecycle_module, "request_cancel", boom)
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.PENDING
        )
        resp = await _post(
            TaskStoreSession([task]), user_id, _cancel_url(playlist_id, task.id)
        )
        assert resp.status_code == 200
        assert _CANCEL_ERROR in resp.text
        assert 'role="alert"' in resp.text
        assert resp.headers["HX-Retarget"] == f"#export-error-{task.id}"


def _render(template: str, **ctx: Any) -> str:
    return common_ui.templates.get_template(template).render(ctx)


class TestExportRowPartial:
    def test_deferred_row_is_stoppable(self, user_id: uuid.UUID) -> None:
        playlist_id = uuid.uuid4()
        task = _export_task(
            user_id, playlist_id, status=types_module.SyncStatus.DEFERRED
        )
        row = playlists_ui._export_row(task)
        html = _render(
            "partials/export_row.html", row=row, playlist_id=str(playlist_id)
        )
        assert 'class="outline secondary btn-sm export-stop"' in html
        assert 'hx-disabled-elt="this"' in html
        assert "hx-confirm=" in html
        assert 'aria-busy="true"' in html
        assert "autofocus" not in html

    def test_unknown_status_renders_like_failed_with_raw_status(
        self, user_id: uuid.UUID
    ) -> None:
        row = {
            "task_id": str(uuid.uuid4()),
            "description": "Export",
            "status": "exploded",
            "error": None,
            "cancellable": False,
        }
        html = _render("partials/export_row.html", row=row, playlist_id="p")
        assert "<mark>Failed: Unknown error (status: exploded)</mark>" in html
        assert ">Stop</button>" not in html
        assert 'aria-busy="true"' not in html


class TestPlaylistDetailExportSection:
    def test_in_progress_exports_render_shared_rows(self, user_id: uuid.UUID) -> None:
        playlist = _playlist(user_id, "My Playlist")
        playlist.track_count = 0
        pending = _export_task(
            user_id, playlist.id, status=types_module.SyncStatus.PENDING
        )
        running = _export_task(user_id, playlist.id)
        html = _render(
            "playlist_detail.html",
            playlist=playlist,
            tracks=[],
            spotify_connections=[object()],
            export_in_progress=True,
            export_task_ids=f"{pending.id},{running.id}",
            export_rows=[
                playlists_ui._export_row(pending),
                playlists_ui._export_row(running),
            ],
        )
        assert 'aria-live="polite" aria-atomic="false"' in html
        assert f'id="export-row-{pending.id}"' in html
        assert f'id="export-row-{running.id}"' in html
        assert html.count(">Stop</button>") == 1
        assert "Can't be stopped once it has started." in html
        assert "View progress" in html


class TestGeneratingTerminalRule:
    async def _partial(self, user_id: uuid.UUID, status: Any) -> str:
        task = _task(user_id, task_type=types_module.TaskType.PLAYLIST_GENERATION)
        # Any: the unknown-status case passes a stand-in with only ``.value``.
        task.status = status
        resp = await _get(
            TaskStoreSession([task]), user_id, f"/partials/generating-status/{task.id}"
        )
        return resp.text

    @pytest.mark.parametrize(
        "status",
        [
            types_module.SyncStatus.PENDING,
            types_module.SyncStatus.RUNNING,
            types_module.SyncStatus.DEFERRED,
        ],
    )
    async def test_active_statuses_keep_polling(
        self, user_id: uuid.UUID, status: types_module.SyncStatus
    ) -> None:
        assert _keeps_polling(await self._partial(user_id, status))

    async def test_cancelled_stops_with_start_over(self, user_id: uuid.UUID) -> None:
        html = await self._partial(user_id, types_module.SyncStatus.CANCELLED)
        assert "Generation was cancelled." in html
        assert (
            '<a href="/playlists/new" role="button" class="secondary">Start over</a>'
            in html
        )
        assert "<mark>" not in html
        assert not _keeps_polling(html)

    async def test_unknown_status_renders_like_failed(self, user_id: uuid.UUID) -> None:
        status = mock.Mock(value="exploded")
        html = await self._partial(user_id, status)
        assert (
            "<mark>Generation failed: Unknown error (status: exploded)</mark>" in html
        )
        assert not _keeps_polling(html)

    async def test_page_has_stable_live_region(self, user_id: uuid.UUID) -> None:
        task = _task(user_id, task_type=types_module.TaskType.PLAYLIST_GENERATION)
        resp = await _get(
            TaskStoreSession([task]), user_id, f"/playlists/generating/{task.id}"
        )
        assert '<div aria-live="polite">' in resp.text
