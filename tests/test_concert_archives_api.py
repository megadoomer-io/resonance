"""Tests for the Concert Archives CSV upload API endpoint."""

from __future__ import annotations

import datetime
import uuid
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import httpx
import pytest

import resonance.config as config_module
import resonance.middleware.session as session_middleware
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


# ---------------------------------------------------------------------------
# Minimal valid CSV for testing
# ---------------------------------------------------------------------------

_VALID_CSV = (
    "Start Date,End Date,Status,Concert Name,"
    "Bands Seen,Bands Not Seen,Venue,Location,URL\n"
    "05/15/2023,,Past,Summer Fest,"
    "The National / Arcade Fire,,The Fillmore,"
    '"San Francisco, California, United States",'
    "https://www.concertarchives.org/mike.dougherty/concerts/123\n"
)

_INVALID_CSV_HEADERS = "Name,Date,Venue\nfoo,bar,baz\n"


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


def _make_settings() -> config_module.Settings:
    return config_module.Settings(
        spotify_client_id="test-id",
        spotify_client_secret="test-secret",
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
    )


class FakeRedis:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self._sets: dict[str, set[str]] = {}

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def setex(self, key: str, ttl: int, value: str) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> int:
        return sum(1 for k in keys if self._store.pop(k, None) is not None)

    async def sadd(self, key: str, *values: str) -> int:
        self._sets.setdefault(key, set()).update(values)
        return len(values)

    async def smembers(self, key: str) -> set[bytes]:
        return {v.encode() for v in self._sets.get(key, set())}

    async def expire(self, key: str, ttl: int) -> bool:
        return key in self._store or key in self._sets

    async def aclose(self) -> None:
        pass

    def inject_session(self, session_id: str, data: dict[str, Any]) -> None:
        import json

        self._store[f"session:{session_id}"] = json.dumps(data)


class FakeResult:
    """Fake DB result supporting scalars().all() and scalar_one_or_none()."""

    def __init__(self, items: list[Any] | None = None) -> None:
        self._items = items or []

    def unique(self) -> FakeResult:
        return self

    def scalars(self) -> FakeResult:
        return self

    def all(self) -> list[Any]:
        return self._items

    def scalar_one_or_none(self) -> Any:
        return self._items[0] if self._items else None


class FakeAsyncSession:
    def __init__(self) -> None:
        self._results: list[Any] = []
        self._call_count = 0
        self._added: list[Any] = []
        self._flushed = False

    def set_results(self, results: list[Any]) -> None:
        self._results = results

    async def execute(self, *args: Any, **kwargs: Any) -> Any:
        if self._call_count < len(self._results):
            result = self._results[self._call_count]
            self._call_count += 1
            return result
        return FakeResult()

    def add(self, obj: Any) -> None:
        self._added.append(obj)

    async def flush(self) -> None:
        self._flushed = True

    async def commit(self) -> None:
        pass

    async def __aenter__(self) -> FakeAsyncSession:
        return self

    async def __aexit__(self, *args: Any) -> None:
        pass


class FakeSessionFactory:
    def __init__(self, session: FakeAsyncSession | None = None) -> None:
        self._session = session or FakeAsyncSession()

    def __call__(self) -> FakeAsyncSession:
        return self._session


class FakeArqRedis:
    """Minimal arq redis mock that records enqueued jobs."""

    def __init__(self) -> None:
        self.jobs: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> None:
        self.jobs.append((function, args, kwargs))


def _make_session_cookie(secret_key: str) -> str:
    import itsdangerous

    signer = itsdangerous.TimestampSigner(secret_key)
    return signer.sign("test-session-id").decode("utf-8")


def _create_app(
    user_id: uuid.UUID,
    db_session: FakeAsyncSession | None = None,
    arq_redis: FakeArqRedis | None = None,
) -> Any:
    import fastapi

    import resonance.api.v1 as api_v1_module
    import resonance.connectors.registry as registry_module

    settings = _make_settings()
    fake_redis = FakeRedis()
    fake_redis.inject_session(
        "test-session-id", {"user_id": str(user_id), "user_role": "owner"}
    )

    app = fastapi.FastAPI(title="test", lifespan=None)
    app.state.settings = settings
    app.state.session_factory = FakeSessionFactory(db_session)
    app.state.arq_redis = arq_redis or FakeArqRedis()
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(api_v1_module.router)
    app.state.connector_registry = registry_module.ConnectorRegistry()

    return app


def _make_connection(
    *,
    connection_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    sync_watermark: dict[str, Any] | None = None,
    external_user_id: str | None = "mike.dougherty",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=connection_id or uuid.uuid4(),
        user_id=user_id or uuid.uuid4(),
        service_type=types_module.ServiceType.CONCERT_ARCHIVES,
        external_user_id=external_user_id,
        sync_watermark=sync_watermark or {},
        enabled=True,
        connected_at=datetime.datetime(2026, 5, 1, tzinfo=datetime.UTC),
        last_synced_at=None,
    )


def _make_task(
    *,
    task_id: uuid.UUID | None = None,
    status: types_module.SyncStatus = types_module.SyncStatus.PENDING,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=task_id or uuid.uuid4(),
        status=status,
        task_type=types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def user_id() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def arq_redis() -> FakeArqRedis:
    return FakeArqRedis()


@pytest.fixture
async def unauthed_client() -> AsyncIterator[httpx.AsyncClient]:
    """Unauthenticated client."""
    import fastapi

    import resonance.api.v1 as api_v1_module
    import resonance.connectors.registry as registry_module

    settings = _make_settings()
    fake_redis = FakeRedis()

    app = fastapi.FastAPI(title="test", lifespan=None)
    app.state.settings = settings
    app.state.session_factory = FakeSessionFactory()
    app.state.arq_redis = FakeArqRedis()
    app.add_middleware(
        session_middleware.SessionMiddleware,
        redis=fake_redis,  # type: ignore[arg-type]
        secret_key=settings.session_secret_key,
    )
    app.include_router(api_v1_module.router)
    app.state.connector_registry = registry_module.ConnectorRegistry()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

_UPLOAD_URL = "/api/v1/connections/concert-archives/upload"


class TestUploadAuth:
    async def test_unauthenticated_upload_returns_401(
        self, unauthed_client: httpx.AsyncClient
    ) -> None:
        resp = await unauthed_client.post(
            _UPLOAD_URL,
            files={"file": ("test.csv", _VALID_CSV, "text/csv")},
        )
        assert resp.status_code == 401


class TestUploadValidCSV:
    async def test_valid_csv_creates_connection_and_returns_task_id(
        self, user_id: uuid.UUID, arq_redis: FakeArqRedis
    ) -> None:
        db = FakeAsyncSession()
        # Query 1: find existing connection -> None
        # Query 2: check concurrent task -> None
        db.set_results([FakeResult(), FakeResult()])

        app = _create_app(user_id, db, arq_redis)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={
                    "file": (
                        "mike.dougherty - Concert Archives Export - 05-19-2026.csv",
                        _VALID_CSV,
                        "text/csv",
                    )
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "started"
        assert "task_id" in data

        # Should have added a ServiceConnection and a Task
        assert len(db._added) == 2

        # Should have enqueued an arq job
        assert len(arq_redis.jobs) == 1
        job_name, _job_args, _job_kwargs = arq_redis.jobs[0]
        assert job_name == "sync_concert_archives"

    async def test_reupload_with_existing_connection_updates_watermark(
        self, user_id: uuid.UUID, arq_redis: FakeArqRedis
    ) -> None:
        conn = _make_connection(
            user_id=user_id,
            sync_watermark={"last_export_date": "2026-05-01"},
        )

        db = FakeAsyncSession()
        # Query 1: find existing connection -> exists
        # Query 2: check concurrent task -> None
        db.set_results([FakeResult([conn]), FakeResult()])

        app = _create_app(user_id, db, arq_redis)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={
                    "file": (
                        "mike.dougherty - Concert Archives Export - 05-19-2026.csv",
                        _VALID_CSV,
                        "text/csv",
                    )
                },
            )

        assert resp.status_code == 200
        # Should have updated watermark on existing connection
        assert conn.sync_watermark["last_export_date"] == "2026-05-19"
        # Should only add a Task (not a new connection)
        assert len(db._added) == 1


class TestUploadExportDate:
    async def test_export_date_from_form_field(
        self, user_id: uuid.UUID, arq_redis: FakeArqRedis
    ) -> None:
        conn = _make_connection(
            user_id=user_id,
            sync_watermark={"last_export_date": "2026-05-01"},
        )

        db = FakeAsyncSession()
        db.set_results([FakeResult([conn]), FakeResult()])

        app = _create_app(user_id, db, arq_redis)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={"file": ("export.csv", _VALID_CSV, "text/csv")},
                data={"export_date": "2026-06-01"},
            )

        assert resp.status_code == 200
        # Form field date should be used, not filename or today
        assert conn.sync_watermark["last_export_date"] == "2026-06-01"

    async def test_export_date_from_filename_fallback(
        self, user_id: uuid.UUID, arq_redis: FakeArqRedis
    ) -> None:
        db = FakeAsyncSession()
        db.set_results([FakeResult(), FakeResult()])

        app = _create_app(user_id, db, arq_redis)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={
                    "file": (
                        "user - Concert Archives Export - 03-15-2026.csv",
                        _VALID_CSV,
                        "text/csv",
                    )
                },
            )

        assert resp.status_code == 200
        # Connection should have watermark from filename date
        added_conn = db._added[0]  # first added object is the connection
        assert added_conn.sync_watermark["last_export_date"] == "2026-03-15"


class TestUploadValidation:
    async def test_invalid_csv_headers_returns_422(self, user_id: uuid.UUID) -> None:
        db = FakeAsyncSession()
        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={"file": ("test.csv", _INVALID_CSV_HEADERS, "text/csv")},
            )

        assert resp.status_code == 422

    async def test_non_utf8_file_returns_422(self, user_id: uuid.UUID) -> None:
        db = FakeAsyncSession()
        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={"file": ("test.csv", b"\xff\xfe not utf8", "text/csv")},
            )

        assert resp.status_code == 422
        assert "utf-8" in resp.json()["detail"].lower()

    async def test_file_too_large_returns_413(self, user_id: uuid.UUID) -> None:
        # Create content just over 5MB
        large_content = "x" * (5 * 1024 * 1024 + 1)

        db = FakeAsyncSession()
        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={"file": ("test.csv", large_content, "text/csv")},
            )

        assert resp.status_code == 413


class TestUploadConflicts:
    async def test_stale_export_returns_409(self, user_id: uuid.UUID) -> None:
        """Uploading a CSV older than the last import should be rejected."""
        conn = _make_connection(
            user_id=user_id,
            sync_watermark={"last_export_date": "2026-05-19"},
        )

        db = FakeAsyncSession()
        # Query 1: find existing connection -> exists with newer watermark
        db.set_results([FakeResult([conn])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={
                    "file": (
                        "user - Concert Archives Export - 05-18-2026.csv",
                        _VALID_CSV,
                        "text/csv",
                    )
                },
            )

        assert resp.status_code == 409
        assert "stale" in resp.json()["detail"].lower()

    async def test_concurrent_import_returns_409(self, user_id: uuid.UUID) -> None:
        conn = _make_connection(user_id=user_id)
        running_task = _make_task(status=types_module.SyncStatus.RUNNING)

        db = FakeAsyncSession()
        # Query 1: find existing connection -> exists
        # Query 2: check concurrent task -> running
        db.set_results([FakeResult([conn]), FakeResult([running_task])])

        app = _create_app(user_id, db)
        settings = _make_settings()
        cookie = _make_session_cookie(settings.session_secret_key)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
            cookies={"session_id": cookie},
        ) as c:
            resp = await c.post(
                _UPLOAD_URL,
                files={
                    "file": (
                        "user - Concert Archives Export - 05-20-2026.csv",
                        _VALID_CSV,
                        "text/csv",
                    )
                },
            )

        assert resp.status_code == 409
        assert "already" in resp.json()["detail"].lower()
