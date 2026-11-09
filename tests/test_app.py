"""App-level wiring tests: security headers, docs gating, playground gating.

Covers security review #141 findings #8 (CSP/headers), #9 (Swagger gated to
debug), and #11 (component playground not registered in production).
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock

import httpx

import resonance.app as app_module
import resonance.config as config_module
import resonance.types as types_module

if TYPE_CHECKING:
    import pytest

    from tests.conftest import TaskDB


def _prod_settings(**overrides: object) -> config_module.Settings:
    """Production-mode settings (debug off) with real secrets so the #4 guard
    passes."""
    base: dict[str, object] = {
        "debug": False,
        "session_secret_key": "real-session-signing-key",
        "token_encryption_key": "real-fernet-encryption-key",
        "pgpassword": "real-db-password",
    }
    base.update(overrides)
    return config_module.Settings(**base)  # type: ignore[arg-type]


def _client(settings: config_module.Settings) -> httpx.AsyncClient:
    app = app_module.create_app(settings)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        follow_redirects=False,
    )


class TestSecurityHeaders:
    """CSP + hardening headers on every response (#141, finding #8)."""

    async def test_headers_present_on_response(self) -> None:
        async with _client(_prod_settings()) as c:
            resp = await c.get("/login")
        csp = resp.headers.get("Content-Security-Policy", "")
        assert "default-src 'self'" in csp
        assert "frame-ancestors 'none'" in csp
        assert "object-src 'none'" in csp
        # Inline scripts + the external CDNs the templates actually use.
        assert "https://unpkg.com" in csp
        # Google Fonts: theme.css @imports the stylesheet, which pulls font
        # files from gstatic. Both must be allowed or the DESIGN.md fonts break
        # (caught by live browser QA, not header-shape tests).
        assert "https://fonts.googleapis.com" in csp
        assert "https://fonts.gstatic.com" in csp
        assert resp.headers["X-Content-Type-Options"] == "nosniff"
        assert resp.headers["X-Frame-Options"] == "DENY"
        assert resp.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"


class TestDocsGating:
    """Swagger UI + OpenAPI spec are dev-only (#141, finding #9)."""

    async def test_docs_and_openapi_hidden_in_prod(self) -> None:
        async with _client(_prod_settings()) as c:
            assert (await c.get("/docs")).status_code == 404
            assert (await c.get("/openapi.json")).status_code == 404

    async def test_docs_available_in_debug(self) -> None:
        async with _client(config_module.Settings(debug=True)) as c:
            assert (await c.get("/openapi.json")).status_code == 200


class TestSeedRouteSchema:
    """The dev seed endpoint only appears in the spec when it's enabled."""

    async def test_hidden_from_openapi_when_disabled(self) -> None:
        async with _client(config_module.Settings(debug=True)) as c:
            paths = (await c.get("/openapi.json")).json()["paths"]
        assert "/api/v1/admin/seed" not in paths
        assert "/api/v1/admin/stats" in paths

    async def test_listed_in_openapi_when_enabled(self) -> None:
        settings = config_module.Settings(debug=True, dev_seed_enabled=True)
        async with _client(settings) as c:
            paths = (await c.get("/openapi.json")).json()["paths"]
        assert "/api/v1/admin/seed" in paths


class TestPlaygroundGating:
    """The dev component playground is not exposed in production (#141, #11)."""

    async def test_playground_not_registered_in_prod(self) -> None:
        async with _client(_prod_settings()) as c:
            # Route isn't registered at all -> 404 (not an auth redirect).
            assert (await c.get("/dev/components")).status_code == 404

    async def test_playground_registered_but_gated_in_debug(self) -> None:
        async with _client(config_module.Settings(debug=True)) as c:
            resp = await c.get("/dev/components")
        # Registered in dev, but still admin-gated -> redirect to login.
        assert resp.status_code == 307
        assert resp.headers["location"] == "/login"


class TestLifespanLeavesTasksAlone:
    """The worker owns task recovery; API startup must not reset RUNNING rows."""

    async def test_running_row_untouched(
        self, task_db: TaskDB, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        started = datetime.datetime(2024, 1, 1, tzinfo=datetime.UTC)
        task = await task_db.add(
            status=types_module.SyncStatus.RUNNING, started_at=started
        )
        engine = MagicMock(dispose=AsyncMock())
        monkeypatch.setattr(
            app_module.database_module, "create_async_engine", lambda _s: engine
        )
        monkeypatch.setattr(
            app_module.database_module,
            "create_session_factory",
            lambda _e: task_db.factory,
        )
        monkeypatch.setattr(
            app_module.migrations_module, "assert_schema_current", AsyncMock()
        )
        monkeypatch.setattr(
            app_module.aioredis,
            "from_url",
            lambda *a, **k: MagicMock(aclose=AsyncMock()),
        )
        monkeypatch.setattr(
            app_module.arq_connections,
            "create_pool",
            AsyncMock(return_value=MagicMock(aclose=AsyncMock())),
        )
        application = app_module.create_app(_prod_settings())

        async with app_module.lifespan(application):
            pass

        row = await task_db.get(task.id)
        assert row.status == types_module.SyncStatus.RUNNING
        assert row.started_at is not None
