"""Tests for admin-token assume-user identity (#135).

A request carrying a valid ADMIN_API_TOKEN may assume a specific user
identity via the ``X-Assume-User`` header or ``?as_user=`` query param, so
an agent can drive user-scoped endpoints live. Gated by
``admin_assume_user_enabled`` and audit-logged.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import MagicMock

import fastapi
import pytest

import resonance.config as config_module
import resonance.dependencies as dependencies

_ADMIN_TOKEN = "test-admin-token"
_ASSUMED = uuid.UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
_OWNER = uuid.UUID("00000000-0000-4000-8000-000000000001")


def _settings(*, enabled: bool = True) -> config_module.Settings:
    return config_module.Settings(
        token_encryption_key="y4s2fMagCz79NWhqQfaAPbTBl9vnamqcvlGM6GRH2cQ=",
        admin_api_token=_ADMIN_TOKEN,
        admin_assume_user_enabled=enabled,
    )


class _FakeResult:
    def __init__(self, value: Any) -> None:
        self._value = value

    def scalar_one_or_none(self) -> Any:
        return self._value


class _FakeDB:
    """Async-context DB whose execute() always yields a fixed scalar."""

    def __init__(self, scalar_value: Any) -> None:
        self._value = scalar_value

    async def execute(self, *args: Any, **kwargs: Any) -> _FakeResult:
        return _FakeResult(self._value)

    async def __aenter__(self) -> _FakeDB:
        return self

    async def __aexit__(self, *args: Any) -> None:
        return None


class _FakeFactory:
    def __init__(self, scalar_value: Any) -> None:
        self._value = scalar_value

    def __call__(self) -> _FakeDB:
        return _FakeDB(self._value)


def _make_request(
    *,
    settings: config_module.Settings,
    db_scalar: Any,
    auth: str | None = None,
    assume_header: str | None = None,
    as_user_query: str | None = None,
) -> fastapi.Request:
    request = MagicMock(spec=fastapi.Request)
    headers: dict[str, str] = {}
    if auth is not None:
        headers["authorization"] = auth
    if assume_header is not None:
        headers["x-assume-user"] = assume_header
    request.headers = headers
    request.query_params = {"as_user": as_user_query} if as_user_query else {}
    request.url = MagicMock()
    request.url.path = "/api/v1/generator-profiles/"
    request.app.state.settings = settings
    request.app.state.session_factory = _FakeFactory(db_scalar)
    return request


class TestAssumeUser:
    async def test_token_plus_header_resolves_to_assumed_user(self) -> None:
        req = _make_request(
            settings=_settings(),
            db_scalar=_ASSUMED,
            auth=f"Bearer {_ADMIN_TOKEN}",
            assume_header=str(_ASSUMED),
        )
        result = await dependencies.get_current_user_id(req, {})
        assert result == _ASSUMED

    async def test_token_plus_query_param_resolves_to_assumed_user(self) -> None:
        req = _make_request(
            settings=_settings(),
            db_scalar=_ASSUMED,
            auth=f"Bearer {_ADMIN_TOKEN}",
            as_user_query=str(_ASSUMED),
        )
        result = await dependencies.get_current_user_id(req, {})
        assert result == _ASSUMED

    async def test_selector_without_token_is_ignored(self) -> None:
        # Valid session present; assume header must NOT override it.
        req = _make_request(
            settings=_settings(),
            db_scalar=None,
            assume_header=str(_ASSUMED),
        )
        result = await dependencies.get_current_user_id(req, {"user_id": str(_OWNER)})
        assert result == _OWNER

    async def test_disabled_setting_refuses_selector(self) -> None:
        req = _make_request(
            settings=_settings(enabled=False),
            db_scalar=_ASSUMED,
            auth=f"Bearer {_ADMIN_TOKEN}",
            assume_header=str(_ASSUMED),
        )
        with pytest.raises(fastapi.HTTPException) as exc:
            await dependencies.get_current_user_id(req, {})
        assert exc.value.status_code == 403

    async def test_nonexistent_assumed_user_is_404(self) -> None:
        req = _make_request(
            settings=_settings(),
            db_scalar=None,  # user lookup returns nothing
            auth=f"Bearer {_ADMIN_TOKEN}",
            assume_header=str(_ASSUMED),
        )
        with pytest.raises(fastapi.HTTPException) as exc:
            await dependencies.get_current_user_id(req, {})
        assert exc.value.status_code == 404

    async def test_malformed_selector_is_400(self) -> None:
        req = _make_request(
            settings=_settings(),
            db_scalar=_ASSUMED,
            auth=f"Bearer {_ADMIN_TOKEN}",
            assume_header="not-a-uuid",
        )
        with pytest.raises(fastapi.HTTPException) as exc:
            await dependencies.get_current_user_id(req, {})
        assert exc.value.status_code == 400

    async def test_admin_token_without_selector_resolves_owner(self) -> None:
        # Regression: existing admin-token → owner behavior is preserved.
        req = _make_request(
            settings=_settings(),
            db_scalar=_OWNER,
            auth=f"Bearer {_ADMIN_TOKEN}",
        )
        result = await dependencies.get_current_user_id(req, {})
        assert result == _OWNER

    async def test_assume_emits_audit_log(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_logger = MagicMock()
        monkeypatch.setattr(dependencies, "logger", fake_logger)
        req = _make_request(
            settings=_settings(),
            db_scalar=_ASSUMED,
            auth=f"Bearer {_ADMIN_TOKEN}",
            assume_header=str(_ASSUMED),
        )
        await dependencies.get_current_user_id(req, {})
        assert fake_logger.info.called
        assert fake_logger.info.call_args.args[0] == "admin_assume_user"


class TestRequireUserBearer:
    """UI require_user honors the admin bearer + assume path (#133)."""

    @staticmethod
    def _ui_request(**kw: Any) -> fastapi.Request:
        req = _make_request(**kw)
        req.state.session = {}  # no cookie session
        return req

    async def test_session_wins_over_bearer(self) -> None:
        import resonance.ui.common as common

        req = _make_request(settings=_settings(), db_scalar=_OWNER)
        req.state.session = {"user_id": str(_OWNER)}
        assert await common.require_user(req) == _OWNER

    async def test_bearer_plus_assume_resolves_user(self) -> None:
        import resonance.ui.common as common

        req = self._ui_request(
            settings=_settings(),
            db_scalar=_ASSUMED,
            auth=f"Bearer {_ADMIN_TOKEN}",
            assume_header=str(_ASSUMED),
        )
        assert await common.require_user(req) == _ASSUMED

    async def test_bearer_without_selector_resolves_owner(self) -> None:
        import resonance.ui.common as common

        req = self._ui_request(
            settings=_settings(), db_scalar=_OWNER, auth=f"Bearer {_ADMIN_TOKEN}"
        )
        assert await common.require_user(req) == _OWNER

    async def test_no_auth_redirects_to_login(self) -> None:
        import resonance.ui.common as common

        req = self._ui_request(settings=_settings(), db_scalar=None)
        with pytest.raises(fastapi.HTTPException) as exc:
            await common.require_user(req)
        assert exc.value.status_code == 307
        assert exc.value.headers["Location"] == "/login"

    async def test_invalid_token_is_403(self) -> None:
        import resonance.ui.common as common

        req = self._ui_request(
            settings=_settings(), db_scalar=None, auth="Bearer wrong-token"
        )
        with pytest.raises(fastapi.HTTPException) as exc:
            await common.require_user(req)
        assert exc.value.status_code == 403
