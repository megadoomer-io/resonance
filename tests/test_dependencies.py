"""Tests for resonance.dependencies auth validation functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import fastapi
import pytest

import resonance.dependencies as deps_module
import resonance.types as types_module


class TestRequireAdmin:
    """Tests for require_admin dependency."""

    def test_admin_passes(self) -> None:
        deps_module.require_admin(types_module.UserRole.ADMIN)  # no exception

    def test_owner_passes(self) -> None:
        deps_module.require_admin(types_module.UserRole.OWNER)  # no exception

    def test_user_raises_403(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            deps_module.require_admin(types_module.UserRole.USER)
        assert exc_info.value.status_code == 403


class TestRequireOwner:
    """Tests for require_owner dependency."""

    def test_owner_passes(self) -> None:
        deps_module.require_owner(types_module.UserRole.OWNER)

    def test_admin_raises_403(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            deps_module.require_owner(types_module.UserRole.ADMIN)
        assert exc_info.value.status_code == 403

    def test_user_raises_403(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            deps_module.require_owner(types_module.UserRole.USER)
        assert exc_info.value.status_code == 403


class TestVerifyAdminAccessTokenCompare:
    """Admin bearer-token comparison is constant-time (#141, finding #7).

    secrets.compare_digest preserves the accept/reject behavior, so these guard
    against a regression in the comparison logic.
    """

    def _request(self, *, authorization: str, admin_token: str) -> MagicMock:
        req = MagicMock(spec=fastapi.Request)
        req.headers = {"authorization": authorization}
        req.app.state.settings.admin_api_token = admin_token
        return req

    def test_correct_token_passes(self) -> None:
        deps_module.verify_admin_access(
            self._request(authorization="Bearer s3cret", admin_token="s3cret")
        )

    def test_wrong_token_raises_403(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            deps_module.verify_admin_access(
                self._request(authorization="Bearer wrong", admin_token="s3cret")
            )
        assert exc_info.value.status_code == 403
