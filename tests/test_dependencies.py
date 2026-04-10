"""Tests for resonance.dependencies auth validation functions."""

from __future__ import annotations

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
