"""Tests for view-as role impersonation."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import fastapi
import pytest

import resonance.ui.common as common


class FakeSession(dict[str, Any]):
    """Dict-like session with pop support."""

    pass


_TEST_USER_ID = "12345678-1234-5678-1234-567812345678"


def _make_request(
    user_role: str = "admin",
    view_as: str | None = None,
    user_id: str = _TEST_USER_ID,
) -> fastapi.Request:
    """Create a fake request with session data."""
    request = MagicMock(spec=fastapi.Request)
    session = FakeSession(user_id=user_id, user_role=user_role)
    if view_as:
        session["view_as"] = view_as
    request.state.session = session
    return request


class TestEffectiveRole:
    def test_no_view_as_returns_actual(self) -> None:
        session = FakeSession(user_role="admin")
        assert common._effective_role(session) == "admin"

    def test_view_as_lower_role(self) -> None:
        session = FakeSession(user_role="admin", view_as="user")
        assert common._effective_role(session) == "user"

    def test_view_as_same_role_ignored(self) -> None:
        session = FakeSession(user_role="admin", view_as="admin")
        assert common._effective_role(session) == "admin"

    def test_view_as_higher_role_ignored(self) -> None:
        session = FakeSession(user_role="user", view_as="admin")
        assert common._effective_role(session) == "user"

    def test_owner_view_as_admin(self) -> None:
        session = FakeSession(user_role="owner", view_as="admin")
        assert common._effective_role(session) == "admin"

    def test_owner_view_as_user(self) -> None:
        session = FakeSession(user_role="owner", view_as="user")
        assert common._effective_role(session) == "user"

    def test_empty_view_as_returns_actual(self) -> None:
        session = FakeSession(user_role="admin", view_as="")
        assert common._effective_role(session) == "admin"


class TestRequireAdminViewAs:
    @pytest.mark.anyio
    async def test_admin_without_view_as_passes(self) -> None:
        request = _make_request(user_role="admin")
        result = await common.require_admin(request)
        assert str(result) == _TEST_USER_ID

    @pytest.mark.anyio
    async def test_admin_viewing_as_user_still_passes(self) -> None:
        request = _make_request(user_role="admin", view_as="user")
        result = await common.require_admin(request)
        assert str(result) == _TEST_USER_ID

    @pytest.mark.anyio
    async def test_owner_viewing_as_admin_passes(self) -> None:
        request = _make_request(user_role="owner", view_as="admin")
        result = await common.require_admin(request)
        assert str(result) == _TEST_USER_ID

    @pytest.mark.anyio
    async def test_owner_viewing_as_user_still_passes(self) -> None:
        request = _make_request(user_role="owner", view_as="user")
        result = await common.require_admin(request)
        assert str(result) == _TEST_USER_ID


class TestBaseContextViewAs:
    def test_includes_effective_and_actual_role(self) -> None:
        request = _make_request(user_role="admin", view_as="user")
        ctx = common.base_context(request)
        assert ctx["user_role"] == "user"
        assert ctx["actual_role"] == "admin"
        assert ctx["viewing_as"] == "user"

    def test_no_view_as_roles_match(self) -> None:
        request = _make_request(user_role="admin")
        ctx = common.base_context(request)
        assert ctx["user_role"] == "admin"
        assert ctx["actual_role"] == "admin"
        assert ctx["viewing_as"] is None
