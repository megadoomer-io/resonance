"""Tests for ui/common.py — auth dependencies, pagination, context builder."""

from __future__ import annotations

from unittest.mock import MagicMock

import fastapi
import pytest

import resonance.ui.common as common_module


def _make_request(
    session: dict[str, str] | None = None,
) -> fastapi.Request:
    """Build a fake Request with a session dict on request.state."""
    request = MagicMock(spec=fastapi.Request)
    request.state = MagicMock()
    request.state.session = session or {}
    return request


# ---------------------------------------------------------------------------
# require_user
# ---------------------------------------------------------------------------


class TestRequireUser:
    async def test_returns_uuid_when_authenticated(self) -> None:
        request = _make_request({"user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"})
        result = await common_module.require_user(request)
        assert str(result) == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    async def test_redirects_when_no_user_id(self) -> None:
        request = _make_request({})
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await common_module.require_user(request)
        assert exc_info.value.status_code == 307
        assert exc_info.value.headers["Location"] == "/login"  # type: ignore[index]

    async def test_redirects_when_user_id_is_none(self) -> None:
        request = _make_request({"user_id": None})  # type: ignore[dict-item]
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await common_module.require_user(request)
        assert exc_info.value.status_code == 307


# ---------------------------------------------------------------------------
# require_admin
# ---------------------------------------------------------------------------


class TestRequireAdmin:
    async def test_returns_uuid_for_admin(self) -> None:
        request = _make_request(
            {
                "user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "user_role": "admin",
            }
        )
        result = await common_module.require_admin(request)
        assert str(result) == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    async def test_returns_uuid_for_owner(self) -> None:
        request = _make_request(
            {
                "user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "user_role": "owner",
            }
        )
        result = await common_module.require_admin(request)
        assert str(result) == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    async def test_redirects_when_no_user_id(self) -> None:
        request = _make_request({})
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await common_module.require_admin(request)
        assert exc_info.value.status_code == 307

    async def test_forbids_non_admin(self) -> None:
        request = _make_request(
            {
                "user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "user_role": "user",
            }
        )
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await common_module.require_admin(request)
        assert exc_info.value.status_code == 403

    async def test_forbids_default_role(self) -> None:
        request = _make_request(
            {
                "user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            }
        )
        with pytest.raises(fastapi.HTTPException) as exc_info:
            await common_module.require_admin(request)
        assert exc_info.value.status_code == 403


# ---------------------------------------------------------------------------
# base_context
# ---------------------------------------------------------------------------


class TestBaseContext:
    def test_includes_all_fields(self) -> None:
        request = _make_request(
            {
                "user_id": "abc123",
                "user_tz": "America/Los_Angeles",
                "user_role": "admin",
            }
        )
        ctx = common_module.base_context(request)
        assert ctx["request"] is request
        assert ctx["user_id"] == "abc123"
        assert ctx["user_tz"] == "America/Los_Angeles"
        assert ctx["user_role"] == "admin"

    def test_defaults_role_to_user(self) -> None:
        request = _make_request({"user_id": "abc123"})
        ctx = common_module.base_context(request)
        assert ctx["user_role"] == "user"

    def test_none_for_missing_fields(self) -> None:
        request = _make_request({})
        ctx = common_module.base_context(request)
        assert ctx["user_id"] is None
        assert ctx["user_tz"] is None


# ---------------------------------------------------------------------------
# paginate
# ---------------------------------------------------------------------------


class TestPaginate:
    def test_normal_page(self) -> None:
        items = list(range(51))
        result = common_module.paginate(items, page=2, page_size=50)
        assert len(result.items) == 50
        assert result.has_next is True
        assert result.has_prev is True
        assert result.page == 2

    def test_last_page(self) -> None:
        items = list(range(30))
        result = common_module.paginate(items, page=3, page_size=50)
        assert len(result.items) == 30
        assert result.has_next is False
        assert result.has_prev is True

    def test_first_page(self) -> None:
        items = list(range(51))
        result = common_module.paginate(items, page=1, page_size=50)
        assert result.has_prev is False
        assert result.has_next is True

    def test_exactly_page_size(self) -> None:
        items = list(range(50))
        result = common_module.paginate(items, page=1, page_size=50)
        assert len(result.items) == 50
        assert result.has_next is False

    def test_empty_items(self) -> None:
        result = common_module.paginate([], page=1)
        assert result.items == []
        assert result.has_next is False
        assert result.has_prev is False

    def test_single_item(self) -> None:
        result = common_module.paginate(["a"], page=1, page_size=50)
        assert result.items == ["a"]
        assert result.has_next is False


# ---------------------------------------------------------------------------
# page_offset
# ---------------------------------------------------------------------------


class TestPageOffset:
    def test_page_one(self) -> None:
        assert common_module.page_offset(1) == 0

    def test_page_two(self) -> None:
        assert common_module.page_offset(2) == 50

    def test_custom_page_size(self) -> None:
        assert common_module.page_offset(3, page_size=25) == 50


# ---------------------------------------------------------------------------
# escape_ilike
# ---------------------------------------------------------------------------


class TestEscapeIlike:
    def test_escapes_percent(self) -> None:
        assert common_module.escape_ilike("100%") == r"100\%"

    def test_escapes_underscore(self) -> None:
        assert common_module.escape_ilike("foo_bar") == r"foo\_bar"

    def test_no_escaping_needed(self) -> None:
        assert common_module.escape_ilike("hello") == "hello"
