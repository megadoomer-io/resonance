from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.middleware.session as session_module


class TestSessionData:
    """Tests for the SessionData container."""

    def _make_session(
        self,
        data: dict[str, Any] | None = None,
        *,
        session_id: str = "test-id",
        is_new: bool = False,
    ) -> session_module.SessionData:
        return session_module.SessionData(
            session_id=session_id,
            data=data if data is not None else {},
            is_new=is_new,
        )

    def test_get_and_set(self) -> None:
        session = self._make_session()
        session["foo"] = "bar"
        assert session["foo"] == "bar"

    def test_get_missing_key_with_default(self) -> None:
        session = self._make_session()
        assert session.get("missing") is None
        assert session.get("missing", "fallback") == "fallback"

    def test_modified_flag(self) -> None:
        session = self._make_session()
        assert session.modified is False
        session["key"] = "value"
        assert session.modified is True

    def test_clear_sets_modified(self) -> None:
        session = self._make_session(data={"a": 1, "b": 2})
        assert session.modified is False
        session.clear()
        assert session.modified is True
        assert "a" not in session

    def test_contains(self) -> None:
        session = self._make_session(data={"exists": True})
        assert "exists" in session
        assert "nope" not in session

    def test_is_new_flag(self) -> None:
        session = self._make_session(is_new=True)
        assert session.is_new is True

    def test_session_id(self) -> None:
        session = self._make_session(session_id="abc-123")
        assert session.session_id == "abc-123"


class TestInvalidateUserSessions:
    """Tests for invalidate_user_sessions."""

    @pytest.fixture
    def redis(self) -> AsyncMock:
        return AsyncMock()

    async def test_deletes_all_sessions_for_user(self, redis: AsyncMock) -> None:
        user_id = "user-123"
        redis.smembers.return_value = {b"sess-a", b"sess-b"}
        redis.delete.return_value = 2

        deleted = await session_module.invalidate_user_sessions(redis, user_id)

        redis.smembers.assert_awaited_once_with("user_sessions:user-123")
        # One call for the session keys, one for the user_sessions key
        assert redis.delete.await_count == 2
        # First call deletes the session keys
        session_keys = set(redis.delete.call_args_list[0].args)
        assert session_keys == {"session:sess-a", "session:sess-b"}
        # Second call deletes the reverse-index key
        assert redis.delete.call_args_list[1].args == ("user_sessions:user-123",)
        assert deleted == 2

    async def test_returns_zero_when_no_sessions(self, redis: AsyncMock) -> None:
        redis.smembers.return_value = set()

        deleted = await session_module.invalidate_user_sessions(redis, "user-x")

        assert deleted == 0
        redis.delete.assert_not_awaited()


class TestSessionCookieSecureFlag:
    """The session cookie's Secure attribute (security review #141, finding #5)."""

    def _middleware(self, *, secure: bool) -> session_module.SessionMiddleware:
        return session_module.SessionMiddleware(
            app=AsyncMock(),
            redis=AsyncMock(),
            secret_key="test-secret-key",
            secure=secure,
        )

    async def _save_and_capture(self, *, secure: bool) -> MagicMock:
        mw = self._middleware(secure=secure)
        session = session_module.SessionData(
            session_id="sid", data={"user_id": "u1"}, is_new=True
        )
        response = MagicMock()
        await mw._save_session(session, response)
        return response

    async def test_secure_cookie_when_enabled(self) -> None:
        """secure=True (production) sets the Secure attribute on the cookie."""
        response = await self._save_and_capture(secure=True)
        assert response.set_cookie.call_args.kwargs["secure"] is True

    async def test_insecure_cookie_when_disabled(self) -> None:
        """secure=False (local-http dev) omits Secure so the cookie works."""
        response = await self._save_and_capture(secure=False)
        assert response.set_cookie.call_args.kwargs["secure"] is False

    def test_secure_defaults_off(self) -> None:
        """The middleware default is off; create_app opts in via not debug."""
        assert self._middleware(secure=False).secure is False
        assert (
            session_module.SessionMiddleware(
                app=AsyncMock(), redis=AsyncMock(), secret_key="k"
            ).secure
            is False
        )
