from typing import Any

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
