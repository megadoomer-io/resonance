"""Tests for the generic `resonance-api api` CLI command."""

from __future__ import annotations

import json
import sys
from typing import Any

import httpx
import pytest

import resonance.cli as cli_module


@pytest.fixture()
def _mock_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set env vars so _get_api_config() works without real settings."""
    monkeypatch.setenv("RESONANCE_URL", "http://test-host")
    monkeypatch.setenv("RESONANCE_API_TOKEN", "test-token")


def _fake_response(
    status_code: int = 200,
    json_body: Any = None,
    text: str = "",
) -> httpx.Response:
    """Build a fake httpx.Response."""
    if json_body is not None:
        content = json.dumps(json_body).encode()
        headers = {"content-type": "application/json"}
    else:
        content = text.encode()
        headers = {"content-type": "text/plain"}
    return httpx.Response(
        status_code=status_code,
        content=content,
        headers=headers,
    )


class TestApiCommandMethodParsing:
    """Method defaults to GET; explicit methods are recognized."""

    @pytest.mark.usefixtures("_mock_config")
    def test_defaults_to_get_when_path_is_first_arg(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["method"] = method
            captured["url"] = url
            return _fake_response(json_body={"ok": True})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api", "/healthz"])
        cli_module._cmd_api()

        assert captured["method"] == "GET"
        assert captured["url"] == "http://test-host/healthz"

    @pytest.mark.usefixtures("_mock_config")
    @pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
    def test_explicit_method(
        self, monkeypatch: pytest.MonkeyPatch, method: str
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["method"] = m
            captured["url"] = url
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "api", method, "/api/v1/tracks"]
        )
        cli_module._cmd_api()

        assert captured["method"] == method
        assert captured["url"] == "http://test-host/api/v1/tracks"

    @pytest.mark.usefixtures("_mock_config")
    def test_method_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["method"] = m
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "api", "post", "/admin/dedup-events"]
        )
        cli_module._cmd_api()

        assert captured["method"] == "POST"


class TestApiCommandAuth:
    """Bearer token is injected from config."""

    @pytest.mark.usefixtures("_mock_config")
    def test_injects_bearer_token(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["headers"] = kwargs.get("headers", {})
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api", "/healthz"])
        cli_module._cmd_api()

        assert captured["headers"]["Authorization"] == "Bearer test-token"


class TestApiCommandData:
    """The -d/--data flag sends a JSON body."""

    @pytest.mark.usefixtures("_mock_config")
    def test_sends_json_body_with_d_flag(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured.update(kwargs)
            return _fake_response(json_body={"created": True})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "resonance-api",
                "api",
                "POST",
                "/api/v1/calendar-feeds/songkick",
                "-d",
                '{"username": "mike"}',
            ],
        )
        cli_module._cmd_api()

        assert captured["json"] == {"username": "mike"}

    @pytest.mark.usefixtures("_mock_config")
    def test_sends_json_body_with_data_flag(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured.update(kwargs)
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "resonance-api",
                "api",
                "POST",
                "/admin/dedup-events",
                "--data",
                "{}",
            ],
        )
        cli_module._cmd_api()

        assert captured["json"] == {}

    @pytest.mark.usefixtures("_mock_config")
    def test_sends_raw_text_when_data_is_not_json(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured.update(kwargs)
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            ["resonance-api", "api", "POST", "/some/path", "-d", "plain text"],
        )
        cli_module._cmd_api()

        assert captured["content"] == "plain text"
        assert "json" not in captured


class TestApiCommandHeaders:
    """The -H flag adds extra headers."""

    @pytest.mark.usefixtures("_mock_config")
    def test_adds_extra_header(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["headers"] = kwargs.get("headers", {})
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "resonance-api",
                "api",
                "/healthz",
                "-H",
                "X-Custom: my-value",
            ],
        )
        cli_module._cmd_api()

        assert captured["headers"]["X-Custom"] == "my-value"
        assert captured["headers"]["Authorization"] == "Bearer test-token"

    @pytest.mark.usefixtures("_mock_config")
    def test_adds_multiple_extra_headers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["headers"] = kwargs.get("headers", {})
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "resonance-api",
                "api",
                "/healthz",
                "-H",
                "X-One: 1",
                "-H",
                "X-Two: 2",
            ],
        )
        cli_module._cmd_api()

        assert captured["headers"]["X-One"] == "1"
        assert captured["headers"]["X-Two"] == "2"


class TestApiCommandOutput:
    """JSON responses are pretty-printed; errors show status."""

    @pytest.mark.usefixtures("_mock_config")
    def test_pretty_prints_json_on_success(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            return _fake_response(json_body={"status": "ok", "count": 42})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api", "/healthz"])
        cli_module._cmd_api()

        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed == {"status": "ok", "count": 42}
        assert "  " in out  # indented

    @pytest.mark.usefixtures("_mock_config")
    def test_shows_status_and_body_on_error(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            return _fake_response(status_code=404, json_body={"detail": "Not found"})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api", "/bad/path"])

        with pytest.raises(SystemExit) as exc_info:
            cli_module._cmd_api()
        assert exc_info.value.code == 1

        captured = capsys.readouterr()
        assert "404" in captured.err
        assert "Not found" in captured.out

    @pytest.mark.usefixtures("_mock_config")
    def test_prints_raw_text_when_not_json(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            return _fake_response(text="OK")

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api", "/healthz"])
        cli_module._cmd_api()

        out = capsys.readouterr().out
        assert "OK" in out


class TestApiCommandHelp:
    """Shows usage when no arguments provided."""

    def test_no_args_shows_usage(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api"])

        with pytest.raises(SystemExit) as exc_info:
            cli_module._cmd_api()
        assert exc_info.value.code == 0

        out = capsys.readouterr().out
        assert "Usage:" in out


class TestApiCommandErrors:
    """Connection and timeout errors handled gracefully."""

    @pytest.mark.usefixtures("_mock_config")
    def test_connection_error(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            raise httpx.ConnectError("Connection refused")

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api", "/healthz"])

        with pytest.raises(SystemExit) as exc_info:
            cli_module._cmd_api()
        assert exc_info.value.code == 1

        out = capsys.readouterr().out
        assert "Could not connect" in out

    @pytest.mark.usefixtures("_mock_config")
    def test_timeout_error(
        self, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
    ) -> None:
        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            raise httpx.TimeoutException("timed out")

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "api", "/healthz"])

        with pytest.raises(SystemExit) as exc_info:
            cli_module._cmd_api()
        assert exc_info.value.code == 1

        out = capsys.readouterr().out
        assert "timed out" in out


class TestApiCommandRegistration:
    """The api command is registered in the CLI dispatch table."""

    def test_api_in_commands_dict(self) -> None:
        assert "api" in cli_module._COMMANDS

    def test_api_in_usage_text(self) -> None:
        assert "api" in cli_module._USAGE
