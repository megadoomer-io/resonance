"""Tests for the generic `resonance-api api` CLI command."""

from __future__ import annotations

import json
import os
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


class TestBackfillMbidsCommand:
    """Tests for `resonance-api backfill-mbids` (#71)."""

    def test_registered(self) -> None:
        assert "backfill-mbids" in cli_module._COMMANDS

    def test_status_does_get(
        self, monkeypatch: pytest.MonkeyPatch, _mock_config: None
    ) -> None:
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "backfill-mbids", "--status"]
        )
        captured: dict[str, Any] = {}

        def fake_req(method: str, path: str, **_: Any) -> httpx.Response:
            captured["method"], captured["path"] = method, path
            return _fake_response(json_body={"track": {"by_status": {"matched": 5}}})

        monkeypatch.setattr(cli_module, "_api_request", fake_req)
        cli_module._cmd_backfill_mbids()
        assert captured["method"] == "GET"
        assert captured["path"] == "/api/v1/admin/backfill-mbids"

    def test_retry_no_wait_posts_with_query(
        self, monkeypatch: pytest.MonkeyPatch, _mock_config: None
    ) -> None:
        monkeypatch.setattr(
            sys,
            "argv",
            ["resonance-api", "backfill-mbids", "--retry", "--no-wait"],
        )
        captured: dict[str, Any] = {}

        def fake_req(method: str, path: str, **_: Any) -> httpx.Response:
            captured["method"], captured["path"] = method, path
            return _fake_response(json_body={"task_id": "T1", "status": "started"})

        monkeypatch.setattr(cli_module, "_api_request", fake_req)
        cli_module._cmd_backfill_mbids()
        assert captured["method"] == "POST"
        assert "retry=true" in captured["path"]

    def test_tracks_only_sets_entity_types(
        self, monkeypatch: pytest.MonkeyPatch, _mock_config: None
    ) -> None:
        monkeypatch.setattr(
            sys,
            "argv",
            ["resonance-api", "backfill-mbids", "--tracks", "--no-wait"],
        )
        captured: dict[str, Any] = {}

        def fake_req(method: str, path: str, **_: Any) -> httpx.Response:
            captured["path"] = path
            return _fake_response(json_body={"task_id": "T1", "status": "started"})

        monkeypatch.setattr(cli_module, "_api_request", fake_req)
        cli_module._cmd_backfill_mbids()
        assert "entity_types=track" in captured["path"]


class TestParsePoolSources:
    """Tests for the layered --source / --exclude grammar (#128 T8)."""

    def test_no_flags_returns_none(self) -> None:
        # No --source/--exclude -> caller falls back to legacy --input.
        assert cli_module._parse_pool_sources(["--input", "event_id=abc"]) is None

    def test_event_source(self) -> None:
        result = cli_module._parse_pool_sources(["--source", "event:e1"])
        assert result == {
            "sources": [{"kind": "event", "event_id": "e1", "enabled": True}],
            "exclude_artist_ids": [],
        }

    def test_artist_source(self) -> None:
        result = cli_module._parse_pool_sources(["--source", "artist:a1"])
        assert result is not None
        assert result["sources"] == [
            {"kind": "artist", "artist_id": "a1", "enabled": True}
        ]

    def test_related_source_default_seed(self) -> None:
        result = cli_module._parse_pool_sources(["--source", "related:5"])
        assert result is not None
        assert result["sources"] == [
            {"kind": "related", "amount": 5, "seed": "target", "enabled": True}
        ]

    def test_related_source_explicit_seed(self) -> None:
        result = cli_module._parse_pool_sources(["--source", "related:3:target"])
        assert result is not None
        assert result["sources"][0] == {
            "kind": "related",
            "amount": 3,
            "seed": "target",
            "enabled": True,
        }

    def test_multiple_sources_and_excludes(self) -> None:
        result = cli_module._parse_pool_sources(
            [
                "--source",
                "event:e1",
                "--source",
                "artist:a1",
                "--exclude",
                "x1",
                "--exclude",
                "x2",
            ]
        )
        assert result is not None
        assert len(result["sources"]) == 2  # type: ignore[arg-type]
        assert result["exclude_artist_ids"] == ["x1", "x2"]

    def test_exclude_only_returns_dict(self) -> None:
        # --exclude alone still opts into the layered shape (empty sources).
        result = cli_module._parse_pool_sources(["--exclude", "x1"])
        assert result == {"sources": [], "exclude_artist_ids": ["x1"]}

    def test_unknown_kind_exits(self) -> None:
        with pytest.raises(SystemExit):
            cli_module._parse_pool_sources(["--source", "bogus:1"])

    def test_related_non_integer_exits(self) -> None:
        with pytest.raises(SystemExit):
            cli_module._parse_pool_sources(["--source", "related:lots"])


class TestApiCommandAsUser:
    """The global --as-user flag sends X-Assume-User and is stripped from argv."""

    _UID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

    @pytest.mark.usefixtures("_mock_config")
    def test_as_user_space_form_sets_header(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["headers"] = kwargs.get("headers", {})
            captured["url"] = url
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "--as-user", self._UID, "api", "/healthz"]
        )
        try:
            cli_module.api()
        finally:
            monkeypatch.delenv("RESONANCE_ASSUME_USER", raising=False)

        assert captured["headers"]["X-Assume-User"] == self._UID
        # Command still parsed correctly after the flag was stripped.
        assert captured["url"].endswith("/healthz")

    @pytest.mark.usefixtures("_mock_config")
    def test_as_user_equals_form_sets_header(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_request(m: str, url: str, **kwargs: Any) -> httpx.Response:
            captured["headers"] = kwargs.get("headers", {})
            return _fake_response(json_body={})

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", f"--as-user={self._UID}", "api", "/healthz"]
        )
        try:
            cli_module.api()
        finally:
            monkeypatch.delenv("RESONANCE_ASSUME_USER", raising=False)

        assert captured["headers"]["X-Assume-User"] == self._UID

    def test_extract_strips_argv_and_sets_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "--as-user", self._UID, "status"]
        )
        try:
            cli_module._extract_as_user()
            assert sys.argv == ["resonance-api", "status"]
            assert os.environ["RESONANCE_ASSUME_USER"] == self._UID
        finally:
            monkeypatch.delenv("RESONANCE_ASSUME_USER", raising=False)

    def test_no_flag_leaves_argv_untouched(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("RESONANCE_ASSUME_USER", raising=False)
        monkeypatch.setattr(sys, "argv", ["resonance-api", "status"])
        cli_module._extract_as_user()
        assert sys.argv == ["resonance-api", "status"]
        assert "RESONANCE_ASSUME_USER" not in os.environ


class TestEnrichCommand:
    """resonance-api enrich builds the right request and reports results."""

    @pytest.mark.usefixtures("_mock_config")
    def test_lineup_posts_lineup_scope(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[dict[str, Any]] = []

        def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
            calls.append({"method": method, "url": url, "json": kwargs.get("json")})
            if url.endswith("/enrich"):
                return _fake_response(json_body={"status": "started", "task_id": "t1"})
            return _fake_response(
                json_body={
                    "status": "completed",
                    "result": {"found": 4, "requested": 5},
                }
            )

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "enrich", "p1", "--lineup", "--n", "5"]
        )
        cli_module._cmd_enrich()

        post = calls[0]
        assert post["method"] == "POST"
        assert post["url"].endswith("/api/v1/generator-profiles/p1/enrich")
        assert post["json"] == {"n": 5, "seed_artist_ids": "lineup"}

    @pytest.mark.usefixtures("_mock_config")
    def test_seeds_post_id_list(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: list[dict[str, Any]] = []

        def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
            calls.append({"json": kwargs.get("json")})
            if url.endswith("/enrich"):
                return _fake_response(json_body={"task_id": "t2"})
            return _fake_response(
                json_body={
                    "status": "completed",
                    "result": {"found": 2, "requested": 2},
                }
            )

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            ["resonance-api", "enrich", "p1", "--seed", "a1", "--seed", "a2"],
        )
        cli_module._cmd_enrich()

        assert calls[0]["json"] == {"n": 10, "seed_artist_ids": ["a1", "a2"]}

    @pytest.mark.usefixtures("_mock_config")
    def test_requires_lineup_or_seed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sys, "argv", ["resonance-api", "enrich", "p1"])
        with pytest.raises(SystemExit):
            cli_module._cmd_enrich()

    @pytest.mark.usefixtures("_mock_config")
    def test_rejects_lineup_and_seed_together(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "enrich", "p1", "--lineup", "--seed", "a1"]
        )
        with pytest.raises(SystemExit):
            cli_module._cmd_enrich()


class TestProfileExcludeTrack:
    """`profile exclude-track`: append to exclude_track_ids and PATCH."""

    @pytest.mark.usefixtures("_mock_config")
    def test_appends_to_existing_excludes_and_preserves_refs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        existing_track = "11111111-1111-1111-1111-111111111111"
        new_track = "22222222-2222-2222-2222-222222222222"
        source = {"kind": "artist", "artist_id": "a1", "enabled": True}
        calls: list[dict[str, Any]] = []

        def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
            calls.append({"method": method, "url": url, "json": kwargs.get("json")})
            if method == "GET":
                return _fake_response(
                    json_body={
                        "id": "p1",
                        "name": "P",
                        "generator_type": "concert_prep",
                        "input_references": {
                            "sources": [source],
                            "exclude_track_ids": [existing_track],
                        },
                    }
                )
            # PATCH
            return _fake_response(
                json_body={"id": "p1", "name": "P", "generator_type": "concert_prep"}
            )

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            ["resonance-api", "profile", "exclude-track", "p1", new_track],
        )
        cli_module._cmd_profile()

        # A GET then a PATCH, no generate (no --regenerate).
        assert [c["method"] for c in calls] == ["GET", "PATCH"]
        patched_refs = calls[1]["json"]["input_references"]
        # New track appended; existing exclude preserved; sources untouched.
        assert patched_refs["exclude_track_ids"] == [existing_track, new_track]
        assert patched_refs["sources"] == [source]

    @pytest.mark.usefixtures("_mock_config")
    def test_dedupes_already_excluded_track(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        track = "11111111-1111-1111-1111-111111111111"
        calls: list[dict[str, Any]] = []

        def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
            calls.append({"method": method, "json": kwargs.get("json")})
            if method == "GET":
                return _fake_response(
                    json_body={
                        "id": "p1",
                        "name": "P",
                        "generator_type": "concert_prep",
                        "input_references": {"exclude_track_ids": [track]},
                    }
                )
            return _fake_response(
                json_body={"id": "p1", "name": "P", "generator_type": "concert_prep"}
            )

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys, "argv", ["resonance-api", "profile", "exclude-track", "p1", track]
        )
        cli_module._cmd_profile()

        # Re-excluding the same track does not duplicate it.
        assert calls[1]["json"]["input_references"]["exclude_track_ids"] == [track]

    @pytest.mark.usefixtures("_mock_config")
    def test_regenerate_flag_triggers_generate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        track = "22222222-2222-2222-2222-222222222222"
        calls: list[dict[str, Any]] = []

        def fake_request(method: str, url: str, **kwargs: Any) -> httpx.Response:
            calls.append({"method": method, "url": url})
            if method == "GET" and url.endswith("/p1"):
                return _fake_response(
                    json_body={
                        "id": "p1",
                        "name": "P",
                        "generator_type": "concert_prep",
                        "input_references": {},
                    }
                )
            if method == "POST":
                return _fake_response(json_body={"task_id": "t1"})
            if method == "GET":  # _poll_task status poll
                return _fake_response(
                    json_body={
                        "status": "completed",
                        "result": {"playlist_id": "pl1"},
                    }
                )
            return _fake_response(json_body={"id": "p1"})  # PATCH

        monkeypatch.setattr(httpx, "request", fake_request)
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "resonance-api",
                "profile",
                "exclude-track",
                "p1",
                track,
                "--regenerate",
            ],
        )
        cli_module._cmd_profile()

        methods = [c["method"] for c in calls]
        # GET profile -> PATCH excludes -> POST generate -> GET poll(s).
        assert methods[0] == "GET"
        assert "PATCH" in methods
        assert "POST" in methods
