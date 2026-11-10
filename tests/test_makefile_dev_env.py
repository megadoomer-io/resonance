"""The dev-* Make targets honor ENV and friends only from the command line."""

from __future__ import annotations

import os
import pathlib
import shutil
import subprocess

import pytest

_REPO = pathlib.Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.skipif(shutil.which("make") is None, reason="needs make")


def _dry_run(
    *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", "-n", "-C", str(_REPO), *args],
        capture_output=True,
        text=True,
        env={**os.environ, **(env or {})},
        check=False,
    )


def test_empty_env_on_command_line_is_an_error() -> None:
    result = _dry_run("dev-reset", "ENV=")
    assert result.returncode != 0
    assert "ENV is empty" in result.stderr
    assert "docker-compose" not in result.stdout


def test_exported_env_does_not_select_a_named_env() -> None:
    result = _dry_run("dev-reset", env={"ENV": "foo", "PORT": "1", "NAME": "n"})
    assert result.returncode == 0
    assert "docker-compose down -v" in result.stdout
    assert "dev-env.sh" not in result.stdout


def test_exported_env_does_not_satisfy_required_env() -> None:
    result = _dry_run("dev-app", env={"ENV": "foo", "REF": "main"})
    assert result.returncode != 0
    assert "ENV=<name> is required" in result.stderr


def test_command_line_vars_reach_the_script() -> None:
    result = _dry_run("dev-app", "ENV=x", "REF=main", "NAME=n", "PORT=9")
    assert result.returncode == 0
    assert 'dev-env.sh app x "main" "n" "9"' in result.stdout


def test_exported_port_is_ignored() -> None:
    result = _dry_run("dev-app", "ENV=x", "REF=main", env={"PORT": "1"})
    assert 'dev-env.sh app x "main" "app" ""' in result.stdout
