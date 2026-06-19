"""Tests for the startup schema guard (resonance.migrations)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

import resonance.migrations as migrations_module

if TYPE_CHECKING:
    from collections.abc import Callable


class _FakeConn:
    """Stands in for a sync connection; run_sync returns preset db heads."""

    def __init__(self, heads: set[str]) -> None:
        self._heads = heads

    async def run_sync(self, fn: Callable[[Any], set[str]]) -> set[str]:
        return self._heads


class _FakeConnCtx:
    def __init__(self, heads: set[str]) -> None:
        self._heads = heads

    async def __aenter__(self) -> _FakeConn:
        return _FakeConn(self._heads)

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakeEngine:
    """Minimal async-engine stand-in exposing connect() -> async context."""

    def __init__(self, db_heads: set[str]) -> None:
        self._db_heads = db_heads

    def connect(self) -> _FakeConnCtx:
        return _FakeConnCtx(self._db_heads)


def test_script_heads_nonempty() -> None:
    """The shipped migrations define at least one head revision."""
    assert migrations_module._script_heads()


def test_alembic_ini_is_locatable() -> None:
    """The guard can find alembic.ini from the package layout."""
    assert migrations_module._alembic_ini_path().is_file()


async def test_passes_when_db_at_head() -> None:
    """No error when applied heads match the code's heads."""
    heads = migrations_module._script_heads()
    await migrations_module.assert_schema_current(_FakeEngine(heads))  # type: ignore[arg-type]


async def test_raises_when_db_behind() -> None:
    """A fresh DB (no applied revisions) must fail fast."""
    with pytest.raises(migrations_module.SchemaOutOfDateError):
        await migrations_module.assert_schema_current(_FakeEngine(set()))  # type: ignore[arg-type]


async def test_raises_on_unknown_head() -> None:
    """An applied head the code doesn't know about must fail fast."""
    with pytest.raises(migrations_module.SchemaOutOfDateError):
        await migrations_module.assert_schema_current(_FakeEngine({"deadbeef"}))  # type: ignore[arg-type]
