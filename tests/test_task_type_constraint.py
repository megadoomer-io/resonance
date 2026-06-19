"""Guard test: the sync_tasks.task_type CHECK constraint must list every TaskType.

#71 shipped TaskType.MBID_BACKFILL in code but the sync_tasks.task_type CHECK
constraint did not list it, so inserting a backfill task failed in production with
a check violation until a follow-up migration (f7g8h9i0j1k2) added it.

This test makes that class of regression fail in CI instead of prod: it finds the
head-most migration that (re)defines the ck_sync_tasks_task_type_tasktype
constraint, executes its upgrade() with a mocked Alembic op to capture the real
emitted SQL (so f-string/constant construction is exercised, not pattern-matched),
parses the CHECK (task_type IN (...)) value list, and asserts it equals the set of
TaskType enum names. Adding a TaskType without the matching constraint migration
breaks this test.
"""

from __future__ import annotations

import importlib.util
import pathlib
import re
from typing import TYPE_CHECKING
from unittest import mock

import resonance.types as types_module

if TYPE_CHECKING:
    import types

VERSIONS_DIR = pathlib.Path(__file__).resolve().parent.parent / "alembic" / "versions"

CONSTRAINT_NAME = "ck_sync_tasks_task_type_tasktype"

# Matches the value list inside `CHECK (task_type IN ( ... ))`, tolerant of
# whitespace and casing, across the emitted SQL string.
_IN_LIST_RE = re.compile(
    r"task_type\s+IN\s*\((?P<values>[^)]*)\)", re.IGNORECASE | re.DOTALL
)
_TOKEN_RE = re.compile(r"'([^']+)'")
_REVISION_RE = re.compile(r"^revision(?::\s*[^=]+)?\s*=\s*['\"]([^'\"]+)['\"]", re.M)
_DOWN_REVISION_RE = re.compile(
    r"^down_revision(?::\s*[^=]+)?\s*=\s*['\"]([^'\"]+)['\"]", re.M
)


def _migration_files() -> list[pathlib.Path]:
    return sorted(p for p in VERSIONS_DIR.glob("*.py") if not p.name.startswith("_"))


def _build_chain() -> dict[str, tuple[str | None, pathlib.Path]]:
    """Map revision -> (down_revision, path) for every migration file."""
    chain: dict[str, tuple[str | None, pathlib.Path]] = {}
    for path in _migration_files():
        src = path.read_text()
        rev_match = _REVISION_RE.search(src)
        if not rev_match:
            continue
        down_match = _DOWN_REVISION_RE.search(src)
        down = down_match.group(1) if down_match else None
        chain[rev_match.group(1)] = (down, path)
    return chain


def _head_order(chain: dict[str, tuple[str | None, pathlib.Path]]) -> list[str]:
    """Return revisions ordered head -> base by walking down_revision links."""
    downs = {down for down, _ in chain.values() if down}
    heads = [rev for rev in chain if rev not in downs]
    assert len(heads) == 1, f"expected a single migration head, found: {heads}"
    order: list[str] = []
    cur: str | None = heads[0]
    seen: set[str] = set()
    while cur is not None and cur in chain:
        if cur in seen:  # defensive: cycle guard
            break
        seen.add(cur)
        order.append(cur)
        cur = chain[cur][0]
    return order


def _load_module(path: pathlib.Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(f"_mig_{path.stem}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _captured_upgrade_sql(path: pathlib.Path) -> list[str]:
    """Run a migration's upgrade() with a mocked op, returning executed SQL strings."""
    module = _load_module(path)
    executed: list[str] = []

    fake_op = mock.MagicMock(name="op")
    fake_op.execute.side_effect = lambda clause, *a, **k: executed.append(str(clause))
    # Migrations do `from alembic import op`; rebinding the module global routes
    # op.* calls (execute, add_column, create_table, ...) through the mock.
    module.op = fake_op  # type: ignore[attr-defined]
    module.upgrade()
    return executed


def _latest_constraint_values() -> set[str]:
    chain = _build_chain()
    for rev in _head_order(chain):
        _, path = chain[rev]
        if CONSTRAINT_NAME not in path.read_text():
            continue
        for sql in _captured_upgrade_sql(path):
            if CONSTRAINT_NAME not in sql:
                continue
            match = _IN_LIST_RE.search(sql)
            if match:
                return set(_TOKEN_RE.findall(match.group("values")))
    raise AssertionError(
        f"no migration defines an upgrade CHECK constraint named {CONSTRAINT_NAME!r}"
    )


def test_task_type_check_constraint_covers_every_enum_value() -> None:
    """Every TaskType member must appear in the latest task_type CHECK constraint.

    SQLAlchemy stores StrEnum .name (UPPERCASE) for native_enum=False columns, so
    the constraint lists names like 'MBID_BACKFILL', which is what we compare to.
    """
    enum_names = {member.name for member in types_module.TaskType}
    constraint_values = _latest_constraint_values()

    missing = enum_names - constraint_values
    extra = constraint_values - enum_names
    assert not missing, (
        f"TaskType values missing from the {CONSTRAINT_NAME} CHECK constraint: "
        f"{sorted(missing)}. Add a migration extending the constraint."
    )
    assert not extra, (
        f"{CONSTRAINT_NAME} lists values with no matching TaskType member: "
        f"{sorted(extra)}. Remove them from the constraint or restore the enum member."
    )
