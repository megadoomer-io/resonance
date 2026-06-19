"""Runtime guard: refuse to start if the DB schema is behind the code's migrations.

The #71 deploy hit a silent failure: new application code ran against a schema
missing its new columns because the migration step executed on a stale image
during an unrelated config sync (the coverage endpoint 500'd until migrations
were applied by hand). This guard turns that class of failure from a silent
runtime 500 into a loud fail-fast at startup — the new pod exits non-zero, the
rolling update stalls, and the previous (schema-compatible) pods keep serving.

This is the safety net that holds regardless of how or when migrations are
triggered (PreSync hook, init container, or manual upgrade).
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

import alembic.config
import alembic.runtime.migration as alembic_migration
import alembic.script
import structlog

if TYPE_CHECKING:
    import sqlalchemy as sa
    import sqlalchemy.ext.asyncio as sa_async

logger = structlog.get_logger()


class SchemaOutOfDateError(RuntimeError):
    """Raised when the database is not migrated to the code's head revision(s)."""


def _alembic_ini_path() -> pathlib.Path:
    """Locate alembic.ini, which sits at the project/app root next to alembic/.

    Walk up from this package so the check works both in the repo (``<repo>/``)
    and in the container (``/app``), where the layout is ``<root>/src/resonance``
    alongside ``<root>/alembic.ini``.
    """
    for base in pathlib.Path(__file__).resolve().parents:
        candidate = base / "alembic.ini"
        if candidate.is_file():
            return candidate
    raise SchemaOutOfDateError("could not locate alembic.ini for the schema check")


def _script_heads() -> set[str]:
    """Head revision(s) defined by the migration scripts shipped in this image."""
    cfg = alembic.config.Config(str(_alembic_ini_path()))
    script = alembic.script.ScriptDirectory.from_config(cfg)
    return set(script.get_heads())


def _db_heads(connection: sa.Connection) -> set[str]:
    """Revision(s) currently applied to the database (empty on a fresh DB)."""
    context = alembic_migration.MigrationContext.configure(connection=connection)
    return set(context.get_current_heads())


async def assert_schema_current(engine: sa_async.AsyncEngine) -> None:
    """Raise if the database is not migrated to the code's head revision(s).

    Compares the applied revisions to the migration scripts' heads. A mismatch
    means the DB is behind the code (or carries an unknown head), so the process
    must not start.

    Raises:
        SchemaOutOfDateError: when applied heads != code heads.
    """
    script_heads = _script_heads()
    async with engine.connect() as conn:
        db_heads = await conn.run_sync(_db_heads)

    if db_heads != script_heads:
        logger.error(
            "schema_out_of_date",
            db_heads=sorted(db_heads),
            code_heads=sorted(script_heads),
        )
        raise SchemaOutOfDateError(
            f"database schema is not current: applied heads {sorted(db_heads)} != "
            f"code heads {sorted(script_heads)}. Run 'alembic upgrade heads'."
        )

    logger.info("schema_current", heads=sorted(script_heads))
