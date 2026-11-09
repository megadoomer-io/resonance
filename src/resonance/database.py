from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm

if TYPE_CHECKING:
    import resonance.config as config_module


def create_async_engine(
    settings: config_module.Settings,
    *,
    server_settings: dict[str, str] | None = None,
) -> sa_async.AsyncEngine:
    """Create an async SQLAlchemy engine from settings.

    Args:
        settings: Application settings.
        server_settings: Postgres session parameters set on every connection
            the engine opens (asyncpg ``server_settings``).
    """
    connect_args: dict[str, object] = {}
    if server_settings:
        connect_args["server_settings"] = dict(server_settings)
    return sa_async.create_async_engine(
        settings.database_url,
        echo=settings.debug,
        pool_pre_ping=True,
        connect_args=connect_args,
    )


def create_session_factory(
    engine: sa_async.AsyncEngine,
) -> sa_async.async_sessionmaker[sa_async.AsyncSession]:
    """Create an async session factory bound to the given engine."""
    return sa_async.async_sessionmaker(engine, expire_on_commit=False)


# ---------------------------------------------------------------------------
# Shared query helpers
# ---------------------------------------------------------------------------


def escape_ilike(q: str) -> str:
    """Escape ``%`` and ``_`` for safe use in ILIKE patterns."""
    return q.replace("%", r"\%").replace("_", r"\_")


async def count_rows(
    db: sa_async.AsyncSession,
    model: type[sa_orm.DeclarativeBase],
    *filters: sa.ColumnElement[bool],
) -> int:
    """Return the row count for *model*, optionally filtered."""
    stmt = sa.select(sa.func.count()).select_from(model)
    for f in filters:
        stmt = stmt.where(f)
    result = await db.execute(stmt)
    return int(result.scalar_one())
