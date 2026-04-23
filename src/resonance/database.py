from typing import TYPE_CHECKING

import sqlalchemy.ext.asyncio as sa_async

if TYPE_CHECKING:
    import resonance.config as config_module


def create_async_engine(settings: config_module.Settings) -> sa_async.AsyncEngine:
    """Create an async SQLAlchemy engine from settings."""
    return sa_async.create_async_engine(
        settings.database_url,
        echo=settings.debug,
        pool_pre_ping=True,
    )


def create_session_factory(
    engine: sa_async.AsyncEngine,
) -> sa_async.async_sessionmaker[sa_async.AsyncSession]:
    """Create an async session factory bound to the given engine."""
    return sa_async.async_sessionmaker(engine, expire_on_commit=False)
