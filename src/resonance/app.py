from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import fastapi
import redis.asyncio as aioredis

import resonance.config as config_module
import resonance.database as database_module


@asynccontextmanager
async def lifespan(application: fastapi.FastAPI) -> AsyncIterator[None]:
    """Manage application lifecycle — database engine, Redis pool."""
    settings: config_module.Settings = application.state.settings
    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)
    redis_pool = aioredis.from_url(settings.redis_url, decode_responses=True)

    application.state.engine = engine
    application.state.session_factory = session_factory
    application.state.redis = redis_pool

    yield

    await redis_pool.aclose()
    await engine.dispose()


def create_app() -> fastapi.FastAPI:
    """Create and configure the FastAPI application."""
    settings = config_module.Settings()
    application = fastapi.FastAPI(
        title=settings.app_name,
        docs_url="/docs" if settings.debug else None,
        redoc_url=None,
        lifespan=lifespan,
    )
    application.state.settings = settings

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return application
