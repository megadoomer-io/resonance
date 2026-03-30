from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

import fastapi
import redis.asyncio as aioredis

import resonance.api.v1 as api_v1_module
import resonance.config as config_module
import resonance.connectors.registry as registry_module
import resonance.connectors.spotify as spotify_module
import resonance.database as database_module
import resonance.middleware.session as session_middleware
import resonance.ui.routes as ui_routes_module


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

    session_redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=session_redis,
        secret_key=settings.session_secret_key,
    )

    # Register API routes
    application.include_router(api_v1_module.router)

    # Register UI routes
    application.include_router(ui_routes_module.router)

    # Set up connector registry
    connector_registry = registry_module.ConnectorRegistry()
    connector_registry.register(spotify_module.SpotifyConnector(settings=settings))
    application.state.connector_registry = connector_registry

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return application
