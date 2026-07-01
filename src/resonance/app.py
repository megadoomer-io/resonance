from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

import pathlib

import arq.connections as arq_connections
import fastapi
import fastapi.staticfiles
import redis.asyncio as aioredis
import sqlalchemy as sa
import structlog

import resonance.api.v1 as api_v1_module
import resonance.config as config_module
import resonance.connectors.concert_archives as concert_archives_module
import resonance.connectors.github as github_module
import resonance.connectors.ical as ical_module
import resonance.connectors.lastfm as lastfm_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.registry as registry_module
import resonance.connectors.songkick as songkick_module
import resonance.connectors.spotify as spotify_module
import resonance.connectors.test as test_connector_module
import resonance.database as database_module
import resonance.logging as logging_module
import resonance.middleware.rate_limit as rate_limit_module
import resonance.middleware.security_headers as security_headers_module
import resonance.middleware.session as session_middleware
import resonance.migrations as migrations_module
import resonance.models.task as task_models
import resonance.types as types_module
import resonance.ui.account as ui_account_module
import resonance.ui.admin as ui_admin_module
import resonance.ui.artists as ui_artists_module
import resonance.ui.common as ui_common_module
import resonance.ui.dashboard as ui_dashboard_module
import resonance.ui.events as ui_events_module
import resonance.ui.playground as ui_playground_module
import resonance.ui.playlists as ui_playlists_module
import resonance.ui.sync as ui_sync_module
import resonance.ui.tracks as ui_tracks_module

logger = structlog.get_logger()


@asynccontextmanager
async def lifespan(application: fastapi.FastAPI) -> AsyncIterator[None]:
    """Manage application lifecycle — database engine, Redis pool."""
    settings: config_module.Settings = application.state.settings
    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)

    # Fail fast if the DB schema is behind this image's migrations, rather than
    # serving new code against an old schema (see resonance.migrations).
    await migrations_module.assert_schema_current(engine)

    redis_pool = aioredis.from_url(settings.redis_url, decode_responses=True)  # type: ignore[no-untyped-call]  # redis 5.x lacks stubs

    arq_redis = await arq_connections.create_pool(
        arq_connections.RedisSettings(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password or None,
        )
    )

    application.state.engine = engine
    application.state.session_factory = session_factory
    application.state.redis = redis_pool
    application.state.arq_redis = arq_redis

    # Reset RUNNING tasks back to PENDING (interrupted by pod restart)
    async with session_factory() as db:
        result = await db.execute(
            sa.update(task_models.Task)
            .where(task_models.Task.status == types_module.SyncStatus.RUNNING)
            .values(
                status=types_module.SyncStatus.PENDING,
                started_at=None,
            )
        )
        row_count = result.rowcount if hasattr(result, "rowcount") else 0
        if row_count:
            logger.info("Reset %d interrupted sync tasks back to pending", row_count)
        await db.commit()

    yield

    await arq_redis.aclose()
    await redis_pool.aclose()
    await engine.dispose()


def create_app(settings: config_module.Settings | None = None) -> fastapi.FastAPI:
    """Create and configure the FastAPI application.

    Args:
        settings: Optional settings override (tests inject a dev-mode config);
            defaults to loading from the environment.
    """
    if settings is None:
        settings = config_module.Settings()
    # Fail fast on placeholder secrets in production (#141, finding #4).
    settings.ensure_secure_secrets()
    logging_module.configure_logging(settings.log_level)
    # Swagger UI and the raw OpenAPI spec are dev-only (#141, finding #9):
    # exposed in debug, withheld in production to avoid endpoint enumeration.
    application = fastapi.FastAPI(
        title=settings.app_name,
        docs_url="/docs" if settings.debug else None,
        openapi_url="/openapi.json" if settings.debug else None,
        redoc_url=None,
        lifespan=lifespan,
    )
    application.state.settings = settings

    # Security headers + CSP on every response (#141, finding #8).
    application.add_middleware(
        security_headers_module.SecurityHeadersMiddleware,
        csp=security_headers_module.DEFAULT_CSP,
    )

    session_redis = aioredis.from_url(settings.redis_url, decode_responses=True)  # type: ignore[no-untyped-call]  # redis 5.x lacks stubs
    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=session_redis,
        secret_key=settings.session_secret_key,
        secure=not settings.debug,
    )

    # Per-IP rate limiting on auth + admin paths (#141, finding #10). Added after
    # session so it sits outermost and rejects abusive clients early.
    if settings.rate_limit_enabled:
        application.add_middleware(
            rate_limit_module.RateLimitMiddleware,
            redis=session_redis,
        )

    # Register API routes
    application.include_router(api_v1_module.router)

    # Register UI routes
    application.include_router(ui_account_module.router)
    application.include_router(ui_admin_module.router)
    application.include_router(ui_artists_module.router)
    application.include_router(ui_dashboard_module.router)
    application.include_router(ui_events_module.router)
    # The component playground is a dev tool; don't expose it in production at
    # all (#141, finding #11). It's also admin-gated for defense in depth.
    if settings.debug:
        application.include_router(ui_playground_module.router)
    application.include_router(ui_playlists_module.router)
    application.include_router(ui_sync_module.router)
    application.include_router(ui_tracks_module.router)

    # Serve static assets (CSS, JS)
    _static_dir = pathlib.Path(__file__).resolve().parent / "static"
    application.mount(
        "/static",
        fastapi.staticfiles.StaticFiles(directory=str(_static_dir)),
        name="static",
    )

    # Set up connector registry
    connector_registry = registry_module.ConnectorRegistry()
    connector_registry.register(spotify_module.SpotifyConnector(settings=settings))
    connector_registry.register(
        listenbrainz_module.ListenBrainzConnector(settings=settings)
    )
    connector_registry.register(lastfm_module.LastFmConnector(settings=settings))
    connector_registry.register(test_connector_module.TestConnector())
    connector_registry.register(songkick_module.SongkickConnector())
    connector_registry.register(ical_module.ICalConnector())
    connector_registry.register(concert_archives_module.ConcertArchivesConnector())
    if settings.dex_client_id:
        connector_registry.register(github_module.GitHubConnector(settings=settings))
    application.state.connector_registry = connector_registry
    ui_common_module.set_connector_registry(connector_registry)

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        import os

        return {"status": "ok", "revision": os.environ.get("GIT_SHA", "dev")}

    return application
