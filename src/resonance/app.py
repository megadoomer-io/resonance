import fastapi

import resonance.config as config_module


def create_app() -> fastapi.FastAPI:
    """Create and configure the FastAPI application."""
    settings = config_module.Settings()
    application = fastapi.FastAPI(
        title=settings.app_name,
        docs_url="/docs" if settings.debug else None,
        redoc_url=None,
    )

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return application
