import pydantic_settings


class Settings(pydantic_settings.BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "resonance"
    debug: bool = False
