import pydantic_settings


class Settings(pydantic_settings.BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "resonance"
    debug: bool = False

    # Database
    database_url: str = (
        "postgresql+asyncpg://resonance:resonance@localhost:5432/resonance"
    )

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Session
    session_secret_key: str = "change-me-in-production"

    # Token encryption (Fernet key)
    # Generate with: python -c "from cryptography.fernet import Fernet; ..."
    token_encryption_key: str = "change-me-in-production"

    # Spotify OAuth
    spotify_client_id: str = ""
    spotify_client_secret: str = ""
    spotify_redirect_uri: str = "http://localhost:8000/api/v1/auth/spotify/callback"
