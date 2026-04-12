import pydantic_settings


class Settings(pydantic_settings.BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "resonance"
    debug: bool = False
    log_level: str = "INFO"

    # Database (standard PG env vars)
    pghost: str = "localhost"
    pgport: int = 5432
    pguser: str = "resonance"
    pgpassword: str = "resonance"
    pgdatabase: str = "resonance"

    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""

    # Session
    session_secret_key: str = "change-me-in-production"

    # Token encryption (Fernet key)
    # Generate with: python -c "from cryptography.fernet import Fernet; ..."
    token_encryption_key: str = "change-me-in-production"

    # Base URL for constructing OAuth redirect URIs
    base_url: str = "http://localhost:8000"

    # Spotify OAuth
    spotify_client_id: str = ""
    spotify_client_secret: str = ""
    spotify_redirect_path: str = "/api/v1/auth/spotify/callback"

    # Worker mode
    worker_mode: str = "external"  # "external" (prod) or "inline" (dev)

    # MusicBrainz OAuth (for ListenBrainz auth)
    musicbrainz_client_id: str = ""
    musicbrainz_client_secret: str = ""
    musicbrainz_redirect_path: str = "/api/v1/auth/listenbrainz/callback"

    # Last.fm API
    lastfm_api_key: str = ""
    lastfm_shared_secret: str = ""

    # Admin API token (for CLI/programmatic access)
    admin_api_token: str = ""

    @property
    def spotify_redirect_uri(self) -> str:
        """Full Spotify OAuth redirect URI."""
        return f"{self.base_url}{self.spotify_redirect_path}"

    @property
    def musicbrainz_redirect_uri(self) -> str:
        """Full MusicBrainz OAuth redirect URI."""
        return f"{self.base_url}{self.musicbrainz_redirect_path}"

    @property
    def database_url(self) -> str:
        """Async PostgreSQL URL for SQLAlchemy."""
        return f"postgresql+asyncpg://{self.pguser}:{self.pgpassword}@{self.pghost}:{self.pgport}/{self.pgdatabase}"

    @property
    def sync_database_url(self) -> str:
        """Sync PostgreSQL URL for Alembic."""
        return f"postgresql://{self.pguser}:{self.pgpassword}@{self.pghost}:{self.pgport}/{self.pgdatabase}"

    @property
    def redis_url(self) -> str:
        """Redis connection URL."""
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/0"
