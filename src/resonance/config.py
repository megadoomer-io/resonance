import pydantic_settings

# Placeholder values shipped as defaults. Safe for local dev, unsafe in
# production — ensure_secure_secrets() refuses to start on them (#141, finding #4).
_PLACEHOLDER_SECRET = "change-me-in-production"
_DEFAULT_PGPASSWORD = "resonance"


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

    # ListenBrainz user token + MBID backfill (#71)
    # lb_user_token authenticates the /1/metadata/lookup mapper endpoint.
    lb_user_token: str = ""
    mbid_mapper_batch_size: int = 50
    mbid_match_min_similarity: float = 0.85

    # Last.fm API
    lastfm_api_key: str = ""
    lastfm_shared_secret: str = ""

    # Admin API token (for CLI/programmatic access)
    admin_api_token: str = ""

    # Allow the admin token to assume a user identity on user-scoped endpoints
    # (X-Assume-User header / ?as_user=) for agent-first live testing (#135).
    # The admin token is already omnipotent, so this is not a privilege
    # escalation; the flag exists so it can be disabled. Every assumption is
    # audit-logged regardless.
    admin_assume_user_enabled: bool = True

    # Dex OIDC (GitHub identity via Dex broker)
    dex_client_id: str = ""
    dex_client_secret: str = ""
    dex_issuer_url: str = ""
    dex_redirect_path: str = "/api/v1/auth/github/callback"

    @property
    def dex_redirect_uri(self) -> str:
        """Full Dex OIDC redirect URI."""
        return f"{self.base_url}{self.dex_redirect_path}"

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

    def ensure_secure_secrets(self) -> None:
        """Refuse to start in production with default/empty secrets (#141, #4).

        ``session_secret_key`` (session cookie signer) and
        ``token_encryption_key`` (Fernet key for stored OAuth tokens) shipping at
        their placeholder would mean publicly-known keys — session forgery and
        decryption of every stored token. ``pgpassword`` left at its default is
        the same class of slip. In ``debug`` (local dev) the defaults are
        tolerated so setup is frictionless; outside it, an unset secret is a
        startup error rather than a silent live exposure.

        Raises:
            RuntimeError: if any guarded secret is empty or still its default
                and ``debug`` is False.
        """
        if self.debug:
            return
        insecure = [
            name
            for name, value, default in (
                ("SESSION_SECRET_KEY", self.session_secret_key, _PLACEHOLDER_SECRET),
                (
                    "TOKEN_ENCRYPTION_KEY",
                    self.token_encryption_key,
                    _PLACEHOLDER_SECRET,
                ),
                ("PGPASSWORD", self.pgpassword, _DEFAULT_PGPASSWORD),
            )
            if not value or value == default
        ]
        if insecure:
            raise RuntimeError(
                "Refusing to start: "
                + ", ".join(insecure)
                + " must be overridden from the default in production "
                "(set DEBUG=true for local development)."
            )
