import resonance.config as config_module


def test_database_url_constructed_from_components() -> None:
    """database_url property should build async URL from PG env vars."""
    settings = config_module.Settings()
    assert "postgresql+asyncpg://" in settings.database_url
    assert "resonance" in settings.database_url
    assert "localhost" in settings.database_url


def test_sync_database_url_uses_sync_driver() -> None:
    """sync_database_url should use psycopg2 driver, not asyncpg."""
    settings = config_module.Settings()
    assert settings.sync_database_url.startswith("postgresql://")
    assert "asyncpg" not in settings.sync_database_url


def test_redis_url_constructed_from_components() -> None:
    """redis_url property should build URL from host/port/password."""
    settings = config_module.Settings()
    assert "redis://" in settings.redis_url
    assert "localhost" in settings.redis_url


def test_redis_url_includes_password_when_set() -> None:
    settings = config_module.Settings(redis_password="secret")
    assert ":secret@" in settings.redis_url


def test_redis_url_omits_auth_when_no_password() -> None:
    settings = config_module.Settings(redis_password="")
    assert "@" not in settings.redis_url


def test_settings_has_session_secret_key() -> None:
    settings = config_module.Settings()
    assert hasattr(settings, "session_secret_key")


def test_settings_has_token_encryption_key() -> None:
    settings = config_module.Settings()
    assert hasattr(settings, "token_encryption_key")


def test_settings_has_spotify_credentials() -> None:
    settings = config_module.Settings()
    assert hasattr(settings, "spotify_client_id")
    assert hasattr(settings, "spotify_client_secret")
    assert hasattr(settings, "spotify_redirect_uri")


def test_settings_has_musicbrainz_credentials() -> None:
    settings = config_module.Settings()
    assert hasattr(settings, "musicbrainz_client_id")
    assert hasattr(settings, "musicbrainz_client_secret")
    assert hasattr(settings, "musicbrainz_redirect_uri")
