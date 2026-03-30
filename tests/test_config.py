import resonance.config as config_module


def test_settings_has_database_url() -> None:
    """Settings should expose DATABASE_URL with a default for local dev."""
    settings = config_module.Settings()
    assert hasattr(settings, "database_url")
    assert isinstance(settings.database_url, str)


def test_settings_has_redis_url() -> None:
    settings = config_module.Settings()
    assert hasattr(settings, "redis_url")
    assert isinstance(settings.redis_url, str)


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
