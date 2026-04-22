# Data Foundation + Spotify Integration

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the data layer (PostgreSQL, Redis, SQLAlchemy models), authentication system (OAuth + sessions), and first service connector (Spotify) so that a user can log in via Spotify and sync their followed artists, saved tracks, and recent listening history.

**Architecture:** SQLAlchemy 2.0 async models over PostgreSQL (asyncpg), Redis-backed session middleware with signed cookies, pluggable connector framework with Spotify as the first implementation. Background sync tasks run via `asyncio.create_task()` with status tracking in a SyncJob model. Infrastructure deploys as Bitnami Helm charts alongside the existing app-template in the resonance namespace.

**Tech Stack:** Python 3.13, FastAPI, SQLAlchemy 2.0 (async), asyncpg, PostgreSQL (Bitnami Helm), Redis (Bitnami Helm), Alembic, cryptography (Fernet), httpx, itsdangerous

---

### Task 1: Dependencies + Config

**Files:**
- Modify: `pyproject.toml`
- Modify: `src/resonance/config.py`
- Test: `tests/test_config.py`

**Step 1: Add dependencies to pyproject.toml**

Add to `dependencies`:

```toml
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.34",
    "pydantic-settings>=2.7",
    "alembic>=1.18.4",
    "sqlalchemy[asyncio]>=2.0",
    "asyncpg>=0.30",
    "redis[hiredis]>=5.2",
    "cryptography>=44.0",
    "httpx>=0.28",
    "itsdangerous>=2.2",
]
```

Remove `httpx` from `[project.optional-dependencies] dev` (it moved to main dependencies).

Add sqlalchemy mypy plugin to `[tool.mypy]`:

```toml
[tool.mypy]
strict = true
warn_return_any = true
disallow_untyped_defs = true
plugins = ["pydantic.mypy", "sqlalchemy.ext.mypy.plugin"]
```

**Step 2: Lock dependencies**

Run: `uv sync`
Expected: `uv.lock` updated with new dependencies.

**Step 3: Write the failing test for expanded config**

Create `tests/test_config.py`:

```python
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
```

**Step 4: Run tests to verify they fail**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL — `AttributeError` for missing fields.

**Step 5: Expand Settings class**

Update `src/resonance/config.py`:

```python
import pydantic_settings


class Settings(pydantic_settings.BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "resonance"
    debug: bool = False

    # Database
    database_url: str = "postgresql+asyncpg://resonance:resonance@localhost:5432/resonance"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Session
    session_secret_key: str = "change-me-in-production"

    # Token encryption (Fernet key — generate with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
    token_encryption_key: str = "change-me-in-production"

    # Spotify OAuth
    spotify_client_id: str = ""
    spotify_client_secret: str = ""
    spotify_redirect_uri: str = "http://localhost:8000/api/v1/auth/spotify/callback"
```

**Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS

**Step 7: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 8: Commit**

```bash
git add pyproject.toml uv.lock src/resonance/config.py tests/test_config.py
git commit -m "chore: add data layer dependencies and expand config settings"
```

---

### Task 2: SQLAlchemy Base + All Models

**Files:**
- Create: `src/resonance/types.py`
- Create: `src/resonance/models/__init__.py`
- Create: `src/resonance/models/base.py`
- Create: `src/resonance/models/user.py`
- Create: `src/resonance/models/music.py`
- Create: `src/resonance/models/taste.py`
- Create: `src/resonance/models/sync.py`
- Test: `tests/test_models.py`

**Step 1: Write the failing tests**

Create `tests/test_models.py`:

```python
import datetime
import uuid

import resonance.models as models_module
import resonance.types as types_module


class TestUserModel:
    def test_user_has_expected_columns(self) -> None:
        user = models_module.User(
            id=uuid.uuid4(),
            display_name="Test User",
        )
        assert isinstance(user.id, uuid.UUID)
        assert user.display_name == "Test User"
        assert user.email is None

    def test_user_table_name(self) -> None:
        assert models_module.User.__tablename__ == "users"


class TestServiceConnectionModel:
    def test_service_connection_has_expected_columns(self) -> None:
        conn = models_module.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SPOTIFY,
            external_user_id="spotify_user_123",
            encrypted_access_token="encrypted_data",
        )
        assert conn.service_type == types_module.ServiceType.SPOTIFY
        assert conn.external_user_id == "spotify_user_123"

    def test_service_connection_table_name(self) -> None:
        assert models_module.ServiceConnection.__tablename__ == "service_connections"


class TestArtistModel:
    def test_artist_has_expected_columns(self) -> None:
        artist = models_module.Artist(
            id=uuid.uuid4(),
            name="Slowdive",
        )
        assert artist.name == "Slowdive"

    def test_artist_table_name(self) -> None:
        assert models_module.Artist.__tablename__ == "artists"


class TestTrackModel:
    def test_track_has_expected_columns(self) -> None:
        track = models_module.Track(
            id=uuid.uuid4(),
            title="Alison",
            artist_id=uuid.uuid4(),
        )
        assert track.title == "Alison"

    def test_track_table_name(self) -> None:
        assert models_module.Track.__tablename__ == "tracks"


class TestListeningEventModel:
    def test_listening_event_has_expected_columns(self) -> None:
        event = models_module.ListeningEvent(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            track_id=uuid.uuid4(),
            source_service=types_module.ServiceType.SPOTIFY,
            listened_at=datetime.datetime.now(tz=datetime.UTC),
        )
        assert event.source_service == types_module.ServiceType.SPOTIFY

    def test_listening_event_table_name(self) -> None:
        assert models_module.ListeningEvent.__tablename__ == "listening_events"


class TestUserArtistRelationModel:
    def test_relation_has_expected_columns(self) -> None:
        rel = models_module.UserArtistRelation(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            artist_id=uuid.uuid4(),
            relation_type=types_module.ArtistRelationType.FOLLOW,
            source_service=types_module.ServiceType.SPOTIFY,
            source_connection_id=uuid.uuid4(),
        )
        assert rel.relation_type == types_module.ArtistRelationType.FOLLOW

    def test_relation_table_name(self) -> None:
        assert models_module.UserArtistRelation.__tablename__ == "user_artist_relations"


class TestUserTrackRelationModel:
    def test_relation_has_expected_columns(self) -> None:
        rel = models_module.UserTrackRelation(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            track_id=uuid.uuid4(),
            relation_type=types_module.TrackRelationType.LIKE,
            source_service=types_module.ServiceType.SPOTIFY,
            source_connection_id=uuid.uuid4(),
        )
        assert rel.relation_type == types_module.TrackRelationType.LIKE

    def test_relation_table_name(self) -> None:
        assert models_module.UserTrackRelation.__tablename__ == "user_track_relations"


class TestSyncJobModel:
    def test_sync_job_has_expected_columns(self) -> None:
        job = models_module.SyncJob(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_connection_id=uuid.uuid4(),
            sync_type=types_module.SyncType.FULL,
            status=types_module.SyncStatus.PENDING,
        )
        assert job.sync_type == types_module.SyncType.FULL
        assert job.status == types_module.SyncStatus.PENDING

    def test_sync_job_table_name(self) -> None:
        assert models_module.SyncJob.__tablename__ == "sync_jobs"


class TestServiceType:
    def test_spotify_value(self) -> None:
        assert types_module.ServiceType.SPOTIFY == "spotify"

    def test_lastfm_value(self) -> None:
        assert types_module.ServiceType.LASTFM == "lastfm"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'resonance.models'`

**Step 3: Create shared types module**

Create `src/resonance/types.py`:

```python
import enum


class ServiceType(enum.StrEnum):
    """External service identifiers."""

    SPOTIFY = "spotify"
    LASTFM = "lastfm"
    LISTENBRAINZ = "listenbrainz"
    SONGKICK = "songkick"
    BANDSINTOWN = "bandsintown"
    BANDCAMP = "bandcamp"
    SOUNDCLOUD = "soundcloud"


class ArtistRelationType(enum.StrEnum):
    """Types of user-artist relationships."""

    FOLLOW = "follow"
    FAVORITE = "favorite"


class TrackRelationType(enum.StrEnum):
    """Types of user-track relationships."""

    LIKE = "like"
    LOVE = "love"


class SyncType(enum.StrEnum):
    """Sync job types."""

    FULL = "full"
    INCREMENTAL = "incremental"


class SyncStatus(enum.StrEnum):
    """Sync job statuses."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
```

**Step 4: Create model base**

Create `src/resonance/models/base.py`:

```python
import datetime

import sqlalchemy as sa
import sqlalchemy.orm as orm


class Base(orm.DeclarativeBase):
    """SQLAlchemy declarative base for all models."""


class TimestampMixin:
    """Mixin providing created_at and updated_at timestamp columns."""

    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True),
        server_default=sa.func.now(),
        onupdate=sa.func.now(),
    )
```

**Step 5: Create user models**

Create `src/resonance/models/user.py`:

```python
from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class User(base_module.TimestampMixin, base_module.Base):
    __tablename__ = "users"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    display_name: orm.Mapped[str] = orm.mapped_column(sa.String(255))
    email: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(255), nullable=True, default=None
    )

    connections: orm.Mapped[list[ServiceConnection]] = orm.relationship(
        back_populates="user", cascade="all, delete-orphan"
    )


class ServiceConnection(base_module.TimestampMixin, base_module.Base):
    __tablename__ = "service_connections"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE")
    )
    service_type: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False)
    )
    external_user_id: orm.Mapped[str] = orm.mapped_column(sa.String(255))
    encrypted_access_token: orm.Mapped[str] = orm.mapped_column(sa.Text)
    encrypted_refresh_token: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    token_expires_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    scopes: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    connected_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )
    last_used_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )

    user: orm.Mapped[User] = orm.relationship(back_populates="connections")

    __table_args__ = (
        sa.UniqueConstraint(
            "user_id", "service_type", "external_user_id",
            name="uq_user_service_external_id",
        ),
    )
```

**Step 6: Create music domain models**

Create `src/resonance/models/music.py`:

```python
from __future__ import annotations

import datetime
import uuid
from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class Artist(base_module.TimestampMixin, base_module.Base):
    __tablename__ = "artists"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512))
    service_links: orm.Mapped[dict[str, Any]] = orm.mapped_column(
        sa.JSON, default=dict
    )

    tracks: orm.Mapped[list[Track]] = orm.relationship(back_populates="artist")


class Track(base_module.TimestampMixin, base_module.Base):
    __tablename__ = "tracks"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    title: orm.Mapped[str] = orm.mapped_column(sa.String(512))
    artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE")
    )
    service_links: orm.Mapped[dict[str, Any]] = orm.mapped_column(
        sa.JSON, default=dict
    )

    artist: orm.Mapped[Artist] = orm.relationship(back_populates="tracks")


class ListeningEvent(base_module.Base):
    __tablename__ = "listening_events"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE")
    )
    track_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("tracks.id", ondelete="CASCADE")
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False)
    )
    listened_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True)
    )

    __table_args__ = (
        sa.Index("ix_listening_events_user_listened", "user_id", "listened_at"),
    )
```

**Step 7: Create taste signal models**

Create `src/resonance/models/taste.py`:

```python
from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class UserArtistRelation(base_module.Base):
    __tablename__ = "user_artist_relations"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE")
    )
    artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE")
    )
    relation_type: orm.Mapped[types_module.ArtistRelationType] = orm.mapped_column(
        sa.Enum(types_module.ArtistRelationType, native_enum=False)
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False)
    )
    source_connection_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE")
    )
    discovered_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )

    __table_args__ = (
        sa.UniqueConstraint(
            "user_id", "artist_id", "relation_type", "source_service",
            name="uq_user_artist_relation_source",
        ),
    )


class UserTrackRelation(base_module.Base):
    __tablename__ = "user_track_relations"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE")
    )
    track_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("tracks.id", ondelete="CASCADE")
    )
    relation_type: orm.Mapped[types_module.TrackRelationType] = orm.mapped_column(
        sa.Enum(types_module.TrackRelationType, native_enum=False)
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False)
    )
    source_connection_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE")
    )
    discovered_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )

    __table_args__ = (
        sa.UniqueConstraint(
            "user_id", "track_id", "relation_type", "source_service",
            name="uq_user_track_relation_source",
        ),
    )
```

**Step 8: Create sync job model**

Create `src/resonance/models/sync.py`:

```python
from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class SyncJob(base_module.Base):
    __tablename__ = "sync_jobs"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE")
    )
    service_connection_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE")
    )
    sync_type: orm.Mapped[types_module.SyncType] = orm.mapped_column(
        sa.Enum(types_module.SyncType, native_enum=False)
    )
    status: orm.Mapped[types_module.SyncStatus] = orm.mapped_column(
        sa.Enum(types_module.SyncStatus, native_enum=False),
        default=types_module.SyncStatus.PENDING,
    )
    progress_current: orm.Mapped[int] = orm.mapped_column(sa.Integer, default=0)
    progress_total: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True, default=None
    )
    error_message: orm.Mapped[str | None] = orm.mapped_column(
        sa.Text, nullable=True, default=None
    )
    items_created: orm.Mapped[int] = orm.mapped_column(sa.Integer, default=0)
    items_updated: orm.Mapped[int] = orm.mapped_column(sa.Integer, default=0)
    started_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    completed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now()
    )

    __table_args__ = (
        sa.Index("ix_sync_jobs_user_status", "user_id", "status"),
    )
```

**Step 9: Create models package init**

Create `src/resonance/models/__init__.py`:

```python
from resonance.models.base import Base
from resonance.models.music import Artist, ListeningEvent, Track
from resonance.models.sync import SyncJob
from resonance.models.taste import UserArtistRelation, UserTrackRelation
from resonance.models.user import ServiceConnection, User

__all__ = [
    "Artist",
    "Base",
    "ListeningEvent",
    "ServiceConnection",
    "SyncJob",
    "Track",
    "User",
    "UserArtistRelation",
    "UserTrackRelation",
]
```

**Step 10: Run tests to verify they pass**

Run: `uv run pytest tests/test_models.py -v`
Expected: PASS

**Step 11: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass. Fix any issues before proceeding.

**Step 12: Commit**

```bash
git add src/resonance/types.py src/resonance/models/ tests/test_models.py
git commit -m "feat: add SQLAlchemy models for users, music, taste signals, and sync jobs"
```

---

### Task 3: Token Encryption

**Files:**
- Create: `src/resonance/crypto.py`
- Test: `tests/test_crypto.py`

**Step 1: Write the failing tests**

Create `tests/test_crypto.py`:

```python
import cryptography.fernet as fernet_module
import pytest

import resonance.crypto as crypto_module


@pytest.fixture
def fernet_key() -> str:
    return fernet_module.Fernet.generate_key().decode()


def test_encrypt_returns_different_string(fernet_key: str) -> None:
    plaintext = "my-secret-token"
    encrypted = crypto_module.encrypt_token(plaintext, fernet_key)
    assert encrypted != plaintext


def test_decrypt_recovers_original(fernet_key: str) -> None:
    plaintext = "my-secret-token"
    encrypted = crypto_module.encrypt_token(plaintext, fernet_key)
    decrypted = crypto_module.decrypt_token(encrypted, fernet_key)
    assert decrypted == plaintext


def test_decrypt_with_wrong_key_raises(fernet_key: str) -> None:
    other_key = fernet_module.Fernet.generate_key().decode()
    encrypted = crypto_module.encrypt_token("secret", fernet_key)
    with pytest.raises(crypto_module.TokenDecryptionError):
        crypto_module.decrypt_token(encrypted, other_key)


def test_encrypt_empty_string(fernet_key: str) -> None:
    encrypted = crypto_module.encrypt_token("", fernet_key)
    decrypted = crypto_module.decrypt_token(encrypted, fernet_key)
    assert decrypted == ""
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_crypto.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write implementation**

Create `src/resonance/crypto.py`:

```python
import cryptography.fernet as fernet_module


class TokenDecryptionError(Exception):
    """Raised when a token cannot be decrypted."""


def encrypt_token(plaintext: str, key: str) -> str:
    """Encrypt a plaintext string using Fernet symmetric encryption."""
    f = fernet_module.Fernet(key.encode())
    return f.encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str, key: str) -> str:
    """Decrypt a Fernet-encrypted string back to plaintext."""
    f = fernet_module.Fernet(key.encode())
    try:
        return f.decrypt(ciphertext.encode()).decode()
    except fernet_module.InvalidToken as exc:
        raise TokenDecryptionError("Failed to decrypt token") from exc
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_crypto.py -v`
Expected: PASS

**Step 5: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 6: Commit**

```bash
git add src/resonance/crypto.py tests/test_crypto.py
git commit -m "feat: add Fernet-based token encryption utilities"
```

---

### Task 4: Alembic Wiring

**Files:**
- Modify: `alembic/env.py`

This task wires Alembic to use the app's Settings for the database URL and to reference the SQLAlchemy model metadata for autogenerate support. No test — this is infrastructure configuration verified by running `alembic check`.

**Step 1: Update alembic/env.py**

Replace the full contents of `alembic/env.py`:

```python
from logging.config import fileConfig

import sqlalchemy as sa
import sqlalchemy.pool as pool
from alembic import context

import resonance.config as config_module
import resonance.models as models_module  # noqa: F401 — registers all models

alembic_config = context.config

if alembic_config.config_file_name is not None:
    fileConfig(alembic_config.config_file_name)

target_metadata = models_module.Base.metadata

settings = config_module.Settings()
# Replace asyncpg with psycopg2 for Alembic (sync driver)
sync_database_url = settings.database_url.replace(
    "postgresql+asyncpg://", "postgresql://"
)
alembic_config.set_main_option("sqlalchemy.url", sync_database_url)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode."""
    url = alembic_config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connectable = sa.engine_from_config(
        alembic_config.get_section(alembic_config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

**Step 2: Add psycopg2-binary dependency**

Alembic runs synchronously and needs a sync PostgreSQL driver. Add to `pyproject.toml` dev dependencies:

```toml
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.25",
    "mypy>=1.15",
    "ruff>=0.11",
    "psycopg2-binary>=2.9",
]
```

Run: `uv sync`

**Step 3: Verify Alembic can generate a migration (offline mode)**

Run: `uv run alembic revision --autogenerate -m "initial schema" --sql`
Expected: Outputs SQL DDL for all tables. This validates that env.py loads correctly and sees all models. Remove the generated file — the real migration will be created when we have a live database in Task 12.

**Step 4: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 5: Commit**

```bash
git add alembic/env.py pyproject.toml uv.lock
git commit -m "chore: wire Alembic to app config and model metadata"
```

---

### Task 5: Database + Redis Lifecycle in App Factory

**Files:**
- Create: `src/resonance/database.py`
- Modify: `src/resonance/app.py`
- Test: `tests/test_health.py` (update existing)

**Step 1: Write the failing test**

The existing health test should still pass, but we add a test confirming the app has a lifespan that sets up database and Redis state. Update `tests/test_health.py`:

```python
from collections.abc import AsyncIterator

import httpx
import pytest

import resonance.app as app_module
import resonance.config as config_module


@pytest.fixture
def settings() -> config_module.Settings:
    """Settings with dummy values for testing without real services."""
    return config_module.Settings(
        database_url="postgresql+asyncpg://test:test@localhost:5432/test",
        redis_url="redis://localhost:6379/0",
    )


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_healthz_returns_ok(client: httpx.AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
```

Note: The health endpoint should continue working without a live database (it's a simple liveness check). The app factory must handle missing/unavailable database gracefully at startup in test environments. See implementation note below.

**Step 2: Create database module**

Create `src/resonance/database.py`:

```python
import sqlalchemy.ext.asyncio as sa_async

import resonance.config as config_module


def create_async_engine(settings: config_module.Settings) -> sa_async.AsyncEngine:
    """Create an async SQLAlchemy engine from settings."""
    return sa_async.create_async_engine(
        settings.database_url,
        echo=settings.debug,
    )


def create_session_factory(
    engine: sa_async.AsyncEngine,
) -> sa_async.async_sessionmaker[sa_async.AsyncSession]:
    """Create an async session factory bound to the given engine."""
    return sa_async.async_sessionmaker(engine, expire_on_commit=False)
```

**Step 3: Update app factory with lifespan**

Update `src/resonance/app.py`:

```python
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
    redis_pool = aioredis.from_url(
        settings.redis_url, decode_responses=True
    )

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
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_health.py -v`
Expected: PASS. The health endpoint doesn't hit the database, and the lifespan context manager only creates the engine/pool objects (no active connections until queries are made). The test client may warn about connection refusal for Redis but should not fail because we don't call Redis during health check.

Note: If tests fail because Redis/PostgreSQL aren't running locally, the lifespan should still allow the app to start (engine creation is lazy). If needed, add connection error handling in lifespan to log warnings instead of crashing.

**Step 5: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 6: Commit**

```bash
git add src/resonance/database.py src/resonance/app.py tests/test_health.py
git commit -m "feat: add database engine and Redis pool lifecycle to app factory"
```

---

### Task 6: Session Middleware

**Files:**
- Create: `src/resonance/middleware/__init__.py`
- Create: `src/resonance/middleware/session.py`
- Create: `src/resonance/dependencies.py`
- Modify: `src/resonance/app.py`
- Test: `tests/test_session.py`

**Step 1: Write the failing tests**

Create `tests/test_session.py`:

```python
import json
import uuid

import itsdangerous
import pytest

import resonance.middleware.session as session_module


class TestSessionData:
    def test_get_and_set(self) -> None:
        session = session_module.SessionData(session_id="abc", data={})
        session["user_id"] = "123"
        assert session["user_id"] == "123"

    def test_get_missing_key_with_default(self) -> None:
        session = session_module.SessionData(session_id="abc", data={})
        assert session.get("missing", "default") == "default"

    def test_modified_flag(self) -> None:
        session = session_module.SessionData(session_id="abc", data={})
        assert not session.modified
        session["key"] = "value"
        assert session.modified

    def test_clear_sets_modified(self) -> None:
        session = session_module.SessionData(
            session_id="abc", data={"key": "value"}
        )
        session.clear()
        assert session.modified
        assert session.get("key") is None
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_session.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write session data class and middleware**

Create `src/resonance/middleware/__init__.py` (empty file).

Create `src/resonance/middleware/session.py`:

```python
from __future__ import annotations

import json
import uuid
from typing import Any

import itsdangerous
import redis.asyncio as aioredis
import starlette.middleware.base as middleware_base
import starlette.requests as starlette_requests
import starlette.responses as starlette_responses


class SessionData:
    """Server-side session data container."""

    def __init__(
        self,
        session_id: str,
        data: dict[str, Any],
        is_new: bool = False,
    ) -> None:
        self.session_id = session_id
        self.data = data
        self.is_new = is_new
        self.modified = False

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.modified = True

    def __contains__(self, key: str) -> bool:
        return key in self.data

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def clear(self) -> None:
        self.data.clear()
        self.modified = True


class SessionMiddleware(middleware_base.BaseHTTPMiddleware):
    """Redis-backed server-side session middleware with signed cookies."""

    def __init__(
        self,
        app: Any,
        redis: aioredis.Redis,  # type: ignore[type-arg]
        secret_key: str,
        cookie_name: str = "session_id",
        max_age: int = 86400 * 30,
    ) -> None:
        super().__init__(app)
        self.redis = redis
        self.signer = itsdangerous.TimestampSigner(secret_key)
        self.cookie_name = cookie_name
        self.max_age = max_age

    async def dispatch(
        self,
        request: starlette_requests.Request,
        call_next: middleware_base.RequestResponseEndpoint,
    ) -> starlette_responses.Response:
        session = await self._load_session(request)
        request.state.session = session

        response = await call_next(request)

        if session.modified or session.is_new:
            await self._save_session(session, response)

        return response

    async def _load_session(
        self, request: starlette_requests.Request
    ) -> SessionData:
        cookie = request.cookies.get(self.cookie_name)
        if cookie:
            try:
                session_id = self.signer.unsign(
                    cookie, max_age=self.max_age
                ).decode()
                raw = await self.redis.get(f"session:{session_id}")
                data: dict[str, Any] = json.loads(raw) if raw else {}
                return SessionData(session_id=session_id, data=data)
            except (itsdangerous.BadSignature, itsdangerous.SignatureExpired):
                pass

        return SessionData(
            session_id=str(uuid.uuid4()), data={}, is_new=True
        )

    async def _save_session(
        self, session: SessionData, response: starlette_responses.Response
    ) -> None:
        await self.redis.setex(
            f"session:{session.session_id}",
            self.max_age,
            json.dumps(session.data),
        )
        signed = self.signer.sign(session.session_id).decode()
        response.set_cookie(
            self.cookie_name,
            signed,
            max_age=self.max_age,
            httponly=True,
            samesite="lax",
        )


async def destroy_session(
    session: SessionData,
    redis: aioredis.Redis,  # type: ignore[type-arg]
    response: starlette_responses.Response,
    cookie_name: str = "session_id",
) -> None:
    """Delete session from Redis and clear the cookie."""
    await redis.delete(f"session:{session.session_id}")
    response.delete_cookie(cookie_name, httponly=True, samesite="lax")
```

**Step 4: Create dependencies module**

Create `src/resonance/dependencies.py`:

```python
from __future__ import annotations

import uuid

import fastapi
import sqlalchemy.ext.asyncio as sa_async

import resonance.middleware.session as session_module


async def get_session(
    request: fastapi.Request,
) -> session_module.SessionData:
    """Get the current session from request state."""
    return request.state.session  # type: ignore[no-any-return]


async def get_db(
    request: fastapi.Request,
) -> sa_async.AsyncSession:
    """Get a database session from the app's session factory."""
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = (
        request.app.state.session_factory
    )
    async with session_factory() as session:
        yield session  # type: ignore[misc]


async def get_current_user_id(
    session: session_module.SessionData = fastapi.Depends(get_session),
) -> uuid.UUID:
    """Get the current user ID from session, or raise 401."""
    user_id = session.get("user_id")
    if not user_id:
        raise fastapi.HTTPException(status_code=401, detail="Not authenticated")
    return uuid.UUID(user_id)
```

**Step 5: Wire session middleware into app factory**

Update the `lifespan` function in `src/resonance/app.py` — the middleware must be added after `create_app` creates the application, but it needs the Redis instance from lifespan. Restructure:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import fastapi
import redis.asyncio as aioredis

import resonance.config as config_module
import resonance.database as database_module
import resonance.middleware.session as session_middleware


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

    # Session middleware — must be added before route registration.
    # Redis connection is created in lifespan, but middleware init needs
    # a Redis instance. Use a lazy approach: middleware reads redis from
    # app.state at request time (handled inside SessionMiddleware._load_session).
    # For this to work, we pass a sentinel and let the middleware resolve
    # redis from app.state. Alternatively, we can defer middleware setup.
    #
    # Simpler approach: create a second Redis connection just for sessions.
    # This is fine — Redis connections are lightweight.
    session_redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    application.add_middleware(
        session_middleware.SessionMiddleware,
        redis=session_redis,
        secret_key=settings.session_secret_key,
    )

    return application
```

**Step 6: Run tests to verify they pass**

Run: `uv run pytest -v`
Expected: All tests pass (health, config, models, crypto, session).

**Step 7: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 8: Commit**

```bash
git add src/resonance/middleware/ src/resonance/dependencies.py src/resonance/app.py tests/test_session.py
git commit -m "feat: add Redis-backed session middleware and auth dependencies"
```

---

### Task 7: Connector Framework

**Files:**
- Create: `src/resonance/connectors/__init__.py`
- Create: `src/resonance/connectors/base.py`
- Create: `src/resonance/connectors/registry.py`
- Test: `tests/test_connectors.py`

**Step 1: Write the failing tests**

Create `tests/test_connectors.py`:

```python
import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.types as types_module


class TestConnectorCapability:
    def test_listening_history_value(self) -> None:
        assert base_module.ConnectorCapability.LISTENING_HISTORY == "listening_history"

    def test_follows_value(self) -> None:
        assert base_module.ConnectorCapability.FOLLOWS == "follows"


class TestConnectorRegistry:
    def test_register_and_retrieve(self) -> None:
        registry = registry_module.ConnectorRegistry()

        class FakeConnector(base_module.BaseConnector):
            service_type = types_module.ServiceType.SPOTIFY
            capabilities = frozenset({base_module.ConnectorCapability.FOLLOWS})

        connector = FakeConnector()
        registry.register(connector)
        assert registry.get(types_module.ServiceType.SPOTIFY) is connector

    def test_get_unknown_service_returns_none(self) -> None:
        registry = registry_module.ConnectorRegistry()
        assert registry.get(types_module.ServiceType.LASTFM) is None

    def test_get_by_capability(self) -> None:
        registry = registry_module.ConnectorRegistry()

        class FakeConnector(base_module.BaseConnector):
            service_type = types_module.ServiceType.SPOTIFY
            capabilities = frozenset({
                base_module.ConnectorCapability.FOLLOWS,
                base_module.ConnectorCapability.LISTENING_HISTORY,
            })

        registry.register(FakeConnector())
        matches = registry.get_by_capability(
            base_module.ConnectorCapability.FOLLOWS
        )
        assert len(matches) == 1
        assert matches[0].service_type == types_module.ServiceType.SPOTIFY

    def test_get_by_capability_no_matches(self) -> None:
        registry = registry_module.ConnectorRegistry()
        matches = registry.get_by_capability(
            base_module.ConnectorCapability.EVENTS
        )
        assert matches == []
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_connectors.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Create connector base**

Create `src/resonance/connectors/__init__.py` (empty file).

Create `src/resonance/connectors/base.py`:

```python
from __future__ import annotations

import abc
import enum

import pydantic

import resonance.types as types_module


class ConnectorCapability(enum.StrEnum):
    """Capabilities that a service connector can provide."""

    AUTHENTICATION = "authentication"
    LISTENING_HISTORY = "listening_history"
    RECOMMENDATIONS = "recommendations"
    PLAYLIST_WRITE = "playlist_write"
    ARTIST_DATA = "artist_data"
    EVENTS = "events"
    FOLLOWS = "follows"
    TRACK_RATINGS = "track_ratings"
    NEW_RELEASES = "new_releases"


class TokenResponse(pydantic.BaseModel):
    """Response from an OAuth token exchange or refresh."""

    access_token: str
    refresh_token: str | None = None
    expires_in: int | None = None
    scope: str | None = None


class SpotifyArtistData(pydantic.BaseModel):
    """Artist data from an external service."""

    external_id: str
    name: str
    service: types_module.ServiceType


class SpotifyTrackData(pydantic.BaseModel):
    """Track data from an external service."""

    external_id: str
    title: str
    artist_external_id: str
    artist_name: str
    service: types_module.ServiceType


class BaseConnector(abc.ABC):
    """Abstract base class for service connectors."""

    service_type: types_module.ServiceType
    capabilities: frozenset[ConnectorCapability]

    def has_capability(self, capability: ConnectorCapability) -> bool:
        return capability in self.capabilities
```

**Step 4: Create connector registry**

Create `src/resonance/connectors/registry.py`:

```python
from __future__ import annotations

import resonance.connectors.base as base_module
import resonance.types as types_module


class ConnectorRegistry:
    """Registry of available service connectors."""

    def __init__(self) -> None:
        self._connectors: dict[types_module.ServiceType, base_module.BaseConnector] = {}

    def register(self, connector: base_module.BaseConnector) -> None:
        self._connectors[connector.service_type] = connector

    def get(
        self, service_type: types_module.ServiceType
    ) -> base_module.BaseConnector | None:
        return self._connectors.get(service_type)

    def get_by_capability(
        self, capability: base_module.ConnectorCapability
    ) -> list[base_module.BaseConnector]:
        return [
            c for c in self._connectors.values() if c.has_capability(capability)
        ]

    def all(self) -> list[base_module.BaseConnector]:
        return list(self._connectors.values())
```

**Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_connectors.py -v`
Expected: PASS

**Step 6: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 7: Commit**

```bash
git add src/resonance/connectors/ tests/test_connectors.py
git commit -m "feat: add connector framework with capability-based registry"
```

---

### Task 8: Spotify Connector

**Files:**
- Create: `src/resonance/connectors/spotify.py`
- Test: `tests/test_spotify_connector.py`

This connector implements OAuth flow methods and data-fetching methods for Spotify's Web API. Tests mock HTTP calls using `httpx`'s mock transport.

**Step 1: Write the failing tests**

Create `tests/test_spotify_connector.py`:

```python
import httpx
import pytest

import resonance.connectors.base as base_module
import resonance.connectors.spotify as spotify_module
import resonance.config as config_module


@pytest.fixture
def settings() -> config_module.Settings:
    return config_module.Settings(
        spotify_client_id="test-client-id",
        spotify_client_secret="test-client-secret",
        spotify_redirect_uri="http://localhost:8000/api/v1/auth/spotify/callback",
    )


@pytest.fixture
def connector(settings: config_module.Settings) -> spotify_module.SpotifyConnector:
    return spotify_module.SpotifyConnector(settings=settings)


class TestSpotifyConnectorProperties:
    def test_service_type(self, connector: spotify_module.SpotifyConnector) -> None:
        from resonance.types import ServiceType
        assert connector.service_type == ServiceType.SPOTIFY

    def test_capabilities(self, connector: spotify_module.SpotifyConnector) -> None:
        caps = connector.capabilities
        assert base_module.ConnectorCapability.AUTHENTICATION in caps
        assert base_module.ConnectorCapability.LISTENING_HISTORY in caps
        assert base_module.ConnectorCapability.FOLLOWS in caps
        assert base_module.ConnectorCapability.TRACK_RATINGS in caps


class TestSpotifyOAuth:
    def test_get_auth_url_contains_client_id(
        self, connector: spotify_module.SpotifyConnector
    ) -> None:
        url = connector.get_auth_url(state="random-state")
        assert "test-client-id" in url
        assert "random-state" in url
        assert "response_type=code" in url

    def test_get_auth_url_contains_scopes(
        self, connector: spotify_module.SpotifyConnector
    ) -> None:
        url = connector.get_auth_url(state="state")
        assert "user-read-recently-played" in url
        assert "user-follow-read" in url
        assert "user-library-read" in url

    async def test_exchange_code(
        self, connector: spotify_module.SpotifyConnector
    ) -> None:
        mock_response = httpx.Response(
            200,
            json={
                "access_token": "access-123",
                "refresh_token": "refresh-456",
                "expires_in": 3600,
                "scope": "user-read-recently-played",
            },
        )

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return mock_response

        mock_transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=mock_transport) as mock_client:
            connector._http_client = mock_client
            result = await connector.exchange_code("auth-code-xyz")

        assert result.access_token == "access-123"
        assert result.refresh_token == "refresh-456"
        assert result.expires_in == 3600


class TestSpotifyDataFetching:
    async def test_get_followed_artists(
        self, connector: spotify_module.SpotifyConnector
    ) -> None:
        mock_response = httpx.Response(
            200,
            json={
                "artists": {
                    "items": [
                        {"id": "artist1", "name": "Slowdive"},
                        {"id": "artist2", "name": "Cocteau Twins"},
                    ],
                    "cursors": {"after": None},
                    "total": 2,
                }
            },
        )

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return mock_response

        mock_transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=mock_transport) as mock_client:
            connector._http_client = mock_client
            artists = await connector.get_followed_artists("access-token")

        assert len(artists) == 2
        assert artists[0].name == "Slowdive"
        assert artists[0].external_id == "artist1"

    async def test_get_saved_tracks(
        self, connector: spotify_module.SpotifyConnector
    ) -> None:
        mock_response = httpx.Response(
            200,
            json={
                "items": [
                    {
                        "track": {
                            "id": "track1",
                            "name": "Alison",
                            "artists": [{"id": "artist1", "name": "Slowdive"}],
                        }
                    },
                ],
                "next": None,
            },
        )

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return mock_response

        mock_transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=mock_transport) as mock_client:
            connector._http_client = mock_client
            tracks = await connector.get_saved_tracks("access-token")

        assert len(tracks) == 1
        assert tracks[0].title == "Alison"
        assert tracks[0].artist_name == "Slowdive"

    async def test_get_recently_played(
        self, connector: spotify_module.SpotifyConnector
    ) -> None:
        mock_response = httpx.Response(
            200,
            json={
                "items": [
                    {
                        "played_at": "2026-03-30T10:00:00Z",
                        "track": {
                            "id": "track1",
                            "name": "Alison",
                            "artists": [{"id": "artist1", "name": "Slowdive"}],
                        },
                    },
                ],
                "next": None,
            },
        )

        async def mock_handler(request: httpx.Request) -> httpx.Response:
            return mock_response

        mock_transport = httpx.MockTransport(mock_handler)
        async with httpx.AsyncClient(transport=mock_transport) as mock_client:
            connector._http_client = mock_client
            events = await connector.get_recently_played("access-token")

        assert len(events) == 1
        assert events[0].track.title == "Alison"
        assert events[0].played_at == "2026-03-30T10:00:00Z"
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_spotify_connector.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write the Spotify connector**

Create `src/resonance/connectors/spotify.py`:

```python
from __future__ import annotations

import urllib.parse

import httpx
import pydantic

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.types as types_module

SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"

SPOTIFY_SCOPES = [
    "user-read-recently-played",
    "user-follow-read",
    "user-library-read",
    "user-read-email",
    "user-read-private",
]


class PlayedTrackItem(pydantic.BaseModel):
    """A recently played track with timestamp."""

    track: base_module.SpotifyTrackData
    played_at: str


class SpotifyConnector(base_module.BaseConnector):
    """Spotify Web API connector."""

    service_type = types_module.ServiceType.SPOTIFY
    capabilities = frozenset({
        base_module.ConnectorCapability.AUTHENTICATION,
        base_module.ConnectorCapability.LISTENING_HISTORY,
        base_module.ConnectorCapability.FOLLOWS,
        base_module.ConnectorCapability.TRACK_RATINGS,
    })

    def __init__(self, settings: config_module.Settings) -> None:
        self._client_id = settings.spotify_client_id
        self._client_secret = settings.spotify_client_secret
        self._redirect_uri = settings.spotify_redirect_uri
        self._http_client: httpx.AsyncClient | None = None

    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient()
        return self._http_client

    def get_auth_url(self, state: str) -> str:
        """Build the Spotify OAuth authorization URL."""
        params = {
            "client_id": self._client_id,
            "response_type": "code",
            "redirect_uri": self._redirect_uri,
            "scope": " ".join(SPOTIFY_SCOPES),
            "state": state,
        }
        return f"{SPOTIFY_AUTH_URL}?{urllib.parse.urlencode(params)}"

    async def exchange_code(self, code: str) -> base_module.TokenResponse:
        """Exchange an authorization code for access + refresh tokens."""
        response = await self.http_client.post(
            SPOTIFY_TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": self._redirect_uri,
            },
            auth=(self._client_id, self._client_secret),
        )
        response.raise_for_status()
        data = response.json()
        return base_module.TokenResponse(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token"),
            expires_in=data.get("expires_in"),
            scope=data.get("scope"),
        )

    async def refresh_access_token(
        self, refresh_token: str
    ) -> base_module.TokenResponse:
        """Refresh an expired access token."""
        response = await self.http_client.post(
            SPOTIFY_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            auth=(self._client_id, self._client_secret),
        )
        response.raise_for_status()
        data = response.json()
        return base_module.TokenResponse(
            access_token=data["access_token"],
            refresh_token=data.get("refresh_token", refresh_token),
            expires_in=data.get("expires_in"),
            scope=data.get("scope"),
        )

    async def get_current_user(self, access_token: str) -> dict[str, str]:
        """Get the current user's Spotify profile."""
        response = await self.http_client.get(
            f"{SPOTIFY_API_BASE}/me",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        data = response.json()
        return {"id": data["id"], "display_name": data.get("display_name", "")}

    async def get_followed_artists(
        self, access_token: str
    ) -> list[base_module.SpotifyArtistData]:
        """Fetch all followed artists (handles pagination)."""
        artists: list[base_module.SpotifyArtistData] = []
        url = f"{SPOTIFY_API_BASE}/me/following?type=artist&limit=50"
        headers = {"Authorization": f"Bearer {access_token}"}

        while url:
            response = await self.http_client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            for item in data["artists"]["items"]:
                artists.append(
                    base_module.SpotifyArtistData(
                        external_id=item["id"],
                        name=item["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    )
                )
            after = data["artists"].get("cursors", {}).get("after")
            url = (
                f"{SPOTIFY_API_BASE}/me/following?type=artist&limit=50&after={after}"
                if after
                else ""
            )

        return artists

    async def get_saved_tracks(
        self, access_token: str
    ) -> list[base_module.SpotifyTrackData]:
        """Fetch all saved (liked) tracks (handles pagination)."""
        tracks: list[base_module.SpotifyTrackData] = []
        url: str | None = f"{SPOTIFY_API_BASE}/me/tracks?limit=50"
        headers = {"Authorization": f"Bearer {access_token}"}

        while url:
            response = await self.http_client.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            for item in data["items"]:
                track = item["track"]
                primary_artist = track["artists"][0]
                tracks.append(
                    base_module.SpotifyTrackData(
                        external_id=track["id"],
                        title=track["name"],
                        artist_external_id=primary_artist["id"],
                        artist_name=primary_artist["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    )
                )
            url = data.get("next")

        return tracks

    async def get_recently_played(
        self, access_token: str
    ) -> list[PlayedTrackItem]:
        """Fetch recently played tracks (max 50 from Spotify API)."""
        response = await self.http_client.get(
            f"{SPOTIFY_API_BASE}/me/player/recently-played?limit=50",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        response.raise_for_status()
        data = response.json()
        items: list[PlayedTrackItem] = []
        for item in data["items"]:
            track = item["track"]
            primary_artist = track["artists"][0]
            items.append(
                PlayedTrackItem(
                    track=base_module.SpotifyTrackData(
                        external_id=track["id"],
                        title=track["name"],
                        artist_external_id=primary_artist["id"],
                        artist_name=primary_artist["name"],
                        service=types_module.ServiceType.SPOTIFY,
                    ),
                    played_at=item["played_at"],
                )
            )
        return items
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_spotify_connector.py -v`
Expected: PASS

**Step 5: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 6: Commit**

```bash
git add src/resonance/connectors/spotify.py tests/test_spotify_connector.py
git commit -m "feat: add Spotify connector with OAuth and data fetching"
```

---

### Task 9: Auth API Routes

**Files:**
- Create: `src/resonance/api/__init__.py`
- Create: `src/resonance/api/v1/__init__.py`
- Create: `src/resonance/api/v1/auth.py`
- Modify: `src/resonance/app.py` (register router)
- Test: `tests/test_api_auth.py`

**Step 1: Write the failing tests**

Create `tests/test_api_auth.py`:

```python
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, patch

import httpx
import pytest

import resonance.app as app_module


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestAuthInitiate:
    async def test_spotify_auth_redirects(self, client: httpx.AsyncClient) -> None:
        response = await client.get(
            "/api/v1/auth/spotify", follow_redirects=False
        )
        assert response.status_code == 307
        location = response.headers["location"]
        assert "accounts.spotify.com/authorize" in location
        assert "response_type=code" in location

    async def test_unknown_service_returns_404(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get(
            "/api/v1/auth/unknown_service", follow_redirects=False
        )
        assert response.status_code == 404


class TestAuthLogout:
    async def test_logout_returns_ok(self, client: httpx.AsyncClient) -> None:
        response = await client.post("/api/v1/auth/logout")
        assert response.status_code == 200
        assert response.json() == {"status": "logged_out"}
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api_auth.py -v`
Expected: FAIL — routes not found, 404.

**Step 3: Create API route modules**

Create `src/resonance/api/__init__.py` (empty file).

Create `src/resonance/api/v1/__init__.py`:

```python
import fastapi

import resonance.api.v1.auth as auth_module

router = fastapi.APIRouter(prefix="/api/v1")
router.include_router(auth_module.router)
```

Create `src/resonance/api/v1/auth.py`:

```python
from __future__ import annotations

import uuid

import fastapi
import starlette.responses as starlette_responses

import resonance.connectors.base as base_module
import resonance.connectors.registry as registry_module
import resonance.connectors.spotify as spotify_module
import resonance.dependencies as deps_module
import resonance.middleware.session as session_module
import resonance.types as types_module

router = fastapi.APIRouter(prefix="/auth", tags=["auth"])


def _get_registry(request: fastapi.Request) -> registry_module.ConnectorRegistry:
    return request.app.state.connector_registry  # type: ignore[no-any-return]


@router.get("/{service}")
async def auth_initiate(
    service: str,
    request: fastapi.Request,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
) -> starlette_responses.RedirectResponse:
    """Initiate OAuth flow for a service."""
    try:
        service_type = types_module.ServiceType(service)
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail="Unknown service")

    registry = _get_registry(request)
    connector = registry.get(service_type)
    if connector is None:
        raise fastapi.HTTPException(status_code=404, detail="Service not available")

    if not connector.has_capability(base_module.ConnectorCapability.AUTHENTICATION):
        raise fastapi.HTTPException(
            status_code=400, detail="Service does not support authentication"
        )

    state = str(uuid.uuid4())
    session["oauth_state"] = state
    session["oauth_service"] = service

    if not isinstance(connector, spotify_module.SpotifyConnector):
        raise fastapi.HTTPException(status_code=500, detail="Unsupported connector")

    auth_url = connector.get_auth_url(state=state)
    return starlette_responses.RedirectResponse(url=auth_url, status_code=307)


@router.get("/{service}/callback")
async def auth_callback(
    service: str,
    code: str,
    state: str,
    request: fastapi.Request,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
    db: object = fastapi.Depends(deps_module.get_db),
) -> dict[str, str]:
    """Handle OAuth callback — exchange code, create/update user and connection."""
    expected_state = session.get("oauth_state")
    if state != expected_state:
        raise fastapi.HTTPException(status_code=400, detail="Invalid OAuth state")

    try:
        service_type = types_module.ServiceType(service)
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail="Unknown service")

    registry = _get_registry(request)
    connector = registry.get(service_type)
    if connector is None or not isinstance(connector, spotify_module.SpotifyConnector):
        raise fastapi.HTTPException(status_code=404, detail="Service not available")

    settings = request.app.state.settings

    # Exchange code for tokens
    token_response = await connector.exchange_code(code)

    # Get Spotify user profile
    user_profile = await connector.get_current_user(token_response.access_token)

    # Import here to avoid circular — models need DB session
    import datetime

    import sqlalchemy as sa

    import resonance.crypto as crypto_module
    import resonance.models as models_module

    # Type-narrow db to AsyncSession
    import sqlalchemy.ext.asyncio as sa_async

    assert isinstance(db, sa_async.AsyncSession)

    # Find or create user via service connection
    result = await db.execute(
        sa.select(models_module.ServiceConnection).where(
            models_module.ServiceConnection.service_type == service_type,
            models_module.ServiceConnection.external_user_id == user_profile["id"],
        )
    )
    existing_connection = result.scalar_one_or_none()

    if existing_connection:
        # Update tokens on existing connection
        existing_connection.encrypted_access_token = crypto_module.encrypt_token(
            token_response.access_token, settings.token_encryption_key
        )
        if token_response.refresh_token:
            existing_connection.encrypted_refresh_token = crypto_module.encrypt_token(
                token_response.refresh_token, settings.token_encryption_key
            )
        if token_response.expires_in:
            existing_connection.token_expires_at = datetime.datetime.now(
                tz=datetime.UTC
            ) + datetime.timedelta(seconds=token_response.expires_in)
        existing_connection.scopes = token_response.scope
        existing_connection.last_used_at = datetime.datetime.now(tz=datetime.UTC)
        user_id = existing_connection.user_id
    else:
        # Check if there's a logged-in user to attach this connection to
        current_user_id = session.get("user_id")
        if current_user_id:
            user_id = uuid.UUID(current_user_id)
        else:
            # Create a new user
            new_user = models_module.User(
                display_name=user_profile.get("display_name", ""),
            )
            db.add(new_user)
            await db.flush()
            user_id = new_user.id

        # Create the service connection
        new_connection = models_module.ServiceConnection(
            user_id=user_id,
            service_type=service_type,
            external_user_id=user_profile["id"],
            encrypted_access_token=crypto_module.encrypt_token(
                token_response.access_token, settings.token_encryption_key
            ),
            encrypted_refresh_token=(
                crypto_module.encrypt_token(
                    token_response.refresh_token, settings.token_encryption_key
                )
                if token_response.refresh_token
                else None
            ),
            token_expires_at=(
                datetime.datetime.now(tz=datetime.UTC)
                + datetime.timedelta(seconds=token_response.expires_in)
                if token_response.expires_in
                else None
            ),
            scopes=token_response.scope,
        )
        db.add(new_connection)

    await db.commit()

    # Set session
    session["user_id"] = str(user_id)

    return {"status": "connected", "service": service}


@router.post("/logout")
async def logout(
    request: fastapi.Request,
    session: session_module.SessionData = fastapi.Depends(deps_module.get_session),
) -> dict[str, str]:
    """End the current session."""
    session.clear()
    return {"status": "logged_out"}
```

**Step 4: Register router and connector registry in app factory**

Update `src/resonance/app.py` — add after `application.state.settings = settings`:

```python
    # Register API routes
    import resonance.api.v1 as api_v1_module
    application.include_router(api_v1_module.router)

    # Set up connector registry
    import resonance.connectors.registry as registry_module
    import resonance.connectors.spotify as spotify_module

    connector_registry = registry_module.ConnectorRegistry()
    connector_registry.register(spotify_module.SpotifyConnector(settings=settings))
    application.state.connector_registry = connector_registry
```

**Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_api_auth.py -v`
Expected: PASS

**Step 6: Run full test suite**

Run: `uv run pytest -v`
Expected: All tests pass.

**Step 7: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 8: Commit**

```bash
git add src/resonance/api/ src/resonance/app.py tests/test_api_auth.py
git commit -m "feat: add auth API routes with OAuth initiate, callback, and logout"
```

---

### Task 10: Account API Routes

**Files:**
- Create: `src/resonance/api/v1/account.py`
- Modify: `src/resonance/api/v1/__init__.py` (register router)
- Test: `tests/test_api_account.py`

**Step 1: Write the failing tests**

Create `tests/test_api_account.py`:

```python
from collections.abc import AsyncIterator

import httpx
import pytest

import resonance.app as app_module


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestAccountProfile:
    async def test_unauthenticated_returns_401(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/api/v1/account")
        assert response.status_code == 401


class TestAccountConnections:
    async def test_unauthenticated_returns_401(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/api/v1/account/connections")
        assert response.status_code == 401
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api_account.py -v`
Expected: FAIL — 404 (routes not registered).

**Step 3: Create account routes**

Create `src/resonance/api/v1/account.py`:

```python
from __future__ import annotations

import uuid

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.dependencies as deps_module
import resonance.models as models_module

router = fastapi.APIRouter(prefix="/account", tags=["account"])


@router.get("")
async def get_profile(
    user_id: uuid.UUID = fastapi.Depends(deps_module.get_current_user_id),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> dict[str, str | None]:
    """Get the current user's profile."""
    result = await db.execute(
        sa.select(models_module.User).where(models_module.User.id == user_id)
    )
    user = result.scalar_one_or_none()
    if user is None:
        raise fastapi.HTTPException(status_code=404, detail="User not found")
    return {
        "id": str(user.id),
        "display_name": user.display_name,
        "email": user.email,
    }


@router.get("/connections")
async def list_connections(
    user_id: uuid.UUID = fastapi.Depends(deps_module.get_current_user_id),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> list[dict[str, str | None]]:
    """List the current user's connected services."""
    result = await db.execute(
        sa.select(models_module.ServiceConnection).where(
            models_module.ServiceConnection.user_id == user_id
        )
    )
    connections = result.scalars().all()
    return [
        {
            "id": str(conn.id),
            "service_type": conn.service_type.value,
            "external_user_id": conn.external_user_id,
            "connected_at": conn.connected_at.isoformat() if conn.connected_at else None,
        }
        for conn in connections
    ]


@router.delete("/connections/{connection_id}")
async def unlink_connection(
    connection_id: uuid.UUID,
    user_id: uuid.UUID = fastapi.Depends(deps_module.get_current_user_id),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> dict[str, str]:
    """Unlink a service connection. Blocked if it's the user's last connection."""
    # Count user's connections
    count_result = await db.execute(
        sa.select(sa.func.count()).where(
            models_module.ServiceConnection.user_id == user_id
        )
    )
    connection_count = count_result.scalar_one()

    if connection_count <= 1:
        raise fastapi.HTTPException(
            status_code=400,
            detail="Cannot unlink last connected service — would cause lockout",
        )

    # Find and delete the specific connection
    result = await db.execute(
        sa.select(models_module.ServiceConnection).where(
            models_module.ServiceConnection.id == connection_id,
            models_module.ServiceConnection.user_id == user_id,
        )
    )
    connection = result.scalar_one_or_none()
    if connection is None:
        raise fastapi.HTTPException(status_code=404, detail="Connection not found")

    await db.delete(connection)
    await db.commit()
    return {"status": "unlinked"}
```

**Step 4: Register router**

Update `src/resonance/api/v1/__init__.py`:

```python
import fastapi

import resonance.api.v1.account as account_module
import resonance.api.v1.auth as auth_module

router = fastapi.APIRouter(prefix="/api/v1")
router.include_router(auth_module.router)
router.include_router(account_module.router)
```

**Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_api_account.py -v`
Expected: PASS

**Step 6: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 7: Commit**

```bash
git add src/resonance/api/v1/account.py src/resonance/api/v1/__init__.py tests/test_api_account.py
git commit -m "feat: add account API routes for profile and connection management"
```

---

### Task 11: Sync Runner

**Files:**
- Create: `src/resonance/sync/__init__.py`
- Create: `src/resonance/sync/runner.py`
- Test: `tests/test_sync_runner.py`

The sync runner is an async function that takes a SyncJob and connector, pulls data from the external service, and upserts it into the database. It runs as a background task via `asyncio.create_task()`.

**Step 1: Write the failing tests**

Create `tests/test_sync_runner.py`:

```python
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.connectors.base as base_module
import resonance.sync.runner as runner_module
import resonance.types as types_module


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.execute = AsyncMock()
    return session


@pytest.fixture
def mock_connector() -> MagicMock:
    connector = MagicMock()
    connector.service_type = types_module.ServiceType.SPOTIFY
    connector.get_followed_artists = AsyncMock(return_value=[
        base_module.SpotifyArtistData(
            external_id="artist1",
            name="Slowdive",
            service=types_module.ServiceType.SPOTIFY,
        ),
    ])
    connector.get_saved_tracks = AsyncMock(return_value=[
        base_module.SpotifyTrackData(
            external_id="track1",
            title="Alison",
            artist_external_id="artist1",
            artist_name="Slowdive",
            service=types_module.ServiceType.SPOTIFY,
        ),
    ])
    connector.get_recently_played = AsyncMock(return_value=[])
    return connector


class TestSyncRunner:
    async def test_run_sync_updates_job_status(
        self,
        mock_session: AsyncMock,
        mock_connector: MagicMock,
    ) -> None:
        job = MagicMock()
        job.id = uuid.uuid4()
        job.user_id = uuid.uuid4()
        job.service_connection_id = uuid.uuid4()
        job.sync_type = types_module.SyncType.FULL
        job.status = types_module.SyncStatus.PENDING

        # Mock the session to return None for existing records (all new)
        mock_result = MagicMock()
        mock_result.scalar_one_or_none.return_value = None
        mock_session.execute.return_value = mock_result

        await runner_module.run_sync(
            job=job,
            connector=mock_connector,
            session=mock_session,
            access_token="test-token",
        )

        assert job.status == types_module.SyncStatus.COMPLETED
        assert job.completed_at is not None
        mock_session.commit.assert_called()
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_runner.py -v`
Expected: FAIL — `ModuleNotFoundError`

**Step 3: Write the sync runner**

Create `src/resonance/sync/__init__.py` (empty file).

Create `src/resonance/sync/runner.py`:

```python
from __future__ import annotations

import datetime
import logging
import uuid

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.connectors.spotify as spotify_module
import resonance.models as models_module
import resonance.types as types_module

logger = logging.getLogger(__name__)


async def run_sync(
    job: models_module.SyncJob,
    connector: spotify_module.SpotifyConnector,
    session: sa_async.AsyncSession,
    access_token: str,
) -> None:
    """Execute a sync job — pull data from connector and upsert into database."""
    job.status = types_module.SyncStatus.RUNNING
    job.started_at = datetime.datetime.now(tz=datetime.UTC)
    await session.commit()

    try:
        items_created = 0
        items_updated = 0

        # Sync followed artists
        artists = await connector.get_followed_artists(access_token)
        for artist_data in artists:
            created = await _upsert_artist(session, artist_data)
            if created:
                items_created += 1
            else:
                items_updated += 1

            await _upsert_user_artist_relation(
                session,
                user_id=job.user_id,
                artist_data=artist_data,
                connection_id=job.service_connection_id,
            )

        # Sync saved tracks (likes)
        tracks = await connector.get_saved_tracks(access_token)
        for track_data in tracks:
            await _upsert_artist_from_track(session, track_data)
            created = await _upsert_track(session, track_data)
            if created:
                items_created += 1
            else:
                items_updated += 1

            await _upsert_user_track_relation(
                session,
                user_id=job.user_id,
                track_data=track_data,
                connection_id=job.service_connection_id,
            )

        # Sync recently played
        played_items = await connector.get_recently_played(access_token)
        for played in played_items:
            await _upsert_artist_from_track(session, played.track)
            await _upsert_track(session, played.track)
            await _upsert_listening_event(
                session,
                user_id=job.user_id,
                track_data=played.track,
                played_at=played.played_at,
            )
            items_created += 1

        job.status = types_module.SyncStatus.COMPLETED
        job.items_created = items_created
        job.items_updated = items_updated

    except Exception:
        logger.exception("Sync job %s failed", job.id)
        job.status = types_module.SyncStatus.FAILED
        job.error_message = "Sync failed — check logs for details"

    job.completed_at = datetime.datetime.now(tz=datetime.UTC)
    await session.commit()


async def _upsert_artist(
    session: sa_async.AsyncSession,
    artist_data: object,
) -> bool:
    """Upsert an artist by external ID. Returns True if created, False if updated."""
    from resonance.connectors.base import SpotifyArtistData

    assert isinstance(artist_data, SpotifyArtistData)

    result = await session.execute(
        sa.select(models_module.Artist).where(
            models_module.Artist.service_links[artist_data.service.value].as_string()
            == artist_data.external_id
        )
    )
    existing = result.scalar_one_or_none()

    if existing:
        existing.name = artist_data.name
        return False

    new_artist = models_module.Artist(
        name=artist_data.name,
        service_links={artist_data.service.value: artist_data.external_id},
    )
    session.add(new_artist)
    await session.flush()
    return True


async def _upsert_artist_from_track(
    session: sa_async.AsyncSession,
    track_data: object,
) -> None:
    """Ensure the artist from a track exists in the database."""
    from resonance.connectors.base import SpotifyArtistData, SpotifyTrackData

    assert isinstance(track_data, SpotifyTrackData)

    await _upsert_artist(
        session,
        SpotifyArtistData(
            external_id=track_data.artist_external_id,
            name=track_data.artist_name,
            service=track_data.service,
        ),
    )


async def _upsert_track(
    session: sa_async.AsyncSession,
    track_data: object,
) -> bool:
    """Upsert a track by external ID. Returns True if created."""
    from resonance.connectors.base import SpotifyTrackData

    assert isinstance(track_data, SpotifyTrackData)

    result = await session.execute(
        sa.select(models_module.Track).where(
            models_module.Track.service_links[track_data.service.value].as_string()
            == track_data.external_id
        )
    )
    existing = result.scalar_one_or_none()

    if existing:
        existing.title = track_data.title
        return False

    # Find the artist
    artist_result = await session.execute(
        sa.select(models_module.Artist).where(
            models_module.Artist.service_links[
                track_data.service.value
            ].as_string()
            == track_data.artist_external_id
        )
    )
    artist = artist_result.scalar_one()

    new_track = models_module.Track(
        title=track_data.title,
        artist_id=artist.id,
        service_links={track_data.service.value: track_data.external_id},
    )
    session.add(new_track)
    await session.flush()
    return True


async def _upsert_user_artist_relation(
    session: sa_async.AsyncSession,
    user_id: uuid.UUID,
    artist_data: object,
    connection_id: uuid.UUID,
) -> None:
    """Create a follow relation if it doesn't exist."""
    from resonance.connectors.base import SpotifyArtistData

    assert isinstance(artist_data, SpotifyArtistData)

    # Find artist by service link
    artist_result = await session.execute(
        sa.select(models_module.Artist).where(
            models_module.Artist.service_links[
                artist_data.service.value
            ].as_string()
            == artist_data.external_id
        )
    )
    artist = artist_result.scalar_one_or_none()
    if artist is None:
        return

    # Check if relation already exists
    existing = await session.execute(
        sa.select(models_module.UserArtistRelation).where(
            models_module.UserArtistRelation.user_id == user_id,
            models_module.UserArtistRelation.artist_id == artist.id,
            models_module.UserArtistRelation.relation_type
            == types_module.ArtistRelationType.FOLLOW,
            models_module.UserArtistRelation.source_service
            == artist_data.service,
        )
    )
    if existing.scalar_one_or_none():
        return

    relation = models_module.UserArtistRelation(
        user_id=user_id,
        artist_id=artist.id,
        relation_type=types_module.ArtistRelationType.FOLLOW,
        source_service=artist_data.service,
        source_connection_id=connection_id,
    )
    session.add(relation)
    await session.flush()


async def _upsert_user_track_relation(
    session: sa_async.AsyncSession,
    user_id: uuid.UUID,
    track_data: object,
    connection_id: uuid.UUID,
) -> None:
    """Create a like relation if it doesn't exist."""
    from resonance.connectors.base import SpotifyTrackData

    assert isinstance(track_data, SpotifyTrackData)

    track_result = await session.execute(
        sa.select(models_module.Track).where(
            models_module.Track.service_links[
                track_data.service.value
            ].as_string()
            == track_data.external_id
        )
    )
    track = track_result.scalar_one_or_none()
    if track is None:
        return

    existing = await session.execute(
        sa.select(models_module.UserTrackRelation).where(
            models_module.UserTrackRelation.user_id == user_id,
            models_module.UserTrackRelation.track_id == track.id,
            models_module.UserTrackRelation.relation_type
            == types_module.TrackRelationType.LIKE,
            models_module.UserTrackRelation.source_service == track_data.service,
        )
    )
    if existing.scalar_one_or_none():
        return

    relation = models_module.UserTrackRelation(
        user_id=user_id,
        track_id=track.id,
        relation_type=types_module.TrackRelationType.LIKE,
        source_service=track_data.service,
        source_connection_id=connection_id,
    )
    session.add(relation)
    await session.flush()


async def _upsert_listening_event(
    session: sa_async.AsyncSession,
    user_id: uuid.UUID,
    track_data: object,
    played_at: str,
) -> None:
    """Create a listening event if it doesn't already exist."""
    from resonance.connectors.base import SpotifyTrackData

    assert isinstance(track_data, SpotifyTrackData)

    track_result = await session.execute(
        sa.select(models_module.Track).where(
            models_module.Track.service_links[
                track_data.service.value
            ].as_string()
            == track_data.external_id
        )
    )
    track = track_result.scalar_one_or_none()
    if track is None:
        return

    listened_dt = datetime.datetime.fromisoformat(played_at)

    # Check for duplicate (same user, track, timestamp)
    existing = await session.execute(
        sa.select(models_module.ListeningEvent).where(
            models_module.ListeningEvent.user_id == user_id,
            models_module.ListeningEvent.track_id == track.id,
            models_module.ListeningEvent.listened_at == listened_dt,
        )
    )
    if existing.scalar_one_or_none():
        return

    event = models_module.ListeningEvent(
        user_id=user_id,
        track_id=track.id,
        source_service=track_data.service,
        listened_at=listened_dt,
    )
    session.add(event)
    await session.flush()
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_runner.py -v`
Expected: PASS

**Step 5: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 6: Commit**

```bash
git add src/resonance/sync/ tests/test_sync_runner.py
git commit -m "feat: add sync runner for pulling Spotify data into database"
```

---

### Task 12: Sync API Routes

**Files:**
- Create: `src/resonance/api/v1/sync.py`
- Modify: `src/resonance/api/v1/__init__.py` (register router)
- Test: `tests/test_api_sync.py`

**Step 1: Write the failing tests**

Create `tests/test_api_sync.py`:

```python
from collections.abc import AsyncIterator

import httpx
import pytest

import resonance.app as app_module


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestSyncTrigger:
    async def test_unauthenticated_returns_401(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.post("/api/v1/sync/spotify")
        assert response.status_code == 401


class TestSyncStatus:
    async def test_unauthenticated_returns_401(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.get("/api/v1/sync/status")
        assert response.status_code == 401
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_api_sync.py -v`
Expected: FAIL — 404 (routes not registered).

**Step 3: Create sync routes**

Create `src/resonance/api/v1/sync.py`:

```python
from __future__ import annotations

import asyncio
import datetime
import uuid

import fastapi
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.connectors.registry as registry_module
import resonance.connectors.spotify as spotify_module
import resonance.crypto as crypto_module
import resonance.dependencies as deps_module
import resonance.models as models_module
import resonance.sync.runner as runner_module
import resonance.types as types_module

router = fastapi.APIRouter(prefix="/sync", tags=["sync"])


@router.post("/{service}")
async def trigger_sync(
    service: str,
    request: fastapi.Request,
    user_id: uuid.UUID = fastapi.Depends(deps_module.get_current_user_id),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> dict[str, str]:
    """Trigger a data sync from the specified service."""
    try:
        service_type = types_module.ServiceType(service)
    except ValueError:
        raise fastapi.HTTPException(status_code=404, detail="Unknown service")

    settings = request.app.state.settings

    # Find the user's connection for this service
    result = await db.execute(
        sa.select(models_module.ServiceConnection).where(
            models_module.ServiceConnection.user_id == user_id,
            models_module.ServiceConnection.service_type == service_type,
        )
    )
    connection = result.scalar_one_or_none()
    if connection is None:
        raise fastapi.HTTPException(
            status_code=400, detail="No connection for this service"
        )

    # Check for an already-running sync
    running_result = await db.execute(
        sa.select(models_module.SyncJob).where(
            models_module.SyncJob.service_connection_id == connection.id,
            models_module.SyncJob.status.in_([
                types_module.SyncStatus.PENDING,
                types_module.SyncStatus.RUNNING,
            ]),
        )
    )
    if running_result.scalar_one_or_none():
        raise fastapi.HTTPException(
            status_code=409, detail="Sync already in progress for this service"
        )

    # Create sync job
    sync_job = models_module.SyncJob(
        user_id=user_id,
        service_connection_id=connection.id,
        sync_type=types_module.SyncType.FULL,
        status=types_module.SyncStatus.PENDING,
    )
    db.add(sync_job)
    await db.commit()
    await db.refresh(sync_job)

    # Decrypt access token
    access_token = crypto_module.decrypt_token(
        connection.encrypted_access_token, settings.token_encryption_key
    )

    # Get connector
    registry: registry_module.ConnectorRegistry = (
        request.app.state.connector_registry
    )
    connector = registry.get(service_type)
    if connector is None or not isinstance(connector, spotify_module.SpotifyConnector):
        raise fastapi.HTTPException(status_code=500, detail="Connector not available")

    # Launch background sync task
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession] = (
        request.app.state.session_factory
    )

    async def _background_sync() -> None:
        async with session_factory() as bg_session:
            # Re-load the job in the new session
            result = await bg_session.execute(
                sa.select(models_module.SyncJob).where(
                    models_module.SyncJob.id == sync_job.id
                )
            )
            bg_job = result.scalar_one()
            await runner_module.run_sync(
                job=bg_job,
                connector=connector,
                session=bg_session,
                access_token=access_token,
            )

    asyncio.create_task(_background_sync())

    return {"status": "started", "sync_job_id": str(sync_job.id)}


@router.get("/status")
async def sync_status(
    user_id: uuid.UUID = fastapi.Depends(deps_module.get_current_user_id),
    db: sa_async.AsyncSession = fastapi.Depends(deps_module.get_db),
) -> list[dict[str, str | int | None]]:
    """Get status of recent sync jobs for the current user."""
    result = await db.execute(
        sa.select(models_module.SyncJob)
        .where(models_module.SyncJob.user_id == user_id)
        .order_by(models_module.SyncJob.created_at.desc())
        .limit(10)
    )
    jobs = result.scalars().all()
    return [
        {
            "id": str(job.id),
            "status": job.status.value,
            "sync_type": job.sync_type.value,
            "progress_current": job.progress_current,
            "progress_total": job.progress_total,
            "items_created": job.items_created,
            "items_updated": job.items_updated,
            "error_message": job.error_message,
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "completed_at": job.completed_at.isoformat() if job.completed_at else None,
        }
        for job in jobs
    ]
```

**Step 4: Register router**

Update `src/resonance/api/v1/__init__.py`:

```python
import fastapi

import resonance.api.v1.account as account_module
import resonance.api.v1.auth as auth_module
import resonance.api.v1.sync as sync_module

router = fastapi.APIRouter(prefix="/api/v1")
router.include_router(auth_module.router)
router.include_router(account_module.router)
router.include_router(sync_module.router)
```

**Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_api_sync.py -v`
Expected: PASS

**Step 6: Run full test suite, lint, and type check**

Run: `uv run pytest -v && uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass.

**Step 7: Commit**

```bash
git add src/resonance/api/v1/sync.py src/resonance/api/v1/__init__.py tests/test_api_sync.py
git commit -m "feat: add sync API routes for triggering and monitoring data syncs"
```

---

### Task 13: Kubernetes Infrastructure (megadoomer-config)

**Repo:** `megadoomer-config` (not resonance)
**Working directory:** `~/src/github.com/megadoomer-io/megadoomer-config`

This task adds PostgreSQL and Redis Helm charts to the resonance deployment, configures secrets, and wires the app pod with the necessary env vars and an Alembic init container.

**Step 1: Update kustomization.yaml with PostgreSQL and Redis Helm charts**

Update `applications/resonance/resonance/do/kustomization.yaml`:

```yaml
---
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

helmCharts:
  - name: app-template
    namespace: resonance
    releaseName: resonance
    repo: https://bjw-s-labs.github.io/helm-charts/
    version: 4.6.2
    apiVersions:
      - gateway.networking.k8s.io/v1/HTTPRoute
    additionalValuesFiles:
      - helm-values.yaml

  - name: postgresql
    namespace: resonance
    releaseName: resonance-postgresql
    repo: oci://registry-1.docker.io/bitnamicharts
    version: 16.7.4
    additionalValuesFiles:
      - postgresql-values.yaml

  - name: redis
    namespace: resonance
    releaseName: resonance-redis
    repo: oci://registry-1.docker.io/bitnamicharts
    version: 20.11.3
    additionalValuesFiles:
      - redis-values.yaml

resources:
  - sealed-secret.yaml

images:
  - name: app
    newName: ghcr.io/megadoomer-io/resonance
    newTag: 20260330T165231-9a89908

namespace: resonance
```

Note: Check Bitnami chart versions — use the latest stable at time of implementation. The versions above are examples.

**Step 2: Create PostgreSQL values**

Create `applications/resonance/resonance/do/postgresql-values.yaml`:

```yaml
---
auth:
  database: resonance
  username: resonance
  existingSecret: resonance-db-credentials
  secretKeys:
    adminPasswordKey: postgres-password
    userPasswordKey: password

primary:
  persistence:
    size: 5Gi
    storageClass: do-block-storage

  resources:
    limits:
      cpu: 500m
      memory: 512Mi
    requests:
      cpu: 100m
      memory: 256Mi

# IMPORTANT: Retain PVC on Helm release delete to prevent data loss
volumePermissions:
  enabled: true

persistentVolumeClaimRetentionPolicy:
  enabled: true
  whenDeleted: Retain
  whenScaled: Retain
```

**Step 3: Create Redis values**

Create `applications/resonance/resonance/do/redis-values.yaml`:

```yaml
---
auth:
  enabled: true
  existingSecret: resonance-redis-credentials
  existingSecretPasswordKey: redis-password

master:
  persistence:
    enabled: false

  resources:
    limits:
      cpu: 250m
      memory: 256Mi
    requests:
      cpu: 50m
      memory: 128Mi

replica:
  replicaCount: 0
```

**Step 4: Create SealedSecrets**

Generate secrets and seal them using `kubeseal`. The sealed secret file will contain:

- `resonance-db-credentials`: `postgres-password`, `password`
- `resonance-redis-credentials`: `redis-password`
- `resonance-app-secrets`: `session-secret-key`, `token-encryption-key`, `spotify-client-id`, `spotify-client-secret`

Create the secret manifests, seal with kubeseal, and save to `applications/resonance/resonance/do/sealed-secret.yaml`.

The actual values must be generated/provided at implementation time:
- Database passwords: generate random strings
- Redis password: generate random string
- Session secret key: generate random string
- Token encryption key: generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`
- Spotify credentials: from Spotify Developer Dashboard

**Step 5: Update helm-values.yaml with env vars and init container**

Update `applications/resonance/resonance/do/helm-values.yaml`:

```yaml
---
global:
  fullnameOverride: resonance

defaultPodOptions:
  imagePullSecrets:
    - name: ghcr

controllers:
  main:
    initContainers:
      migrations:
        image:
          repository: app
          tag: latest
          pullPolicy: Always
        command:
          - alembic
          - upgrade
          - head
        env:
          DATABASE_URL:
            value: "postgresql+asyncpg://resonance:$(DB_PASSWORD)@resonance-postgresql:5432/resonance"
        envFrom:
          - secretRef:
              name: resonance-app-secrets
        resources:
          limits:
            cpu: 100m
            memory: 256Mi
          requests:
            cpu: 10m
            memory: 128Mi

    containers:
      main:
        env:
          PYTHONDONTWRITEBYTECODE: "1"
          PYTHONUNBUFFERED: "1"
          DATABASE_URL: "postgresql+asyncpg://resonance:$(DB_PASSWORD)@resonance-postgresql:5432/resonance"
          REDIS_URL: "redis://:$(REDIS_PASSWORD)@resonance-redis-master:6379/0"
          SPOTIFY_REDIRECT_URI: "https://resonance.megadoomer.io/api/v1/auth/spotify/callback"

        envFrom:
          - secretRef:
              name: resonance-app-secrets

        image:
          repository: app
          tag: latest
          pullPolicy: Always

        ports:
          - containerPort: 8000
            name: http

        probes:
          startup:
            enabled: true
            custom: true
            spec:
              httpGet:
                path: /healthz
                port: http
              failureThreshold: 30
              periodSeconds: 2
          liveness:
            enabled: true
            custom: true
            spec:
              httpGet:
                path: /healthz
                port: http
          readiness:
            enabled: true
            custom: true
            spec:
              httpGet:
                path: /healthz
                port: http

        resources:
          limits:
            cpu: 200m
            memory: 512Mi
          requests:
            cpu: 50m
            memory: 256Mi

        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop:
              - ALL
          readOnlyRootFilesystem: true

persistence:
  tmp:
    enabled: true
    type: emptyDir
    globalMounts:
      - path: /tmp

route:
  main:
    enabled: true
    kind: HTTPRoute
    annotations:
      external-dns.alpha.kubernetes.io/hostname: resonance.megadoomer.io
    parentRefs:
      - group: gateway.networking.k8s.io
        kind: Gateway
        name: megadoomer-gateway
        namespace: gateway-system
    hostnames:
      - resonance.megadoomer.io
    rules:
      - matches:
          - path:
              type: PathPrefix
              value: /
        backendRefs:
          - name: resonance
            port: 80

service:
  main:
    controller: main
    type: ClusterIP
    ports:
      http:
        appProtocol: http
        port: 80
        targetPort: http
```

Note: The `envFrom` approach loads all keys from the `resonance-app-secrets` sealed secret as env vars. Secret keys should be mapped to Settings field names: `SESSION_SECRET_KEY`, `TOKEN_ENCRYPTION_KEY`, `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`. The `DB_PASSWORD` and `REDIS_PASSWORD` are referenced via variable substitution in the explicit `DATABASE_URL` and `REDIS_URL` env vars.

**Step 6: Handle Alembic init container driver**

The init container runs `alembic upgrade head`, which uses the sync PostgreSQL driver (psycopg2). Since psycopg2-binary is a dev dependency, we need it available in the Docker image. Options:

1. Move `psycopg2-binary` to main dependencies
2. Use a separate migration image
3. Install it in the Docker build

Simplest: move `psycopg2-binary` to main dependencies in `pyproject.toml`. The init container runs the same image as the app.

Also note: Alembic's env.py converts the async URL to a sync one. The init container's `DATABASE_URL` should use the async format (same as the app) — env.py handles the conversion.

**Step 7: Verify kustomize build**

Run: `kustomize build --enable-helm --load-restrictor LoadRestrictionsNone applications/resonance/resonance/do/`
Expected: Renders all manifests without errors.

**Step 8: Commit to megadoomer-config**

```bash
cd ~/src/github.com/megadoomer-io/megadoomer-config
git add applications/resonance/resonance/do/
git commit -m "feat(resonance): add PostgreSQL, Redis, secrets, and app config for data layer"
git push origin main
```

---

## Summary

After all 13 tasks, the resonance project will have:

| Component | Status |
|-----------|--------|
| SQLAlchemy models | User, ServiceConnection, Artist, Track, ListeningEvent, UserArtistRelation, UserTrackRelation, SyncJob |
| Token encryption | Fernet-based encrypt/decrypt for OAuth tokens at rest |
| Alembic | Wired to real models and database URL |
| App lifecycle | Database engine + Redis pool created on startup, disposed on shutdown |
| Session middleware | Redis-backed, signed cookie, with auth dependency |
| Connector framework | BaseConnector ABC, capability enum, registry |
| Spotify connector | OAuth flow, followed artists, saved tracks, recently played |
| Auth API | OAuth initiate, callback (user creation/login), logout |
| Account API | Profile, list connections, unlink connection |
| Sync runner | Background async task for pulling Spotify data |
| Sync API | Trigger sync, check status |
| K8s infrastructure | PostgreSQL (Bitnami, retain PVC), Redis (Bitnami), SealedSecrets, init container for migrations |

**Not in scope (future phases):**
- UI templates (Jinja2)
- Playlist generation engine
- Additional connectors (Last.fm, ListenBrainz, etc.)
- Granular sync progress reporting
- Token refresh automation
- Rate limiting
