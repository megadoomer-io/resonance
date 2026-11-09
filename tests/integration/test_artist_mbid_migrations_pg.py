"""The artists.musicbrainz_id migrations on a real Postgres database (#203)."""

from __future__ import annotations

import json
import pathlib
import uuid
from typing import TYPE_CHECKING, Any

import alembic.command
import alembic.config
import pytest
import sqlalchemy as sa

import resonance.config as config_module

if TYPE_CHECKING:
    from collections.abc import Iterator

pytestmark = pytest.mark.integration

_ROOT = pathlib.Path(__file__).parent.parent.parent
_BEFORE_COLUMN = "a7b8c9d0e1f2"
_COLUMN = "b8c9d0e1f2a3"
_INDEX = "c9d0e1f2a3b4"


class MigrationDB:
    """A throwaway database migrated by alembic, with a sync engine onto it."""

    def __init__(self, name: str, monkeypatch: pytest.MonkeyPatch) -> None:
        self.name = name
        self._monkeypatch = monkeypatch
        monkeypatch.setenv("PGDATABASE", name)
        self.engine = sa.create_engine(config_module.Settings().sync_database_url)

    def upgrade(self, revision: str) -> None:
        cfg = alembic.config.Config(str(_ROOT / "alembic.ini"))
        alembic.command.upgrade(cfg, revision)

    def insert_artist(self, name: str, links: Any) -> uuid.UUID:
        artist_id = uuid.uuid4()
        with self.engine.begin() as conn:
            conn.execute(
                sa.text(
                    "INSERT INTO artists (id, name, service_links, created_at,"
                    " updated_at) VALUES (:id, :name, CAST(:links AS json),"
                    " now(), now())"
                ),
                {
                    "id": artist_id,
                    "name": name,
                    "links": None if links is None else json.dumps(links),
                },
            )
        return artist_id

    def mbids(self) -> dict[uuid.UUID, str | None]:
        with self.engine.connect() as conn:
            rows = conn.execute(sa.text("SELECT id, musicbrainz_id FROM artists"))
            return {row.id: row.musicbrainz_id for row in rows}

    def index_exists(self) -> bool:
        with self.engine.connect() as conn:
            found = conn.execute(
                sa.text("SELECT 1 FROM pg_class WHERE relname = :name"),
                {"name": "uq_artists_musicbrainz_id"},
            )
            return found.first() is not None

    def revision(self) -> str:
        with self.engine.connect() as conn:
            return str(
                conn.execute(
                    sa.text("SELECT version_num FROM alembic_version")
                ).scalar_one()
            )


@pytest.fixture
def migration_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[MigrationDB]:
    settings = config_module.Settings()
    admin = sa.create_engine(settings.sync_database_url, isolation_level="AUTOCOMMIT")
    name = f"mig_{uuid.uuid4().hex[:12]}"
    try:
        with admin.connect() as conn:
            conn.execute(sa.text(f'CREATE DATABASE "{name}"'))
    except sa.exc.OperationalError as exc:
        admin.dispose()
        pytest.skip(f"PostgreSQL unavailable: {exc}")
    db = MigrationDB(name, monkeypatch)
    try:
        yield db
    finally:
        db.engine.dispose()
        with admin.connect() as conn:
            conn.execute(sa.text(f'DROP DATABASE "{name}" WITH (FORCE)'))
        admin.dispose()


def test_column_migration_backfills_normalized_mbid(migration_db: MigrationDB) -> None:
    migration_db.upgrade(_BEFORE_COLUMN)
    canonical = migration_db.insert_artist("A", {"musicbrainz": {"id": " AbC "}})
    legacy = migration_db.insert_artist("B", {"listenbrainz": "DEF"})
    both = migration_db.insert_artist(
        "C", {"musicbrainz": {"id": "new"}, "listenbrainz": "old"}
    )
    legacy_dict = migration_db.insert_artist("D", {"listenbrainz": {"id": "x"}})
    none = migration_db.insert_artist("E", None)
    blank = migration_db.insert_artist("F", {"musicbrainz": {"id": ""}})

    migration_db.upgrade(_COLUMN)

    assert migration_db.mbids() == {
        canonical: "abc",
        legacy: "def",
        both: "new",
        legacy_dict: None,
        none: None,
        blank: None,
    }


def test_index_migration_refuses_duplicates_without_half_applying(
    migration_db: MigrationDB,
) -> None:
    migration_db.upgrade(_COLUMN)
    migration_db.insert_artist("A", {"musicbrainz": {"id": "same"}})
    migration_db.insert_artist("B", {"listenbrainz": "SAME"})

    with pytest.raises(Exception, match="same \\(2\\)"):
        migration_db.upgrade(_INDEX)

    assert not migration_db.index_exists()
    assert migration_db.revision() == _COLUMN


def test_index_migration_resyncs_stale_rows_then_builds(
    migration_db: MigrationDB,
) -> None:
    migration_db.upgrade(_COLUMN)
    # Written by a pod older than the column: service_links set, column NULL.
    stale = migration_db.insert_artist("A", {"listenbrainz": "Late-Mbid"})
    migration_db.insert_artist("B", None)

    migration_db.upgrade(_INDEX)

    assert migration_db.index_exists()
    assert migration_db.mbids()[stale] == "late-mbid"
    other = migration_db.insert_artist("C", None)
    with pytest.raises(sa.exc.IntegrityError), migration_db.engine.begin() as conn:
        conn.execute(
            sa.text("UPDATE artists SET musicbrainz_id = 'late-mbid' WHERE id = :id"),
            {"id": other},
        )
