"""Tests for SQLAlchemy models and shared types."""

from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models as models_module
import resonance.models.base as base_module
import resonance.models.concert as concert_module
import resonance.models.generator as generator_module
import resonance.models.music as music_module
import resonance.models.playlist as playlist_module
import resonance.models.task as task_module
import resonance.models.taste as taste_module
import resonance.models.user as user_module
import resonance.types as types_module

# ---------------------------------------------------------------------------
# ServiceType enum
# ---------------------------------------------------------------------------


class TestServiceType:
    """Verify ServiceType enum values match expected services."""

    def test_service_type_values(self) -> None:
        assert types_module.ServiceType.SPOTIFY == "spotify"
        assert types_module.ServiceType.LASTFM == "lastfm"
        assert types_module.ServiceType.LISTENBRAINZ == "listenbrainz"
        assert types_module.ServiceType.SONGKICK == "songkick"
        assert types_module.ServiceType.BANDSINTOWN == "bandsintown"
        assert types_module.ServiceType.BANDCAMP == "bandcamp"
        assert types_module.ServiceType.SOUNDCLOUD == "soundcloud"
        assert types_module.ServiceType.ICAL == "ical"
        assert types_module.ServiceType.TEST == "test"

    def test_service_type_count(self) -> None:
        assert len(types_module.ServiceType) == 9


class TestArtistRelationType:
    """Verify ArtistRelationType enum values."""

    def test_values(self) -> None:
        assert types_module.ArtistRelationType.FOLLOW == "follow"
        assert types_module.ArtistRelationType.FAVORITE == "favorite"


class TestTrackRelationType:
    """Verify TrackRelationType enum values."""

    def test_values(self) -> None:
        assert types_module.TrackRelationType.LIKE == "like"
        assert types_module.TrackRelationType.LOVE == "love"


class TestUserRole:
    """Verify UserRole enum values."""

    def test_values(self) -> None:
        assert types_module.UserRole.USER == "user"
        assert types_module.UserRole.ADMIN == "admin"
        assert types_module.UserRole.OWNER == "owner"

    def test_user_role_count(self) -> None:
        assert len(types_module.UserRole) == 3


class TestSyncType:
    """Verify SyncType enum values."""

    def test_values(self) -> None:
        assert types_module.SyncType.FULL == "full"
        assert types_module.SyncType.INCREMENTAL == "incremental"


class TestSyncStatus:
    """Verify SyncStatus enum values."""

    def test_values(self) -> None:
        assert types_module.SyncStatus.PENDING == "pending"
        assert types_module.SyncStatus.RUNNING == "running"
        assert types_module.SyncStatus.COMPLETED == "completed"
        assert types_module.SyncStatus.FAILED == "failed"


class TestTaskType:
    """Verify TaskType enum values."""

    def test_values(self) -> None:
        assert types_module.TaskType.SYNC_JOB == "sync_job"
        assert types_module.TaskType.TIME_RANGE == "time_range"
        assert types_module.TaskType.PAGE_FETCH == "page_fetch"
        assert types_module.TaskType.BULK_JOB == "bulk_job"
        assert types_module.TaskType.CALENDAR_SYNC == "calendar_sync"

    def test_task_type_count(self) -> None:
        assert len(types_module.TaskType) == 8


class TestAttendanceStatus:
    """Verify AttendanceStatus enum values."""

    def test_values(self) -> None:
        assert types_module.AttendanceStatus.GOING == "going"
        assert types_module.AttendanceStatus.INTERESTED == "interested"
        assert types_module.AttendanceStatus.NONE == "none"

    def test_attendance_status_count(self) -> None:
        assert len(types_module.AttendanceStatus) == 3


class TestCandidateStatus:
    """Verify CandidateStatus enum values."""

    def test_values(self) -> None:
        assert types_module.CandidateStatus.PENDING == "pending"
        assert types_module.CandidateStatus.ACCEPTED == "accepted"
        assert types_module.CandidateStatus.REJECTED == "rejected"

    def test_candidate_status_count(self) -> None:
        assert len(types_module.CandidateStatus) == 3


# ---------------------------------------------------------------------------
# Helper: column inspector
# ---------------------------------------------------------------------------


def _get_column(table: sa.Table, name: str) -> sa.Column[object]:
    """Return a column from a table by name, raising if not found."""
    col = table.columns.get(name)
    assert col is not None, f"Column {name!r} not found in table {table.name!r}"
    return col


# ---------------------------------------------------------------------------
# User models
# ---------------------------------------------------------------------------


class TestUserModel:
    """Tests for the User model."""

    def test_user_table_name(self) -> None:
        assert user_module.User.__tablename__ == "users"

    def test_user_has_expected_columns(self) -> None:
        table: sa.Table = user_module.User.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "display_name",
            "email",
            "created_at",
            "updated_at",
        }

    def test_user_id_is_uuid(self) -> None:
        col = _get_column(user_module.User.__table__, "id")  # type: ignore[arg-type]
        assert isinstance(col.type, sa.Uuid)

    def test_user_email_is_nullable(self) -> None:
        col = _get_column(user_module.User.__table__, "email")  # type: ignore[arg-type]
        assert col.nullable is True

    def test_user_has_connections_relationship(self) -> None:
        mapper: orm.Mapper[user_module.User] = orm.class_mapper(user_module.User)
        assert "connections" in mapper.relationships

    def test_user_has_role_column(self) -> None:
        table: sa.Table = user_module.User.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert "role" in col_names

    def test_user_role_defaults_to_user(self) -> None:
        user = user_module.User(display_name="Test User")
        assert user.role == types_module.UserRole.USER

    def test_user_role_can_be_set_on_creation(self) -> None:
        user = user_module.User(
            display_name="Admin User",
            role=types_module.UserRole.ADMIN,
        )
        assert user.role == types_module.UserRole.ADMIN

    def test_user_role_column_is_enum(self) -> None:
        col = _get_column(user_module.User.__table__, "role")  # type: ignore[arg-type]
        assert isinstance(col.type, sa.Enum)

    def test_user_role_column_not_nullable(self) -> None:
        col = _get_column(user_module.User.__table__, "role")  # type: ignore[arg-type]
        assert col.nullable is False


class TestServiceConnectionModel:
    """Tests for the ServiceConnection model."""

    def test_table_name(self) -> None:
        assert user_module.ServiceConnection.__tablename__ == "service_connections"

    def test_expected_columns(self) -> None:
        table: sa.Table = user_module.ServiceConnection.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        expected = {
            "id",
            "user_id",
            "service_type",
            "external_user_id",
            "encrypted_access_token",
            "encrypted_refresh_token",
            "token_expires_at",
            "scopes",
            "url",
            "label",
            "enabled",
            "connected_at",
            "last_synced_at",
            "sync_watermark",
            "created_at",
            "updated_at",
        }
        assert col_names >= expected

    def test_service_type_column_uses_enum(self) -> None:
        col = _get_column(
            user_module.ServiceConnection.__table__,  # type: ignore[arg-type]
            "service_type",
        )
        assert isinstance(col.type, sa.Enum)

    def test_sync_watermark_defaults_to_empty_dict(self) -> None:
        conn = user_module.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SPOTIFY,
            external_user_id="test",
            encrypted_access_token="enc",
        )
        assert conn.sync_watermark == {}

    def test_sync_watermark_stores_dict(self) -> None:
        watermark = {"listens": {"last_listened_at": 1700000000}}
        conn = user_module.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SPOTIFY,
            external_user_id="test",
            encrypted_access_token="enc",
            sync_watermark=watermark,
        )
        assert conn.sync_watermark == watermark

    def test_unique_constraint_exists(self) -> None:
        table: sa.Table = user_module.ServiceConnection.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset({"user_id", "service_type", "external_user_id"})
        assert expected in uc_col_sets


class TestServiceConnectionUnified:
    """Tests for the unified ServiceConnection model (feed connection fields)."""

    def test_feed_connection_no_token(self) -> None:
        """A Songkick connection needs no access token."""
        conn = user_module.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id="mike123",
        )
        assert conn.encrypted_access_token is None
        assert conn.external_user_id == "mike123"

    def test_ical_connection_with_url(self) -> None:
        """An iCal connection uses url and label; external_user_id can be None."""
        conn = user_module.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.ICAL,
            url="https://example.com/calendar.ics",
            label="My concert calendar",
        )
        assert conn.external_user_id is None
        assert conn.url == "https://example.com/calendar.ics"
        assert conn.label == "My concert calendar"

    def test_enabled_defaults_true(self) -> None:
        """The enabled field defaults to True when not specified."""
        conn = user_module.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id="mike123",
        )
        assert conn.enabled is True

    def test_last_synced_at(self) -> None:
        """last_synced_at persists when set."""
        now = datetime.datetime.now(datetime.UTC)
        conn = user_module.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SONGKICK,
            external_user_id="mike123",
            last_synced_at=now,
        )
        assert conn.last_synced_at == now


# ---------------------------------------------------------------------------
# Music models
# ---------------------------------------------------------------------------


class TestArtistModel:
    """Tests for the Artist model."""

    def test_table_name(self) -> None:
        assert music_module.Artist.__tablename__ == "artists"

    def test_expected_columns(self) -> None:
        table: sa.Table = music_module.Artist.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "name",
            "service_links",
            "created_at",
            "updated_at",
        }

    def test_tracks_relationship(self) -> None:
        mapper: orm.Mapper[music_module.Artist] = orm.class_mapper(music_module.Artist)
        assert "tracks" in mapper.relationships


class TestTrackModel:
    """Tests for the Track model."""

    def test_table_name(self) -> None:
        assert music_module.Track.__tablename__ == "tracks"

    def test_expected_columns(self) -> None:
        table: sa.Table = music_module.Track.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "title",
            "artist_id",
            "service_links",
            "created_at",
            "updated_at",
        }

    def test_artist_fk(self) -> None:
        col = _get_column(music_module.Track.__table__, "artist_id")  # type: ignore[arg-type]
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "artists.id" in fk_targets

    def test_artist_relationship(self) -> None:
        mapper: orm.Mapper[music_module.Track] = orm.class_mapper(music_module.Track)
        assert "artist" in mapper.relationships


class TestListeningEventModel:
    """Tests for the ListeningEvent model."""

    def test_table_name(self) -> None:
        assert music_module.ListeningEvent.__tablename__ == "listening_events"

    def test_expected_columns(self) -> None:
        table: sa.Table = music_module.ListeningEvent.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "user_id",
            "track_id",
            "source_service",
            "listened_at",
            "created_at",
            "updated_at",
        }

    def test_source_service_is_enum(self) -> None:
        col = _get_column(
            music_module.ListeningEvent.__table__,  # type: ignore[arg-type]
            "source_service",
        )
        assert isinstance(col.type, sa.Enum)

    def test_index_on_user_listened_at(self) -> None:
        table: sa.Table = music_module.ListeningEvent.__table__  # type: ignore[assignment]
        index_col_sets = [
            frozenset(col.name for col in idx.columns) for idx in table.indexes
        ]
        expected = frozenset({"user_id", "listened_at"})
        assert expected in index_col_sets

    def test_unique_constraint_on_user_track_listened_at(self) -> None:
        table: sa.Table = music_module.ListeningEvent.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset({"user_id", "track_id", "listened_at"})
        assert expected in uc_col_sets


# ---------------------------------------------------------------------------
# Taste models
# ---------------------------------------------------------------------------


class TestUserArtistRelationModel:
    """Tests for the UserArtistRelation model."""

    def test_table_name(self) -> None:
        assert taste_module.UserArtistRelation.__tablename__ == "user_artist_relations"

    def test_expected_columns(self) -> None:
        table: sa.Table = taste_module.UserArtistRelation.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "user_id",
            "artist_id",
            "relation_type",
            "source_service",
            "source_connection_id",
            "discovered_at",
            "created_at",
            "updated_at",
        }

    def test_relation_type_is_enum(self) -> None:
        col = _get_column(
            taste_module.UserArtistRelation.__table__,  # type: ignore[arg-type]
            "relation_type",
        )
        assert isinstance(col.type, sa.Enum)

    def test_unique_constraint(self) -> None:
        table: sa.Table = taste_module.UserArtistRelation.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset(
            {
                "user_id",
                "artist_id",
                "relation_type",
                "source_service",
            }
        )
        assert expected in uc_col_sets


class TestUserTrackRelationModel:
    """Tests for the UserTrackRelation model."""

    def test_table_name(self) -> None:
        assert taste_module.UserTrackRelation.__tablename__ == "user_track_relations"

    def test_expected_columns(self) -> None:
        table: sa.Table = taste_module.UserTrackRelation.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "user_id",
            "track_id",
            "relation_type",
            "source_service",
            "source_connection_id",
            "discovered_at",
            "created_at",
            "updated_at",
        }

    def test_relation_type_is_enum(self) -> None:
        col = _get_column(
            taste_module.UserTrackRelation.__table__,  # type: ignore[arg-type]
            "relation_type",
        )
        assert isinstance(col.type, sa.Enum)

    def test_unique_constraint(self) -> None:
        table: sa.Table = taste_module.UserTrackRelation.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset(
            {
                "user_id",
                "track_id",
                "relation_type",
                "source_service",
            }
        )
        assert expected in uc_col_sets


# ---------------------------------------------------------------------------
# Task model
# ---------------------------------------------------------------------------


class TestTask:
    """Tests for the Task model."""

    def test_progress_fields_are_biginteger(self) -> None:
        table: sa.Table = task_module.Task.__table__  # type: ignore[assignment]
        current_col = _get_column(table, "progress_current")
        total_col = _get_column(table, "progress_total")
        assert isinstance(current_col.type, sa.BigInteger)
        assert isinstance(total_col.type, sa.BigInteger)

    def test_sync_task_has_expected_columns(self) -> None:
        task = task_module.Task(
            user_id=uuid.uuid4(),
            service_connection_id=uuid.uuid4(),
            task_type=types_module.TaskType.SYNC_JOB,
            status=types_module.SyncStatus.PENDING,
        )
        assert task.parent_id is None
        assert task.params == {}
        assert task.result == {}
        assert task.progress_current == 0
        assert task.progress_total is None
        assert task.error_message is None

    def test_sync_task_tablename(self) -> None:
        assert task_module.Task.__tablename__ == "sync_tasks"


# ---------------------------------------------------------------------------
# Concert models
# ---------------------------------------------------------------------------


class TestVenueModel:
    """Tests for the Venue model."""

    def test_table_name(self) -> None:
        assert concert_module.Venue.__tablename__ == "venues"

    def test_expected_columns(self) -> None:
        table: sa.Table = concert_module.Venue.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "name",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "service_links",
            "created_at",
            "updated_at",
        }

    def test_unique_constraint(self) -> None:
        table: sa.Table = concert_module.Venue.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset({"name", "city", "state", "country"})
        assert expected in uc_col_sets

    def test_events_relationship(self) -> None:
        mapper: orm.Mapper[concert_module.Venue] = orm.class_mapper(
            concert_module.Venue
        )
        assert "events" in mapper.relationships


class TestEventModel:
    """Tests for the Event model."""

    def test_table_name(self) -> None:
        assert concert_module.Event.__tablename__ == "events"

    def test_expected_columns(self) -> None:
        table: sa.Table = concert_module.Event.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "title",
            "event_date",
            "venue_id",
            "source_service",
            "external_id",
            "external_url",
            "service_links",
            "created_at",
            "updated_at",
        }

    def test_venue_id_fk(self) -> None:
        col = _get_column(concert_module.Event.__table__, "venue_id")  # type: ignore[arg-type]
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "venues.id" in fk_targets

    def test_source_service_is_enum(self) -> None:
        col = _get_column(
            concert_module.Event.__table__,  # type: ignore[arg-type]
            "source_service",
        )
        assert isinstance(col.type, sa.Enum)

    def test_unique_constraint(self) -> None:
        table: sa.Table = concert_module.Event.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset({"source_service", "external_id"})
        assert expected in uc_col_sets

    def test_venue_relationship(self) -> None:
        mapper: orm.Mapper[concert_module.Event] = orm.class_mapper(
            concert_module.Event
        )
        assert "venue" in mapper.relationships

    def test_artist_candidates_relationship(self) -> None:
        mapper: orm.Mapper[concert_module.Event] = orm.class_mapper(
            concert_module.Event
        )
        assert "artist_candidates" in mapper.relationships

    def test_artists_relationship(self) -> None:
        mapper: orm.Mapper[concert_module.Event] = orm.class_mapper(
            concert_module.Event
        )
        assert "artists" in mapper.relationships


class TestEventArtistCandidateModel:
    """Tests for the EventArtistCandidate model."""

    def test_table_name(self) -> None:
        assert (
            concert_module.EventArtistCandidate.__tablename__
            == "event_artist_candidates"
        )

    def test_expected_columns(self) -> None:
        table: sa.Table = concert_module.EventArtistCandidate.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "event_id",
            "raw_name",
            "matched_artist_id",
            "position",
            "confidence_score",
            "status",
            "created_at",
            "updated_at",
        }

    def test_event_id_fk(self) -> None:
        col = _get_column(
            concert_module.EventArtistCandidate.__table__,  # type: ignore[arg-type]
            "event_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "events.id" in fk_targets

    def test_matched_artist_id_fk(self) -> None:
        col = _get_column(
            concert_module.EventArtistCandidate.__table__,  # type: ignore[arg-type]
            "matched_artist_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "artists.id" in fk_targets

    def test_status_is_enum(self) -> None:
        col = _get_column(
            concert_module.EventArtistCandidate.__table__,  # type: ignore[arg-type]
            "status",
        )
        assert isinstance(col.type, sa.Enum)

    def test_unique_constraint(self) -> None:
        table: sa.Table = concert_module.EventArtistCandidate.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset({"event_id", "raw_name"})
        assert expected in uc_col_sets

    def test_event_relationship(self) -> None:
        mapper: orm.Mapper[concert_module.EventArtistCandidate] = orm.class_mapper(
            concert_module.EventArtistCandidate
        )
        assert "event" in mapper.relationships


class TestEventArtistModel:
    """Tests for the EventArtist model."""

    def test_table_name(self) -> None:
        assert concert_module.EventArtist.__tablename__ == "event_artists"

    def test_expected_columns(self) -> None:
        table: sa.Table = concert_module.EventArtist.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "event_id",
            "artist_id",
            "position",
            "raw_name",
            "created_at",
            "updated_at",
        }

    def test_event_id_fk(self) -> None:
        col = _get_column(
            concert_module.EventArtist.__table__,  # type: ignore[arg-type]
            "event_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "events.id" in fk_targets

    def test_artist_id_fk(self) -> None:
        col = _get_column(
            concert_module.EventArtist.__table__,  # type: ignore[arg-type]
            "artist_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "artists.id" in fk_targets

    def test_unique_constraint(self) -> None:
        table: sa.Table = concert_module.EventArtist.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset({"event_id", "artist_id"})
        assert expected in uc_col_sets

    def test_event_relationship(self) -> None:
        mapper: orm.Mapper[concert_module.EventArtist] = orm.class_mapper(
            concert_module.EventArtist
        )
        assert "event" in mapper.relationships


class TestUserEventAttendanceModel:
    """Tests for the UserEventAttendance model."""

    def test_table_name(self) -> None:
        assert (
            concert_module.UserEventAttendance.__tablename__ == "user_event_attendance"
        )

    def test_expected_columns(self) -> None:
        table: sa.Table = concert_module.UserEventAttendance.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "user_id",
            "event_id",
            "status",
            "source_service",
            "created_at",
            "updated_at",
        }

    def test_user_id_fk(self) -> None:
        col = _get_column(
            concert_module.UserEventAttendance.__table__,  # type: ignore[arg-type]
            "user_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "users.id" in fk_targets

    def test_event_id_fk(self) -> None:
        col = _get_column(
            concert_module.UserEventAttendance.__table__,  # type: ignore[arg-type]
            "event_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "events.id" in fk_targets

    def test_status_is_enum(self) -> None:
        col = _get_column(
            concert_module.UserEventAttendance.__table__,  # type: ignore[arg-type]
            "status",
        )
        assert isinstance(col.type, sa.Enum)

    def test_source_service_is_enum(self) -> None:
        col = _get_column(
            concert_module.UserEventAttendance.__table__,  # type: ignore[arg-type]
            "source_service",
        )
        assert isinstance(col.type, sa.Enum)

    def test_unique_constraint(self) -> None:
        table: sa.Table = concert_module.UserEventAttendance.__table__  # type: ignore[assignment]
        unique_constraints = [
            c for c in table.constraints if isinstance(c, sa.UniqueConstraint)
        ]
        uc_col_sets = [
            frozenset(col.name for col in uc.columns) for uc in unique_constraints
        ]
        expected = frozenset({"user_id", "event_id"})
        assert expected in uc_col_sets


# ---------------------------------------------------------------------------
# Playlist models
# ---------------------------------------------------------------------------


class TestPlaylistModel:
    """Tests for the Playlist model."""

    def test_table_name(self) -> None:
        assert playlist_module.Playlist.__tablename__ == "playlists"

    def test_expected_columns(self) -> None:
        table: sa.Table = playlist_module.Playlist.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "user_id",
            "name",
            "description",
            "track_count",
            "is_pinned",
            "created_at",
            "updated_at",
        }

    def test_user_id_fk(self) -> None:
        col = _get_column(
            playlist_module.Playlist.__table__,  # type: ignore[arg-type]
            "user_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "users.id" in fk_targets

    def test_tracks_relationship(self) -> None:
        mapper: orm.Mapper[playlist_module.Playlist] = orm.class_mapper(
            playlist_module.Playlist
        )
        assert "tracks" in mapper.relationships

    def test_playlist_fields(self) -> None:
        playlist = models_module.Playlist(
            user_id=uuid.uuid4(),
            name="Concert Prep",
        )
        assert playlist.name == "Concert Prep"
        assert playlist.track_count == 0
        assert playlist.is_pinned is False
        assert playlist.description is None


class TestPlaylistTrackModel:
    """Tests for the PlaylistTrack model."""

    def test_table_name(self) -> None:
        assert playlist_module.PlaylistTrack.__tablename__ == "playlist_tracks"

    def test_expected_columns(self) -> None:
        table: sa.Table = playlist_module.PlaylistTrack.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "playlist_id",
            "track_id",
            "position",
            "score",
            "source",
            "created_at",
            "updated_at",
        }

    def test_playlist_id_fk(self) -> None:
        col = _get_column(
            playlist_module.PlaylistTrack.__table__,  # type: ignore[arg-type]
            "playlist_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "playlists.id" in fk_targets

    def test_track_id_fk(self) -> None:
        col = _get_column(
            playlist_module.PlaylistTrack.__table__,  # type: ignore[arg-type]
            "track_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "tracks.id" in fk_targets

    def test_playlist_relationship(self) -> None:
        mapper: orm.Mapper[playlist_module.PlaylistTrack] = orm.class_mapper(
            playlist_module.PlaylistTrack
        )
        assert "playlist" in mapper.relationships

    def test_track_relationship(self) -> None:
        mapper: orm.Mapper[playlist_module.PlaylistTrack] = orm.class_mapper(
            playlist_module.PlaylistTrack
        )
        assert "track" in mapper.relationships

    def test_playlist_track_fields(self) -> None:
        track = models_module.PlaylistTrack(
            playlist_id=uuid.uuid4(),
            track_id=uuid.uuid4(),
            position=1,
            source="library",
        )
        assert track.position == 1
        assert track.source == "library"
        assert track.score is None


# ---------------------------------------------------------------------------
# Package re-exports
# ---------------------------------------------------------------------------


class TestModelsPackageExports:
    """Verify the models package re-exports key classes."""

    def test_base_exported(self) -> None:
        assert models_module.Base is base_module.Base

    def test_user_exported(self) -> None:
        assert models_module.User is user_module.User

    def test_service_connection_exported(self) -> None:
        assert models_module.ServiceConnection is user_module.ServiceConnection

    def test_artist_exported(self) -> None:
        assert models_module.Artist is music_module.Artist

    def test_track_exported(self) -> None:
        assert models_module.Track is music_module.Track

    def test_listening_event_exported(self) -> None:
        assert models_module.ListeningEvent is music_module.ListeningEvent

    def test_user_artist_relation_exported(self) -> None:
        assert models_module.UserArtistRelation is taste_module.UserArtistRelation

    def test_user_track_relation_exported(self) -> None:
        assert models_module.UserTrackRelation is taste_module.UserTrackRelation

    def test_sync_task_exported(self) -> None:
        assert models_module.Task is task_module.Task

    def test_venue_exported(self) -> None:
        assert models_module.Venue is concert_module.Venue

    def test_event_exported(self) -> None:
        assert models_module.Event is concert_module.Event

    def test_event_artist_exported(self) -> None:
        assert models_module.EventArtist is concert_module.EventArtist

    def test_event_artist_candidate_exported(self) -> None:
        assert models_module.EventArtistCandidate is concert_module.EventArtistCandidate

    def test_user_event_attendance_exported(self) -> None:
        assert models_module.UserEventAttendance is concert_module.UserEventAttendance

    def test_playlist_exported(self) -> None:
        assert models_module.Playlist is playlist_module.Playlist

    def test_playlist_track_exported(self) -> None:
        assert models_module.PlaylistTrack is playlist_module.PlaylistTrack

    def test_generator_profile_exported(self) -> None:
        assert models_module.GeneratorProfile is generator_module.GeneratorProfile

    def test_generation_record_exported(self) -> None:
        assert models_module.GenerationRecord is generator_module.GenerationRecord


# ---------------------------------------------------------------------------
# Generator models
# ---------------------------------------------------------------------------


class TestGeneratorModels:
    def test_generator_profile_fields(self) -> None:
        profile = models_module.GeneratorProfile(
            user_id=uuid.uuid4(),
            name="Show Prep",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
            input_references={"event_id": str(uuid.uuid4())},
            parameter_values={"hit_depth": 75, "familiarity": 40},
        )
        assert profile.name == "Show Prep"
        assert profile.generator_type == types_module.GeneratorType.CONCERT_PREP
        assert profile.parameter_values["hit_depth"] == 75
        assert profile.auto_sync_targets is None

    def test_generation_record_fields(self) -> None:
        record = models_module.GenerationRecord(
            profile_id=uuid.uuid4(),
            playlist_id=uuid.uuid4(),
            parameter_snapshot={"hit_depth": 75},
            freshness_target=50,
        )
        assert record.freshness_target == 50
        assert record.freshness_actual is None
        assert record.generation_duration_ms is None

    def test_generator_profile_table_name(self) -> None:
        assert generator_module.GeneratorProfile.__tablename__ == "generator_profiles"

    def test_generation_record_table_name(self) -> None:
        assert generator_module.GenerationRecord.__tablename__ == "generation_records"

    def test_generator_profile_expected_columns(self) -> None:
        table: sa.Table = generator_module.GeneratorProfile.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "user_id",
            "name",
            "generator_type",
            "input_references",
            "parameter_values",
            "auto_sync_targets",
            "created_at",
            "updated_at",
        }

    def test_generation_record_expected_columns(self) -> None:
        table: sa.Table = generator_module.GenerationRecord.__table__  # type: ignore[assignment]
        col_names = {c.name for c in table.columns}
        assert col_names >= {
            "id",
            "profile_id",
            "playlist_id",
            "parameter_snapshot",
            "freshness_target",
            "freshness_actual",
            "generation_duration_ms",
            "track_sources_summary",
            "created_at",
            "updated_at",
        }

    def test_profile_user_id_fk(self) -> None:
        col = _get_column(
            generator_module.GeneratorProfile.__table__,  # type: ignore[arg-type]
            "user_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "users.id" in fk_targets

    def test_record_profile_id_fk(self) -> None:
        col = _get_column(
            generator_module.GenerationRecord.__table__,  # type: ignore[arg-type]
            "profile_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "generator_profiles.id" in fk_targets

    def test_record_playlist_id_fk(self) -> None:
        col = _get_column(
            generator_module.GenerationRecord.__table__,  # type: ignore[arg-type]
            "playlist_id",
        )
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "playlists.id" in fk_targets

    def test_generator_type_is_enum(self) -> None:
        col = _get_column(
            generator_module.GeneratorProfile.__table__,  # type: ignore[arg-type]
            "generator_type",
        )
        assert isinstance(col.type, sa.Enum)

    def test_generations_relationship(self) -> None:
        mapper: orm.Mapper[generator_module.GeneratorProfile] = orm.class_mapper(
            generator_module.GeneratorProfile
        )
        assert "generations" in mapper.relationships

    def test_profile_relationship(self) -> None:
        mapper: orm.Mapper[generator_module.GenerationRecord] = orm.class_mapper(
            generator_module.GenerationRecord
        )
        assert "profile" in mapper.relationships

    def test_playlist_relationship(self) -> None:
        mapper: orm.Mapper[generator_module.GenerationRecord] = orm.class_mapper(
            generator_module.GenerationRecord
        )
        assert "playlist" in mapper.relationships

    def test_generator_profile_json_defaults(self) -> None:
        """JSON columns with mutable defaults get fresh dicts at construction."""
        profile = models_module.GeneratorProfile(
            user_id=uuid.uuid4(),
            name="Test",
            generator_type=types_module.GeneratorType.CONCERT_PREP,
        )
        assert profile.input_references == {}
        assert profile.parameter_values == {}
