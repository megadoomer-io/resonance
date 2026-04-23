"""Concert domain models: venues, events, artist candidates, and attendance."""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class Venue(base_module.TimestampMixin, base_module.Base):
    """A concert venue with optional location details."""

    __tablename__ = "venues"
    __table_args__ = (
        sa.UniqueConstraint(
            "name",
            "city",
            "state",
            "country",
            name="uq_venues_name_location",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    address: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(512), nullable=True, default=None
    )
    city: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    state: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    postal_code: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(32), nullable=True, default=None
    )
    country: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(2), nullable=True, default=None
    )
    service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        sa.JSON, nullable=True, default=None
    )

    events: orm.Mapped[list[Event]] = orm.relationship(back_populates="venue")


class Event(base_module.TimestampMixin, base_module.Base):
    """A concert or live music event from an external service."""

    __tablename__ = "events"
    __table_args__ = (
        sa.UniqueConstraint(
            "source_service",
            "external_id",
            name="uq_events_source_external",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    title: orm.Mapped[str] = orm.mapped_column(sa.String(1024), nullable=False)
    event_date: orm.Mapped[datetime.date] = orm.mapped_column(sa.Date, nullable=False)
    venue_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("venues.id", ondelete="SET NULL"), nullable=True
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
    external_id: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    external_url: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(1024), nullable=True, default=None
    )
    service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        sa.JSON, nullable=True, default=None
    )

    venue: orm.Mapped[Venue | None] = orm.relationship(back_populates="events")
    artist_candidates: orm.Mapped[list[EventArtistCandidate]] = orm.relationship(
        back_populates="event", cascade="all, delete-orphan"
    )
    artists: orm.Mapped[list[EventArtist]] = orm.relationship(
        back_populates="event", cascade="all, delete-orphan"
    )


class EventArtistCandidate(base_module.TimestampMixin, base_module.Base):
    """A raw artist name extracted from an event, pending entity resolution."""

    __tablename__ = "event_artist_candidates"
    __table_args__ = (
        sa.UniqueConstraint(
            "event_id",
            "raw_name",
            name="uq_event_artist_candidates_event_name",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    event_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    raw_name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    matched_artist_id: orm.Mapped[uuid.UUID | None] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="SET NULL"), nullable=True
    )
    position: orm.Mapped[int] = orm.mapped_column(sa.Integer, nullable=False, default=0)
    confidence_score: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    status: orm.Mapped[types_module.CandidateStatus] = orm.mapped_column(
        sa.Enum(types_module.CandidateStatus, native_enum=False),
        nullable=False,
        default=types_module.CandidateStatus.PENDING,
    )

    event: orm.Mapped[Event] = orm.relationship(back_populates="artist_candidates")


class EventArtist(base_module.TimestampMixin, base_module.Base):
    """A confirmed artist performing at an event."""

    __tablename__ = "event_artists"
    __table_args__ = (
        sa.UniqueConstraint(
            "event_id",
            "artist_id",
            name="uq_event_artists_event_artist",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    event_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE"), nullable=False
    )
    position: orm.Mapped[int] = orm.mapped_column(sa.Integer, nullable=False, default=0)
    raw_name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)

    event: orm.Mapped[Event] = orm.relationship(back_populates="artists")


class UserEventAttendance(base_module.TimestampMixin, base_module.Base):
    """A user's attendance status for an event."""

    __tablename__ = "user_event_attendance"
    __table_args__ = (
        sa.UniqueConstraint(
            "user_id",
            "event_id",
            name="uq_user_event_attendance_user_event",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    event_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False
    )
    status: orm.Mapped[types_module.AttendanceStatus] = orm.mapped_column(
        sa.Enum(types_module.AttendanceStatus, native_enum=False), nullable=False
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
