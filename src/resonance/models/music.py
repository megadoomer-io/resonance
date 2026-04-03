"""Music domain models: artists, tracks, and listening events."""

from __future__ import annotations

import datetime
import uuid
from typing import Any

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class Artist(base_module.TimestampMixin, base_module.Base):
    """A musical artist, potentially linked across multiple services."""

    __tablename__ = "artists"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        sa.JSON, nullable=True, default=None
    )

    tracks: orm.Mapped[list[Track]] = orm.relationship(
        back_populates="artist", cascade="all, delete-orphan"
    )


class Track(base_module.TimestampMixin, base_module.Base):
    """A musical track belonging to an artist."""

    __tablename__ = "tracks"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    title: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE"), nullable=False
    )
    service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        sa.JSON, nullable=True, default=None
    )

    artist: orm.Mapped[Artist] = orm.relationship(back_populates="tracks")


class ListeningEvent(base_module.TimestampMixin, base_module.Base):
    """A record of a user listening to a track on a specific service."""

    __tablename__ = "listening_events"
    __table_args__ = (
        sa.Index("ix_listening_events_user_listened", "user_id", "listened_at"),
        sa.UniqueConstraint(
            "user_id",
            "track_id",
            "listened_at",
            name="uq_listening_events_user_track_time",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    track_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("tracks.id", ondelete="CASCADE"), nullable=False
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
    listened_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=False
    )

    track: orm.Mapped[Track] = orm.relationship()
