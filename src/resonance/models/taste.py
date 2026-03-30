"""User taste signal models: artist and track relations."""

from __future__ import annotations

import datetime
import uuid

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module


class UserArtistRelation(base_module.TimestampMixin, base_module.Base):
    """A user's relationship with an artist (follow, favorite, etc.)."""

    __tablename__ = "user_artist_relations"
    __table_args__ = (
        sa.UniqueConstraint(
            "user_id",
            "artist_id",
            "relation_type",
            "source_service",
            name="uq_user_artist_relations_composite",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE"), nullable=False
    )
    relation_type: orm.Mapped[types_module.ArtistRelationType] = orm.mapped_column(
        sa.Enum(types_module.ArtistRelationType, native_enum=False), nullable=False
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
    source_connection_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE"), nullable=False
    )
    discovered_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )


class UserTrackRelation(base_module.TimestampMixin, base_module.Base):
    """A user's relationship with a track (like, love, etc.)."""

    __tablename__ = "user_track_relations"
    __table_args__ = (
        sa.UniqueConstraint(
            "user_id",
            "track_id",
            "relation_type",
            "source_service",
            name="uq_user_track_relations_composite",
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
    relation_type: orm.Mapped[types_module.TrackRelationType] = orm.mapped_column(
        sa.Enum(types_module.TrackRelationType, native_enum=False), nullable=False
    )
    source_service: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
    source_connection_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("service_connections.id", ondelete="CASCADE"), nullable=False
    )
    discovered_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )
