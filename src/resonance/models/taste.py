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


class ArtistSimilarity(base_module.TimestampMixin, base_module.Base):
    """A directed artist->neighbor similarity edge from a connector (#133).

    Durable domain data, NOT a cache: each row records that ``connector`` reported
    ``neighbor_name`` (with ``neighbor_mbid`` when known) as the rank-``rank``
    similar artist of ``source_artist_id``. The enrich task reads stored edges
    first and falls back to a live ``get_similar_artists`` fetch, recording the
    result. ``fetched_at`` drives refresh-if-old, not eviction -- stale edges are
    re-fetched and replaced, never expired away.

    The neighbor is stored by name + MBID rather than a FK because a neighbor may
    not be imported as an Artist yet (it becomes one only if enrichment imports
    it). Edges for a (source_artist, connector) pair are replaced wholesale on
    refresh, so the unique constraint guards against accidental duplicates within
    a batch.
    """

    __tablename__ = "artist_similarities"
    __table_args__ = (
        sa.UniqueConstraint(
            "source_artist_id",
            "connector",
            "neighbor_name",
            name="uq_artist_similarities_source_connector_neighbor",
        ),
        sa.Index(
            "ix_artist_similarities_source_connector",
            "source_artist_id",
            "connector",
        ),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    source_artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE"), nullable=False
    )
    connector: orm.Mapped[types_module.ServiceType] = orm.mapped_column(
        sa.Enum(types_module.ServiceType, native_enum=False), nullable=False
    )
    neighbor_name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    neighbor_mbid: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(64), nullable=True, default=None
    )
    rank: orm.Mapped[int] = orm.mapped_column(sa.Integer, nullable=False)
    fetched_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
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
