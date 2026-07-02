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
    origin: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    disambiguation: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(512), nullable=True, default=None
    )
    artist_type: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(255), nullable=True, default=None
    )
    area: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(256), nullable=True, default=None
    )
    begin_year: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True, default=None
    )
    end_year: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True, default=None
    )
    # MusicBrainz MBID-backfill bookkeeping (#71). mb_attempted_at IS NULL means
    # "not yet attempted" and is the resume key for the backfill task; the MBID
    # itself lives in service_links["musicbrainz"]["id"], not here.
    mb_attempted_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    mb_match_status: orm.Mapped[types_module.MatchStatus | None] = orm.mapped_column(
        sa.Enum(types_module.MatchStatus, native_enum=False),
        nullable=True,
        default=None,
    )
    # Genre-tag backfill bookkeeping (#136 genre model, Arc 1). genre_attempted_at
    # IS NULL means "not yet attempted" and is the resume key for GENRE_BACKFILL;
    # the tags themselves live in the artist_tags table. "attempted but no tags"
    # is genre_attempted_at IS NOT NULL AND no ArtistTag rows -- distinct from
    # unattempted (NULL).
    genre_attempted_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )

    tracks: orm.Mapped[list[Track]] = orm.relationship(
        back_populates="artist", cascade="all, delete-orphan"
    )
    # Named "tags" not "genres" because rows include non-genre folksonomy tags
    # ("seen live", "american"); the genre subset is those with genre_mbid set.
    # Async: access requires eager loading (selectinload) or awaitable_attrs.
    tags: orm.Mapped[list[ArtistTag]] = orm.relationship(
        back_populates="artist", cascade="all, delete-orphan"
    )


class ArtistTag(base_module.TimestampMixin, base_module.Base):
    """A genre/folksonomy tag for an artist (#136 genre model, Arc 1).

    Durable domain data, NOT a cache -- mirrors ArtistSimilarity. Each row records
    that ``source`` (default MusicBrainz, fetched via the ListenBrainz artist
    metadata endpoint) reports ``tag`` on the artist with folksonomy ``count``.
    ``genre_mbid`` is non-NULL only for canonical MusicBrainz *genres* (e.g.
    "electronic", "death metal"); it is NULL for free folksonomy tags ("seen
    live", "american"), so genre-vs-noise filtering is data-driven, not a
    hand-maintained stoplist. ``fetched_at`` drives refresh-if-old, not eviction.

    ``source`` is a plain string (default "musicbrainz"), NOT the ServiceType
    enum used for connector provenance elsewhere: ServiceType has no MUSICBRAINZ
    member, and this records the tag *taxonomy origin* (MusicBrainz) rather than
    the fetch transport (ListenBrainz). The writer normalizes it lowercase. Kept
    a string (not an enum + CHECK) so a future taxonomy source needs no migration.

    Tags for an artist are replaced wholesale on refresh; the unique constraint
    guards against in-batch duplicates.
    """

    __tablename__ = "artist_tags"
    # The (artist_id, tag) unique constraint's composite btree also serves every
    # artist_id lookup (leftmost prefix): per-artist reads, the batch-load
    # WHERE artist_id IN (...), and the coverage NOT EXISTS anti-join. No separate
    # ix_artist_tags_artist_id is needed (the ArtistSimilarity precedent's extra
    # index is redundant for the same reason) -- do not "restore" it.
    __table_args__ = (
        sa.UniqueConstraint(
            "artist_id",
            "tag",
            name="uq_artist_tags_artist_tag",
        ),
        sa.CheckConstraint("count >= 0", name="ck_artist_tags_count_nonneg"),
    )

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    artist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.ForeignKey("artists.id", ondelete="CASCADE"), nullable=False
    )
    tag: orm.Mapped[str] = orm.mapped_column(sa.String(256), nullable=False)
    # Non-NULL only for canonical MusicBrainz genres; NULL for folksonomy tags.
    genre_mbid: orm.Mapped[str | None] = orm.mapped_column(
        sa.String(64), nullable=True, default=None
    )
    count: orm.Mapped[int] = orm.mapped_column(sa.Integer, nullable=False)
    source: orm.Mapped[str] = orm.mapped_column(
        sa.String(64), nullable=False, default="musicbrainz"
    )
    fetched_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
    )

    artist: orm.Mapped[Artist] = orm.relationship(back_populates="tags")


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
    duration_ms: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True, default=None
    )
    service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
        sa.JSON, nullable=True, default=None
    )
    # MusicBrainz MBID-backfill bookkeeping (#71). See Artist for semantics; the
    # recording MBID lives in service_links["musicbrainz"]["id"].
    mb_attempted_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sa.DateTime(timezone=True), nullable=True, default=None
    )
    mb_match_status: orm.Mapped[types_module.MatchStatus | None] = orm.mapped_column(
        sa.Enum(types_module.MatchStatus, native_enum=False),
        nullable=True,
        default=None,
    )
    # Track popularity (0-100) feeding the hit_depth generator parameter (#114).
    # NULL = unknown (treated as 0 at scoring time). Populated by track discovery
    # and, later, by Spotify's authoritative popularity.
    popularity_score: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True, default=None
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
