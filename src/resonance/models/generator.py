"""Generator domain models: profiles (recipes) and generation records."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module
import resonance.types as types_module

if TYPE_CHECKING:
    import resonance.models.playlist as playlist_module


class GeneratorProfile(base_module.TimestampMixin, base_module.Base):
    """A saved playlist generation recipe."""

    __tablename__ = "generator_profiles"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    generator_type: orm.Mapped[types_module.GeneratorType] = orm.mapped_column(
        sa.Enum(types_module.GeneratorType, native_enum=False), nullable=False
    )
    # Draft until first generate flips it active (#133). The lineup builder
    # eagerly creates a draft so every edit persists; the profile list filters
    # status=active so half-built drafts don't show.
    status: orm.Mapped[types_module.ProfileStatus] = orm.mapped_column(
        sa.Enum(types_module.ProfileStatus, native_enum=False),
        nullable=False,
        default=types_module.ProfileStatus.DRAFT,
        server_default=types_module.ProfileStatus.DRAFT.name,
    )
    # Optimistic-concurrency token (#133). The builder (PATCH), the CLI/agent,
    # and the enrich worker all write input_references; an assert-and-bump on
    # this column turns the lost-update race into a 409/retry. Wired as the
    # mapper's version_id_col (below), so every flush of a dirty profile bumps
    # the version and a stale UPDATE raises StaleDataError instead of clobbering.
    version: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=1, server_default="1"
    )
    input_references: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, insert_default=dict
    )
    parameter_values: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False, insert_default=dict
    )
    auto_sync_targets: orm.Mapped[list[dict[str, str]] | None] = orm.mapped_column(
        sa.JSON, nullable=True
    )

    generations: orm.Mapped[list[GenerationRecord]] = orm.relationship(
        back_populates="profile",
        cascade="all, delete-orphan",
        order_by="GenerationRecord.created_at.desc()",
    )

    # Optimistic concurrency (#133): SQLAlchemy bumps `version` on every flush and
    # adds `WHERE version = <loaded>` to UPDATEs, raising StaleDataError when a
    # concurrent writer already advanced it. The editor PATCH surfaces that as a
    # 409; the enrich worker reloads and re-applies onto the fresh row.
    # RUF012 is suppressed below: ruff wants a ClassVar annotation, but
    # SQLAlchemy's DeclarativeBase types __mapper_args__ as an instance
    # attribute, so a ClassVar annotation makes mypy reject the override. The
    # dict is the documented SQLAlchemy idiom and is never mutated.
    __mapper_args__ = {"version_id_col": version}  # noqa: RUF012


_GENERATOR_PROFILE_DEFAULTS: dict[str, object] = {
    "input_references": dict,
    "parameter_values": dict,
}


@sa.event.listens_for(GeneratorProfile, "init")
def _set_generator_profile_defaults(
    target: GeneratorProfile,
    _args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    """Apply Python-side defaults for GeneratorProfile fields at construction."""
    for attr, default in _GENERATOR_PROFILE_DEFAULTS.items():
        if attr not in kwargs:
            value = default() if callable(default) else default
            setattr(target, attr, value)


class GenerationRecord(base_module.TimestampMixin, base_module.Base):
    """Links a Playlist to the GeneratorProfile run that created it."""

    __tablename__ = "generation_records"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    profile_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid,
        sa.ForeignKey("generator_profiles.id", ondelete="CASCADE"),
        nullable=False,
    )
    playlist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False
    )
    parameter_snapshot: orm.Mapped[dict[str, object]] = orm.mapped_column(
        sa.JSON, nullable=False
    )
    freshness_target: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True
    )
    freshness_actual: orm.Mapped[float | None] = orm.mapped_column(
        sa.Float, nullable=True
    )
    generation_duration_ms: orm.Mapped[int | None] = orm.mapped_column(
        sa.Integer, nullable=True
    )
    track_sources_summary: orm.Mapped[dict[str, int] | None] = orm.mapped_column(
        sa.JSON, nullable=True
    )
    # Resolved pool snapshot (#128): the artist ids + provenance that fed this
    # generation, captured for reproducibility/audit since sources re-resolve
    # live. Shape: [{"artist_id": "<uuid>", "via": "event|artist|related"}].
    # Nullable: records written before the snapshot column have none.
    pool_snapshot: orm.Mapped[list[dict[str, str]] | None] = orm.mapped_column(
        sa.JSON, nullable=True
    )
    # Resolved track snapshot (#versions): the ordered track ids this generation
    # produced, captured so prior versions stay recoverable even though the live
    # PlaylistTrack rows are replaced in place on regenerate (the Playlist row is
    # reused so the Spotify export link survives). Also the freshness baseline for
    # the NEXT regenerate (read this, not the live rows, to avoid self-comparison).
    # Shape: ["<track_uuid>", ...] in playlist order. Nullable: pre-snapshot rows
    # have none. UI to browse/restore versions is deferred -- this is data only.
    track_snapshot: orm.Mapped[list[str] | None] = orm.mapped_column(
        sa.JSON, nullable=True
    )

    profile: orm.Mapped[GeneratorProfile] = orm.relationship(
        back_populates="generations"
    )
    playlist: orm.Mapped[playlist_module.Playlist] = orm.relationship("Playlist")
