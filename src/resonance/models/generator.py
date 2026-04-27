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

    profile: orm.Mapped[GeneratorProfile] = orm.relationship(
        back_populates="generations"
    )
    playlist: orm.Mapped[playlist_module.Playlist] = orm.relationship("Playlist")
