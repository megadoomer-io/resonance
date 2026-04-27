"""Playlist domain models: playlists and their track memberships."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

import sqlalchemy as sa
import sqlalchemy.orm as orm

import resonance.models.base as base_module

if TYPE_CHECKING:
    import resonance.models.music as music_module


class Playlist(base_module.TimestampMixin, base_module.Base):
    """An ordered collection of tracks."""

    __tablename__ = "playlists"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    user_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    name: orm.Mapped[str] = orm.mapped_column(sa.String(512), nullable=False)
    description: orm.Mapped[str | None] = orm.mapped_column(sa.Text, nullable=True)
    track_count: orm.Mapped[int] = orm.mapped_column(
        sa.Integer, nullable=False, default=0
    )
    is_pinned: orm.Mapped[bool] = orm.mapped_column(
        sa.Boolean, nullable=False, default=False
    )

    tracks: orm.Mapped[list[PlaylistTrack]] = orm.relationship(
        back_populates="playlist",
        cascade="all, delete-orphan",
        order_by="PlaylistTrack.position",
    )


_PLAYLIST_DEFAULTS: dict[str, object] = {
    "track_count": 0,
    "is_pinned": False,
}


@sa.event.listens_for(Playlist, "init")
def _set_playlist_defaults(
    target: Playlist,
    _args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    """Apply Python-side defaults for Playlist fields at construction."""
    for attr, default in _PLAYLIST_DEFAULTS.items():
        if attr not in kwargs:
            value = default() if callable(default) else default
            setattr(target, attr, value)


class PlaylistTrack(base_module.TimestampMixin, base_module.Base):
    """A track's position and metadata within a playlist."""

    __tablename__ = "playlist_tracks"

    id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, primary_key=True, default=uuid.uuid4
    )
    playlist_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("playlists.id", ondelete="CASCADE"), nullable=False
    )
    track_id: orm.Mapped[uuid.UUID] = orm.mapped_column(
        sa.Uuid, sa.ForeignKey("tracks.id", ondelete="CASCADE"), nullable=False
    )
    position: orm.Mapped[int] = orm.mapped_column(sa.Integer, nullable=False)
    score: orm.Mapped[float | None] = orm.mapped_column(sa.Float, nullable=True)
    source: orm.Mapped[str] = orm.mapped_column(sa.String(64), nullable=False)

    playlist: orm.Mapped[Playlist] = orm.relationship(back_populates="tracks")
    track: orm.Mapped[music_module.Track] = orm.relationship(
        "Track",
    )
