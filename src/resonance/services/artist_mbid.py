"""Find artists that share a MusicBrainz artist MBID (#203).

An artist's MBID is read from the canonical ``service_links["musicbrainz"]["id"]``
or, failing that, the legacy flat ``service_links["listenbrainz"]`` string,
trimmed and lowercased. ``artist_utils.normalized_mbid`` is the Python twin.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa

import resonance.models.music as music_models

if TYPE_CHECKING:
    import datetime
    import uuid

    import sqlalchemy.ext.asyncio as sa_async


def _string_or_null(element: Any) -> sa.ColumnElement[str]:
    """Trimmed, lowercased text of a JSON string value; NULL otherwise or if blank."""
    as_text = sa.case(
        (sa.func.json_typeof(element) == "string", element.as_string()),
        else_=sa.null(),
    )
    return sa.func.nullif(sa.func.lower(sa.func.trim(as_text)), "")


def normalized_mbid_expr() -> sa.ColumnElement[str]:
    """SQL for an artist's normalized MBID (Postgres; ``service_links`` is json)."""
    links = music_models.Artist.service_links
    canonical = _string_or_null(links["musicbrainz"]["id"])
    legacy = _string_or_null(links["listenbrainz"])
    return sa.func.coalesce(canonical, legacy)


@dataclasses.dataclass(frozen=True)
class DuplicateArtist:
    """One artist in an MBID duplicate group."""

    id: uuid.UUID
    name: str
    created_at: datetime.datetime


@dataclasses.dataclass(frozen=True)
class MbidDuplicateGroup:
    """Artists sharing one normalized MBID, oldest first."""

    mbid: str
    artists: list[DuplicateArtist]

    @property
    def count(self) -> int:
        return len(self.artists)


def _duplicate_mbids() -> sa.Select[tuple[str]]:
    mbid = normalized_mbid_expr().label("mbid")
    norm = sa.select(mbid).subquery()
    return (
        sa.select(norm.c.mbid)
        .where(norm.c.mbid.isnot(None))
        .group_by(norm.c.mbid)
        .having(sa.func.count() > 1)
    )


async def count_mbid_duplicate_groups(session: sa_async.AsyncSession) -> int:
    """Number of distinct MBIDs held by more than one artist."""
    stmt = sa.select(sa.func.count()).select_from(_duplicate_mbids().subquery())
    return int((await session.execute(stmt)).scalar_one())


async def find_mbid_duplicate_groups(
    session: sa_async.AsyncSession,
) -> list[MbidDuplicateGroup]:
    """Every group of artists sharing a normalized MBID, ordered by MBID."""
    mbid = normalized_mbid_expr()
    dupes = _duplicate_mbids().subquery()
    stmt = (
        sa.select(
            mbid.label("mbid"),
            music_models.Artist.id,
            music_models.Artist.name,
            music_models.Artist.created_at,
        )
        .where(mbid.in_(sa.select(dupes.c.mbid)))
        .order_by(mbid, music_models.Artist.created_at, music_models.Artist.id)
    )
    groups: dict[str, list[DuplicateArtist]] = {}
    for row in (await session.execute(stmt)).all():
        groups.setdefault(row.mbid, []).append(
            DuplicateArtist(id=row.id, name=row.name, created_at=row.created_at)
        )
    return [MbidDuplicateGroup(mbid=k, artists=v) for k, v in groups.items()]
