"""Scenario seeding for local dev environments (``POST /api/v1/admin/seed``).

One request body describes one scenario: an optional user, then artists with
their tags, similarity edges, tracks and listens. Rows go through the ORM, so
every database constraint applies; states the current schema refuses are built
by seeding through an older app and migrating (see docs/dev-environments.md).
"""

from __future__ import annotations

import typing
import uuid
from typing import Any

import pydantic
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async

import resonance.models.music as music_models
import resonance.models.taste as taste_models
import resonance.models.user as user_models
import resonance.types as types_module


class _Strict(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(extra="forbid")


_INT32_MAX = 2**31 - 1


class SeedUser(_Strict):
    display_name: str = pydantic.Field(min_length=1, max_length=255)
    email: str | None = pydantic.Field(default=None, max_length=255)
    # Never owner: one owner per database, and listens without a user go to it.
    role: typing.Literal[types_module.UserRole.USER, types_module.UserRole.ADMIN] = (
        types_module.UserRole.USER
    )


class SeedTag(_Strict):
    tag: str = pydantic.Field(min_length=1, max_length=256)
    genre_mbid: str | None = pydantic.Field(default=None, max_length=64)
    count: int = pydantic.Field(default=1, ge=0, le=_INT32_MAX)
    source: str = pydantic.Field(default="musicbrainz", min_length=1, max_length=64)


class SeedSimilar(_Strict):
    neighbor_name: str = pydantic.Field(min_length=1, max_length=512)
    neighbor_mbid: str | None = pydantic.Field(default=None, max_length=64)
    rank: int = pydantic.Field(default=1, ge=0, le=_INT32_MAX)
    connector: types_module.ServiceType = types_module.ServiceType.LASTFM


class SeedListen(_Strict):
    listened_at: pydantic.AwareDatetime
    source_service: types_module.ServiceType = types_module.ServiceType.LISTENBRAINZ


class SeedTrack(_Strict):
    title: str = pydantic.Field(min_length=1, max_length=512)
    duration_ms: int | None = pydantic.Field(default=None, ge=0, le=_INT32_MAX)
    popularity_score: int | None = pydantic.Field(default=None, ge=0, le=100)
    service_links: dict[str, Any] | None = None
    listens: list[SeedListen] = []


class SeedArtist(_Strict):
    name: str = pydantic.Field(min_length=1, max_length=512)
    # Stored verbatim, so legacy shapes (a flat "listenbrainz" MBID string) work.
    service_links: dict[str, Any] | None = None
    tags: list[SeedTag] = []
    similar: list[SeedSimilar] = []
    tracks: list[SeedTrack] = []


class SeedScenario(_Strict):
    """A scenario. Listens belong to ``user`` if given, else the owner."""

    user: SeedUser | None = None
    artists: list[SeedArtist] = []


async def _owner(db: sa_async.AsyncSession) -> user_models.User:
    owner = (
        await db.execute(
            sa.select(user_models.User)
            .where(user_models.User.role == types_module.UserRole.OWNER)
            .limit(1)
        )
    ).scalar_one_or_none()
    if owner is None:
        # A fresh dev database has no owner, and the admin token resolves to it.
        owner = user_models.User(
            id=uuid.uuid4(), display_name="Dev Owner", role=types_module.UserRole.OWNER
        )
        db.add(owner)
    return owner


async def seed(db: sa_async.AsyncSession, scenario: SeedScenario) -> dict[str, Any]:
    """Create the scenario's rows in one transaction and return their ids.

    Raises:
        sqlalchemy.exc.IntegrityError: when a row violates a constraint; the
            caller rolls back, so nothing from the scenario is kept.
    """
    if scenario.user is not None:
        user = user_models.User(
            id=uuid.uuid4(),
            display_name=scenario.user.display_name,
            email=scenario.user.email,
            role=scenario.user.role,
        )
        db.add(user)
    else:
        user = await _owner(db)

    artists_out: list[dict[str, Any]] = []
    counts = {"artists": 0, "tags": 0, "similar": 0, "tracks": 0, "listens": 0}
    for spec in scenario.artists:
        artist = music_models.Artist(
            id=uuid.uuid4(), name=spec.name, service_links=spec.service_links
        )
        db.add(artist)
        counts["artists"] += 1
        for tag in spec.tags:
            db.add(
                music_models.ArtistTag(
                    artist_id=artist.id,
                    tag=tag.tag,
                    genre_mbid=tag.genre_mbid,
                    count=tag.count,
                    source=tag.source,
                )
            )
            counts["tags"] += 1
        for edge in spec.similar:
            db.add(
                taste_models.ArtistSimilarity(
                    source_artist_id=artist.id,
                    connector=edge.connector,
                    neighbor_name=edge.neighbor_name,
                    neighbor_mbid=edge.neighbor_mbid,
                    rank=edge.rank,
                )
            )
            counts["similar"] += 1
        track_ids: list[str] = []
        for track_spec in spec.tracks:
            track = music_models.Track(
                id=uuid.uuid4(),
                title=track_spec.title,
                artist_id=artist.id,
                duration_ms=track_spec.duration_ms,
                popularity_score=track_spec.popularity_score,
                service_links=track_spec.service_links,
            )
            db.add(track)
            counts["tracks"] += 1
            track_ids.append(str(track.id))
            for listen in track_spec.listens:
                db.add(
                    music_models.ListeningEvent(
                        user_id=user.id,
                        track_id=track.id,
                        source_service=listen.source_service,
                        listened_at=listen.listened_at,
                    )
                )
                counts["listens"] += 1
        artists_out.append(
            {"id": str(artist.id), "name": artist.name, "track_ids": track_ids}
        )

    await db.flush()
    result = {"user_id": str(user.id), "artists": artists_out, "counts": counts}
    await db.commit()
    return result
