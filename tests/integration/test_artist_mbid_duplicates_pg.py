"""Detect and merge artists that share a MusicBrainz MBID, on Postgres (#203)."""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING, Any

import pytest
import sqlalchemy as sa

import resonance.dedup as dedup_module
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.user as user_models
import resonance.services.artist_mbid as artist_mbid
import resonance.types as types_module

if TYPE_CHECKING:
    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration

_BASE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)


async def _artists(db: TaskDB, *specs: tuple[str, Any]) -> list[uuid.UUID]:
    """Insert artists (name, service_links) with increasing created_at."""
    ids: list[uuid.UUID] = []
    async with db.factory() as session:
        for offset, (name, links) in enumerate(specs):
            artist = music_models.Artist(
                id=uuid.uuid4(),
                name=name,
                service_links=links,
                created_at=_BASE + datetime.timedelta(minutes=offset),
            )
            session.add(artist)
            ids.append(artist.id)
        await session.commit()
    return ids


async def _groups(db: TaskDB) -> dict[str, list[uuid.UUID]]:
    async with db.factory() as session:
        found = await artist_mbid.find_mbid_duplicate_groups(session)
    return {g.mbid: [a.id for a in g.artists] for g in found}


async def test_canonical_legacy_and_mixed_duplicates(pg_task_db: TaskDB) -> None:
    canon_a, canon_b, legacy_a, legacy_b, mixed_a, mixed_b, solo = await _artists(
        pg_task_db,
        ("A", {"musicbrainz": {"id": "aaaa"}}),
        ("A2", {"musicbrainz": {"id": "aaaa"}}),
        ("B", {"listenbrainz": "bbbb"}),
        ("B2", {"listenbrainz": "bbbb"}),
        ("C", {"musicbrainz": {"id": "cccc"}}),
        ("C2", {"listenbrainz": "cccc"}),
        ("D", {"musicbrainz": {"id": "dddd"}}),
    )

    groups = await _groups(pg_task_db)

    assert groups == {
        "aaaa": [canon_a, canon_b],
        "bbbb": [legacy_a, legacy_b],
        "cccc": [mixed_a, mixed_b],
    }
    assert solo not in {i for ids in groups.values() for i in ids}
    async with pg_task_db.factory() as session:
        assert await artist_mbid.count_mbid_duplicate_groups(session) == 3


async def test_case_and_whitespace_are_normalized(pg_task_db: TaskDB) -> None:
    upper, lower = await _artists(
        pg_task_db,
        ("Up", {"musicbrainz": {"id": " ABCD-EF "}}),
        ("Low", {"listenbrainz": "abcd-ef"}),
    )

    assert await _groups(pg_task_db) == {"abcd-ef": [upper, lower]}


async def test_flat_musicbrainz_string_counts(pg_task_db: TaskDB) -> None:
    flat, legacy = await _artists(
        pg_task_db,
        ("F", {"musicbrainz": "FLAT-1", "listenbrainz": "other"}),
        ("G", {"listenbrainz": "flat-1"}),
    )

    assert await _groups(pg_task_db) == {"flat-1": [flat, legacy]}


async def test_canonical_wins_over_a_different_legacy_value(
    pg_task_db: TaskDB,
) -> None:
    await _artists(
        pg_task_db,
        ("X", {"musicbrainz": {"id": "new"}, "listenbrainz": "old"}),
        ("Y", {"listenbrainz": "old"}),
    )

    assert await _groups(pg_task_db) == {}


async def test_artists_without_an_mbid_are_ignored(pg_task_db: TaskDB) -> None:
    await _artists(
        pg_task_db,
        ("N1", None),
        ("N2", None),
        ("E1", {}),
        ("E2", {}),
        ("S1", {"spotify": {"id": "s"}}),
        ("S2", {"spotify": {"id": "s"}}),
        ("B1", {"musicbrainz": {"id": ""}, "listenbrainz": " "}),
        ("B2", {"musicbrainz": {"id": ""}, "listenbrainz": " "}),
        ("L1", {"listenbrainz": {"id": "dict"}}),
        ("L2", {"listenbrainz": {"id": "dict"}}),
        ("J1", {"listenbrainz": None}),
        ("J2", {"listenbrainz": None}),
    )

    assert await _groups(pg_task_db) == {}
    async with pg_task_db.factory() as session:
        assert await artist_mbid.count_mbid_duplicate_groups(session) == 0


async def test_dedup_merges_an_mbid_group(pg_task_db: TaskDB) -> None:
    """Same MBID, names equal but for case and spacing: one artist survives."""
    keeper, dup = await _artists(
        pg_task_db,
        ("Elder", {"musicbrainz": {"id": "e-mbid"}, "lastfm": "elder"}),
        ("  ELDER ", {"listenbrainz": "E-MBID", "spotify": {"id": "sp"}}),
    )
    user = user_models.User(id=uuid.uuid4(), display_name="t")
    profile_id = uuid.uuid4()
    async with pg_task_db.factory() as session:
        session.add(user)
        await session.flush()
        session.add(music_models.Track(id=uuid.uuid4(), title="Lore", artist_id=dup))
        session.add(
            generator_models.GeneratorProfile(
                id=profile_id,
                user_id=user.id,
                name="p",
                generator_type=types_module.GeneratorType.CONCERT_PREP,
                input_references={
                    "sources": [
                        {"kind": "artist", "artist_id": str(dup), "enabled": True}
                    ],
                    "exclude_artist_ids": [],
                },
            )
        )
        await session.commit()

    async with pg_task_db.factory() as session:
        stats = await dedup_module.find_and_merge_duplicate_artists(session)

    assert stats.mbid_artists_merged == 1
    assert stats.artists_merged == 1
    assert stats.tracks_repointed == 1
    assert stats.profiles_repointed == 1
    assert await _groups(pg_task_db) == {}
    async with pg_task_db.factory() as session:
        remaining = (await session.execute(sa.select(music_models.Artist))).scalars()
        survivor = remaining.one()
        assert survivor.id == keeper
        assert survivor.service_links is not None
        assert survivor.service_links["spotify"] == {"id": "sp"}
        track = (await session.execute(sa.select(music_models.Track))).scalar_one()
        assert track.artist_id == keeper
        profile = await session.get(generator_models.GeneratorProfile, profile_id)
        assert profile is not None
        sources = profile.input_references["sources"]
        assert isinstance(sources, list)
        assert sources[0]["artist_id"] == str(keeper)


async def test_mbid_group_with_different_names_is_left_for_review(
    pg_task_db: TaskDB,
) -> None:
    a, b = await _artists(
        pg_task_db,
        ("Elder", {"musicbrainz": {"id": "e-mbid"}}),
        ("Elder (band)", {"listenbrainz": "e-mbid"}),
    )

    async with pg_task_db.factory() as session:
        stats = await dedup_module.find_and_merge_duplicate_artists(session)
        report = await artist_mbid.find_mbid_duplicate_groups(session)

    assert stats.artists_merged == 0
    assert stats.groups_skipped == 1
    assert [(g.mbid, g.names_differ) for g in report] == [("e-mbid", True)]
    assert await _groups(pg_task_db) == {"e-mbid": [a, b]}
