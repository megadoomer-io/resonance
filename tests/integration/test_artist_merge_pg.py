"""Artist merge and dedup-run behavior on Postgres (#203)."""

from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING, Any

import pytest
import sqlalchemy as sa

import resonance.dedup as dedup_module
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.taste as taste_models
import resonance.models.user as user_models
import resonance.services.artist_mbid as artist_mbid
import resonance.types as types_module

if TYPE_CHECKING:
    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration

_BASE = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
LB = types_module.ServiceType.LISTENBRAINZ
LASTFM = types_module.ServiceType.LASTFM


async def _artist(
    db: TaskDB, name: str, links: Any, minute: int, **fields: Any
) -> uuid.UUID:
    artist = music_models.Artist(
        id=uuid.uuid4(),
        name=name,
        service_links=links,
        created_at=_BASE + datetime.timedelta(minutes=minute),
        **fields,
    )
    async with db.factory() as session:
        session.add(artist)
        await session.commit()
    return artist.id


async def _load(db: TaskDB, artist_id: uuid.UUID) -> music_models.Artist | None:
    async with db.factory() as session:
        return await session.get(music_models.Artist, artist_id)


async def _profile(db: TaskDB, artist_id: uuid.UUID) -> uuid.UUID:
    user = user_models.User(id=uuid.uuid4(), display_name="t")
    profile_id = uuid.uuid4()
    async with db.factory() as session:
        session.add(user)
        await session.flush()
        session.add(
            generator_models.GeneratorProfile(
                id=profile_id,
                user_id=user.id,
                name="p",
                generator_type=types_module.GeneratorType.CONCERT_PREP,
                input_references={
                    "sources": [{"kind": "artist", "artist_id": str(artist_id)}],
                    "exclude_artist_ids": [],
                },
            )
        )
        await session.commit()
    return profile_id


async def test_name_pass_skips_same_name_artists_with_different_mbids(
    pg_task_db: TaskDB,
) -> None:
    legacy = await _artist(pg_task_db, "Sleep", {"listenbrainz": "l-mbid"}, 0)
    canonical = await _artist(pg_task_db, "SLEEP", {"musicbrainz": {"id": "x-mbid"}}, 1)

    async with pg_task_db.factory() as session:
        stats = await dedup_module.find_and_merge_duplicate_artists(session)
        conflicts = await artist_mbid.find_name_conflicts(session)

    assert stats.artists_merged == 0
    assert stats.groups_skipped == 1
    for artist_id, links in (
        (legacy, {"listenbrainz": "l-mbid"}),
        (canonical, {"musicbrainz": {"id": "x-mbid"}}),
    ):
        row = await _load(pg_task_db, artist_id)
        assert row is not None
        assert row.service_links == links
    assert [(c.name, sorted(m for _, m in c.artists)) for c in conflicts] == [
        ("sleep", ["l-mbid", "x-mbid"])
    ]


async def test_merge_refuses_different_mbids(pg_task_db: TaskDB) -> None:
    a = await _artist(pg_task_db, "Om", {"listenbrainz": "one"}, 0)
    b = await _artist(pg_task_db, "Om", {"musicbrainz": {"id": "two"}}, 1)

    async with pg_task_db.factory() as session:
        first = await session.get(music_models.Artist, a)
        second = await session.get(music_models.Artist, b)
        assert first is not None and second is not None
        with pytest.raises(dedup_module.ConflictingMbidError):
            await dedup_module.merge_artists(session, first, second)


async def test_name_pass_adopts_mbid_and_normalizes_it(pg_task_db: TaskDB) -> None:
    holder = await _artist(
        pg_task_db, "Kyuss", {"musicbrainz": {"id": "  K-MBID ", "extra": 1}}, 0
    )
    plain = await _artist(pg_task_db, "kyuss", {"spotify": {"id": "s"}}, 1)

    async with pg_task_db.factory() as session:
        stats = await dedup_module.find_and_merge_duplicate_artists(session)

    assert stats.artists_merged == 1
    assert await _load(pg_task_db, plain) is None
    survivor = await _load(pg_task_db, holder)
    assert survivor is not None
    assert survivor.service_links == {
        "musicbrainz": {"id": "k-mbid", "extra": 1},
        "spotify": {"id": "s"},
    }


async def test_winner_without_mbid_takes_the_losers_canonically(
    pg_task_db: TaskDB,
) -> None:
    # More links make the MBID-less row the older and richer one, but the MBID
    # holder still wins (pick_canonical rule 1); check the reverse too.
    winner_links = {"listenbrainz": "LEGACY-ONE"}
    with_mbid = await _artist(pg_task_db, "Goatsnake", winner_links, 1)
    without = await _artist(
        pg_task_db, "Goatsnake", {"spotify": {"id": "s"}, "lastfm": "g"}, 0
    )

    async with pg_task_db.factory() as session:
        await dedup_module.find_and_merge_duplicate_artists(session)

    assert await _load(pg_task_db, without) is None
    survivor = await _load(pg_task_db, with_mbid)
    assert survivor is not None
    assert survivor.service_links == {
        "listenbrainz": "LEGACY-ONE",
        "musicbrainz": {"id": "legacy-one"},
        "spotify": {"id": "s"},
        "lastfm": "g",
    }


async def test_tags_move_only_to_a_winner_without_tags(pg_task_db: TaskDB) -> None:
    stamp = _BASE + datetime.timedelta(days=3)
    winner = await _artist(pg_task_db, "Acid King", {"musicbrainz": {"id": "ak"}}, 0)
    loser = await _artist(
        pg_task_db, "acid king", {"listenbrainz": "ak"}, 1, genre_attempted_at=stamp
    )
    async with pg_task_db.factory() as session:
        session.add(music_models.ArtistTag(artist_id=loser, tag="stoner", count=3))
        await session.commit()

    async with pg_task_db.factory() as session:
        await dedup_module.find_and_merge_duplicate_artists(session)

    async with pg_task_db.factory() as session:
        tags = (await session.execute(sa.select(music_models.ArtistTag))).scalars()
        assert [(t.artist_id, t.tag) for t in tags] == [(winner, "stoner")]
        survivor = await session.get(music_models.Artist, winner)
        assert survivor is not None
        assert survivor.genre_attempted_at == stamp


async def test_winners_tags_stand_over_the_losers(pg_task_db: TaskDB) -> None:
    winner = await _artist(
        pg_task_db, "Electric Wizard", {"musicbrainz": {"id": "ew"}}, 0
    )
    loser = await _artist(pg_task_db, "electric wizard", {"listenbrainz": "ew"}, 1)
    async with pg_task_db.factory() as session:
        session.add_all(
            [
                music_models.ArtistTag(artist_id=winner, tag="doom", count=5),
                music_models.ArtistTag(artist_id=loser, tag="sludge", count=2),
            ]
        )
        await session.commit()

    async with pg_task_db.factory() as session:
        await dedup_module.find_and_merge_duplicate_artists(session)

    async with pg_task_db.factory() as session:
        tags = (await session.execute(sa.select(music_models.ArtistTag))).scalars()
        assert [(t.artist_id, t.tag) for t in tags] == [(winner, "doom")]


async def test_similarity_edges_move_for_connectors_the_winner_lacks(
    pg_task_db: TaskDB,
) -> None:
    winner = await _artist(pg_task_db, "Sleep", {"musicbrainz": {"id": "sl"}}, 0)
    loser = await _artist(pg_task_db, "sleep", {"listenbrainz": "sl"}, 1)

    def edge(source: uuid.UUID, connector: Any, neighbor: str) -> Any:
        return taste_models.ArtistSimilarity(
            source_artist_id=source, connector=connector, neighbor_name=neighbor, rank=1
        )

    async with pg_task_db.factory() as session:
        session.add_all(
            [
                edge(winner, LB, "Om"),
                edge(loser, LB, "High on Fire"),
                edge(loser, LASTFM, "Matt Pike"),
            ]
        )
        await session.commit()

    async with pg_task_db.factory() as session:
        await dedup_module.find_and_merge_duplicate_artists(session)

    async with pg_task_db.factory() as session:
        rows = (
            await session.execute(sa.select(taste_models.ArtistSimilarity))
        ).scalars()
        edges = sorted(
            (r.source_artist_id == winner, r.connector, r.neighbor_name) for r in rows
        )
    assert edges == [(True, LASTFM, "Matt Pike"), (True, LB, "Om")]


async def test_concurrent_profile_edit_fails_only_its_group(
    pg_task_db: TaskDB, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A profile PATCH mid-merge rolls back that group; the next still merges.

    The edit lands on every attempt, so the name pass's retry fails too.
    """
    contested = await _artist(pg_task_db, "Alpha", {"musicbrainz": {"id": "a"}}, 0)
    contested_dup = await _artist(pg_task_db, "alpha", {"listenbrainz": "a"}, 1)
    other = await _artist(pg_task_db, "Beta", {"musicbrainz": {"id": "b"}}, 2)
    other_dup = await _artist(pg_task_db, "beta", {"listenbrainz": "b"}, 3)
    profile_id = await _profile(pg_task_db, contested_dup)

    real_repoint = dedup_module._repoint_profiles

    async def repoint_then_race(session: Any, old_id: Any, new_id: Any) -> int:
        repointed = await real_repoint(session, old_id, new_id)
        if old_id == contested_dup:
            async with pg_task_db.factory() as editor:
                prof = await editor.get(generator_models.GeneratorProfile, profile_id)
                assert prof is not None
                prof.name = f"edited {uuid.uuid4()}"
                await editor.commit()
        return repointed

    monkeypatch.setattr(dedup_module, "_repoint_profiles", repoint_then_race)

    async with pg_task_db.factory() as session:
        stats = await dedup_module.find_and_merge_duplicate_artists(session)

    # The MBID pass and the name pass each try (and fail) the contested group.
    assert stats.groups_failed == 2
    assert stats.groups_merged == 1
    assert await _load(pg_task_db, contested_dup) is not None
    assert await _load(pg_task_db, other_dup) is None
    assert await _load(pg_task_db, contested) is not None
    assert await _load(pg_task_db, other) is not None
