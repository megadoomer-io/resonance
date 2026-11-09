"""Enrich and calendar sync hold no row locks across external calls (#199)."""

from __future__ import annotations

import asyncio
import datetime
import uuid
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import sqlalchemy as sa

import resonance.concerts.worker as concert_worker
import resonance.config as config_module
import resonance.generators.pool as pool_module
import resonance.models.concert as concert_models
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.task as task_models
import resonance.models.taste as taste_models
import resonance.models.user as user_models
import resonance.services.artist_tags as artist_tags_module
import resonance.sync.lifecycle as lifecycle_module
import resonance.types as types_module
import resonance.worker as worker_module

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    import sqlalchemy.ext.asyncio as sa_async

    from tests.conftest import TaskDB

pytestmark = pytest.mark.integration

S = types_module.SyncStatus
LB = types_module.ServiceType.LISTENBRAINZ


async def _seed_artist(db: TaskDB, **fields: Any) -> music_models.Artist:
    fields.setdefault("name", "Seed")
    artist = music_models.Artist(id=uuid.uuid4(), **fields)
    async with db.factory() as session:
        session.add(artist)
        await session.commit()
    return artist


async def _seed_user(db: TaskDB) -> user_models.User:
    user = user_models.User(id=uuid.uuid4(), display_name="t")
    async with db.factory() as session:
        session.add(user)
        await session.commit()
    return user


class _Neighbors:
    service_type = LB

    def __init__(self, names: list[str]) -> None:
        self.names = names

    async def get_similar_artists(
        self, name: str, *, mbid: str | None, limit: int
    ) -> list[dict[str, Any]]:
        return [{"name": n, "mbid": None} for n in self.names]


async def test_concurrent_similarity_refresh_does_not_conflict(
    pg_task_db: TaskDB,
) -> None:
    """Two refreshes of one source both succeed; the result is one whole batch."""
    artist = await _seed_artist(pg_task_db)
    now = datetime.datetime.now(datetime.UTC)
    hold_a, a_written = asyncio.Event(), asyncio.Event()

    async with pg_task_db.factory() as a, pg_task_db.factory() as b:
        commit_a = a.commit
        calls = 0

        async def held_commit() -> None:
            nonlocal calls
            calls += 1
            if calls == 2:  # A's edge write stays uncommitted until released
                a_written.set()
                await hold_a.wait()
            await commit_a()

        a.commit = held_commit  # type: ignore[method-assign]
        run_a = asyncio.create_task(
            worker_module._fetch_similar_with_store(
                a, _Neighbors(["Elder", "Kyuss", "Sleep"]), artist, limit=30, now=now
            )
        )
        try:
            await asyncio.wait_for(a_written.wait(), 5)
            # B arrives while A's replace is uncommitted.
            run_b = asyncio.create_task(
                worker_module._fetch_similar_with_store(
                    b, _Neighbors(["Elder", "Om"]), artist, limit=30, now=now
                )
            )
            await asyncio.sleep(0.2)
            hold_a.set()
            await asyncio.wait_for(asyncio.gather(run_a, run_b), 10)
        finally:
            hold_a.set()
            run_a.cancel()

    async with pg_task_db.factory() as session:
        names = (
            await session.execute(
                sa.select(taste_models.ArtistSimilarity.neighbor_name).where(
                    taste_models.ArtistSimilarity.source_artist_id == artist.id
                )
            )
        ).scalars()
        assert sorted(names) == ["Elder", "Om"]  # B replaced A whole, no union


def _tag(name: str) -> artist_tags_module.ArtistTagResult:
    return artist_tags_module.ArtistTagResult(tag=name, count=1)


async def test_concurrent_tag_writes_replace_cleanly(pg_task_db: TaskDB) -> None:
    """Two writers persisting one artist's tags both succeed; one set wins whole."""
    import resonance.sync.backfill as backfill_module

    artist = await _seed_artist(pg_task_db)
    batch_a = [_tag("doom"), _tag("sludge"), _tag("stoner")]
    batch_b = [_tag("doom"), _tag("drone")]
    a_written = asyncio.Event()

    async def write(batch: list[artist_tags_module.ArtistTagResult]) -> None:
        async with pg_task_db.factory() as session:
            row = await session.get(music_models.Artist, artist.id)
            assert row is not None
            await backfill_module._persist_artist_tags(session, row, batch)
            await session.flush()
            if batch is batch_a:
                a_written.set()
                await asyncio.sleep(0.3)  # B arrives while A is uncommitted
            await session.commit()

    run_a = asyncio.create_task(write(batch_a))
    await asyncio.wait_for(a_written.wait(), 5)
    await asyncio.wait_for(asyncio.gather(run_a, write(batch_b)), 10)

    async with pg_task_db.factory() as session:
        tags = set(
            (
                await session.execute(
                    sa.select(music_models.ArtistTag.tag).where(
                        music_models.ArtistTag.artist_id == artist.id
                    )
                )
            ).scalars()
        )
    assert tags == {"doom", "drone"}  # B ran after A committed: a clean replace


# ---------------------------------------------------------------------------
# Lock checks: a second session must write the rows a run has touched while
# the run is blocked inside an external call.
# ---------------------------------------------------------------------------


async def _writable(
    factory: sa_async.async_sessionmaker[sa_async.AsyncSession], *stmts: Any
) -> None:
    async with factory() as session:
        await session.execute(sa.text("SET LOCAL lock_timeout = '500ms'"))
        for stmt in stmts:
            await session.execute(stmt)
        await session.commit()


async def _idle_in_tx_worker_backends(
    factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
) -> int:
    async with factory() as session:
        return int(
            (
                await session.execute(
                    sa.text(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE application_name LIKE :app "
                        "AND state = 'idle in transaction'"
                    ),
                    {"app": f"{worker_module.WORKER_APPLICATION_NAME_PREFIX}:%"},
                )
            ).scalar_one()
        )


def _worker_factory(
    db: TaskDB, schema: str
) -> tuple[sa_async.AsyncEngine, sa_async.async_sessionmaker[sa_async.AsyncSession]]:
    import resonance.database as database_module

    settings = config_module.Settings()
    engine = database_module.create_async_engine(
        settings,
        server_settings={
            **worker_module.worker_server_settings(settings),
            "search_path": schema,
        },
    )
    return engine, database_module.create_session_factory(engine)


async def _schema(db: TaskDB) -> str:
    async with db.factory() as session:
        result = await session.execute(sa.text("SELECT current_schema()"))
        return str(result.scalar_one())


class _Gate:
    """Blocks the n-th call of an external stub until released."""

    def __init__(self, block_on: int) -> None:
        self.block_on = block_on
        self.calls = 0
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def __call__(self) -> None:
        self.calls += 1
        if self.calls == self.block_on:
            self.reached.set()
            await self.release.wait()


def _http(handler: Callable[[httpx.Request], Awaitable[httpx.Response]]) -> Any:
    """A real AsyncClient over a stub transport, so the guardrail sees each send."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class _HttpConnector:
    """Connector stub whose calls go through a real httpx client."""

    service_type = LB

    def __init__(
        self, neighbors: list[dict[str, Any]], gate: _Gate, imports: dict[str, str]
    ) -> None:
        self.neighbors = neighbors
        self.gate = gate
        self.imports = imports
        self.client = _http(self._handle)

    async def _handle(self, request: httpx.Request) -> httpx.Response:
        await self.gate()
        return httpx.Response(200, json={})

    async def get_similar_artists(
        self, name: str, *, mbid: str | None, limit: int
    ) -> list[dict[str, Any]]:
        await self.client.get("https://lb.test/similar")
        return self.neighbors

    async def get_artist_by_mbid(self, mbid: str) -> dict[str, Any] | None:
        await self.client.get(f"https://mb.test/artist/{mbid}")
        return {"name": self.imports[mbid]}


async def _tags_handler(request: httpx.Request) -> httpx.Response:
    mbids = request.url.params["artist_mbids"].split(",")
    return httpx.Response(
        200,
        json=[
            {
                "artist_mbid": m,
                "tag": {"artist": [{"tag": "doom", "count": 3, "genre_mbid": "g"}]},
            }
            for m in mbids
        ],
    )


async def test_enrich_holds_no_locks_across_external_calls(
    pg_task_db: TaskDB,
    no_writes_across_http: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed_mbid, new_mbid = str(uuid.uuid4()), str(uuid.uuid4())
    seed = await _seed_artist(
        pg_task_db, service_links={"musicbrainz": {"id": seed_mbid}}
    )
    user = await _seed_user(pg_task_db)
    profile = generator_models.GeneratorProfile(
        user_id=user.id,
        name="P",
        generator_type=types_module.GeneratorType.CONCERT_PREP,
        input_references=pool_module.serialize_input_references(
            [pool_module.ArtistSource(artist_id=seed.id)]
        ),
    )
    async with pg_task_db.factory() as session:
        session.add(profile)
        await session.commit()
    task = await pg_task_db.add(
        task_type=types_module.TaskType.RELATED_ARTIST_ENRICHMENT,
        user_id=user.id,
        params={"profile_id": str(profile.id), "seed_artist_ids": "lineup", "n": 2},
    )

    # External call order: seed tags, similar, candidate tags, MB import.
    # Block on the import, after tags and edges for the seed are written.
    gate = _Gate(block_on=2)
    connector = _HttpConnector(
        [{"name": "Newband", "mbid": new_mbid}], gate, {new_mbid: "Newband"}
    )
    real_tags_client = artist_tags_module.ArtistTagsClient

    def tags_client(settings: Any) -> artist_tags_module.ArtistTagsClient:
        client = real_tags_client(settings)
        client._http_client = _http(_tags_handler)
        return client

    monkeypatch.setattr(artist_tags_module, "ArtistTagsClient", tags_client)
    registry = MagicMock()
    registry.get_by_capability.return_value = [connector]
    registry.get_base_connector.return_value = connector

    engine, factory = _worker_factory(pg_task_db, await _schema(pg_task_db))
    ctx = {
        "session_factory": factory,
        "settings": config_module.Settings(),
        "connector_registry": registry,
        "redis": AsyncMock(),
        "job_id": "j",
    }
    run = asyncio.create_task(
        lifecycle_module.task_job(worker_module.enrich_related_artists)(
            ctx, str(task.id)
        )
    )
    try:
        await asyncio.wait_for(gate.reached.wait(), 10)

        await _writable(
            pg_task_db.factory,
            sa.update(music_models.Artist)
            .where(music_models.Artist.id == seed.id)
            .values(name="Seed renamed"),
            sa.update(music_models.ArtistTag)
            .where(music_models.ArtistTag.artist_id == seed.id)
            .values(count=7),
            sa.update(taste_models.ArtistSimilarity)
            .where(taste_models.ArtistSimilarity.source_artist_id == seed.id)
            .values(rank=9),
            sa.delete(taste_models.ArtistSimilarity).where(
                taste_models.ArtistSimilarity.source_artist_id == seed.id
            ),
        )
        assert await _idle_in_tx_worker_backends(pg_task_db.factory) == 0

        gate.release.set()
        await asyncio.wait_for(run, 10)
    finally:
        gate.release.set()
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        await engine.dispose()

    row = await pg_task_db.get(task.id)
    assert row.status == S.COMPLETED, row.error_message
    assert row.result["found"] == 1
    async with pg_task_db.factory() as session:
        stored = (
            await session.execute(
                sa.select(generator_models.GeneratorProfile).where(
                    generator_models.GeneratorProfile.id == profile.id
                )
            )
        ).scalar_one()
        imported = (
            await session.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.name == "Newband"
                )
            )
        ).scalar_one()
    assert set(pool_module.scope_artist_ids(stored.input_references, "lineup")) == {
        imported.id
    }


async def test_enrich_rerun_after_import_failure_converges(
    pg_task_db: TaskDB,
    no_writes_across_http: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure at import k leaves earlier imports durable; a re-run tops up to N."""
    mbids = [str(uuid.uuid4()) for _ in range(3)]
    seed = await _seed_artist(pg_task_db)
    user = await _seed_user(pg_task_db)
    profile = generator_models.GeneratorProfile(
        user_id=user.id,
        name="P",
        generator_type=types_module.GeneratorType.CONCERT_PREP,
        input_references=pool_module.serialize_input_references(
            [pool_module.ArtistSource(artist_id=seed.id)]
        ),
    )
    async with pg_task_db.factory() as session:
        session.add(profile)
        await session.commit()

    names = {m: f"Band {i}" for i, m in enumerate(mbids)}
    neighbors = [{"name": names[m], "mbid": m} for m in mbids]

    class _Flaky(_HttpConnector):
        fail_on: str | None = mbids[1]

        async def get_artist_by_mbid(self, mbid: str) -> dict[str, Any] | None:
            if mbid == self.fail_on:
                raise RuntimeError("mb down")
            return await super().get_artist_by_mbid(mbid)

    connector = _Flaky(neighbors, _Gate(block_on=0), names)
    registry = MagicMock()
    registry.get_by_capability.return_value = [connector]
    registry.get_base_connector.return_value = connector
    real_tags_client = artist_tags_module.ArtistTagsClient

    def tags_client(settings: Any) -> artist_tags_module.ArtistTagsClient:
        client = real_tags_client(settings)
        client._http_client = _http(_tags_handler)
        return client

    monkeypatch.setattr(artist_tags_module, "ArtistTagsClient", tags_client)

    async def run_once(job_id: str) -> task_models.Task:
        task = await pg_task_db.add(
            task_type=types_module.TaskType.RELATED_ARTIST_ENRICHMENT,
            user_id=user.id,
            params={
                "profile_id": str(profile.id),
                "seed_artist_ids": [str(seed.id)],
                "n": 3,
            },
        )
        ctx = {
            "session_factory": pg_task_db.factory,
            "settings": config_module.Settings(),
            "connector_registry": registry,
            "redis": AsyncMock(),
            "job_id": job_id,
        }
        await lifecycle_module.task_job(worker_module.enrich_related_artists)(
            ctx, str(task.id)
        )
        return await pg_task_db.get(task.id)

    first = await run_once("a")
    assert first.status == S.COMPLETED
    assert first.result["found"] == 2  # import 1 failed and was skipped

    connector.fail_on = None
    second = await run_once("b")
    assert second.status == S.COMPLETED
    assert second.result["found"] == 1  # only the missing one

    async with pg_task_db.factory() as session:
        stored = (
            await session.execute(
                sa.select(generator_models.GeneratorProfile).where(
                    generator_models.GeneratorProfile.id == profile.id
                )
            )
        ).scalar_one()
        artist_names = list(
            (
                await session.execute(
                    sa.select(music_models.Artist.name).where(
                        music_models.Artist.name.like("Band %")
                    )
                )
            ).scalars()
        )
    scope = pool_module.scope_artist_ids(stored.input_references, str(seed.id))
    assert len(scope) == len(set(scope)) == 3
    assert sorted(artist_names) == ["Band 0", "Band 1", "Band 2"]  # no duplicates


_FEED_1 = """\
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:Elder at The Fillmore (15 Mar 26)
DTSTART;VALUE=DATE:20260315
UID:songkick-evt-1
URL:https://songkick.com/concerts/1
LOCATION:The Fillmore, San Francisco, CA, US
DESCRIPTION:You're going.
END:VEVENT
END:VCALENDAR
"""

_FEED_2 = """\
BEGIN:VCALENDAR
VERSION:2.0
BEGIN:VEVENT
SUMMARY:Kyuss at Red Rocks (10 Jul 26)
DTSTART;VALUE=DATE:20260710
UID:songkick-evt-2
URL:https://songkick.com/concerts/2
LOCATION:Red Rocks, Morrison, CO, US
END:VEVENT
END:VCALENDAR
"""


async def test_calendar_holds_no_locks_across_feed_fetches(
    pg_task_db: TaskDB,
    no_writes_across_http: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = await _seed_user(pg_task_db)
    connection = user_models.ServiceConnection(
        id=uuid.uuid4(),
        user_id=user.id,
        service_type=types_module.ServiceType.SONGKICK,
        external_user_id="someone",
    )
    async with pg_task_db.factory() as session:
        session.add(connection)
        await session.commit()

    real_fetch = concert_worker.url_safety_module.fetch_feed

    async def sync(job_id: str, gate: _Gate | None) -> task_models.Task:
        task = await pg_task_db.add(
            task_type=types_module.TaskType.CALENDAR_SYNC,
            user_id=user.id,
            service_connection_id=connection.id,
        )

        feed_1 = _FEED_1
        if gate is not None:  # a changed title, so the re-run really UPDATEs
            feed_1 = feed_1.replace("Elder at", "Elder (late show) at")

        async def handler(request: httpx.Request) -> httpx.Response:
            body = feed_1 if "attendance" in str(request.url) else _FEED_2
            if gate is not None and "tracked_artist" in str(request.url):
                await gate()
            return httpx.Response(200, text=body)

        async def fetch(url: str) -> str:
            return await real_fetch(url, client=_http(handler))

        monkeypatch.setattr(concert_worker.url_safety_module, "fetch_feed", fetch)
        monkeypatch.setattr(
            concert_worker.url_safety_module,
            "resolve_safe_addresses",
            lambda host, port: [(2, "93.184.216.34")],
        )
        ctx = {
            "session_factory": pg_task_db.factory,
            "redis": AsyncMock(),
            "job_id": job_id,
        }
        run = lifecycle_module.task_job(concert_worker.sync_calendar_feed)(
            ctx, str(connection.id), str(task.id)
        )
        if gate is None:
            await run
        else:
            pending = asyncio.create_task(run)
            try:
                await _while_blocked(gate)
            finally:
                gate.release.set()
                await asyncio.wait_for(pending, 10)
        return await pg_task_db.get(task.id)

    async def _while_blocked(gate: _Gate) -> None:
        await asyncio.wait_for(gate.reached.wait(), 10)
        await _writable(
            pg_task_db.factory,
            sa.update(concert_models.Event)
            .where(concert_models.Event.external_id == "songkick-evt-1")
            .values(title="Renamed"),
            sa.update(concert_models.Venue)
            .where(concert_models.Venue.name == "The Fillmore")
            .values(state="CA"),
            sa.update(concert_models.EventCandidate)
            .where(concert_models.EventCandidate.external_id == "songkick-evt-1")
            .values(title="Renamed"),
        )

    # The first sync writes the rows; the re-run blocks fetching feed 2 while
    # another session updates the event/venue feed 1 maps to.
    seeded = await sync("a", None)
    assert seeded.status == S.COMPLETED, seeded.error_message

    rerun = await sync("b", _Gate(block_on=1))
    assert rerun.status == S.COMPLETED, rerun.error_message
    assert rerun.result["total_events"] == 2

    async with pg_task_db.factory() as session:
        events = list(
            (await session.execute(sa.select(concert_models.Event.external_id)))
            .scalars()
            .all()
        )
    assert sorted(events) == ["songkick-evt-1", "songkick-evt-2"]  # idempotent
