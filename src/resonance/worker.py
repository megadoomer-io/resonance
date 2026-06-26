"""Arq worker with plan_sync and sync_range task functions."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import datetime
import traceback
import typing
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import collections.abc

    # Fetches similar-artist neighbors for one (connector, artist): returns
    # ``[{"name": str, "mbid": str | None}, ...]``. Injectable so the enrich task
    # can read/record persistent similarity edges while generation uses the live
    # connector directly.
    # Slots are (connector, artist, limit); typed Any here because the artist
    # model is imported below this block. The concrete fetch functions are fully
    # typed at their own definitions.
    NeighborFetch = collections.abc.Callable[
        [Any, Any, int],
        collections.abc.Awaitable[list[dict[str, Any]]],
    ]

import arq
import arq.connections as arq_connections
import httpx
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import sqlalchemy.orm as sa_orm
import sqlalchemy.orm.exc as orm_exc
import structlog

import resonance.concerts.sync as concert_sync
import resonance.concerts.worker as concert_worker
import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.concert_archives as concert_archives_module
import resonance.connectors.ical as ical_module
import resonance.connectors.lastfm as lastfm_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.registry as registry_module
import resonance.connectors.songkick as songkick_module
import resonance.connectors.spotify as spotify_module
import resonance.connectors.test as test_connector_module
import resonance.crypto as crypto_module
import resonance.database as database_module
import resonance.generators.concert_prep as concert_prep_module
import resonance.generators.parameters as params_module
import resonance.generators.pool as pool_module
import resonance.heartbeat as heartbeat_module
import resonance.logging as logging_module
import resonance.migrations as migrations_module
import resonance.models.concert as concert_models
import resonance.models.generator as generator_models
import resonance.models.music as music_models
import resonance.models.playlist as playlist_models
import resonance.models.task as task_module
import resonance.models.taste as taste_models
import resonance.models.user as user_models
import resonance.services.artist_import as artist_import_module
import resonance.services.artist_utils as artist_utils
import resonance.services.mbid_mapper as mbid_mapper_module
import resonance.sync.backfill as backfill_module
import resonance.sync.base as sync_base
import resonance.sync.lastfm as lastfm_sync
import resonance.sync.lifecycle as lifecycle_module
import resonance.sync.listenbrainz as lb_sync
import resonance.sync.spotify as spotify_sync
import resonance.sync.test as test_sync
import resonance.types as types_module

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Orphan recovery dispatch map
# ---------------------------------------------------------------------------
# Maps each TaskType to (arq_job_name, args_builder). The args_builder
# receives a Task and returns the positional args tuple for enqueue_job.

_TASK_DISPATCH: dict[
    types_module.TaskType,
    tuple[str, collections.abc.Callable[[task_module.Task], tuple[str, ...]]],
] = {
    types_module.TaskType.SYNC_JOB: ("plan_sync", lambda t: (str(t.id),)),
    types_module.TaskType.TIME_RANGE: ("sync_range", lambda t: (str(t.id),)),
    types_module.TaskType.CALENDAR_SYNC: (
        "sync_calendar_feed",
        lambda t: (str(t.service_connection_id), str(t.id)),
    ),
    types_module.TaskType.BULK_JOB: ("run_bulk_job", lambda t: (str(t.id),)),
    types_module.TaskType.PLAYLIST_GENERATION: (
        "generate_playlist",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.TRACK_DISCOVERY: (
        "discover_tracks_for_artist",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.TRACK_SCORING: (
        "score_and_build_playlist",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.CONCERT_ARCHIVES_IMPORT: (
        "sync_concert_archives",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.CONCERT_ARCHIVES_CHUNK: (
        "sync_concert_archives_chunk",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.PLAYLIST_EXPORT: (
        "export_playlist",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.MBID_BACKFILL: (
        "backfill_mbids",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.POPULARITY_BACKFILL: (
        "backfill_popularity",
        lambda t: (str(t.id),),
    ),
    types_module.TaskType.RELATED_ARTIST_ENRICHMENT: (
        "enrich_related_artists",
        lambda t: (str(t.id),),
    ),
}


class WorkerContext(typing.TypedDict):
    """Typed arq worker context dict.

    arq passes ``dict[str, Any]`` at runtime; this TypedDict lets mypy
    catch key-name typos and wrong value types at check time.
    """

    settings: config_module.Settings
    engine: sa_async.AsyncEngine
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession]
    connector_registry: registry_module.ConnectorRegistry
    strategies: dict[types_module.ServiceType, sync_base.SyncStrategy]
    redis: arq.ArqRedis


# ---------------------------------------------------------------------------
# plan_sync: top-level entry point for a sync job
# ---------------------------------------------------------------------------


async def plan_sync(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Load a SYNC_JOB task, mark it RUNNING, and create child tasks.

    Routes to the appropriate planner based on service type (ListenBrainz
    or Spotify), then enqueues the resulting child tasks.

    Args:
        ctx: arq worker context dict (contains session_factory, settings, etc.).
        sync_task_id: UUID string of the SYNC_JOB Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        try:
            task = await _load_task(session, sync_task_id)
            if task is None:
                log.error("plan_sync_task_not_found")
                return

            # Check if this task was cancelled before we start
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("plan_sync_cancelled")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            # Load the service connection
            conn_result = await session.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.id == task.service_connection_id
                )
            )
            connection = conn_result.scalar_one()
            log = log.bind(
                service=connection.service_type.value,
                user_id=str(task.user_id),
            )
            log.info("plan_sync_started")

            # Look up strategy
            strategy = wctx["strategies"].get(connection.service_type)
            if strategy is None:
                await lifecycle_module.fail_task(
                    session,
                    task,
                    f"No sync strategy for {connection.service_type.value}",
                )
                await session.commit()
                return

            # Look up connector (must be a full BaseConnector for sync)
            connector = wctx["connector_registry"].get_base_connector(
                connection.service_type
            )
            if connector is None:
                await lifecycle_module.fail_task(
                    session,
                    task,
                    f"No connector for {connection.service_type.value}",
                )
                await session.commit()
                return

            # Plan
            descriptors = await strategy.plan(session, connection, connector)

            if not descriptors:
                await lifecycle_module.complete_task(
                    session,
                    task,
                    {"items_created": 0, "items_updated": 0},
                )
                await session.commit()
                log.info("plan_sync_no_work")
                return

            # Create child tasks from descriptors
            arq_redis = wctx["redis"]
            parent_step_mode = bool(task.params and task.params.get("step_mode"))
            children: list[task_module.Task] = []
            for desc in descriptors:
                child_params = dict(desc.params) if desc.params else {}
                if parent_step_mode:
                    child_params["step_mode"] = True
                child = task_module.Task(
                    id=uuid.uuid4(),
                    user_id=task.user_id,
                    service_connection_id=task.service_connection_id,
                    parent_id=task.id,
                    task_type=desc.task_type,
                    status=types_module.SyncStatus.PENDING,
                    params=child_params,
                    progress_total=desc.progress_total,
                    description=desc.description,
                )
                session.add(child)
                children.append(child)
            await session.commit()

            # Enqueue based on concurrency policy
            if strategy.concurrency == "parallel":
                for child in children:
                    await arq_redis.enqueue_job(
                        "sync_range",
                        str(child.id),
                        _job_id=f"sync_range:{child.id}",
                    )
                    log.info("child_enqueued", child_id=str(child.id))
            else:
                # Sequential: enqueue only the first
                await arq_redis.enqueue_job(
                    "sync_range",
                    str(children[0].id),
                    _job_id=f"sync_range:{children[0].id}",
                )
                log.info(
                    "child_enqueued",
                    child_id=str(children[0].id),
                    mode="sequential",
                )

        except Exception:
            log.exception("plan_sync_failed")
            # Re-fetch task in case the session was invalidated
            task_reload = await _load_task(session, sync_task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(
                    session, task_reload, traceback.format_exc()
                )
                await session.commit()


# ---------------------------------------------------------------------------
# sync_range: execute a TIME_RANGE task
# ---------------------------------------------------------------------------


async def sync_range(ctx: dict[str, Any], sync_task_id: str) -> None:
    """Execute a TIME_RANGE task using the appropriate sync strategy.

    Delegates to the strategy's execute() method, handling completion,
    deferral (DeferRequest), and failure. Always checks parent completion
    afterward to cascade status or enqueue the next sequential sibling.

    Args:
        ctx: arq worker context dict.
        sync_task_id: UUID string of the TIME_RANGE Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    connector_registry = wctx["connector_registry"]
    log = logger.bind(sync_task_id=sync_task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, sync_task_id)
            if task is None:
                log.error("sync_range_task_not_found")
                return

            # Check if this task or its parent was cancelled
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("sync_range_cancelled")
                # Still check parent completion so sibling pipeline advances
                arq_redis = wctx["redis"]
                await _check_parent_completion(session, task, arq_redis, log)
                return

            # Detect retry: task is still RUNNING from a previous crashed attempt
            is_retry = task.status == types_module.SyncStatus.RUNNING

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            # Load the service connection
            conn_result = await session.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.id == task.service_connection_id
                )
            )
            connection = conn_result.scalar_one()
            log = log.bind(
                service=connection.service_type.value,
                user_id=str(task.user_id),
            )

            # On retry, resume from watermark to avoid re-processing pages
            if is_retry:
                _apply_watermark_resume(task, connection)

            log.info("sync_range_started")

            strategy = wctx["strategies"].get(connection.service_type)
            connector = connector_registry.get_base_connector(connection.service_type)
            if strategy is None or connector is None:
                raise RuntimeError(
                    f"No strategy/connector for {connection.service_type.value}"
                )

            try:
                result = await strategy.execute(session, task, connector, connection)
                await lifecycle_module.complete_task(session, task, result)

                # Write watermark back to connection
                watermark = result.get("watermark")
                if watermark and isinstance(watermark, dict):
                    data_type = str(task.params.get("data_type", ""))
                    if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
                        data_type_key = "listens"
                    else:
                        data_type_key = data_type
                    updated_watermarks = dict(connection.sync_watermark)
                    updated_watermarks[data_type_key] = watermark
                    connection.sync_watermark = updated_watermarks

                await session.commit()
                log.info("sync_range_completed", result=task.result)
            except sync_base.DeferRequest as defer:
                task.status = types_module.SyncStatus.DEFERRED
                task.params = {**task.params, **defer.resume_params}
                task.deferred_until = datetime.datetime.now(
                    datetime.UTC
                ) + datetime.timedelta(seconds=defer.retry_after)
                await session.commit()
                arq_redis_defer = wctx["redis"]
                await arq_redis_defer.enqueue_job(
                    "sync_range",
                    str(task.id),
                    _job_id=f"sync_range:{task.id}",
                    _defer_by=datetime.timedelta(seconds=defer.retry_after),
                )
                log.info(
                    "sync_range_deferred",
                    retry_after=defer.retry_after,
                    deferred_until=str(task.deferred_until),
                )
            except sync_base.ShutdownRequest as shutdown_req:
                task.status = types_module.SyncStatus.PENDING
                task.params = {**task.params, **shutdown_req.resume_params}
                task.started_at = None
                await session.commit()
                log.info(
                    "sync_range_shutdown_checkpoint",
                    resume_params=shutdown_req.resume_params,
                )
                return

        except Exception:
            log.exception("sync_range_failed")
            task_reload = await _load_task(session, sync_task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(
                    session, task_reload, traceback.format_exc()
                )
                await session.commit()
                task = task_reload

        # Check parent completion (may enqueue next sibling)
        # In step_mode, skip auto-advance so user manually triggers next step
        if task is not None:
            # Check step_mode on this task's params
            step_mode = bool(task.params and task.params.get("step_mode"))

            if step_mode and task.status == types_module.SyncStatus.COMPLETED:
                log.info("step_mode_paused", task_id=str(task.id))
            else:
                arq_redis = wctx["redis"]
                await _check_parent_completion(session, task, arq_redis, log)


# ---------------------------------------------------------------------------
# Bulk job execution
# ---------------------------------------------------------------------------

_BULK_OPERATIONS: dict[str, str] = {
    "dedup_artists": "find_and_merge_duplicate_artists",
    "dedup_tracks": "find_and_merge_duplicate_tracks",
    "dedup_events": "delete_cross_service_duplicate_events",
}


async def run_bulk_job(ctx: dict[str, Any], task_id: str) -> None:
    """Execute a BULK_JOB task (dedup, future bulk operations).

    Reads ``params["operation"]`` to dispatch to the correct function
    in the dedup module. Updates task status through the standard
    PENDING -> RUNNING -> COMPLETED/FAILED lifecycle.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the BULK_JOB Task.
    """
    import resonance.dedup as dedup_module

    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("bulk_job_task_not_found")
                return

            # Check if this task or its parent was cancelled
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("bulk_job_cancelled")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            operation = str(task.params.get("operation", ""))
            log = log.bind(operation=operation)
            log.info("bulk_job_started")

            result: dict[str, object]
            if operation == "dedup_artists":
                stats = await dedup_module.find_and_merge_duplicate_artists(session)
                result = {
                    "artists_merged": stats.artists_merged,
                    "tracks_repointed": stats.tracks_repointed,
                    "relations_repointed": stats.artist_relations_repointed,
                    "relations_deleted": stats.artist_relations_deleted,
                }
            elif operation == "dedup_tracks":
                stats = await dedup_module.find_and_merge_duplicate_tracks(session)
                result = {
                    "tracks_merged": stats.tracks_merged,
                    "events_repointed": stats.events_repointed,
                    "relations_repointed": stats.track_relations_repointed,
                    "relations_deleted": stats.track_relations_deleted,
                }
            elif operation == "dedup_events":
                deleted = await dedup_module.delete_cross_service_duplicate_events(
                    session
                )
                result = {"events_deleted": deleted}
            elif operation == "dedup_all":
                result = {**await dedup_module.dedup_all(session)}
            elif operation == "reconcile_event_artists":
                matched = await concert_sync.reconcile_unmatched_candidates(session)
                result = {"candidates_matched": matched}
            else:
                msg = f"Unknown bulk operation: {operation}"
                raise ValueError(msg)

            await lifecycle_module.complete_task(session, task, result)
            await session.commit()
            log.info("bulk_job_completed", result=task.result)

        except Exception:
            log.exception("bulk_job_failed")
            if task is not None:
                await lifecycle_module.fail_task(session, task, traceback.format_exc())
                await session.commit()


async def backfill_mbids(ctx: dict[str, Any], task_id: str) -> None:
    """Execute an MBID_BACKFILL task (#71): resolve missing MusicBrainz IDs.

    Reads ``params``:
      - ``entity_types``: subset of ["track", "artist"] (default both).
      - ``retry``: if true, first clear prior NO_MATCH/BELOW_SIMILARITY markers so
        those rows are re-attempted (transient rows are already unattempted).

    Runs tracks-first (to harvest artist_mbids) then artists via
    ``backfill.run_mbid_backfill``, recording per-entity-type counts in the task
    result. Long-running but resumable (``mb_attempted_at``), so a worker restart
    re-enters and continues from the unattempted remainder.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the MBID_BACKFILL Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    settings = wctx["settings"]
    session_factory = wctx["session_factory"]
    log = logger.bind(task_id=task_id)

    connector = wctx["connector_registry"].get_base_connector(
        types_module.ServiceType.LISTENBRAINZ
    )
    mapper = mbid_mapper_module.MbidMapperClient(settings)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("mbid_backfill_task_not_found")
                return

            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                return

            if not isinstance(connector, listenbrainz_module.ListenBrainzConnector):
                await lifecycle_module.fail_task(
                    session,
                    task,
                    "ListenBrainz connector unavailable for MBID backfill",
                )
                await session.commit()
                log.error("mbid_backfill_no_connector")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            params = task.params or {}
            raw_types = params.get("entity_types")
            entity_types = (
                raw_types if isinstance(raw_types, list) else ["track", "artist"]
            )
            do_tracks = "track" in entity_types
            do_artists = "artist" in entity_types
            retry = bool(params.get("retry", False))

            if retry:
                # Re-attempt prior misses (transient rows are already unattempted).
                # Unrolled per model so the column types stay concrete for mypy.
                reattempt = [
                    types_module.MatchStatus.NO_MATCH,
                    types_module.MatchStatus.BELOW_SIMILARITY,
                ]
                await session.execute(
                    sa.update(music_models.Track)
                    .where(music_models.Track.mb_match_status.in_(reattempt))
                    .values(mb_attempted_at=None, mb_match_status=None)
                )
                await session.execute(
                    sa.update(music_models.Artist)
                    .where(music_models.Artist.mb_match_status.in_(reattempt))
                    .values(mb_attempted_at=None, mb_match_status=None)
                )
                await session.commit()

            log.info(
                "mbid_backfill_started",
                do_tracks=do_tracks,
                do_artists=do_artists,
                retry=retry,
            )
            counts = await backfill_module.run_mbid_backfill(
                session,
                settings,
                mapper=mapper,
                connector=connector,
                do_tracks=do_tracks,
                do_artists=do_artists,
            )
            result: dict[str, object] = {
                etype: dataclasses.asdict(c) for etype, c in counts.items()
            }
            await lifecycle_module.complete_task(session, task, result)
            await session.commit()
            log.info("mbid_backfill_done", result=result)
        except Exception:
            log.exception("mbid_backfill_failed")
            if task is not None:
                await lifecycle_module.fail_task(session, task, traceback.format_exc())
                await session.commit()
        finally:
            await mapper.aclose()


async def backfill_popularity(ctx: dict[str, Any], task_id: str) -> None:
    """Execute a POPULARITY_BACKFILL task: refresh Track.popularity_score from LB.

    Fetches each library track's global listen count from ListenBrainz's public
    recording-popularity endpoint (keyed by MusicBrainz recording MBID),
    normalizes it to a 0-100 score, and overwrites ``popularity_score``. This
    supersedes the discovery-sourced synthetic values seeded in #116. The LB
    endpoint is unauthenticated, so no token is needed. Sequential + batched and
    resumable across worker restarts because the scan is idempotent.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the POPULARITY_BACKFILL Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    settings = wctx["settings"]
    session_factory = wctx["session_factory"]
    log = logger.bind(task_id=task_id)

    connector = wctx["connector_registry"].get_base_connector(
        types_module.ServiceType.LISTENBRAINZ
    )

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("popularity_backfill_task_not_found")
                return

            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                return

            if not isinstance(connector, listenbrainz_module.ListenBrainzConnector):
                await lifecycle_module.fail_task(
                    session,
                    task,
                    "ListenBrainz connector unavailable for popularity backfill",
                )
                await session.commit()
                log.error("popularity_backfill_no_connector")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            log.info("popularity_backfill_started")
            counts = await backfill_module.run_popularity_backfill(
                session, settings, connector
            )
            result: dict[str, object] = dataclasses.asdict(counts)
            await lifecycle_module.complete_task(session, task, result)
            await session.commit()
            log.info("popularity_backfill_done", result=result)
        except Exception:
            log.exception("popularity_backfill_failed")
            if task is not None:
                await lifecycle_module.fail_task(session, task, traceback.format_exc())
                await session.commit()


# ---------------------------------------------------------------------------
# Playlist generation pipeline
# ---------------------------------------------------------------------------

# Minimum number of distinct library tracks per artist before skipping discovery.
_MIN_LIBRARY_TRACKS = 5

# A profile with familiarity below this (0-100 bipolar; <50 leans toward
# discovery) triggers an external catalog fetch for ALL target artists, not just
# under-covered ones, so a well-known artist can still surface unheard tracks.
_DISCOVERY_FAMILIARITY_THRESHOLD = 50


def _artists_needing_discovery(
    artist_ids: collections.abc.Iterable[uuid.UUID],
    track_coverage: collections.abc.Mapping[uuid.UUID, int],
    discovery_wanted: bool,
) -> list[uuid.UUID]:
    """Pick which artists need an external catalog fetch (TRACK_DISCOVERY).

    An artist needs discovery when its distinct-track coverage is below
    ``_MIN_LIBRARY_TRACKS`` OR the profile is leaning toward discovery
    (``discovery_wanted``). The discovery-intent branch is what lets a
    well-covered target artist surface unheard catalog tracks for a
    high-discovery playlist (issue #110); without it, the candidate pool for such
    an artist is 100% scrobble-derived and has nothing unheard to offer.

    Pure logic (no DB) so the gating decision is unit-testable.
    """
    return [
        aid
        for aid in artist_ids
        if track_coverage.get(aid, 0) < _MIN_LIBRARY_TRACKS or discovery_wanted
    ]


async def resolve_pool(
    session: sa_async.AsyncSession,
    input_references: collections.abc.Mapping[str, object],
) -> list[pool_module.ResolvedArtist]:
    """Resolve a profile's ``input_references`` into a deduplicated artist pool.

    Parses the layered source spec, tolerating the legacy ``{"event_id": ...}``
    shape via :func:`pool.normalize_sources` (so a pre-#128 profile still resolves
    during the migration window), resolves the DB-backed sources, then applies the
    global ``exclude_artist_ids`` set last.

    Resolved here:

    * ``event``  -> the event's confirmed ``EventArtist`` rows plus accepted
      ``EventArtistCandidate`` matches. Every enabled event source is resolved in
      one ``event_id IN (...)`` query per table rather than per-event (#128 T14).
    * ``artist`` -> the artist itself (including artists added by related-artist
      enrichment, which persists them as concrete ``artist`` sources, #133).

    Returns the deduplicated, exclude-filtered artists in first-seen order (event
    sources before artist sources), each tagged with provenance. This is the single
    target-resolution path shared by both worker generation functions, replacing the
    event-resolution blocks that were duplicated across them.
    """
    sources = pool_module.normalize_sources(input_references)
    exclude_ids = pool_module.extract_excludes(input_references)

    resolved: list[pool_module.ResolvedArtist] = []

    # Event sources: batch every enabled event into one IN-query per table (T14).
    event_ids = [
        s.event_id
        for s in sources
        if isinstance(s, pool_module.EventSource) and s.enabled
    ]
    if event_ids:
        ea_result = await session.execute(
            sa.select(concert_models.EventArtist).where(
                concert_models.EventArtist.event_id.in_(event_ids)
            )
        )
        for ea in ea_result.scalars().all():
            resolved.append(
                pool_module.ResolvedArtist(
                    artist_id=ea.artist_id, via=pool_module.PoolProvenance.EVENT
                )
            )

        eac_result = await session.execute(
            sa.select(concert_models.EventArtistCandidate).where(
                concert_models.EventArtistCandidate.event_id.in_(event_ids),
                concert_models.EventArtistCandidate.status
                == types_module.CandidateStatus.ACCEPTED,
                concert_models.EventArtistCandidate.matched_artist_id.isnot(None),
            )
        )
        for cand in eac_result.scalars().all():
            if cand.matched_artist_id is not None:
                resolved.append(
                    pool_module.ResolvedArtist(
                        artist_id=cand.matched_artist_id,
                        via=pool_module.PoolProvenance.EVENT,
                    )
                )

    # Artist sources: the artist itself enters the pool directly.
    for source in sources:
        if isinstance(source, pool_module.ArtistSource) and source.enabled:
            resolved.append(
                pool_module.ResolvedArtist(
                    artist_id=source.artist_id, via=pool_module.PoolProvenance.ARTIST
                )
            )

    return pool_module.build_pool(resolved, exclude_ids)


async def generate_playlist(ctx: dict[str, Any], task_id: str) -> None:
    """Orchestrate playlist generation: create discovery + scoring child tasks.

    Loads the PLAYLIST_GENERATION task, resolves the event's artists,
    checks library coverage, creates TRACK_DISCOVERY children for artists
    with few/no tracks, and always creates one TRACK_SCORING child.
    Sequential dispatch ensures discovery tasks run one at a time (for
    MusicBrainz rate limits), then scoring runs last.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the PLAYLIST_GENERATION Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("generate_playlist_task_not_found")
                return

            # Check if this task or its parent was cancelled
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("generate_playlist_cancelled")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            profile_id = str(task.params.get("profile_id", ""))
            log = log.bind(profile_id=profile_id)

            # Load profile
            profile_result = await session.execute(
                sa.select(generator_models.GeneratorProfile).where(
                    generator_models.GeneratorProfile.id == uuid.UUID(profile_id)
                )
            )
            profile = profile_result.scalar_one_or_none()
            if profile is None:
                await lifecycle_module.fail_task(
                    session, task, f"Profile not found: {profile_id}"
                )
                await session.commit()
                return

            # event_id is retained only for the legacy scoring-child param and
            # logging; the actual artist set comes from resolve_pool, which handles
            # both the legacy {"event_id": ...} and layered {"sources": [...]} shapes
            # and applies the global exclude set (#128).
            event_id = str(profile.input_references.get("event_id", ""))
            log = log.bind(event_id=event_id)

            pool = await resolve_pool(session, profile.input_references)
            artist_ids: list[uuid.UUID] = [r.artist_id for r in pool]
            log = log.bind(pool_size=len(artist_ids))

            if not artist_ids:
                await lifecycle_module.complete_task(
                    session,
                    task,
                    {"message": "No artists found for event", "tracks_created": 0},
                )
                await session.commit()
                log.info("generate_playlist_no_artists")
                return

            # Load artist objects for names and service_links
            artists_result = await session.execute(
                sa.select(music_models.Artist).where(
                    music_models.Artist.id.in_(artist_ids)
                )
            )
            artists_by_id = {a.id: a for a in artists_result.scalars().all()}

            # Library coverage = distinct tracks per artist that the user has
            # actually listened to (not total scrobbles). A handful of songs
            # played many times should NOT read as broad coverage.
            track_coverage_result = await session.execute(
                sa.select(
                    music_models.Track.artist_id,
                    sa.func.count(sa.distinct(music_models.Track.id)).label("cnt"),
                )
                .join(
                    music_models.ListeningEvent,
                    music_models.ListeningEvent.track_id == music_models.Track.id,
                )
                .where(
                    music_models.Track.artist_id.in_(artist_ids),
                    music_models.ListeningEvent.user_id == task.user_id,
                )
                .group_by(music_models.Track.artist_id)
            )
            track_coverage: dict[uuid.UUID, int] = {
                row[0]: row[1] for row in track_coverage_result.all()
            }

            # A discovery-leaning profile fetches catalog tracks for every target
            # artist, so well-known artists also surface unheard tracks (#110).
            gen_params = params_module.apply_defaults(dict(profile.parameter_values))
            discovery_wanted = (
                gen_params.get("familiarity", 50) < _DISCOVERY_FAMILIARITY_THRESHOLD
            )
            artists_needing_discovery = _artists_needing_discovery(
                artist_ids, track_coverage, discovery_wanted
            )

            # Generation uses exactly the resolved pool -- the concrete artists in
            # input_references, including any added by related-artist enrichment
            # (#133). There is no generation-time related/adjacent expansion: that
            # moved to the explicit enrich task, which persists discovered artists
            # into the pool as concrete sources beforehand.
            discovery_artist_ids = artists_needing_discovery
            artist_lookup = artists_by_id

            # Create child tasks
            arq_redis = wctx["redis"]
            children: list[task_module.Task] = []

            # Discovery tasks (one per artist needing tracks). Thread max_tracks
            # through so each artist's catalog fetch scales with the playlist
            # target (a single deep artist can fill the whole list; round-robin
            # in scoring balances across artists).
            max_tracks = int(str(task.params.get("max_tracks", 50)))
            for aid in discovery_artist_ids:
                artist_obj = artist_lookup.get(aid)
                artist_name = artist_obj.name if artist_obj else "Unknown"
                service_links = artist_obj.service_links if artist_obj else None
                child = task_module.Task(
                    id=uuid.uuid4(),
                    user_id=task.user_id,
                    parent_id=task.id,
                    task_type=types_module.TaskType.TRACK_DISCOVERY,
                    status=types_module.SyncStatus.PENDING,
                    params={
                        "artist_id": str(aid),
                        "artist_name": artist_name,
                        "service_links": service_links,
                        "max_tracks": max_tracks,
                    },
                    description=f"Discover tracks: {artist_name}",
                )
                session.add(child)
                children.append(child)

            # Scoring task (always created, runs after discovery)
            scoring_child = task_module.Task(
                id=uuid.uuid4(),
                user_id=task.user_id,
                parent_id=task.id,
                task_type=types_module.TaskType.TRACK_SCORING,
                status=types_module.SyncStatus.PENDING,
                params={
                    "profile_id": profile_id,
                    "event_id": event_id,
                },
                description="Score and build playlist",
            )
            session.add(scoring_child)
            children.append(scoring_child)
            await session.commit()

            # Enqueue the first child (sequential dispatch)
            first_child = children[0]
            dispatch = _TASK_DISPATCH.get(first_child.task_type)
            if dispatch is not None:
                job_name, args_builder = dispatch
                args = args_builder(first_child)
                await arq_redis.enqueue_job(
                    job_name,
                    *args,
                    _job_id=f"{job_name}:{first_child.id}",
                )
            log.info(
                "generate_playlist_planned",
                discovery_tasks=len(discovery_artist_ids),
                target_discovery_tasks=len(artists_needing_discovery),
                total_children=len(children),
            )

        except Exception:
            log.exception("generate_playlist_failed")
            task_reload = await _load_task(session, task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(
                    session, task_reload, traceback.format_exc()
                )
                await session.commit()


async def discover_tracks_for_artist(ctx: dict[str, Any], task_id: str) -> None:
    """Discover tracks for a single artist via external connectors.

    Loads the TRACK_DISCOVERY task, calls the discovery connector, upserts
    found tracks into the Track table, and cascades to the next sibling
    or triggers scoring.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the TRACK_DISCOVERY Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    connector_registry = wctx["connector_registry"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("discover_tracks_task_not_found")
                return

            # Check if this task or its parent was cancelled
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("discover_tracks_cancelled")
                # Still check parent completion so pipeline advances
                arq_redis = wctx["redis"]
                await _check_parent_completion(session, task, arq_redis, log)
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            artist_id_str = str(task.params.get("artist_id", ""))
            artist_name = str(task.params.get("artist_name", ""))
            service_links = task.params.get("service_links")
            # Per-artist catalog fetch size scales with the playlist target so a
            # single deep artist can fill the whole list (round-robin balances
            # across artists). Bounding the fetch by max_tracks keeps the
            # MusicBrainz request volume proportional to what the playlist can
            # actually use, never more. Fallback to the playlist default (50) for
            # older child tasks created before max_tracks was threaded through.
            discovery_limit = int(str(task.params.get("max_tracks", 50)))
            log = log.bind(artist_name=artist_name, artist_id=artist_id_str)
            log.info("discover_tracks_started", discovery_limit=discovery_limit)

            # Get a connector with TRACK_DISCOVERY capability
            connectors = connector_registry.get_by_capability(
                base_module.ConnectorCapability.TRACK_DISCOVERY
            )
            if not connectors:
                await lifecycle_module.fail_task(
                    session, task, "No connector with TRACK_DISCOVERY capability"
                )
                await session.commit()
                # Still check parent completion so pipeline doesn't stall
                arq_redis = wctx["redis"]
                await _check_parent_completion(session, task, arq_redis, log)
                return

            connector = connectors[0]
            service_links_dict: dict[str, str] | None = None
            if isinstance(service_links, dict):
                service_links_dict = {str(k): str(v) for k, v in service_links.items()}

            try:
                if not isinstance(connector, base_module.TrackDiscoveryCapable):
                    msg = f"{type(connector).__name__} does not support track discovery"
                    raise TypeError(msg)
                discovered: list[
                    base_module.DiscoveredTrack
                ] = await connector.discover_tracks(
                    artist_name,
                    service_links_dict,
                    limit=discovery_limit,
                )
            except base_module.RateLimitExceededError as exc:
                task.status = types_module.SyncStatus.DEFERRED
                task.deferred_until = datetime.datetime.now(
                    datetime.UTC
                ) + datetime.timedelta(seconds=exc.retry_after)
                await session.commit()
                arq_redis_defer = wctx["redis"]
                await arq_redis_defer.enqueue_job(
                    "discover_tracks_for_artist",
                    str(task.id),
                    _job_id=f"discover_tracks_for_artist:{task.id}",
                    _defer_by=datetime.timedelta(seconds=exc.retry_after),
                )
                log.info(
                    "discover_tracks_deferred",
                    retry_after=exc.retry_after,
                    deferred_until=str(task.deferred_until),
                )
                return

            # Upsert discovered tracks
            tracks_found = 0
            artist_uuid = uuid.UUID(artist_id_str)
            for dt in discovered:
                # Try to find existing track by title + artist
                existing_result = await session.execute(
                    sa.select(music_models.Track).where(
                        music_models.Track.title == dt.title,
                        music_models.Track.artist_id == artist_uuid,
                    )
                )
                existing = existing_result.scalar_one_or_none()
                if existing is None:
                    # Look up artist to confirm it exists
                    artist_result = await session.execute(
                        sa.select(music_models.Artist).where(
                            music_models.Artist.id == artist_uuid,
                        )
                    )
                    artist_obj = artist_result.scalar_one_or_none()
                    if artist_obj is not None:
                        new_track = music_models.Track(
                            id=uuid.uuid4(),
                            title=dt.title,
                            artist_id=artist_uuid,
                            duration_ms=dt.duration_ms,
                            service_links={dt.service.value: dt.external_id},
                            popularity_score=dt.popularity_score,
                        )
                        session.add(new_track)
                        tracks_found += 1
                else:
                    # Update service_links if needed
                    if existing.service_links is None:
                        existing.service_links = {}
                    updated_links = dict(existing.service_links)
                    updated_links[dt.service.value] = dt.external_id
                    existing.service_links = updated_links
                    # Seed popularity from discovery only when unknown; an
                    # authoritative source (Spotify) must not be clobbered by a
                    # discovery connector's synthetic rank.
                    if existing.popularity_score is None:
                        existing.popularity_score = dt.popularity_score
                    tracks_found += 1

            await lifecycle_module.complete_task(
                session, task, {"tracks_found": tracks_found}
            )
            await session.commit()
            log.info("discover_tracks_completed", tracks_found=tracks_found)

        except Exception:
            log.exception("discover_tracks_failed")
            task_reload = await _load_task(session, task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(
                    session, task_reload, traceback.format_exc()
                )
                await session.commit()
                task = task_reload

        # Check parent completion (may enqueue next sibling or scoring)
        if task is not None:
            arq_redis = wctx["redis"]
            await _check_parent_completion(session, task, arq_redis, log)


@dataclasses.dataclass(frozen=True)
class _AdjacentResolution:
    """Resolved adjacent artists for discovery (issue #115 Phase 2).

    ``library_ids`` are similar artists already in the library (free to use).
    ``import_candidates`` are similar artists NOT in the library, as
    ``(name, mbid)`` pairs in provider similarity rank, ready to import. Both
    lists preserve first-seen (similarity) order so the caller can cap by rank.
    """

    library_ids: list[uuid.UUID]
    import_candidates: list[tuple[str, str]]


async def _default_neighbor_fetch(
    connector: Any, artist: music_models.Artist, limit: int
) -> list[dict[str, Any]]:
    """Live similar-artists fetch (no persistence).

    The default for :func:`_resolve_adjacent_artists`; the enrich task injects a
    store-backed variant (:func:`_fetch_similar_with_store`) instead.
    """
    mbid = artist_utils.get_mbid(artist.service_links)
    result = await connector.get_similar_artists(artist.name, mbid=mbid, limit=limit)
    return list(result)


async def _resolve_adjacent_artists(
    session: sa_async.AsyncSession,
    connectors: collections.abc.Sequence[Any],
    target_artists: collections.abc.Sequence[music_models.Artist],
    exclude_ids: set[uuid.UUID],
    *,
    per_artist_limit: int = 30,
    neighbor_fetch: NeighborFetch | None = None,
) -> _AdjacentResolution:
    """Resolve similar artists into library matches AND import candidates.

    Splits each target's neighbors into those already in the library (returned as
    ids) and those NOT in the library (returned as ``(name, MBID)`` import
    candidates), so the caller can import the new ones. Used by the related-artist
    enrich task (#133); origin issue #115 Phase 2.

    Dedup follows design decision (d): a neighbor whose MBID matches a library
    artist is treated as a library match even under a different name (MBID
    first), then by normalized name. A neighbor without an MBID cannot be
    imported cleanly in v1, so it is never an import candidate (it may still
    match the library by name). Target artists are excluded from both lists.

    Args:
        session: Async DB session (reads only; the caller does imports/writes).
        connectors: Connectors exposing ``get_similar_artists``.
        target_artists: The event's target (lineup) artists.
        exclude_ids: Artist IDs to exclude (the target set).
        per_artist_limit: Max neighbors to request per target.
        neighbor_fetch: Optional ``(connector, artist, limit) -> [neighbor]``
            override. Defaults to a live connector fetch; the enrich task injects
            a store-backed fetch that reads/records persistent similarity edges.

    Returns:
        An ``_AdjacentResolution`` with library IDs and import candidates, both
        in provider similarity rank.
    """
    # First-seen order of lowercased neighbor names = approximate similarity
    # rank across providers/targets. Track the original name + best-known MBID
    # per neighbor (a later provider may supply an MBID an earlier one lacked).
    fetch = neighbor_fetch or _default_neighbor_fetch
    ordered: dict[str, dict[str, str | None]] = {}
    for artist in target_artists:
        for connector in connectors:
            neighbors = await fetch(connector, artist, per_artist_limit)
            for neighbor in neighbors:
                name = neighbor.get("name")
                if not name:
                    continue
                key = name.lower()
                nbr_mbid = neighbor.get("mbid") or None
                if key not in ordered:
                    ordered[key] = {"name": name, "mbid": nbr_mbid}
                elif ordered[key]["mbid"] is None and nbr_mbid:
                    ordered[key]["mbid"] = nbr_mbid

    if not ordered:
        return _AdjacentResolution(library_ids=[], import_candidates=[])

    neighbor_mbids = {v["mbid"] for v in ordered.values() if v["mbid"]}
    # Library artists matching any neighbor by name OR by MBID. Select
    # service_links too so MBID-dedup can map a neighbor MBID to a library id
    # even when the stored name differs.
    conds = [sa.func.lower(music_models.Artist.name).in_(ordered.keys())]
    if neighbor_mbids:
        conds.append(
            music_models.Artist.service_links["musicbrainz"]["id"]
            .as_string()
            .in_(neighbor_mbids)
        )
        conds.append(
            music_models.Artist.service_links["listenbrainz"]
            .as_string()
            .in_(neighbor_mbids)
        )
    result = await session.execute(
        sa.select(
            music_models.Artist.id,
            sa.func.lower(music_models.Artist.name),
            music_models.Artist.service_links,
        ).where(sa.or_(*conds))
    )
    id_by_name: dict[str, uuid.UUID] = {}
    id_by_mbid: dict[str, uuid.UUID] = {}
    for row in result.all():
        aid, lname, links = row[0], row[1], row[2]
        id_by_name.setdefault(lname, aid)
        lib_mbid = artist_utils.get_mbid(links)
        if lib_mbid:
            id_by_mbid.setdefault(lib_mbid, aid)

    target_names = {a.name.lower() for a in target_artists}
    library_ids: list[uuid.UUID] = []
    import_candidates: list[tuple[str, str]] = []
    seen_ids: set[uuid.UUID] = set(exclude_ids)
    for key, meta in ordered.items():
        nbr_mbid = meta["mbid"]
        # MBID match first (decision d), then normalized name.
        aid = (id_by_mbid.get(nbr_mbid) if nbr_mbid else None) or id_by_name.get(key)
        if aid is not None:
            if aid not in seen_ids:
                seen_ids.add(aid)
                library_ids.append(aid)
            continue
        # Not in the library: import only with an MBID and not a target name.
        if nbr_mbid and key not in target_names:
            import_candidates.append((str(meta["name"]), nbr_mbid))
    return _AdjacentResolution(
        library_ids=library_ids, import_candidates=import_candidates
    )


async def _import_adjacent_candidates(
    session: sa_async.AsyncSession,
    connector: Any,
    candidates: collections.abc.Sequence[tuple[str, str]],
    *,
    limit: int,
    exclude_ids: set[uuid.UUID],
    log: Any,
) -> list[uuid.UUID]:
    """Import up to ``limit`` recommended artists by MBID (issue #115 Phase 2).

    Each ``(name, mbid)`` candidate is resolved and created via the
    artist-import service. Failures are logged and skipped (best-effort: a
    flaky MusicBrainz lookup degrades to fewer imports rather than failing the
    whole playlist generation). Returns the newly-imported artist IDs in
    candidate order, deduped against ``exclude_ids`` and each other.

    Args:
        session: Async DB session (the caller owns the transaction/commit).
        connector: Connector exposing ``get_artist_by_mbid`` (ListenBrainz).
        candidates: ``(name, mbid)`` import candidates in similarity rank.
        limit: Max number of candidates to import this generation.
        exclude_ids: Artist IDs already selected (library-adjacent), so an
            import that resolves to one of them is not double-counted.
        log: Bound structlog logger for per-candidate failure reporting.

    Returns:
        The imported artist IDs, in candidate order.
    """
    imported_ids: list[uuid.UUID] = []
    seen = set(exclude_ids)
    for name, mbid in candidates[:limit]:
        try:
            imported = await artist_import_module.import_artist_by_mbid(
                session, connector, mbid
            )
        except Exception:
            log.warning("adjacent_import_failed", mbid=mbid, name=name)
            continue
        if imported is not None and imported.id not in seen:
            seen.add(imported.id)
            imported_ids.append(imported.id)
    return imported_ids


# ---------------------------------------------------------------------------
# Related-artist enrichment (#133)
# ---------------------------------------------------------------------------

# Re-fetch a source artist's similarity edges once they are older than this.
# fetched_at drives refresh (re-fetch + replace), not eviction.
_SIMILARITY_REFRESH_AGE = datetime.timedelta(days=30)

# How many times the enrich task re-applies its result after an optimistic
# version conflict before giving up (#133).
_MAX_VERSION_RETRIES = 3


async def _fetch_similar_with_store(
    session: sa_async.AsyncSession,
    connector: Any,
    artist: music_models.Artist,
    *,
    limit: int,
    now: datetime.datetime,
) -> list[dict[str, Any]]:
    """Read a source artist's similarity edges, falling back to a live fetch (#133).

    Returns ``[{"name": str, "mbid": str | None}, ...]`` for ``artist`` from the
    given connector. Stored ``ArtistSimilarity`` edges are returned when present
    and fresh (younger than :data:`_SIMILARITY_REFRESH_AGE`); otherwise the
    connector is queried live and the edges are replaced. Durable domain data,
    not a cache: ``fetched_at`` drives refresh, never eviction.
    """
    service = connector.service_type
    stored = await session.execute(
        sa.select(taste_models.ArtistSimilarity)
        .where(
            taste_models.ArtistSimilarity.source_artist_id == artist.id,
            taste_models.ArtistSimilarity.connector == service,
        )
        .order_by(taste_models.ArtistSimilarity.rank)
    )
    rows = list(stored.scalars().all())
    if rows and rows[0].fetched_at > now - _SIMILARITY_REFRESH_AGE:
        return [{"name": r.neighbor_name, "mbid": r.neighbor_mbid} for r in rows]

    mbid = artist_utils.get_mbid(artist.service_links)
    neighbors = list(
        await connector.get_similar_artists(artist.name, mbid=mbid, limit=limit)
    )

    # Replace this (artist, connector)'s edges wholesale with the fresh batch.
    await session.execute(
        sa.delete(taste_models.ArtistSimilarity).where(
            taste_models.ArtistSimilarity.source_artist_id == artist.id,
            taste_models.ArtistSimilarity.connector == service,
        )
    )
    for rank, neighbor in enumerate(neighbors):
        name = neighbor.get("name")
        if not name:
            continue
        session.add(
            taste_models.ArtistSimilarity(
                source_artist_id=artist.id,
                connector=service,
                neighbor_name=name,
                neighbor_mbid=neighbor.get("mbid") or None,
                rank=rank,
                fetched_at=now,
            )
        )
    return neighbors


async def _collect_related(
    session: sa_async.AsyncSession,
    similar_connectors: collections.abc.Sequence[Any],
    lb_connector: Any,
    seeds: collections.abc.Sequence[music_models.Artist],
    exclude_ids: set[uuid.UUID],
    target_n: int,
    neighbor_fetch: NeighborFetch,
    log: Any,
) -> list[uuid.UUID]:
    """Collect up to ``target_n`` new related artist ids for a scope (#133).

    Iterates ``seeds`` (one seed for per-seed enrich, the whole lineup core for
    global enrich), resolving similar artists via the persistent-edge fetch.
    Library-adjacent matches are preferred; once ``target_n`` of them are found
    the seed loop early-stops (so a global sweep does not fan out to every seed).
    Any shortfall is topped up with freshly imported recommendations by rank.

    Returns the new artist ids (library matches first, then imports), capped at
    ``target_n``, deduped against ``exclude_ids`` and each other.
    """
    seen = set(exclude_ids)
    library: list[uuid.UUID] = []
    import_candidates: list[tuple[str, str]] = []
    for seed in seeds:
        if len(library) >= target_n:
            break  # early stop: enough library matches already
        resolution = await _resolve_adjacent_artists(
            session,
            similar_connectors,
            [seed],
            seen,
            neighbor_fetch=neighbor_fetch,
        )
        for lid in resolution.library_ids:
            if lid not in seen:
                seen.add(lid)
                library.append(lid)
        import_candidates.extend(resolution.import_candidates)

    new_ids = library[:target_n]
    remaining = target_n - len(new_ids)
    if remaining > 0 and lb_connector is not None and import_candidates:
        imported = await _import_adjacent_candidates(
            session,
            lb_connector,
            import_candidates,
            limit=remaining,
            exclude_ids=seen,
            log=log,
        )
        new_ids.extend(imported)
    return new_ids[:target_n]


async def enrich_related_artists(ctx: dict[str, Any], task_id: str) -> None:
    """Resolve related artists for a scope and persist them into the pool (#133).

    Loads the RELATED_ARTIST_ENRICHMENT task and its profile, resolves similar
    artists for the requested scope (a single seed, several seeds, or the whole
    lineup), imports the new ones, and replaces that scope's prior discovered
    artist sources in ``profile.input_references`` with the fresh batch. The
    discovered artists become concrete, curatable ``artist`` sources tagged with
    ``via_seed``; a later generate re-scores tracks against this fixed pool.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the RELATED_ARTIST_ENRICHMENT Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("enrich_related_artists_task_not_found")
                return

            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("enrich_related_artists_cancelled")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            profile_id = str(task.params.get("profile_id", ""))
            scope_param = task.params.get("seed_artist_ids")
            n_raw = task.params.get("n", 10)
            requested_n = n_raw if isinstance(n_raw, int) else 10
            log = log.bind(profile_id=profile_id)

            profile_result = await session.execute(
                sa.select(generator_models.GeneratorProfile).where(
                    generator_models.GeneratorProfile.id == uuid.UUID(profile_id)
                )
            )
            profile = profile_result.scalar_one_or_none()
            if profile is None:
                await lifecycle_module.fail_task(
                    session, task, f"Profile not found: {profile_id}"
                )
                await session.commit()
                return

            # Plan the scopes: a global "lineup" sweep over the curated core, or
            # one independent scope per requested seed.
            is_lineup = scope_param == "lineup"
            refs: dict[str, object] = dict(profile.input_references)
            sources = pool_module.normalize_sources(refs)
            discovered_ids = {
                s.artist_id
                for s in sources
                if isinstance(s, pool_module.ArtistSource) and s.via_seed is not None
            }
            pool = await resolve_pool(session, refs)
            current_pool_ids = {r.artist_id for r in pool}

            # Load every artist we might seed from (pool + explicit seeds).
            seed_id_list: list[uuid.UUID] = []
            if not is_lineup and isinstance(scope_param, list):
                seed_id_list = [uuid.UUID(str(s)) for s in scope_param]
            wanted_ids = current_pool_ids | set(seed_id_list)
            artists_by_id: dict[uuid.UUID, music_models.Artist] = {}
            if wanted_ids:
                artists_result = await session.execute(
                    sa.select(music_models.Artist).where(
                        music_models.Artist.id.in_(wanted_ids)
                    )
                )
                artists_by_id = {a.id: a for a in artists_result.scalars().all()}

            if is_lineup:
                # Lineup seeds = the curated core (everything not itself a
                # discovered artist), so a global sweep expands from what the
                # user chose, not from prior discoveries.
                core = [
                    artists_by_id[aid]
                    for aid in current_pool_ids
                    if aid not in discovered_ids and aid in artists_by_id
                ]
                scopes: list[tuple[str, list[music_models.Artist], int]] = [
                    ("lineup", core, requested_n)
                ]
            else:
                scopes = [
                    (
                        str(sid),
                        [artists_by_id[sid]] if sid in artists_by_id else [],
                        requested_n,
                    )
                    for sid in seed_id_list
                ]

            similar_connectors = wctx["connector_registry"].get_by_capability(
                base_module.ConnectorCapability.SIMILAR_ARTISTS
            )
            target_total = requested_n * len(scopes)
            if not similar_connectors:
                task.progress_total = target_total
                task.progress_current = 0
                await lifecycle_module.complete_task(
                    session,
                    task,
                    {
                        "found": 0,
                        "requested": target_total,
                        "message": "no connector connected",
                    },
                )
                await session.commit()
                log.info("enrich_related_artists_no_connector")
                return

            lb_connector = wctx["connector_registry"].get_base_connector(
                types_module.ServiceType.LISTENBRAINZ
            )
            now = datetime.datetime.now(datetime.UTC)

            async def _store_fetch(
                connector: Any, artist: music_models.Artist, limit: int
            ) -> list[dict[str, Any]]:
                return await _fetch_similar_with_store(
                    session, connector, artist, limit=limit, now=now
                )

            # Phase 1: resolve each scope's new artists, persisting similarity
            # edges + any imported artists. Commit so that work survives a later
            # optimistic-version retry on the profile row.
            scope_results: list[tuple[str, list[uuid.UUID]]] = []
            running_pool_ids = set(current_pool_ids)
            total_found = 0
            for scope, seed_artists, target_n in scopes:
                scope_prior = set(pool_module.scope_artist_ids(refs, scope))
                # Exclude everything currently in the pool EXCEPT this scope's
                # own prior discoveries (so they can be re-found on replace), plus
                # the seeds themselves (an artist is never its own neighbor).
                exclude = (running_pool_ids - scope_prior) | {
                    a.id for a in seed_artists
                }
                new_ids = await _collect_related(
                    session,
                    similar_connectors,
                    lb_connector,
                    seed_artists,
                    exclude,
                    target_n,
                    _store_fetch,
                    log,
                )
                scope_results.append((scope, new_ids))
                running_pool_ids = (running_pool_ids - scope_prior) | set(new_ids)
                total_found += len(new_ids)
            await session.commit()

            # Phase 2: apply the discovered artists to the profile under optimistic
            # concurrency. If a concurrent writer (editor PATCH, CLI, agent)
            # advanced the version, reload the fresh input_references and re-apply
            # our scopes onto it -- a merge that never clobbers the other change --
            # then retry.
            for attempt in range(_MAX_VERSION_RETRIES):
                merged: dict[str, object] = dict(profile.input_references)
                for scope, new_ids in scope_results:
                    merged = pool_module.replace_via_seed_sources(
                        merged, scope, new_ids
                    )
                profile.input_references = merged
                task.progress_total = target_total
                task.progress_current = total_found
                await lifecycle_module.complete_task(
                    session,
                    task,
                    {"found": total_found, "requested": target_total},
                )
                try:
                    await session.commit()
                    break
                except orm_exc.StaleDataError:
                    await session.rollback()
                    if attempt == _MAX_VERSION_RETRIES - 1:
                        raise
                    log.warning(
                        "enrich_related_artists_version_conflict", attempt=attempt
                    )
                    # Reload fresh state (input_references + version) so the next
                    # attempt re-applies onto the concurrent writer's result.
                    await session.refresh(profile)
                    await session.refresh(task)
            log.info(
                "enrich_related_artists_completed",
                found=total_found,
                requested=target_total,
            )

        except Exception:
            log.exception("enrich_related_artists_failed")
            if task is not None:
                await lifecycle_module.fail_task(session, task, traceback.format_exc())
                await session.commit()


async def score_and_build_playlist(ctx: dict[str, Any], task_id: str) -> None:
    """Score tracks and build the final playlist.

    Runs after all discovery tasks complete. Queries library data, builds
    CandidateTrack objects, calls the scoring engine, and creates the
    Playlist + PlaylistTrack + GenerationRecord rows.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the TRACK_SCORING Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            import time as time_module

            task = await _load_task(session, task_id)
            if task is None:
                log.error("score_and_build_task_not_found")
                return

            # Check if this task or its parent was cancelled
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("score_and_build_cancelled")
                arq_redis = wctx["redis"]
                await _check_parent_completion(session, task, arq_redis, log)
                return

            generation_start = time_module.monotonic()
            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            profile_id = str(task.params.get("profile_id", ""))
            log = log.bind(profile_id=profile_id)
            log.info("score_and_build_started")

            # Load profile
            profile_result = await session.execute(
                sa.select(generator_models.GeneratorProfile).where(
                    generator_models.GeneratorProfile.id == uuid.UUID(profile_id)
                )
            )
            profile = profile_result.scalar_one_or_none()
            if profile is None:
                await lifecycle_module.fail_task(
                    session, task, f"Profile not found: {profile_id}"
                )
                await session.commit()
                arq_redis = wctx["redis"]
                await _check_parent_completion(session, task, arq_redis, log)
                return

            # event_id retained for logging only; resolve_pool is the shared
            # target-resolution path (legacy {"event_id"} + layered {"sources"}
            # shapes, exclude set applied last) (#128).
            event_id = str(profile.input_references.get("event_id", ""))
            log = log.bind(event_id=event_id)

            pool = await resolve_pool(session, profile.input_references)
            artist_ids: set[uuid.UUID] = {r.artist_id for r in pool}

            params = params_module.apply_defaults(dict(profile.parameter_values))

            # Load the parent PLAYLIST_GENERATION task for its generation-time
            # options (max_tracks, freshness_target).
            parent_params: dict[str, object] = {}
            if task.parent_id is not None:
                parent_result = await session.execute(
                    sa.select(task_module.Task).where(
                        task_module.Task.id == task.parent_id
                    )
                )
                parent = parent_result.scalar_one_or_none()
                if parent is not None:
                    parent_params = parent.params or {}

            # Scoring spans exactly the resolved pool (#133): the concrete artists
            # in input_references, including any the enrich task added. There is no
            # generation-time similar-artist expansion -- discovery happens up front
            # via the enrich task, which persists found artists into the pool.
            query_artist_ids: set[uuid.UUID] = set(artist_ids)

            # Query all tracks by the pool artists.
            tracks_result = await session.execute(
                sa.select(music_models.Track)
                .where(music_models.Track.artist_id.in_(query_artist_ids))
                .options(sa_orm.joinedload(music_models.Track.artist))
            )
            all_tracks = tracks_result.scalars().all()

            # Query listening event counts grouped by track
            listen_counts_result = await session.execute(
                sa.select(
                    music_models.ListeningEvent.track_id,
                    sa.func.count().label("cnt"),
                )
                .where(
                    music_models.ListeningEvent.user_id == task.user_id,
                    music_models.ListeningEvent.track_id.in_(
                        [t.id for t in all_tracks]
                    ),
                )
                .group_by(music_models.ListeningEvent.track_id)
            )
            listen_counts: dict[uuid.UUID, int] = {
                row[0]: row[1] for row in listen_counts_result.all()
            }

            # Query user track relations (likes/loves)
            utr_result = await session.execute(
                sa.select(taste_models.UserTrackRelation).where(
                    taste_models.UserTrackRelation.user_id == task.user_id,
                    taste_models.UserTrackRelation.track_id.in_(
                        [t.id for t in all_tracks]
                    ),
                )
            )
            liked_track_ids: set[uuid.UUID] = {
                r.track_id for r in utr_result.scalars().all()
            }

            # Freshness baseline + in-place target: load the latest GenerationRecord.
            # CRITICAL (#versions): read the baseline from its track_snapshot, NOT the
            # live PlaylistTrack rows. On an in-place regenerate the "previous"
            # playlist IS the row we are about to overwrite, so reading its live rows
            # would compare the new selection against itself and silently break the
            # freshness target. The snapshot is captured at generation time and never
            # mutated, so it is the correct previous-track set. Read here, before any
            # row replacement below.
            prev_gen_result = await session.execute(
                sa.select(generator_models.GenerationRecord)
                .where(
                    generator_models.GenerationRecord.profile_id
                    == uuid.UUID(profile_id)
                )
                .order_by(generator_models.GenerationRecord.created_at.desc())
                .limit(1)
            )
            prev_gen = prev_gen_result.scalar_one_or_none()
            previous_track_ids: set[uuid.UUID] = set()
            if prev_gen is not None:
                if prev_gen.track_snapshot is not None:
                    previous_track_ids = {
                        uuid.UUID(tid) for tid in prev_gen.track_snapshot
                    }
                else:
                    # Backwards-compat: records written before track_snapshot existed
                    # have no snapshot. Fall back to that generation's live rows --
                    # safe because this read happens before any replacement below.
                    prev_pt_result = await session.execute(
                        sa.select(playlist_models.PlaylistTrack.track_id).where(
                            playlist_models.PlaylistTrack.playlist_id
                            == prev_gen.playlist_id
                        )
                    )
                    previous_track_ids = {row[0] for row in prev_pt_result.all()}

            # Recipe-level track exclusions (#track-exclude): an excluded track never
            # becomes a candidate, so its band deals its next-best track on the next
            # round-robin pass and the freed slot refills. pool.py is artist-only /
            # pre-track, so this filter lives here, not in build_pool.
            exclude_track_ids = pool_module.extract_track_excludes(
                profile.input_references
            )

            # Build CandidateTrack objects
            candidates: list[concert_prep_module.CandidateTrack] = []
            for track in all_tracks:
                if track.id in exclude_track_ids:
                    continue
                lc = listen_counts.get(track.id, 0)
                in_library = lc > 0 or track.id in liked_track_ids
                candidates.append(
                    concert_prep_module.CandidateTrack(
                        track_id=track.id,
                        title=track.title,
                        artist_name=track.artist.name,
                        artist_id=track.artist_id,
                        is_target_artist=track.artist_id in artist_ids,
                        listen_count=lc,
                        in_library=in_library,
                        popularity_score=track.popularity_score or 0,
                        source=(
                            types_module.TrackSource.LIBRARY
                            if in_library
                            else types_module.TrackSource.DISCOVERY
                        ),
                    )
                )

            # max_tracks and freshness_target are generation-time options stored
            # on the parent PLAYLIST_GENERATION task (loaded earlier).
            max_tracks = int(str(parent_params.get("max_tracks", 50)))
            freshness_target_raw = parent_params.get("freshness_target")
            freshness_target: int | None = (
                int(str(freshness_target_raw))
                if freshness_target_raw is not None
                else None
            )

            # Score and select
            selection = concert_prep_module.score_and_select(
                candidates=candidates,
                params=params,
                max_tracks=max_tracks,
                previous_track_ids=previous_track_ids,
                freshness_target=freshness_target,
            )

            # Regenerate in place (#versions): reuse the current Playlist row when one
            # exists, so its identity -- and the Spotify export anchor in
            # service_links -- survives the regenerate. Replace its PlaylistTrack rows
            # with the new selection; the prior version's tracks live on in the
            # GenerationRecord.track_snapshot history, not in this row. Only when there
            # is no prior playlist (first generation, or it was deleted) do we create a
            # fresh one.
            existing_playlist: playlist_models.Playlist | None = None
            if prev_gen is not None:
                existing_playlist = await session.get(
                    playlist_models.Playlist, prev_gen.playlist_id
                )

            if existing_playlist is not None:
                playlist = existing_playlist
                # Drop the old rows first (Core delete executes immediately within the
                # transaction, before the new inserts below). Keep id, service_links
                # (Spotify anchor), and is_pinned untouched.
                await session.execute(
                    sa.delete(playlist_models.PlaylistTrack).where(
                        playlist_models.PlaylistTrack.playlist_id == playlist.id
                    )
                )
                playlist.name = str(profile.name)
                playlist.description = f"Generated from profile: {profile.name}"
                playlist.track_count = len(selection.tracks)
            else:
                playlist = playlist_models.Playlist(
                    id=uuid.uuid4(),
                    user_id=task.user_id,
                    name=str(profile.name),
                    description=f"Generated from profile: {profile.name}",
                    track_count=len(selection.tracks),
                )
                session.add(playlist)

            # Create PlaylistTrack rows (fresh rows start unsynced -- a regenerated
            # version has not been exported yet).
            for scored in selection.tracks:
                pt = playlist_models.PlaylistTrack(
                    id=uuid.uuid4(),
                    playlist_id=playlist.id,
                    track_id=scored.track_id,
                    position=scored.position,
                    score=scored.score,
                    source=scored.source,
                )
                session.add(pt)

            # Create GenerationRecord
            generation_duration_ms = int(
                (time_module.monotonic() - generation_start) * 1000
            )
            # Snapshot the resolved pool for reproducibility/audit (#128): sources
            # re-resolve live, so record the exact artists that fed this run, each
            # with its provenance (event/artist).
            pool_snapshot: list[dict[str, str]] = [
                {"artist_id": str(resolved.artist_id), "via": resolved.via.value}
                for resolved in pool
            ]
            gen_record = generator_models.GenerationRecord(
                id=uuid.uuid4(),
                profile_id=uuid.UUID(profile_id),
                playlist_id=playlist.id,
                parameter_snapshot=params,
                freshness_target=freshness_target,
                freshness_actual=selection.freshness_actual,
                track_sources_summary=selection.sources_summary,
                generation_duration_ms=generation_duration_ms,
                pool_snapshot=pool_snapshot,
                # Ordered track ids this generation produced (#versions): the durable
                # history of this version AND the freshness baseline for the next
                # regenerate (read above, never the live rows).
                track_snapshot=[str(scored.track_id) for scored in selection.tracks],
            )
            session.add(gen_record)

            await lifecycle_module.complete_task(
                session,
                task,
                {
                    "playlist_id": str(playlist.id),
                    "tracks_selected": len(selection.tracks),
                    "sources_summary": selection.sources_summary,
                },
            )
            await session.commit()
            log.info(
                "score_and_build_completed",
                playlist_id=str(playlist.id),
                tracks_selected=len(selection.tracks),
            )

        except Exception:
            log.exception("score_and_build_failed")
            task_reload = await _load_task(session, task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(
                    session, task_reload, traceback.format_exc()
                )
                await session.commit()
                task = task_reload

        # Check parent completion
        if task is not None:
            arq_redis = wctx["redis"]
            await _check_parent_completion(session, task, arq_redis, log)


# ---------------------------------------------------------------------------
# Playlist export to external services
# ---------------------------------------------------------------------------

_HTTP_ERROR_MESSAGES: dict[int, str] = {
    401: "Authorization expired — please reconnect your {service} account.",
    403: (
        "Access denied by {service}"
        " — please reconnect your account or check permissions."
    ),
    404: "Resource not found on {service}.",
    429: "Rate limited by {service} — please try again later.",
}


def _friendly_http_error(exc: httpx.HTTPStatusError, service: str) -> str:
    status = exc.response.status_code
    template = _HTTP_ERROR_MESSAGES.get(status)
    if template:
        return template.format(service=service)
    if status >= 500:
        return f"{service} returned a server error ({status}) — please try again later."
    return f"{service} returned an error ({status})."


async def export_playlist(ctx: dict[str, Any], task_id: str) -> None:
    """Export a playlist to an external service (e.g. Spotify).

    Loads the PLAYLIST_EXPORT task, matches playlist tracks to Spotify IDs
    (via service_links or search), then creates or updates the external
    playlist. Persists track matches back to service_links for future reuse.

    Args:
        ctx: arq worker context dict.
        task_id: UUID string of the PLAYLIST_EXPORT Task.
    """
    wctx = typing.cast("WorkerContext", ctx)
    session_factory = wctx["session_factory"]
    connector_registry = wctx["connector_registry"]
    settings = wctx["settings"]
    log = logger.bind(task_id=task_id)

    async with session_factory() as session:
        task: task_module.Task | None = None
        try:
            task = await _load_task(session, task_id)
            if task is None:
                log.error("export_playlist_task_not_found")
                return

            # Check if this task or its parent was cancelled
            if await lifecycle_module.is_cancelled(session, task):
                await lifecycle_module.fail_task(
                    session, task, "Parent task was cancelled"
                )
                await session.commit()
                log.info("export_playlist_cancelled")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            playlist_id_str = str(task.params.get("playlist_id", ""))
            connection_id_str = str(task.params.get("connection_id", ""))
            log = log.bind(playlist_id=playlist_id_str, connection_id=connection_id_str)
            log.info("export_playlist_started")

            # Load playlist with tracks and artist info
            playlist_result = await session.execute(
                sa.select(playlist_models.Playlist)
                .where(playlist_models.Playlist.id == uuid.UUID(playlist_id_str))
                .options(
                    sa_orm.selectinload(playlist_models.Playlist.tracks)
                    .joinedload(playlist_models.PlaylistTrack.track)
                    .joinedload(music_models.Track.artist)
                )
            )
            playlist = playlist_result.scalar_one_or_none()
            if playlist is None:
                await lifecycle_module.fail_task(
                    session, task, f"Playlist not found: {playlist_id_str}"
                )
                await session.commit()
                return

            # Load service connection
            conn_result = await session.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.id == uuid.UUID(connection_id_str)
                )
            )
            connection = conn_result.scalar_one_or_none()
            if connection is None:
                await lifecycle_module.fail_task(
                    session,
                    task,
                    f"Connection not found: {connection_id_str}",
                )
                await session.commit()
                return

            # Get Spotify connector
            connector = connector_registry.get_base_connector(
                types_module.ServiceType.SPOTIFY
            )
            if connector is None or not isinstance(
                connector, spotify_module.SpotifyConnector
            ):
                await lifecycle_module.fail_task(
                    session, task, "Spotify connector not available"
                )
                await session.commit()
                return

            # Decrypt access token
            assert connection.encrypted_access_token is not None, (
                "Spotify connection requires an access token"
            )
            access_token = crypto_module.decrypt_token(
                connection.encrypted_access_token,
                settings.token_encryption_key,
            )

            # Refresh token if expired
            if (
                connection.token_expires_at is not None
                and connection.token_expires_at <= datetime.datetime.now(datetime.UTC)
                and connection.encrypted_refresh_token is not None
            ):
                refresh_token = crypto_module.decrypt_token(
                    connection.encrypted_refresh_token,
                    settings.token_encryption_key,
                )
                token_response = await connector.refresh_access_token(refresh_token)
                access_token = token_response.access_token
                connection.encrypted_access_token = crypto_module.encrypt_token(
                    access_token, settings.token_encryption_key
                )
                if token_response.expires_in is not None:
                    connection.token_expires_at = datetime.datetime.now(
                        datetime.UTC
                    ) + datetime.timedelta(seconds=token_response.expires_in)
                await session.commit()
                log.info("export_playlist_token_refreshed")

            # Get ListenBrainz connector for MB URL relations lookup
            lb_raw = connector_registry.get_base_connector(
                types_module.ServiceType.LISTENBRAINZ
            )
            lb_connector = (
                lb_raw
                if isinstance(lb_raw, listenbrainz_module.ListenBrainzConnector)
                else None
            )

            # Match tracks to Spotify IDs (two-pass: MB URL relations, then text search)
            spotify_uris: list[str] = []
            skipped_tracks: list[str] = []
            mb_matched = 0
            search_matched = 0

            for pt in playlist.tracks:
                track = pt.track
                track_links = track.service_links or {}
                spotify_id = track_links.get("spotify")

                if spotify_id is None and lb_connector is not None:
                    recording_mbid = artist_utils.get_mbid(track_links)
                    if recording_mbid:
                        found_id = await lb_connector.get_recording_spotify_id(
                            recording_mbid
                        )
                        if found_id is not None:
                            updated_links = dict(track_links)
                            updated_links["spotify"] = found_id
                            track.service_links = updated_links
                            spotify_id = found_id
                            mb_matched += 1

                if spotify_id is None:
                    artist_name = track.artist.name if track.artist else ""
                    found_spotify_id = await connector.search_track(
                        access_token, track.title, artist_name
                    )
                    if found_spotify_id is not None:
                        updated_links = dict(track_links)
                        updated_links["spotify"] = found_spotify_id
                        track.service_links = updated_links
                        spotify_id = found_spotify_id
                        search_matched += 1
                    else:
                        skipped_tracks.append(track.title)
                        continue

                spotify_uris.append(f"spotify:track:{spotify_id}")

            log.info(
                "export_track_matching_summary",
                mb_matched=mb_matched,
                search_matched=search_matched,
                cached=len(spotify_uris) - mb_matched - search_matched,
                skipped=len(skipped_tracks),
            )

            if not spotify_uris:
                await lifecycle_module.fail_task(
                    session,
                    task,
                    "No tracks could be matched to Spotify",
                )
                await session.commit()
                return

            # Check if playlist was already exported to this connection
            playlist_links = playlist.service_links or {}
            spotify_links = playlist_links.get("spotify", {})
            existing_export = (
                spotify_links.get(connection_id_str)
                if isinstance(spotify_links, dict)
                else None
            )

            if existing_export and isinstance(existing_export, dict):
                # Update existing playlist
                spotify_playlist_id = str(existing_export["playlist_id"])
                await connector.replace_playlist_tracks(
                    access_token, spotify_playlist_id, spotify_uris
                )
                log.info(
                    "export_playlist_replaced",
                    spotify_playlist_id=spotify_playlist_id,
                )
            else:
                # Create new playlist
                spotify_playlist_id = await connector.create_playlist(
                    access_token,
                    playlist.name,
                    playlist.description or "",
                )
                await connector.add_tracks_to_playlist(
                    access_token, spotify_playlist_id, spotify_uris
                )
                log.info(
                    "export_playlist_created",
                    spotify_playlist_id=spotify_playlist_id,
                )

            # Update playlist service_links (copy-on-write)
            updated_playlist_links = dict(playlist_links)
            spotify_section = dict(
                updated_playlist_links.get("spotify", {})
                if isinstance(updated_playlist_links.get("spotify"), dict)
                else {}
            )
            spotify_section[connection_id_str] = {
                "playlist_id": spotify_playlist_id,
                "exported_at": datetime.datetime.now(datetime.UTC).isoformat(),
            }
            updated_playlist_links["spotify"] = spotify_section
            playlist.service_links = updated_playlist_links

            await lifecycle_module.complete_task(
                session,
                task,
                {
                    "spotify_playlist_id": spotify_playlist_id,
                    "exported": len(spotify_uris),
                    "skipped": len(skipped_tracks),
                    "skipped_tracks": skipped_tracks,
                },
            )
            await session.commit()
            log.info(
                "export_playlist_completed",
                spotify_playlist_id=spotify_playlist_id,
                exported=len(spotify_uris),
                skipped=len(skipped_tracks),
            )

        except httpx.HTTPStatusError as exc:
            log.exception("export_playlist_failed")
            user_msg = _friendly_http_error(exc, "Spotify")
            task_reload = await _load_task(session, task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(session, task_reload, user_msg)
                await session.commit()
        except Exception:
            log.exception("export_playlist_failed")
            task_reload = await _load_task(session, task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(
                    session,
                    task_reload,
                    "An unexpected error occurred during export. "
                    "Check the worker logs for details.",
                )
                await session.commit()


# ---------------------------------------------------------------------------
# Parent completion check
# ---------------------------------------------------------------------------


async def _check_parent_completion(
    session: sa_async.AsyncSession,
    task: task_module.Task,
    arq_redis: arq.ArqRedis,
    log: structlog.stdlib.BoundLogger,
) -> None:
    """Check sibling tasks; enqueue next pending sibling or mark parent done.

    After a child task completes, this function checks if there are pending
    siblings to enqueue (sequential execution for rate-limit-sensitive
    services like Spotify) or if all children are done (cascade completion
    to parent).

    Args:
        session: Active database session.
        task: The child task that just completed.
        arq_redis: arq Redis pool for enqueuing jobs.
        log: Bound structured logger.
    """
    if task.parent_id is None:
        return

    # Count siblings (including self) that are NOT in a terminal state
    pending_count_result = await session.execute(
        sa.select(sa.func.count()).where(
            task_module.Task.parent_id == task.parent_id,
            task_module.Task.status.notin_(
                [types_module.SyncStatus.COMPLETED, types_module.SyncStatus.FAILED]
            ),
        )
    )
    pending_count: int = pending_count_result.scalar_one()

    if pending_count > 0:
        # Enqueue the next PENDING sibling (sequential execution)
        next_pending_result = await session.execute(
            sa.select(task_module.Task)
            .where(
                task_module.Task.parent_id == task.parent_id,
                task_module.Task.status == types_module.SyncStatus.PENDING,
            )
            .order_by(task_module.Task.created_at)
            .limit(1)
        )
        next_pending = next_pending_result.scalar_one_or_none()
        if next_pending is not None:
            dispatch = _TASK_DISPATCH.get(next_pending.task_type)
            if dispatch is not None:
                job_name, args_builder = dispatch
                args = args_builder(next_pending)
                await arq_redis.enqueue_job(
                    job_name,
                    *args,
                    _job_id=f"{job_name}:{next_pending.id}",
                )
            else:
                # Fallback for unknown task types
                await arq_redis.enqueue_job(
                    "sync_range",
                    str(next_pending.id),
                    _job_id=f"sync_range:{next_pending.id}",
                )
            log.info(
                "next_sibling_enqueued",
                next_task_id=str(next_pending.id),
                remaining=pending_count,
            )
        else:
            log.info("parent_still_pending", pending_children=pending_count)
        return

    # All children are done — load parent (with connection) and aggregate results
    parent_result = await session.execute(
        sa.select(task_module.Task)
        .where(task_module.Task.id == task.parent_id)
        .options(sa_orm.joinedload(task_module.Task.service_connection))
    )
    parent = parent_result.scalar_one_or_none()
    if parent is None:
        log.error("parent_task_not_found", parent_id=str(task.parent_id))
        return

    # Check if any children failed
    failed_count_result = await session.execute(
        sa.select(sa.func.count()).where(
            task_module.Task.parent_id == task.parent_id,
            task_module.Task.status == types_module.SyncStatus.FAILED,
        )
    )
    failed_count: int = failed_count_result.scalar_one()

    # Aggregate results from all children
    children_result = await session.execute(
        sa.select(task_module.Task).where(task_module.Task.parent_id == task.parent_id)
    )
    children = children_result.scalars().all()

    base_result: dict[str, object] = {
        "children_completed": len(children) - failed_count,
        "children_failed": failed_count,
    }

    if parent.task_type == types_module.TaskType.PLAYLIST_GENERATION:
        total_tracks_found = 0
        playlist_id: str | None = None
        tracks_selected = 0
        sources_summary: dict[str, object] = {}
        for child in children:
            child_result = child.result or {}
            total_tracks_found += int(str(child_result.get("tracks_found", 0)))
            if child_result.get("playlist_id"):
                playlist_id = str(child_result["playlist_id"])
                tracks_selected = int(str(child_result.get("tracks_selected", 0)))
                raw = child_result.get("sources_summary", {})
                sources_summary = dict(raw) if isinstance(raw, dict) else {}
        base_result["tracks_found"] = total_tracks_found
        if playlist_id is not None:
            base_result["playlist_id"] = playlist_id
            base_result["tracks_selected"] = tracks_selected
            base_result["sources_summary"] = sources_summary
    elif parent.task_type == types_module.TaskType.CONCERT_ARCHIVES_IMPORT:
        totals: dict[str, int] = {
            "events_created": 0,
            "events_updated": 0,
            "candidates_created": 0,
            "candidates_matched": 0,
            "total_events": 0,
        }
        for child in children:
            child_result = child.result or {}
            for key in totals:
                totals[key] += int(str(child_result.get(key, 0)))
        base_result.update(totals)
        parent_params = parent.params or {}
        if parent_params.get("warnings"):
            base_result["warnings"] = parent_params["warnings"]
    else:
        total_created = 0
        total_updated = 0
        for child in children:
            child_result = child.result or {}
            total_created += int(str(child_result.get("items_created", 0)))
            total_updated += int(str(child_result.get("items_updated", 0)))
        base_result["items_created"] = total_created
        base_result["items_updated"] = total_updated

    parent.result = base_result

    if failed_count > 0:
        parent.status = types_module.SyncStatus.FAILED
        parent.error_message = f"{failed_count} child task(s) failed"
    else:
        parent.status = types_module.SyncStatus.COMPLETED

    parent.completed_at = datetime.datetime.now(datetime.UTC)

    # Update the connection's last_synced_at timestamp.
    # The service_connection relationship is eagerly loaded by sync_range callers.
    if parent.service_connection is not None:
        parent.service_connection.last_synced_at = datetime.datetime.now(datetime.UTC)

    await session.commit()
    log.info(
        "parent_completed",
        parent_id=str(parent.id),
        status=parent.status.value,
        result=parent.result,
    )

    # After a successful sync, run cross-service event dedup
    if parent.status == types_module.SyncStatus.COMPLETED and parent.task_type in (
        types_module.TaskType.SYNC_JOB,
        types_module.TaskType.CALENDAR_SYNC,
        types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
    ):
        dedup_task = task_module.Task(
            task_type=types_module.TaskType.BULK_JOB,
            status=types_module.SyncStatus.PENDING,
            params={"operation": "dedup_all"},
            description="Post-sync entity resolution",
        )
        session.add(dedup_task)
        await session.commit()
        await arq_redis.enqueue_job(
            "run_bulk_job",
            str(dedup_task.id),
            _job_id=f"bulk:{dedup_task.id}",
        )
        log.info("post_sync_dedup_enqueued", task_id=str(dedup_task.id))

    # After a music sync, reconcile unmatched concert event artists
    if (
        parent.status == types_module.SyncStatus.COMPLETED
        and parent.task_type == types_module.TaskType.SYNC_JOB
    ):
        reconcile_task = task_module.Task(
            task_type=types_module.TaskType.BULK_JOB,
            status=types_module.SyncStatus.PENDING,
            params={"operation": "reconcile_event_artists"},
            description="Post-sync event artist reconciliation",
        )
        session.add(reconcile_task)
        await session.commit()
        await arq_redis.enqueue_job(
            "run_bulk_job",
            str(reconcile_task.id),
            _job_id=f"bulk:{reconcile_task.id}",
        )
        log.info(
            "post_sync_reconcile_enqueued",
            task_id=str(reconcile_task.id),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _load_task(
    session: sa_async.AsyncSession, sync_task_id: str
) -> task_module.Task | None:
    """Load a Task by ID.

    Args:
        session: Active database session.
        sync_task_id: UUID string of the task.

    Returns:
        The Task, or None if not found.
    """
    result = await session.execute(
        sa.select(task_module.Task).where(
            task_module.Task.id == uuid.UUID(sync_task_id)
        )
    )
    return result.scalar_one_or_none()


def _apply_watermark_resume(
    task: task_module.Task,
    connection: user_models.ServiceConnection,
) -> None:
    """Inject watermark position into task params for crash recovery.

    For ListenBrainz TIME_RANGE tasks, reads ``oldest_synced_at`` from
    the connection's sync watermark and sets it as ``max_ts`` so the
    task resumes from where the previous run left off instead of
    re-processing all pages.

    Args:
        task: The orphaned task being re-enqueued.
        connection: The task's ServiceConnection with current watermark.
    """
    if connection.service_type != types_module.ServiceType.LISTENBRAINZ:
        return

    listens_watermark = connection.sync_watermark.get("listens", {})
    oldest_synced_at = listens_watermark.get("oldest_synced_at")
    if oldest_synced_at is not None:
        items_so_far = int(task.progress_current or 0)
        task.params = {
            **task.params,
            "max_ts": int(str(oldest_synced_at)),
            "items_so_far": items_so_far,
        }
        logger.info(
            "watermark_resume_applied",
            task_id=str(task.id),
            max_ts=oldest_synced_at,
            items_so_far=items_so_far,
        )


async def _reenqueue_orphaned_tasks(
    session_factory: sa_async.async_sessionmaker[sa_async.AsyncSession],
    arq_redis: arq.ArqRedis,
) -> None:
    """Re-enqueue orphaned tasks on worker startup.

    Finds tasks stuck in PENDING, expired DEFERRED, or RUNNING status
    (from crashes or ungraceful shutdowns) and re-enqueues them. arq jobs
    in Redis expire after ~1 day, so if the worker was down during that
    window the Task row remains but the arq job is gone.
    """
    async with session_factory() as session:
        now = datetime.datetime.now(datetime.UTC)

        # Find PENDING tasks (orphaned — their arq job likely expired).
        # Exclude children whose parent already completed or failed —
        # those are stale leftovers, not actionable orphans.
        parent_alias = sa.orm.aliased(task_module.Task)
        pending_result = await session.execute(
            sa.select(task_module.Task)
            .outerjoin(
                parent_alias,
                task_module.Task.parent_id == parent_alias.id,
            )
            .where(
                task_module.Task.status == types_module.SyncStatus.PENDING,
                sa.or_(
                    task_module.Task.parent_id.is_(None),
                    parent_alias.status.notin_(
                        [
                            types_module.SyncStatus.COMPLETED,
                            types_module.SyncStatus.FAILED,
                        ]
                    ),
                ),
            )
        )
        pending_tasks = list(pending_result.scalars().all())

        # Find DEFERRED tasks whose deferred_until has passed
        deferred_result = await session.execute(
            sa.select(task_module.Task).where(
                task_module.Task.status == types_module.SyncStatus.DEFERRED,
                sa.or_(
                    task_module.Task.deferred_until <= now,
                    task_module.Task.deferred_until.is_(None),
                ),
            )
        )
        deferred_tasks = list(deferred_result.scalars().all())

        # Mark stale children of terminal parents as FAILED
        stale_result = await session.execute(
            sa.select(task_module.Task)
            .join(
                parent_alias,
                task_module.Task.parent_id == parent_alias.id,
            )
            .where(
                task_module.Task.status == types_module.SyncStatus.PENDING,
                parent_alias.status.in_(
                    [
                        types_module.SyncStatus.COMPLETED,
                        types_module.SyncStatus.FAILED,
                    ]
                ),
            )
        )
        stale_tasks = list(stale_result.scalars().all())
        for task in stale_tasks:
            task.status = types_module.SyncStatus.FAILED
            task.error_message = "Parent task already terminal"
            task.completed_at = now
        if stale_tasks:
            await session.commit()
            logger.info("cleaned_stale_orphans", count=len(stale_tasks))

        # Find RUNNING tasks (interrupted by crash/restart)
        running_result = await session.execute(
            sa.select(task_module.Task)
            .outerjoin(
                parent_alias,
                task_module.Task.parent_id == parent_alias.id,
            )
            .where(
                task_module.Task.status == types_module.SyncStatus.RUNNING,
                sa.or_(
                    task_module.Task.parent_id.is_(None),
                    parent_alias.status.notin_(
                        [
                            types_module.SyncStatus.COMPLETED,
                            types_module.SyncStatus.FAILED,
                        ]
                    ),
                ),
            )
        )
        running_tasks = list(running_result.scalars().all())

        # Reset deferred tasks back to PENDING before re-enqueueing
        for task in deferred_tasks:
            task.status = types_module.SyncStatus.PENDING
        if deferred_tasks:
            await session.commit()

        # Reset RUNNING tasks back to PENDING, with watermark resume.
        # Preserve started_at so the UI shows continuous elapsed time.
        for task in running_tasks:
            task.status = types_module.SyncStatus.PENDING

            # Attempt watermark-based resume for TIME_RANGE tasks
            if task.task_type == types_module.TaskType.TIME_RANGE:
                conn_result = await session.execute(
                    sa.select(user_models.ServiceConnection).where(
                        user_models.ServiceConnection.id == task.service_connection_id
                    )
                )
                connection = conn_result.scalar_one_or_none()
                if connection is not None:
                    _apply_watermark_resume(task, connection)

        if running_tasks:
            await session.commit()
            logger.info("reset_running_orphans", count=len(running_tasks))

        all_tasks = pending_tasks + deferred_tasks + running_tasks
        if not all_tasks:
            return

        enqueued = 0
        for task in all_tasks:
            if task.task_type in (
                types_module.TaskType.SYNC_JOB,
                types_module.TaskType.CONCERT_ARCHIVES_IMPORT,
            ):
                # Skip parent tasks that already have children — re-planning
                # would create duplicate child tasks.
                children_count_result = await session.execute(
                    sa.select(sa.func.count()).where(
                        task_module.Task.parent_id == task.id,
                    )
                )
                if children_count_result.scalar_one() > 0:
                    logger.info(
                        "skipped_parent_with_children",
                        task_id=str(task.id),
                        task_type=task.task_type.value,
                    )
                    continue

            dispatch = _TASK_DISPATCH.get(task.task_type)
            if dispatch is None:
                logger.warning(
                    "no_dispatch_for_task_type",
                    task_id=str(task.id),
                    task_type=task.task_type.value,
                )
                continue

            job_name, args_builder = dispatch
            args = args_builder(task)
            await arq_redis.enqueue_job(
                job_name, *args, _job_id=f"{job_name}:{task.id}"
            )
            enqueued += 1

        if enqueued:
            logger.info("reenqueued_orphaned_tasks", count=enqueued)


# ---------------------------------------------------------------------------
# arq startup / shutdown hooks
# ---------------------------------------------------------------------------


async def startup(ctx: dict[str, Any]) -> None:
    """Initialize database engine, session factory, and connector registry.

    Called by arq when the worker process starts. Stores shared resources
    in the worker context dict for use by task functions.

    Args:
        ctx: arq worker context dict.
    """
    wctx = typing.cast("WorkerContext", ctx)
    settings = config_module.Settings()
    logging_module.configure_logging(settings.log_level)

    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)

    # Fail fast if the DB schema is behind this image's migrations, rather than
    # running new code against an old schema (see resonance.migrations).
    await migrations_module.assert_schema_current(engine)

    connector_registry = registry_module.ConnectorRegistry()
    connector_registry.register(spotify_module.SpotifyConnector(settings=settings))
    connector_registry.register(
        listenbrainz_module.ListenBrainzConnector(settings=settings)
    )
    connector_registry.register(lastfm_module.LastFmConnector(settings=settings))
    connector_registry.register(test_connector_module.TestConnector())
    connector_registry.register(songkick_module.SongkickConnector())
    connector_registry.register(ical_module.ICalConnector())
    connector_registry.register(concert_archives_module.ConcertArchivesConnector())

    wctx["settings"] = settings
    wctx["engine"] = engine
    wctx["session_factory"] = session_factory
    wctx["connector_registry"] = connector_registry
    wctx["strategies"] = {
        types_module.ServiceType.SPOTIFY: spotify_sync.SpotifySyncStrategy(
            token_encryption_key=settings.token_encryption_key
        ),
        types_module.ServiceType.LISTENBRAINZ: lb_sync.ListenBrainzSyncStrategy(),
        types_module.ServiceType.LASTFM: lastfm_sync.LastFmSyncStrategy(
            token_encryption_key=settings.token_encryption_key
        ),
        types_module.ServiceType.TEST: test_sync.TestSyncStrategy(),
    }

    # Register this worker and clean up stale locks from dead workers
    await heartbeat_module.register_worker(wctx["redis"])
    ctx["_idle_heartbeat"] = heartbeat_module.start_idle_heartbeat(wctx["redis"])
    cleaned = await heartbeat_module.cleanup_stale_locks(wctx["redis"])
    if cleaned:
        logger.info("startup_cleaned_stale_locks", count=cleaned)

    # Re-enqueue orphaned tasks that lost their arq jobs
    await _reenqueue_orphaned_tasks(session_factory, wctx["redis"])

    logger.info("worker_started")


async def shutdown(ctx: dict[str, Any]) -> None:
    """Signal graceful shutdown, then dispose of resources.

    Called by arq when the worker process shuts down. Sets the
    shutdown_requested event so in-flight sync tasks can checkpoint
    their progress before the process exits.

    Args:
        ctx: arq worker context dict.
    """
    sync_base.shutdown_requested.set()

    wctx = typing.cast("WorkerContext", ctx)

    # Cancel idle heartbeat and unregister from worker registry
    idle_heartbeat: asyncio.Task[None] | None = ctx.get("_idle_heartbeat")
    if idle_heartbeat is not None:
        idle_heartbeat.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await idle_heartbeat

    await heartbeat_module.unregister_worker(wctx["redis"])

    engine = wctx["engine"]
    await engine.dispose()
    logger.info("worker_shutdown")


# ---------------------------------------------------------------------------
# arq WorkerSettings
# ---------------------------------------------------------------------------


class WorkerSettings:
    """arq worker configuration.

    arq discovers this class by convention. It defines the task functions,
    lifecycle hooks, concurrency limits, and Redis connection settings.
    """

    functions: typing.ClassVar[list[typing.Any]] = [
        arq.func(heartbeat_module.with_heartbeat(plan_sync), timeout=3600),
        arq.func(heartbeat_module.with_heartbeat(sync_range), timeout=3600),
        arq.func(heartbeat_module.with_heartbeat(run_bulk_job), timeout=3600),
        arq.func(
            heartbeat_module.with_heartbeat(concert_worker.sync_calendar_feed),
            timeout=3600,
        ),
        arq.func(
            heartbeat_module.with_heartbeat(concert_worker.sync_concert_archives),
            timeout=3600,
        ),
        arq.func(
            heartbeat_module.with_heartbeat(concert_worker.sync_concert_archives_chunk),
            timeout=600,
        ),
        arq.func(heartbeat_module.with_heartbeat(generate_playlist), timeout=3600),
        arq.func(
            heartbeat_module.with_heartbeat(discover_tracks_for_artist), timeout=600
        ),
        arq.func(
            heartbeat_module.with_heartbeat(score_and_build_playlist), timeout=600
        ),
        arq.func(heartbeat_module.with_heartbeat(export_playlist), timeout=600),
        arq.func(heartbeat_module.with_heartbeat(backfill_mbids), timeout=3600),
        arq.func(heartbeat_module.with_heartbeat(backfill_popularity), timeout=3600),
        arq.func(heartbeat_module.with_heartbeat(enrich_related_artists), timeout=3600),
    ]
    on_startup = startup
    on_shutdown = shutdown
    max_jobs = 10
    job_timeout = 300  # default for future leaf tasks (e.g., page_fetch)
    # arq reads redis_settings as a class attribute (not a method call).
    # Settings() reads env vars, which are available at import time in K8s.
    _cfg = config_module.Settings()
    redis_settings = arq_connections.RedisSettings(
        host=_cfg.redis_host,
        port=_cfg.redis_port,
        password=_cfg.redis_password or None,
    )


def main() -> None:
    """Run the arq worker.

    Python 3.14 removed the implicit event loop from
    asyncio.get_event_loop(), which arq 0.27 calls in Worker.__init__.
    This entrypoint creates a loop first so the Worker can find it.
    """
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    arq.run_worker(WorkerSettings)  # type: ignore[arg-type]


if __name__ == "__main__":
    main()
