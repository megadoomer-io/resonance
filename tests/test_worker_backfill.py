"""Tests for the MBID backfill worker task (#71 T4)."""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import resonance.config as config_module
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.connectors.spotify as spotify_module
import resonance.sync.backfill as backfill_module
import resonance.types as types_module
import resonance.worker as worker_module


def _mock_session_factory(session: AsyncMock) -> MagicMock:
    ctx_manager = AsyncMock()
    ctx_manager.__aenter__.return_value = session
    ctx_manager.__aexit__.return_value = False
    factory = MagicMock()
    factory.return_value = ctx_manager
    return factory


def _ctx(session: AsyncMock) -> dict[str, Any]:
    registry = MagicMock()
    registry.get_base_connector.return_value = MagicMock(
        spec=listenbrainz_module.ListenBrainzConnector
    )
    return {
        "session_factory": _mock_session_factory(session),
        "connector_registry": registry,
        "settings": config_module.Settings(),
    }


def _task(params: dict[str, Any] | None = None) -> MagicMock:
    task = MagicMock()
    task.id = uuid.uuid4()
    task.params = params or {}
    return task


@pytest.mark.asyncio
async def test_runs_both_passes_and_completes() -> None:
    session = AsyncMock()
    task = _task()
    with (
        patch.object(worker_module, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            worker_module.lifecycle_module,
            "is_cancelled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            worker_module.backfill_module,
            "run_mbid_backfill",
            AsyncMock(
                return_value={
                    "track": backfill_module.BackfillCounts(matched=2, no_match=1),
                    "artist": backfill_module.BackfillCounts(matched=1),
                }
            ),
        ) as run_mock,
    ):
        await worker_module.backfill_mbids(_ctx(session), str(task.id))

    # both passes requested by default
    _, kwargs = run_mock.call_args
    assert kwargs["do_tracks"] is True
    assert kwargs["do_artists"] is True
    # result recorded by complete_task (sets status + result on the task)
    assert task.status == types_module.SyncStatus.COMPLETED
    assert task.result["track"]["matched"] == 2
    assert task.result["artist"]["matched"] == 1


@pytest.mark.asyncio
async def test_entity_types_param_limits_passes() -> None:
    session = AsyncMock()
    task = _task({"entity_types": ["track"]})
    with (
        patch.object(worker_module, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            worker_module.lifecycle_module,
            "is_cancelled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            worker_module.backfill_module,
            "run_mbid_backfill",
            AsyncMock(return_value={"track": backfill_module.BackfillCounts()}),
        ) as run_mock,
    ):
        await worker_module.backfill_mbids(_ctx(session), str(task.id))

    _, kwargs = run_mock.call_args
    assert kwargs["do_tracks"] is True
    assert kwargs["do_artists"] is False


@pytest.mark.asyncio
async def test_retry_clears_prior_markers() -> None:
    session = AsyncMock()
    task = _task({"retry": True})
    with (
        patch.object(worker_module, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            worker_module.lifecycle_module,
            "is_cancelled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            worker_module.backfill_module,
            "run_mbid_backfill",
            AsyncMock(return_value={"track": backfill_module.BackfillCounts()}),
        ),
    ):
        await worker_module.backfill_mbids(_ctx(session), str(task.id))

    # retry issues UPDATE statements (one per model) to clear markers
    assert session.execute.await_count >= 2


@pytest.mark.asyncio
async def test_missing_connector_fails_task() -> None:
    session = AsyncMock()
    task = _task()
    ctx = _ctx(session)
    ctx["connector_registry"].get_base_connector.return_value = None  # not registered
    with (
        patch.object(worker_module, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            worker_module.lifecycle_module,
            "is_cancelled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            worker_module.backfill_module, "run_mbid_backfill", AsyncMock()
        ) as run_mock,
    ):
        await worker_module.backfill_mbids(ctx, str(task.id))

    run_mock.assert_not_awaited()
    assert task.status == types_module.SyncStatus.FAILED


def _spotify_ctx(session: AsyncMock) -> dict[str, Any]:
    registry = MagicMock()
    registry.get_base_connector.return_value = MagicMock(
        spec=spotify_module.SpotifyConnector
    )
    return {
        "session_factory": _mock_session_factory(session),
        "connector_registry": registry,
        "settings": config_module.Settings(),
    }


@pytest.mark.asyncio
async def test_popularity_backfill_runs_and_completes() -> None:
    session = AsyncMock()
    task = _task()
    with (
        patch.object(worker_module, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            worker_module.lifecycle_module,
            "is_cancelled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            worker_module,
            "_get_spotify_access_token",
            AsyncMock(return_value="tok"),
        ),
        patch.object(
            worker_module.backfill_module,
            "run_popularity_backfill",
            AsyncMock(
                return_value=backfill_module.PopularityBackfillCounts(
                    candidates=3, updated=2, no_popularity=1
                )
            ),
        ) as run_mock,
    ):
        await worker_module.backfill_popularity(_spotify_ctx(session), str(task.id))

    run_mock.assert_awaited_once()
    assert task.status == types_module.SyncStatus.COMPLETED
    assert task.result["updated"] == 2
    assert task.result["no_popularity"] == 1


@pytest.mark.asyncio
async def test_popularity_backfill_missing_connector_fails_task() -> None:
    session = AsyncMock()
    task = _task()
    ctx = _spotify_ctx(session)
    ctx["connector_registry"].get_base_connector.return_value = None
    with (
        patch.object(worker_module, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            worker_module.lifecycle_module,
            "is_cancelled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            worker_module.backfill_module,
            "run_popularity_backfill",
            AsyncMock(),
        ) as run_mock,
    ):
        await worker_module.backfill_popularity(ctx, str(task.id))

    run_mock.assert_not_awaited()
    assert task.status == types_module.SyncStatus.FAILED


@pytest.mark.asyncio
async def test_popularity_backfill_no_connection_fails_task() -> None:
    session = AsyncMock()
    task = _task()
    with (
        patch.object(worker_module, "_load_task", AsyncMock(return_value=task)),
        patch.object(
            worker_module.lifecycle_module,
            "is_cancelled",
            AsyncMock(return_value=False),
        ),
        patch.object(
            worker_module,
            "_get_spotify_access_token",
            AsyncMock(return_value=None),
        ),
        patch.object(
            worker_module.backfill_module,
            "run_popularity_backfill",
            AsyncMock(),
        ) as run_mock,
    ):
        await worker_module.backfill_popularity(_spotify_ctx(session), str(task.id))

    run_mock.assert_not_awaited()
    assert task.status == types_module.SyncStatus.FAILED
