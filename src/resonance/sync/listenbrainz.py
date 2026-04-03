"""ListenBrainz sync strategy implementation."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

import httpx
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as sa_async
import structlog

import resonance.connectors.base as connector_base
import resonance.connectors.listenbrainz as listenbrainz_module
import resonance.models.task as task_module
import resonance.sync.base as sync_base
import resonance.sync.runner as runner_module
import resonance.types as types_module

if TYPE_CHECKING:
    import uuid

    import resonance.models.user as user_models

logger = structlog.get_logger()


class ListenBrainzSyncStrategy(sync_base.SyncStrategy):
    """Sync strategy for ListenBrainz listening history."""

    concurrency = "parallel"

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: connector_base.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        """Plan a ListenBrainz sync by creating a single TIME_RANGE task.

        Checks for a watermark from the most recent completed sync to enable
        incremental sync. Fetches listen count for progress tracking.

        Args:
            session: Active database session.
            connection: The user's ListenBrainz service connection.
            connector: The ListenBrainz connector instance.

        Returns:
            A single-element list with a TIME_RANGE task descriptor.
        """
        lb_connector = _cast_connector(connector)
        username = connection.external_user_id

        # Get listen count for progress tracking
        progress_total: int | None = None
        try:
            progress_total = await lb_connector.get_listen_count(username)
        except httpx.HTTPError, connector_base.RateLimitExceededError:
            logger.warning("could_not_fetch_listen_count", username=username)

        # Check for watermark (incremental sync)
        watermark = await _get_watermark(session, connection.id)

        if watermark is not None:
            listened_at_dt = datetime.datetime.fromtimestamp(watermark, tz=datetime.UTC)
            date_str = listened_at_dt.date().isoformat()
            description = f"Syncing new listens since {date_str}"
        else:
            description = "Syncing listening history"

        return [
            sync_base.SyncTaskDescriptor(
                task_type=types_module.SyncTaskType.TIME_RANGE,
                params={"username": username, "min_ts": watermark},
                progress_total=progress_total,
                description=description,
            )
        ]

    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: connector_base.BaseConnector,
    ) -> dict[str, object]:
        """Paginate through ListenBrainz listens and upsert into the database.

        Fetches listens page by page using max_ts/min_ts pagination. Upserts
        artists, tracks, and listening events for each listen. Commits per
        page and updates task progress.

        Args:
            session: Active database session.
            task: The TIME_RANGE task being executed.
            connector: The ListenBrainz connector instance.

        Returns:
            Dict with items_created count and last_listened_at timestamp.

        Raises:
            DeferRequest: When a RateLimitExceededError is encountered.
        """
        lb_connector = _cast_connector(connector)
        username: str = str(task.params.get("username", ""))
        min_ts_param = task.params.get("min_ts")
        min_ts: int | None = (
            int(str(min_ts_param)) if min_ts_param is not None else None
        )
        max_ts_param = task.params.get("max_ts")
        max_ts: int | None = (
            int(str(max_ts_param)) if max_ts_param is not None else None
        )
        items_created = int(str(task.params.get("items_so_far", 0)))
        # Preserve watermark across deferral/resume cycles
        last_listened_at_param = task.params.get("last_listened_at")
        last_listened_at: int | None = (
            int(str(last_listened_at_param))
            if last_listened_at_param is not None
            else None
        )

        while True:
            try:
                listens = await lb_connector.get_listens(
                    username, max_ts=max_ts, min_ts=min_ts, count=100
                )
            except connector_base.RateLimitExceededError as exc:
                raise sync_base.DeferRequest(
                    retry_after=exc.retry_after,
                    resume_params={
                        "max_ts": max_ts,
                        "items_so_far": items_created,
                        "last_listened_at": last_listened_at,
                    },
                ) from exc

            if not listens:
                break

            # Track last_listened_at from the first page's first listen
            if last_listened_at is None:
                last_listened_at = listens[0].listened_at

            for listen in listens:
                with session.no_autoflush:
                    await runner_module._upsert_artist_from_track(session, listen.track)
                    await session.flush()
                    await runner_module._upsert_track(session, listen.track)
                    await session.flush()
                    played_at = datetime.datetime.fromtimestamp(
                        listen.listened_at, tz=datetime.UTC
                    ).isoformat()
                    await runner_module._upsert_listening_event(
                        session, task.user_id, listen.track, played_at
                    )
                items_created += 1

            # Use the oldest listen's timestamp for next page
            max_ts = listens[-1].listened_at
            task.progress_current = items_created
            await session.commit()

        result: dict[str, object] = {"items_created": items_created}
        if last_listened_at is not None:
            result["last_listened_at"] = last_listened_at
        return result


def _cast_connector(
    connector: connector_base.BaseConnector,
) -> listenbrainz_module.ListenBrainzConnector:
    """Cast a BaseConnector to ListenBrainzConnector with a runtime check.

    Args:
        connector: The connector to cast.

    Returns:
        The connector as a ListenBrainzConnector.

    Raises:
        TypeError: If the connector is not a ListenBrainzConnector.
    """
    if not isinstance(connector, listenbrainz_module.ListenBrainzConnector):
        msg = f"Expected ListenBrainzConnector, got {type(connector).__name__}"
        raise TypeError(msg)
    return connector


async def _get_watermark(
    session: sa_async.AsyncSession,
    connection_id: uuid.UUID,
) -> int | None:
    """Find the most recent completed TIME_RANGE task's last_listened_at.

    Used for incremental ListenBrainz sync -- only fetches listens newer
    than the watermark.

    Args:
        session: Active database session.
        connection_id: The service connection ID.

    Returns:
        Unix timestamp (int) of the watermark, or None for full sync.
    """
    result = await session.execute(
        sa.select(task_module.SyncTask)
        .where(
            task_module.SyncTask.service_connection_id == connection_id,
            task_module.SyncTask.task_type == types_module.SyncTaskType.TIME_RANGE,
            task_module.SyncTask.status == types_module.SyncStatus.COMPLETED,
        )
        .order_by(task_module.SyncTask.completed_at.desc())
        .limit(1)
    )
    last_task = result.scalar_one_or_none()
    if last_task is None:
        return None

    task_result = last_task.result or {}
    watermark = task_result.get("last_listened_at")
    if watermark is not None:
        return int(str(watermark))
    return None
