"""Test sync strategy -- generates deterministic fake data."""

from __future__ import annotations

import datetime
import hashlib
import uuid
from typing import TYPE_CHECKING, Any

import resonance.connectors.base as base_module
import resonance.sync.base as sync_base
import resonance.types as types_module

if TYPE_CHECKING:
    import sqlalchemy.ext.asyncio as sa_async

    import resonance.models.task as task_module
    import resonance.models.user as user_models


class TestSyncStrategy(sync_base.SyncStrategy):
    """Generates deterministic fake data for testing."""

    concurrency: str = "sequential"  # type: ignore[assignment]

    async def plan(
        self,
        session: sa_async.AsyncSession,
        connection: user_models.ServiceConnection,
        connector: base_module.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        """Return a single task descriptor for generating test data."""
        return [
            sync_base.SyncTaskDescriptor(
                task_type=types_module.SyncTaskType.TIME_RANGE,
                params={
                    "artists": 5,
                    "tracks": 20,
                    "listens": 100,
                    "seed": str(connection.user_id),
                },
                progress_total=1,
                description="Generate test data",
            )
        ]

    async def execute(
        self,
        session: sa_async.AsyncSession,
        task: task_module.SyncTask,
        connector: base_module.BaseConnector,
        connection: user_models.ServiceConnection,
    ) -> dict[str, Any]:
        """Generate deterministic fake artists, tracks, and listening events."""
        import resonance.sync.runner as runner_module

        params: dict[str, Any] = task.params or {}
        num_artists = int(str(params.get("artists", 5)))
        num_tracks = int(str(params.get("tracks", 20)))
        num_listens = int(str(params.get("listens", 100)))
        seed = str(params.get("seed", "default"))

        items_created = 0

        # Generate deterministic artists
        artists_data: list[tuple[str, str]] = []
        for i in range(num_artists):
            name = f"Test Artist {_seeded_hex(seed, 'artist', i)}"
            ext_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:artist:{i}"))
            artist_data = base_module.ArtistData(
                external_id=ext_id,
                name=name,
                service=types_module.ServiceType.TEST,
            )
            created = await runner_module._upsert_artist(session, artist_data)
            if created:
                items_created += 1
            artists_data.append((name, ext_id))

        await session.flush()

        # Generate deterministic tracks
        tracks_data: list[base_module.TrackData] = []
        for i in range(num_tracks):
            artist_idx = i % num_artists
            artist_name, artist_ext_id = artists_data[artist_idx]
            ext_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:track:{i}"))
            track_data = base_module.TrackData(
                external_id=ext_id,
                title=f"Test Track {_seeded_hex(seed, 'track', i)}",
                artist_external_id=artist_ext_id,
                artist_name=artist_name,
                service=types_module.ServiceType.TEST,
            )
            created = await runner_module._upsert_track(session, track_data)
            if created:
                items_created += 1
            tracks_data.append(track_data)

        await session.flush()

        # Generate deterministic listening events
        base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        for i in range(num_listens):
            track = tracks_data[i % num_tracks]
            listened_at = base_time + datetime.timedelta(hours=i)
            await runner_module._upsert_listening_event(
                session, connection.user_id, track, listened_at.isoformat()
            )
            items_created += 1

        task.progress_current = 1
        return {"items_created": items_created, "items_updated": 0}


def _seeded_hex(seed: str, kind: str, index: int) -> str:
    """Generate a deterministic hex string from seed."""
    return hashlib.md5(f"{seed}:{kind}:{index}".encode()).hexdigest()[:8]
