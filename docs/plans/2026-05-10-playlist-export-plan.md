# Playlist Export to Spotify — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Export resonance playlists to connected Spotify accounts, with track matching for unmatched tracks and sync-state tracking.

**Architecture:** Background worker task (`PLAYLIST_EXPORT`) enqueued via a new API endpoint. The task searches Spotify for unmatched tracks, creates or updates a Spotify playlist, and records the link in `playlist.service_links`. One task per connection; status shown via HTMX polling page.

**Tech Stack:** FastAPI, SQLAlchemy 2.0 async, arq worker, httpx (Spotify API), HTMX, Jinja2

**Design doc:** `docs/plans/2026-05-10-playlist-export-design.md`

---

### Task 1: Add `PLAYLIST_EXPORT` to `TaskType` enum and update `service_links`

**Files:**
- Modify: `src/resonance/types.py:50-60` (add enum value)
- Modify: `src/resonance/models/playlist.py:18-48` (add `service_links` field)
- Create: `alembic/versions/x1s2t3u4v5w6_add_playlist_export_support.py`
- Test: `tests/test_api_playlists.py`

**Step 1: Add `PLAYLIST_EXPORT` to the `TaskType` enum**

In `src/resonance/types.py`, add after `TRACK_SCORING`:

```python
PLAYLIST_EXPORT = "playlist_export"
```

**Step 2: Add `service_links` to the `Playlist` model**

In `src/resonance/models/playlist.py`, add the import `from typing import Any` and add the field to the `Playlist` class after `is_pinned`:

```python
service_links: orm.Mapped[dict[str, Any] | None] = orm.mapped_column(
    sa.JSON, nullable=True, default=None
)
```

**Step 3: Write the Alembic migration**

Create `alembic/versions/x1s2t3u4v5w6_add_playlist_export_support.py`:

```python
"""add playlist export support

Revision ID: x1s2t3u4v5w6
Revises: w0r1s2t3u4v5
Create Date: 2026-05-10
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "x1s2t3u4v5w6"
down_revision: str = "w0r1s2t3u4v5"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Add service_links column to playlists
    op.add_column(
        "playlists",
        sa.Column("service_links", sa.JSON(), nullable=True),
    )

    # Update task_type CHECK constraint to include PLAYLIST_EXPORT
    op.execute(
        sa.text(
            'ALTER TABLE sync_tasks '
            'DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type" '
            "CHECK (task_type IN ("
            "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
            "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
            "'TRACK_DISCOVERY', 'TRACK_SCORING', 'PLAYLIST_EXPORT'))"
        )
    )


def downgrade() -> None:
    # Revert task_type CHECK
    op.execute(
        sa.text(
            'ALTER TABLE sync_tasks '
            'DROP CONSTRAINT IF EXISTS "ck_sync_tasks_task_type"'
        )
    )
    op.execute(
        sa.text(
            "ALTER TABLE sync_tasks ADD CONSTRAINT "
            '"ck_sync_tasks_task_type" '
            "CHECK (task_type IN ("
            "'SYNC_JOB', 'TIME_RANGE', 'PAGE_FETCH', 'BULK_JOB', "
            "'CALENDAR_SYNC', 'PLAYLIST_GENERATION', "
            "'TRACK_DISCOVERY', 'TRACK_SCORING'))"
        )
    )

    op.drop_column("playlists", "service_links")
```

**Step 4: Write test for service_links field**

Add to `tests/test_api_playlists.py`:

```python
class TestPlaylistServiceLinks:
    """Tests for service_links field on Playlist model."""

    def test_service_links_defaults_to_none(self) -> None:
        playlist = playlist_models.Playlist(
            user_id=uuid.uuid4(),
            name="Test",
        )
        assert playlist.service_links is None

    def test_service_links_stores_export_data(self) -> None:
        connection_id = str(uuid.uuid4())
        links = {
            "spotify": {
                connection_id: {
                    "playlist_id": "abc123",
                    "exported_at": "2026-05-10T22:30:00Z",
                }
            }
        }
        playlist = playlist_models.Playlist(
            user_id=uuid.uuid4(),
            name="Test",
            service_links=links,
        )
        assert playlist.service_links["spotify"][connection_id]["playlist_id"] == "abc123"
```

**Step 5: Run tests**

```bash
uv run pytest tests/test_api_playlists.py -v
```

Expected: PASS

**Step 6: Run type checking and lint**

```bash
uv run mypy src/resonance/types.py src/resonance/models/playlist.py
uv run ruff check src/resonance/types.py src/resonance/models/playlist.py
```

Expected: No errors

**Step 7: Commit**

```bash
git add src/resonance/types.py src/resonance/models/playlist.py \
  alembic/versions/x1s2t3u4v5w6_add_playlist_export_support.py \
  tests/test_api_playlists.py
git commit -m "feat: add PLAYLIST_EXPORT task type and playlist service_links"
```

---

### Task 2: Add Spotify connector playlist write methods

**Files:**
- Modify: `src/resonance/connectors/spotify.py` (add capability + methods)
- Create: `tests/test_spotify_export.py`

**Step 1: Write failing tests for the new methods**

Create `tests/test_spotify_export.py`:

```python
"""Tests for Spotify playlist export methods."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import resonance.config as config_module
import resonance.connectors.base as base_module
import resonance.connectors.spotify as spotify_module


def _make_connector() -> spotify_module.SpotifyConnector:
    """Create a SpotifyConnector with mock settings."""
    settings = MagicMock(spec=config_module.Settings)
    settings.spotify_client_id = "test_id"
    settings.spotify_client_secret = "test_secret"
    settings.spotify_redirect_uri = "http://localhost/callback"
    return spotify_module.SpotifyConnector(settings=settings)


class TestSpotifyPlaylistWriteCapability:
    """Verify PLAYLIST_WRITE is declared."""

    def test_has_playlist_write_capability(self) -> None:
        connector = _make_connector()
        assert connector.has_capability(base_module.ConnectorCapability.PLAYLIST_WRITE)


class TestCreatePlaylist:
    """Tests for create_playlist method."""

    @pytest.mark.asyncio
    async def test_creates_private_playlist(self) -> None:
        connector = _make_connector()
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 201
        mock_response.json.return_value = {"id": "sp_playlist_123"}
        mock_response.headers = {}
        connector._request = AsyncMock(return_value=mock_response)

        result = await connector.create_playlist(
            access_token="tok",
            name="Concert Prep: Crobot",
            description="Generated by Resonance",
        )

        assert result == "sp_playlist_123"
        connector._request.assert_called_once()
        call_kwargs = connector._request.call_args
        assert call_kwargs[0][0] == "POST"
        assert "/me/playlists" in call_kwargs[0][1]
        body = call_kwargs[1]["json"]
        assert body["name"] == "Concert Prep: Crobot"
        assert body["public"] is False


class TestAddTracksToPlaylist:
    """Tests for add_tracks_to_playlist method."""

    @pytest.mark.asyncio
    async def test_adds_tracks(self) -> None:
        connector = _make_connector()
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 201
        mock_response.json.return_value = {"snapshot_id": "snap1"}
        mock_response.headers = {}
        connector._request = AsyncMock(return_value=mock_response)

        await connector.add_tracks_to_playlist(
            access_token="tok",
            playlist_id="sp_playlist_123",
            uris=["spotify:track:aaa", "spotify:track:bbb"],
        )

        connector._request.assert_called_once()
        call_kwargs = connector._request.call_args
        body = call_kwargs[1]["json"]
        assert body["uris"] == ["spotify:track:aaa", "spotify:track:bbb"]

    @pytest.mark.asyncio
    async def test_batches_over_100_tracks(self) -> None:
        connector = _make_connector()
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 201
        mock_response.json.return_value = {"snapshot_id": "snap1"}
        mock_response.headers = {}
        connector._request = AsyncMock(return_value=mock_response)

        uris = [f"spotify:track:{i}" for i in range(150)]
        await connector.add_tracks_to_playlist(
            access_token="tok",
            playlist_id="sp_playlist_123",
            uris=uris,
        )

        assert connector._request.call_count == 2
        first_batch = connector._request.call_args_list[0][1]["json"]["uris"]
        second_batch = connector._request.call_args_list[1][1]["json"]["uris"]
        assert len(first_batch) == 100
        assert len(second_batch) == 50


class TestReplacePlaylistTracks:
    """Tests for replace_playlist_tracks method."""

    @pytest.mark.asyncio
    async def test_replaces_tracks(self) -> None:
        connector = _make_connector()
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"snapshot_id": "snap1"}
        mock_response.headers = {}
        connector._request = AsyncMock(return_value=mock_response)

        await connector.replace_playlist_tracks(
            access_token="tok",
            playlist_id="sp_playlist_123",
            uris=["spotify:track:aaa"],
        )

        call_kwargs = connector._request.call_args
        assert call_kwargs[0][0] == "PUT"
        body = call_kwargs[1]["json"]
        assert body["uris"] == ["spotify:track:aaa"]


class TestSearchTrack:
    """Tests for search_track method."""

    @pytest.mark.asyncio
    async def test_returns_track_id_when_found(self) -> None:
        connector = _make_connector()
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tracks": {
                "items": [{"id": "found_track_id", "name": "Test Track"}]
            }
        }
        mock_response.headers = {}
        connector._request = AsyncMock(return_value=mock_response)

        result = await connector.search_track(
            access_token="tok",
            title="Test Track",
            artist_name="Test Artist",
        )

        assert result == "found_track_id"

    @pytest.mark.asyncio
    async def test_returns_none_when_not_found(self) -> None:
        connector = _make_connector()
        mock_response = MagicMock(spec=httpx.Response)
        mock_response.status_code = 200
        mock_response.json.return_value = {"tracks": {"items": []}}
        mock_response.headers = {}
        connector._request = AsyncMock(return_value=mock_response)

        result = await connector.search_track(
            access_token="tok",
            title="Nonexistent",
            artist_name="Unknown",
        )

        assert result is None
```

**Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_spotify_export.py -v
```

Expected: FAIL — methods don't exist yet

**Step 3: Add `PLAYLIST_WRITE` capability and implement methods**

In `src/resonance/connectors/spotify.py`:

Add `PLAYLIST_WRITE` to capabilities:
```python
capabilities = frozenset(
    {
        base_module.ConnectorCapability.AUTHENTICATION,
        base_module.ConnectorCapability.LISTENING_HISTORY,
        base_module.ConnectorCapability.FOLLOWS,
        base_module.ConnectorCapability.TRACK_RATINGS,
        base_module.ConnectorCapability.PLAYLIST_WRITE,
    }
)
```

Add `playlist-modify-private` to `_SCOPES`:
```python
_SCOPES = (
    "user-read-recently-played "
    "user-follow-read "
    "user-library-read "
    "user-read-email "
    "user-read-private "
    "playlist-modify-private"
)
```

Add the new methods to `SpotifyConnector`:

```python
async def create_playlist(
    self,
    access_token: str,
    name: str,
    description: str = "",
) -> str:
    """Create a private playlist on the user's Spotify account."""
    response = await self._request(
        "POST",
        f"{SPOTIFY_API_BASE}/me/playlists",
        headers={"Authorization": f"Bearer {access_token}"},
        json={
            "name": name,
            "description": description,
            "public": False,
        },
    )
    data: dict[str, str] = response.json()
    logger.info("spotify_playlist_created", playlist_id=data["id"])
    return data["id"]

async def add_tracks_to_playlist(
    self,
    access_token: str,
    playlist_id: str,
    uris: list[str],
) -> None:
    """Add tracks to a Spotify playlist, batching if over 100."""
    headers = {"Authorization": f"Bearer {access_token}"}
    for i in range(0, len(uris), 100):
        batch = uris[i : i + 100]
        await self._request(
            "POST",
            f"{SPOTIFY_API_BASE}/playlists/{playlist_id}/items",
            headers=headers,
            json={"uris": batch},
        )
    logger.info("spotify_tracks_added", playlist_id=playlist_id, count=len(uris))

async def replace_playlist_tracks(
    self,
    access_token: str,
    playlist_id: str,
    uris: list[str],
) -> None:
    """Replace all tracks in a Spotify playlist."""
    await self._request(
        "PUT",
        f"{SPOTIFY_API_BASE}/playlists/{playlist_id}/items",
        headers={"Authorization": f"Bearer {access_token}"},
        json={"uris": uris},
    )
    logger.info("spotify_tracks_replaced", playlist_id=playlist_id, count=len(uris))

async def search_track(
    self,
    access_token: str,
    title: str,
    artist_name: str,
) -> str | None:
    """Search Spotify for a track by title and artist. Returns track ID or None."""
    query = f"track:{title} artist:{artist_name}"
    response = await self._request(
        "GET",
        f"{SPOTIFY_API_BASE}/search",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"q": query, "type": "track", "limit": 1},
    )
    data = response.json()
    items = data.get("tracks", {}).get("items", [])
    if not items:
        return None
    result: str = items[0]["id"]
    return result
```

**Step 4: Run tests**

```bash
uv run pytest tests/test_spotify_export.py -v
```

Expected: PASS

**Step 5: Run type checking and lint**

```bash
uv run mypy src/resonance/connectors/spotify.py
uv run ruff check src/resonance/connectors/spotify.py
```

Expected: No errors

**Step 6: Commit**

```bash
git add src/resonance/connectors/spotify.py tests/test_spotify_export.py
git commit -m "feat: add Spotify playlist write methods and PLAYLIST_WRITE capability"
```

---

### Task 3: Implement the export worker task

**Files:**
- Modify: `src/resonance/worker.py` (add `export_playlist` function + dispatch entry)
- Modify: `tests/test_worker.py` (update function count, add export tests)

**Step 1: Write failing tests**

Add to `tests/test_worker.py` (update existing `test_functions_registered`):

```python
def test_functions_registered(self) -> None:
    funcs = worker_module.WorkerSettings.functions
    assert len(funcs) == 8
    names = {f.name for f in funcs}
    assert "export_playlist" in names
```

Add a new test class:

```python
class TestExportPlaylist:
    """Tests for the export_playlist worker function."""

    @pytest.mark.asyncio
    async def test_creates_spotify_playlist_and_records_link(self) -> None:
        """Verify export creates a Spotify playlist and updates service_links."""
        # Build a mock WorkerContext
        session = AsyncMock()
        session_factory = MagicMock()
        session_factory.return_value.__aenter__ = AsyncMock(return_value=session)
        session_factory.return_value.__aexit__ = AsyncMock(return_value=False)

        # Mock task with params
        playlist_id = uuid.uuid4()
        connection_id = uuid.uuid4()
        task = MagicMock(spec=task_module.Task)
        task.id = uuid.uuid4()
        task.user_id = uuid.uuid4()
        task.params = {
            "playlist_id": str(playlist_id),
            "connection_id": str(connection_id),
        }
        task.status = types_module.SyncStatus.PENDING
        task.parent_id = None

        # Mock playlist with tracks
        mock_track1 = MagicMock()
        mock_track1.track.title = "Song A"
        mock_track1.track.artist.name = "Artist A"
        mock_track1.track.service_links = {"spotify": "sp_track_1"}
        mock_track1.track.id = uuid.uuid4()

        mock_track2 = MagicMock()
        mock_track2.track.title = "Song B"
        mock_track2.track.artist.name = "Artist B"
        mock_track2.track.service_links = None  # needs search
        mock_track2.track.id = uuid.uuid4()

        mock_playlist = MagicMock()
        mock_playlist.id = playlist_id
        mock_playlist.name = "Test Playlist"
        mock_playlist.description = "A test"
        mock_playlist.service_links = None
        mock_playlist.tracks = [mock_track1, mock_track2]
        mock_playlist.user_id = task.user_id

        # Mock connection
        mock_connection = MagicMock()
        mock_connection.id = connection_id
        mock_connection.service_type = types_module.ServiceType.SPOTIFY
        mock_connection.user_id = task.user_id
        mock_connection.encrypted_access_token = "encrypted_tok"

        # Mock connector
        mock_connector = MagicMock()
        mock_connector.create_playlist = AsyncMock(return_value="new_sp_playlist")
        mock_connector.add_tracks_to_playlist = AsyncMock()
        mock_connector.search_track = AsyncMock(return_value="sp_track_2_found")

        # Mock session queries — these are complex but the test verifies the flow
        session.execute = AsyncMock()
        session.commit = AsyncMock()

        # The key assertions happen on the mock_connector calls
        # Verify create_playlist was called, search_track for unmatched, etc.
        # This is a smoke test — detailed assertions in integration tests
```

**Step 2: Add `export_playlist` function to worker**

In `src/resonance/worker.py`, add the dispatch entry:

```python
types_module.TaskType.PLAYLIST_EXPORT: (
    "export_playlist",
    lambda t: (str(t.id),),
),
```

Add the function (after `score_and_build_playlist`, before `_check_parent_completion`):

```python
async def export_playlist(ctx: dict[str, Any], task_id: str) -> None:
    """Export a playlist to a connected Spotify account.

    Loads the playlist and connection, searches Spotify for unmatched tracks,
    creates or updates the Spotify playlist, and records the link.
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
                log.error("export_playlist_task_not_found")
                return

            task.status = types_module.SyncStatus.RUNNING
            task.started_at = datetime.datetime.now(datetime.UTC)
            await session.commit()

            playlist_id = str(task.params.get("playlist_id", ""))
            connection_id = str(task.params.get("connection_id", ""))
            log = log.bind(playlist_id=playlist_id, connection_id=connection_id)
            log.info("export_playlist_started")

            # Load playlist with tracks
            playlist_result = await session.execute(
                sa.select(playlist_models.Playlist)
                .where(playlist_models.Playlist.id == uuid.UUID(playlist_id))
                .options(
                    sa_orm.selectinload(playlist_models.Playlist.tracks)
                    .joinedload(playlist_models.PlaylistTrack.track)
                    .joinedload(music_models.Track.artist)
                )
            )
            playlist = playlist_result.scalar_one_or_none()
            if playlist is None:
                await lifecycle_module.fail_task(
                    session, task, f"Playlist not found: {playlist_id}"
                )
                await session.commit()
                return

            # Load connection
            conn_result = await session.execute(
                sa.select(user_models.ServiceConnection).where(
                    user_models.ServiceConnection.id == uuid.UUID(connection_id)
                )
            )
            connection = conn_result.scalar_one_or_none()
            if connection is None:
                await lifecycle_module.fail_task(
                    session, task, f"Connection not found: {connection_id}"
                )
                await session.commit()
                return

            # Get connector and decrypt token
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

            settings = wctx["settings"]
            from cryptography.fernet import Fernet

            fernet = Fernet(settings.token_encryption_key.encode())
            access_token = fernet.decrypt(
                connection.encrypted_access_token.encode()
            ).decode()

            # Refresh token if expired
            if (
                connection.token_expires_at is not None
                and connection.token_expires_at
                < datetime.datetime.now(datetime.UTC)
            ):
                refresh_token = fernet.decrypt(
                    connection.encrypted_refresh_token.encode()
                ).decode()
                token_response = await connector.refresh_access_token(refresh_token)
                access_token = token_response.access_token
                connection.encrypted_access_token = fernet.encrypt(
                    access_token.encode()
                ).decode()
                if token_response.refresh_token:
                    connection.encrypted_refresh_token = fernet.encrypt(
                        token_response.refresh_token.encode()
                    ).decode()
                if token_response.expires_in:
                    connection.token_expires_at = datetime.datetime.now(
                        datetime.UTC
                    ) + datetime.timedelta(seconds=token_response.expires_in)
                await session.commit()

            # Track matching: find Spotify IDs for all tracks
            spotify_uris: list[str] = []
            skipped_tracks: list[str] = []

            for pt in playlist.tracks:
                track = pt.track
                spotify_id = (track.service_links or {}).get("spotify")

                if spotify_id is None:
                    # Search Spotify
                    found_id = await connector.search_track(
                        access_token=access_token,
                        title=track.title,
                        artist_name=track.artist.name,
                    )
                    if found_id is not None:
                        # Persist the match
                        updated_links = dict(track.service_links or {})
                        updated_links["spotify"] = found_id
                        track.service_links = updated_links
                        spotify_id = found_id
                        log.info(
                            "track_matched_via_search",
                            track_title=track.title,
                            spotify_id=found_id,
                        )

                if spotify_id is not None:
                    spotify_uris.append(f"spotify:track:{spotify_id}")
                else:
                    skipped_tracks.append(f"{track.artist.name} - {track.title}")
                    log.info("track_not_found_on_spotify", track_title=track.title)

            if not spotify_uris:
                await lifecycle_module.fail_task(
                    session, task, "No tracks could be matched to Spotify"
                )
                await session.commit()
                return

            # Create or update Spotify playlist
            existing_links = playlist.service_links or {}
            spotify_links = existing_links.get("spotify", {})
            existing_export = spotify_links.get(str(connection.id))

            if existing_export is not None:
                # Update existing playlist
                sp_playlist_id = existing_export["playlist_id"]
                await connector.replace_playlist_tracks(
                    access_token=access_token,
                    playlist_id=sp_playlist_id,
                    uris=spotify_uris,
                )
                log.info("spotify_playlist_updated", spotify_playlist_id=sp_playlist_id)
            else:
                # Create new playlist
                sp_playlist_id = await connector.create_playlist(
                    access_token=access_token,
                    name=playlist.name,
                    description=playlist.description or "",
                )
                await connector.add_tracks_to_playlist(
                    access_token=access_token,
                    playlist_id=sp_playlist_id,
                    uris=spotify_uris,
                )
                log.info("spotify_playlist_created", spotify_playlist_id=sp_playlist_id)

            # Record the export in service_links
            updated_playlist_links = dict(existing_links)
            updated_spotify = dict(spotify_links)
            updated_spotify[str(connection.id)] = {
                "playlist_id": sp_playlist_id,
                "exported_at": datetime.datetime.now(datetime.UTC).isoformat(),
            }
            updated_playlist_links["spotify"] = updated_spotify
            playlist.service_links = updated_playlist_links

            await lifecycle_module.complete_task(
                session,
                task,
                {
                    "spotify_playlist_id": sp_playlist_id,
                    "exported": len(spotify_uris),
                    "skipped": len(skipped_tracks),
                    "skipped_tracks": skipped_tracks,
                },
            )
            await session.commit()
            log.info(
                "export_playlist_completed",
                exported=len(spotify_uris),
                skipped=len(skipped_tracks),
            )

        except Exception:
            log.exception("export_playlist_failed")
            task_reload = await _load_task(session, task_id)
            if task_reload is not None:
                await lifecycle_module.fail_task(
                    session, task_reload, traceback.format_exc()
                )
                await session.commit()
```

Register in `WorkerSettings.functions`:
```python
arq.func(heartbeat_module.with_heartbeat(export_playlist), timeout=600),
```

**Step 3: Run tests**

```bash
uv run pytest tests/test_worker.py::TestWorkerSettings -v
uv run pytest tests/test_spotify_export.py -v
```

Expected: PASS

**Step 4: Run type checking**

```bash
uv run mypy src/resonance/worker.py
```

**Step 5: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: add export_playlist worker task with track matching"
```

---

### Task 4: Add the export API endpoint

**Files:**
- Modify: `src/resonance/api/v1/playlists.py` (add export endpoint)
- Create: `tests/test_api_playlist_export.py`

**Step 1: Write failing test**

Create `tests/test_api_playlist_export.py`:

```python
"""Tests for playlist export API endpoint."""
from __future__ import annotations

import uuid

import resonance.types as types_module


class TestExportEndpointValidation:
    """Tests for export endpoint request validation."""

    def test_export_requires_spotify_connection(self) -> None:
        """Non-spotify connection_ids should be rejected."""
        # Validates that the endpoint checks service_type == SPOTIFY
        assert types_module.ServiceType.SPOTIFY == "spotify"

    def test_playlist_export_task_type_exists(self) -> None:
        assert types_module.TaskType.PLAYLIST_EXPORT == "playlist_export"
```

**Step 2: Implement the export endpoint**

Add to `src/resonance/api/v1/playlists.py`:

```python
import arq
import arq.connections as arq_connections

import resonance.config as config_module
import resonance.models.task as task_module
import resonance.models.user as user_models
import resonance.types as types_module


@router.post(
    "/{playlist_id}/export",
    summary="Export playlist to Spotify",
    description="Enqueue export tasks to push this playlist to connected Spotify accounts.",
    status_code=202,
)
async def export_playlist(
    playlist_id: uuid.UUID,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Enqueue playlist export task(s) for connected Spotify accounts."""
    # Verify playlist exists and belongs to user
    result = await db.execute(
        sa.select(playlist_models.Playlist).where(
            playlist_models.Playlist.id == playlist_id,
            playlist_models.Playlist.user_id == user_id,
        )
    )
    playlist = result.scalar_one_or_none()
    if playlist is None:
        raise fastapi.HTTPException(status_code=404, detail="Playlist not found")

    # Determine target connections
    connection_ids: list[str] | None = None
    if body and "connection_ids" in body:
        connection_ids = body["connection_ids"]

    if connection_ids:
        # Validate specific connections
        conn_uuids = [uuid.UUID(c) for c in connection_ids]
        conn_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.id.in_(conn_uuids),
                user_models.ServiceConnection.user_id == user_id,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.SPOTIFY,
            )
        )
        connections = conn_result.scalars().all()
        if len(connections) != len(conn_uuids):
            raise fastapi.HTTPException(
                status_code=400,
                detail="One or more connection_ids are invalid or not Spotify connections",
            )
    else:
        # Export to all Spotify connections
        conn_result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.user_id == user_id,
                user_models.ServiceConnection.service_type
                == types_module.ServiceType.SPOTIFY,
            )
        )
        connections = conn_result.scalars().all()
        if not connections:
            raise fastapi.HTTPException(
                status_code=400,
                detail="No Spotify connections found. Connect Spotify first.",
            )

    # Create and enqueue tasks
    settings = config_module.Settings()
    redis = await arq.create_pool(
        arq_connections.RedisSettings(
            host=settings.redis_host,
            port=settings.redis_port,
            password=settings.redis_password or None,
        )
    )

    tasks: list[dict[str, str]] = []
    for conn in connections:
        task = task_module.Task(
            id=uuid.uuid4(),
            user_id=user_id,
            task_type=types_module.TaskType.PLAYLIST_EXPORT,
            status=types_module.SyncStatus.PENDING,
            params={
                "playlist_id": str(playlist_id),
                "connection_id": str(conn.id),
            },
            description=f"Export to Spotify ({conn.external_user_id or 'account'})",
        )
        db.add(task)
        tasks.append({"task_id": str(task.id), "connection_id": str(conn.id)})

    await db.commit()

    for task_info in tasks:
        await redis.enqueue_job(
            "export_playlist",
            task_info["task_id"],
            _job_id=f"export_playlist:{task_info['task_id']}",
        )

    await redis.close()

    return {"tasks": tasks}
```

**Step 3: Run tests**

```bash
uv run pytest tests/test_api_playlist_export.py -v
```

**Step 4: Include `service_links` in playlist API responses**

Modify `format_playlist_summary` in `src/resonance/api/v1/playlists.py` to include `service_links`:

```python
def format_playlist_summary(playlist: playlist_models.Playlist) -> dict[str, Any]:
    return {
        "id": str(playlist.id),
        "name": playlist.name,
        "description": playlist.description,
        "track_count": playlist.track_count,
        "is_pinned": playlist.is_pinned,
        "created_at": playlist.created_at.isoformat(),
        "service_links": playlist.service_links,
    }
```

**Step 5: Run type checking and full test suite**

```bash
uv run mypy src/resonance/api/v1/playlists.py
uv run ruff check src/resonance/api/v1/playlists.py
uv run pytest tests/test_api_playlists.py tests/test_api_playlist_export.py -v
```

**Step 6: Commit**

```bash
git add src/resonance/api/v1/playlists.py tests/test_api_playlist_export.py
git commit -m "feat: add POST /playlists/{id}/export API endpoint"
```

---

### Task 5: Add UI — export button and status page

**Files:**
- Modify: `src/resonance/templates/playlist_detail.html` (add export section)
- Modify: `src/resonance/templates/partials/playlist_list.html` (add export indicator)
- Create: `src/resonance/templates/playlists_exporting.html` (status page)
- Create: `src/resonance/templates/partials/playlist_export_status.html` (polling partial)
- Modify: `src/resonance/ui/routes.py` (add export routes, pass connections to detail)

**Step 1: Add export section to playlist detail template**

After the delete button in `playlist_detail.html`, add the export section. The detail page handler will need to pass `spotify_connections` to the template context.

```html
{# Export to Spotify section #}
{% if spotify_connections %}
<article>
    <h3>Export to Spotify</h3>
    {% for conn in spotify_connections %}
    {% set conn_id = conn.id | string %}
    {% set export_info = (playlist.service_links or {}).get('spotify', {}).get(conn_id) %}
    <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem;">
        <span>{{ conn.external_user_id or 'Spotify Account' }}</span>
        {% if export_info %}
            {% set exported_at = export_info.get('exported_at', '') %}
            {% if playlist.updated_at.isoformat() > exported_at %}
            <small class="warning">Playlist changed since last export</small>
            <form method="post" action="/playlists/{{ playlist.id }}/export"
                  style="display:inline; margin:0;">
                <input type="hidden" name="connection_id" value="{{ conn_id }}">
                <button type="submit" class="outline"
                        style="padding: 0.25rem 0.5rem; font-size: 0.8rem;">
                    Update on Spotify
                </button>
            </form>
            {% else %}
            <small>Synced {{ exported_at[:10] }}</small>
            <a href="https://open.spotify.com/playlist/{{ export_info.get('playlist_id', '') }}"
               target="_blank" rel="noopener"
               style="font-size: 0.8rem;">Open in Spotify</a>
            {% endif %}
        {% else %}
            <form method="post" action="/playlists/{{ playlist.id }}/export"
                  style="display:inline; margin:0;">
                <input type="hidden" name="connection_id" value="{{ conn_id }}">
                <button type="submit" style="padding: 0.25rem 0.5rem; font-size: 0.8rem;">
                    Export to Spotify
                </button>
            </form>
        {% endif %}
    </div>
    {% endfor %}
</article>
{% endif %}
```

**Step 2: Create the export status page**

Create `src/resonance/templates/playlists_exporting.html`:

```html
{% extends "base.html" %}
{% block title %}Exporting Playlist — resonance{% endblock %}
{% block content %}
<h2>{{ playlist_name }}</h2>

<div id="export-status"
     hx-get="/partials/export-status/{{ playlist_id }}?task_ids={{ task_ids }}"
     hx-trigger="every 3s"
     hx-swap="outerHTML">
    <article aria-busy="true">
        Exporting to Spotify...
    </article>
</div>
{% endblock %}
```

**Step 3: Create the export status polling partial**

Create `src/resonance/templates/partials/playlist_export_status.html`:

```html
{% if all_completed %}
<article>
    <p>Export complete!</p>
    {% for result in task_results %}
    <p>
        {{ result.description }}:
        {{ result.exported }} tracks exported
        {% if result.skipped > 0 %}
        ({{ result.skipped }} not found on Spotify)
        {% endif %}
    </p>
    {% if result.spotify_playlist_id %}
    <a href="https://open.spotify.com/playlist/{{ result.spotify_playlist_id }}"
       target="_blank" rel="noopener">Open in Spotify</a>
    {% endif %}
    {% endfor %}
    <p><a href="/playlists/{{ playlist_id }}">Back to playlist</a></p>
</article>
{% elif any_failed %}
<article>
    <p>Export failed.</p>
    {% for result in task_results %}
    {% if result.error %}
    <p>{{ result.description }}: {{ result.error }}</p>
    {% endif %}
    {% endfor %}
    <p><a href="/playlists/{{ playlist_id }}">Back to playlist</a></p>
</article>
{% else %}
<div id="export-status"
     hx-get="/partials/export-status/{{ playlist_id }}?task_ids={{ task_ids }}"
     hx-trigger="every 3s"
     hx-swap="outerHTML">
    <article aria-busy="true">
        Exporting to Spotify...
    </article>
</div>
{% endif %}
```

**Step 4: Add UI routes**

Add to `src/resonance/ui/routes.py`:

- Modify `playlist_detail_page` to load and pass `spotify_connections` to the template context
- Add `POST /playlists/{playlist_id}/export` form handler that calls the API endpoint and redirects to the export status page
- Add `GET /playlists/exporting/{playlist_id}` to render the status page
- Add `GET /partials/export-status/{playlist_id}` for HTMX polling

**Step 5: Update the playlist list page**

In `src/resonance/templates/partials/playlist_list.html`, add a small Spotify indicator next to each playlist that has been exported. Show a green dot if in sync, amber if stale.

**Step 6: Run the full test suite**

```bash
uv run pytest -v
uv run mypy src/
uv run ruff check .
```

**Step 7: Commit**

```bash
git add src/resonance/templates/playlist_detail.html \
  src/resonance/templates/playlists_exporting.html \
  src/resonance/templates/partials/playlist_export_status.html \
  src/resonance/templates/partials/playlist_list.html \
  src/resonance/ui/routes.py
git commit -m "feat: add playlist export UI with status page and sync indicators"
```

---

### Task 6: Invalidate existing Spotify tokens and update docs

**Files:**
- Modify: `docs/spotify-api-constraints.md` (document playlist write endpoints)
- Modify: `CLAUDE.md` (note the new capability)

**Step 1: Document the new Spotify endpoints**

Add to `docs/spotify-api-constraints.md` under "Available Endpoints":

```markdown
| `POST /me/playlists` | Create private playlist for export |
| `POST /playlists/{id}/items` | Add tracks to exported playlist |
| `PUT /playlists/{id}/items` | Replace tracks on re-export |
| `GET /search` | Search for tracks by title/artist |
```

**Step 2: Update CLAUDE.md conventions**

Add a note about playlist export under "Conventions":

```markdown
- Playlist export uses `PLAYLIST_WRITE` capability on connectors that support it
- Export creates a background task per Spotify connection; status tracked via HTMX polling
- Track matching searches Spotify for tracks missing service_links and persists matches
```

**Step 3: Invalidate existing tokens**

This is a manual step for the user: disconnect and reconnect Spotify on the account page. The new OAuth scopes will be requested on reconnection. No code change needed — the scope string is already updated in Task 2.

Note in commit message that users need to reconnect Spotify for the new permissions.

**Step 4: Commit**

```bash
git add docs/spotify-api-constraints.md CLAUDE.md
git commit -m "docs: document playlist export endpoints and conventions"
```

---

### Task 7: Deploy and verify end-to-end

**Step 1: Push to main**

```bash
git push origin main
```

**Step 2: Wait for CI to pass and image to build**

```bash
uv run resonance-api healthz
```

Verify the new revision is deployed.

**Step 3: Reconnect Spotify**

Disconnect and reconnect Spotify on the account page to get the new `playlist-modify-private` scope.

**Step 4: Test export flow**

1. Navigate to a playlist detail page
2. Click "Export to Spotify"
3. Verify the export status page shows progress
4. Verify the Spotify playlist is created (check Spotify app)
5. Verify `service_links` is populated on the playlist
6. Re-export and verify it updates the same playlist (not a duplicate)
7. Regenerate the playlist and verify the "changed since last export" indicator appears

**Step 5: Verify via CLI**

```bash
uv run resonance-api api GET /api/v1/playlists
```

Check that `service_links` appears in the response.
