# Incremental Sync Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace full re-syncs with watermark-based incremental syncs across all services and data types.

**Architecture:** Add `sync_watermark` JSON column to `ServiceConnection`. Each strategy reads watermarks during `plan()` and writes them back after successful `execute()`. Spotify connector gains paginated methods with stop-early support. Worker writes watermarks atomically with task completion.

**Tech Stack:** SQLAlchemy 2.0 (async), Alembic, pytest, arq

**Design doc:** `docs/plans/2026-04-06-incremental-sync-design.md`

---

### Task 1: Add sync_watermark column to ServiceConnection

**Files:**
- Modify: `src/resonance/models/user.py:36-79`
- Create: `alembic/versions/e2f3a4b5c6d7_add_sync_watermark.py`
- Modify: `tests/test_models.py`

**Step 1: Write the failing test**

Add to `tests/test_models.py`:

```python
class TestServiceConnectionSyncWatermark:
    """Tests for the sync_watermark column."""

    def test_sync_watermark_defaults_to_empty_dict(self) -> None:
        conn = user_models.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SPOTIFY,
            external_user_id="test",
            encrypted_access_token="enc",
        )
        assert conn.sync_watermark == {}

    def test_sync_watermark_stores_dict(self) -> None:
        watermark = {"listens": {"last_listened_at": 1700000000}}
        conn = user_models.ServiceConnection(
            id=uuid.uuid4(),
            user_id=uuid.uuid4(),
            service_type=types_module.ServiceType.SPOTIFY,
            external_user_id="test",
            encrypted_access_token="enc",
            sync_watermark=watermark,
        )
        assert conn.sync_watermark == watermark
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_models.py::TestServiceConnectionSyncWatermark -v`
Expected: FAIL — `sync_watermark` attribute doesn't exist

**Step 3: Add sync_watermark column to ServiceConnection model**

In `src/resonance/models/user.py`, add after the `last_used_at` column (line 76):

```python
sync_watermark: orm.Mapped[dict[str, dict[str, object]]] = orm.mapped_column(
    sa.JSON, nullable=False, server_default="{}", default=dict
)
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_models.py::TestServiceConnectionSyncWatermark -v`
Expected: PASS

**Step 5: Create Alembic migration**

Create `alembic/versions/e2f3a4b5c6d7_add_sync_watermark.py`:

```python
"""add sync_watermark column to service_connections

Revision ID: e2f3a4b5c6d7
Revises: d1a2b3c4e5f6
Create Date: 2026-04-06

"""

from __future__ import annotations

from alembic import op

import sqlalchemy as sa

revision: str = "e2f3a4b5c6d7"
down_revision: str | None = "d1a2b3c4e5f6"
branch_labels: tuple[str, ...] | None = None
depends_on: tuple[str, ...] | None = None


def upgrade() -> None:
    op.add_column(
        "service_connections",
        sa.Column("sync_watermark", sa.JSON, nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_column("service_connections", "sync_watermark")
```

**Step 6: Run full test suite and type check**

Run: `uv run pytest -x -q && uv run mypy src/`
Expected: All pass

**Step 7: Commit**

```bash
git add src/resonance/models/user.py alembic/versions/e2f3a4b5c6d7_add_sync_watermark.py tests/test_models.py
git commit -m "feat: add sync_watermark column to ServiceConnection"
```

---

### Task 2: Migrate ListenBrainz plan() to read watermark from connection

**Files:**
- Modify: `src/resonance/sync/listenbrainz.py:35-81` (plan method)
- Modify: `src/resonance/sync/listenbrainz.py:220-254` (remove _get_watermark)
- Modify: `tests/test_sync_listenbrainz.py`

**Step 1: Update tests for new watermark source**

The current tests mock `session.execute` to simulate `_get_watermark` querying task history. Update them to use `connection.sync_watermark` instead.

In `tests/test_sync_listenbrainz.py`, update `_make_connection` helper:

```python
def _make_connection(
    *,
    connection_id: uuid.UUID | None = None,
    external_user_id: str = "testuser",
    sync_watermark: dict[str, dict[str, object]] | None = None,
) -> MagicMock:
    """Create a mock ServiceConnection."""
    conn = MagicMock()
    conn.id = connection_id or uuid.uuid4()
    conn.external_user_id = external_user_id
    conn.service_type = types_module.ServiceType.LISTENBRAINZ
    conn.sync_watermark = sync_watermark or {}
    return conn
```

Update `TestPlan.test_full_sync_no_watermark` — remove the `session.execute` mock for watermark query since `plan()` will read directly from `connection.sync_watermark`:

```python
@pytest.mark.asyncio
async def test_full_sync_no_watermark(self) -> None:
    """Returns a single descriptor with min_ts=None for full sync."""
    strategy = lb_sync_module.ListenBrainzSyncStrategy()
    session = AsyncMock()
    connection = _make_connection()  # empty sync_watermark
    connector = _make_lb_connector()

    descriptors = await strategy.plan(session, connection, connector)

    assert len(descriptors) == 1
    desc = descriptors[0]
    assert desc.task_type == types_module.SyncTaskType.TIME_RANGE
    assert desc.params["username"] == connection.external_user_id
    assert desc.params["min_ts"] is None
    assert desc.progress_total == 500
```

Update `TestPlan.test_incremental_sync_with_watermark`:

```python
@pytest.mark.asyncio
async def test_incremental_sync_with_watermark(self) -> None:
    """Uses watermark for incremental sync (min_ts set)."""
    strategy = lb_sync_module.ListenBrainzSyncStrategy()
    session = AsyncMock()
    watermark_ts = 1700000000
    connection = _make_connection(
        sync_watermark={"listens": {"last_listened_at": watermark_ts}}
    )
    connector = _make_lb_connector()

    descriptors = await strategy.plan(session, connection, connector)

    assert len(descriptors) == 1
    desc = descriptors[0]
    assert desc.params["min_ts"] == watermark_ts
```

Update `TestPlan.test_incremental_description_says_since` similarly (use `sync_watermark` param instead of mocked execute).

Update `TestPlan.test_full_sync_description` similarly (no session.execute mock needed).

Update `TestPlan.test_listen_count_failure_sets_progress_total_none` similarly.

Remove `TestGetWatermark` class entirely — `_get_watermark` is being deleted.

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_listenbrainz.py::TestPlan -v`
Expected: FAIL — tests reference new connection attribute / old function still exists

**Step 3: Modify plan() to read from connection.sync_watermark**

Replace the `plan()` method body in `src/resonance/sync/listenbrainz.py` (lines 54-81):

```python
async def plan(
    self,
    session: sa_async.AsyncSession,
    connection: user_models.ServiceConnection,
    connector: connector_base.BaseConnector,
) -> list[sync_base.SyncTaskDescriptor]:
    lb_connector = _cast_connector(connector)
    username = connection.external_user_id

    # Get listen count for progress tracking
    progress_total: int | None = None
    try:
        progress_total = await lb_connector.get_listen_count(username)
    except (httpx.HTTPError, connector_base.RateLimitExceededError):
        logger.warning("could_not_fetch_listen_count", username=username)

    # Read watermark from connection
    listens_watermark = connection.sync_watermark.get("listens", {})
    watermark: int | None = None
    raw = listens_watermark.get("last_listened_at")
    if raw is not None:
        watermark = int(str(raw))

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
```

Delete the `_get_watermark` function (lines 220-254) and its imports (`uuid` from TYPE_CHECKING is no longer needed if only used there — check before removing).

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_listenbrainz.py -v`
Expected: All pass

**Step 5: Run full suite and type check**

Run: `uv run pytest -x -q && uv run mypy src/`
Expected: All pass

**Step 6: Commit**

```bash
git add src/resonance/sync/listenbrainz.py tests/test_sync_listenbrainz.py
git commit -m "refactor: read ListenBrainz watermark from connection instead of task history"
```

---

### Task 3: Migrate Spotify plan() to read watermarks from connection

**Files:**
- Modify: `src/resonance/sync/spotify.py:77-93` (plan method)
- Modify: `tests/test_sync_spotify_strategy.py`

**Step 1: Update tests**

In `tests/test_sync_spotify_strategy.py`, update `_make_connection`:

```python
def _make_connection(
    access_token: str = "test-token",
    sync_watermark: dict[str, dict[str, object]] | None = None,
) -> MagicMock:
    conn = MagicMock()
    conn.id = uuid.uuid4()
    conn.encrypted_access_token = crypto_module.encrypt_token(
        access_token, _TEST_ENCRYPTION_KEY
    )
    conn.service_type = types_module.ServiceType.SPOTIFY
    conn.sync_watermark = sync_watermark or {}
    return conn
```

Add new tests to `TestSpotifyPlan`:

```python
@pytest.mark.asyncio
async def test_passes_watermarks_to_params(self) -> None:
    """Watermarks from connection are passed into descriptor params."""
    strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
    session = AsyncMock()
    connector = AsyncMock()
    connection = _make_connection(
        sync_watermark={
            "recently_played": {"last_played_at": "2026-04-05T12:00:00Z"},
            "saved_tracks": {"last_saved_at": "2026-04-05T12:00:00Z"},
            "followed_artists": {"after_cursor": "abc123"},
        }
    )

    descriptors = await strategy.plan(session, connection, connector)

    by_type = {d.params["data_type"]: d for d in descriptors}
    assert by_type["recently_played"].params["last_played_at"] == "2026-04-05T12:00:00Z"
    assert by_type["saved_tracks"].params["last_saved_at"] == "2026-04-05T12:00:00Z"
    assert by_type["followed_artists"].params["after_cursor"] == "abc123"

@pytest.mark.asyncio
async def test_no_watermarks_passes_none(self) -> None:
    """Without watermarks, params contain None for watermark fields."""
    strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
    session = AsyncMock()
    connector = AsyncMock()
    connection = _make_connection()  # empty sync_watermark

    descriptors = await strategy.plan(session, connection, connector)

    by_type = {d.params["data_type"]: d for d in descriptors}
    assert by_type["recently_played"].params.get("last_played_at") is None
    assert by_type["saved_tracks"].params.get("last_saved_at") is None
    assert by_type["followed_artists"].params.get("after_cursor") is None

@pytest.mark.asyncio
async def test_incremental_description(self) -> None:
    """Descriptions mention 'new' for incremental sync."""
    strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
    session = AsyncMock()
    connector = AsyncMock()
    connection = _make_connection(
        sync_watermark={
            "recently_played": {"last_played_at": "2026-04-05T12:00:00Z"},
        }
    )

    descriptors = await strategy.plan(session, connection, connector)

    by_type = {d.params["data_type"]: d for d in descriptors}
    assert "new" in by_type["recently_played"].description.lower()
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_spotify_strategy.py::TestSpotifyPlan -v`
Expected: FAIL

**Step 3: Update Spotify plan() to read watermarks**

Replace `plan()` in `src/resonance/sync/spotify.py` (lines 77-93):

```python
async def plan(
    self,
    session: sa_async.AsyncSession,
    connection: user_models.ServiceConnection,
    connector: connector_base.BaseConnector,
) -> list[sync_base.SyncTaskDescriptor]:
    """Create descriptors for followed_artists, saved_tracks, recently_played."""
    watermarks = connection.sync_watermark
    descriptors: list[sync_base.SyncTaskDescriptor] = []

    for data_type, base_description in _DATA_TYPE_DESCRIPTIONS.items():
        wm = watermarks.get(data_type, {})
        params: dict[str, object] = {"data_type": data_type}

        if data_type == "recently_played":
            params["last_played_at"] = wm.get("last_played_at")
        elif data_type == "saved_tracks":
            params["last_saved_at"] = wm.get("last_saved_at")
        elif data_type == "followed_artists":
            params["after_cursor"] = wm.get("after_cursor")

        has_watermark = any(v is not None for k, v in params.items() if k != "data_type")
        description = f"Fetching new {data_type.replace('_', ' ')}" if has_watermark else base_description

        descriptors.append(
            sync_base.SyncTaskDescriptor(
                task_type=types_module.SyncTaskType.TIME_RANGE,
                params=params,
                description=description,
            )
        )
    return descriptors
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_spotify_strategy.py -v`
Expected: All pass

**Step 5: Run full suite and type check**

Run: `uv run pytest -x -q && uv run mypy src/`
Expected: All pass

**Step 6: Commit**

```bash
git add src/resonance/sync/spotify.py tests/test_sync_spotify_strategy.py
git commit -m "feat: read Spotify watermarks from connection in plan()"
```

---

### Task 4: Add paginated Spotify connector methods with incremental support

**Files:**
- Modify: `src/resonance/connectors/spotify.py:118-220`
- Modify: `tests/test_spotify_connector.py`

The current connector methods (`get_followed_artists`, `get_saved_tracks`, `get_recently_played`) eagerly fetch everything into a list and return it. For incremental sync, we need:
- `get_saved_tracks` to return items page-by-page (or at least expose `total` and `added_at`)
- `get_recently_played` to accept an `after` timestamp
- `get_followed_artists` to accept an `after` cursor

**Step 1: Write failing tests for new connector parameters**

Add to `tests/test_spotify_connector.py`:

```python
class TestGetRecentlyPlayedIncremental:
    """Tests for get_recently_played with after parameter."""

    @pytest.mark.asyncio
    async def test_passes_after_param(self) -> None:
        """When after is provided, it's sent as a query parameter."""
        connector = spotify_module.SpotifyConnector(settings=_make_settings())
        response = _mock_response({"items": [], "cursors": {"after": None}})
        with patch.object(connector, "_request", new_callable=AsyncMock, return_value=response):
            await connector.get_recently_played("tok", after="1712345678000")
            connector._request.assert_called_once()
            call_kwargs = connector._request.call_args
            assert call_kwargs.kwargs["params"]["after"] == "1712345678000"
```

Add tests for `get_followed_artists` with `after` cursor parameter.

Add test for `SavedTrackPage` model that `get_saved_tracks_page` returns, including `total` and `added_at`.

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_spotify_connector.py -v -k incremental`
Expected: FAIL

**Step 3: Implement connector changes**

In `src/resonance/connectors/spotify.py`:

Add a `SavedTrackItem` model to expose `added_at`:

```python
class SavedTrackItem(pydantic.BaseModel):
    """A saved track with its added_at timestamp."""

    track: base_module.TrackData
    added_at: str


class SavedTrackPage(pydantic.BaseModel):
    """A page of saved tracks with total count."""

    items: list[SavedTrackItem]
    total: int
    next_url: str | None
```

Update `get_recently_played` to accept `after: str | None = None`:

```python
async def get_recently_played(
    self, access_token: str, *, after: str | None = None
) -> list[PlayedTrackItem]:
    """Get recently played tracks, optionally only after a timestamp."""
    logger.info("Fetching recently played tracks")
    params: dict[str, str | int] = {"limit": 50}
    if after is not None:
        params["after"] = after

    response = await self._request(
        "GET",
        f"{SPOTIFY_API_BASE}/me/player/recently-played",
        headers={"Authorization": f"Bearer {access_token}"},
        params=params,
    )
    data = response.json()
    # ... same item parsing ...
```

Add `get_saved_tracks_page` method for page-by-page fetching:

```python
async def get_saved_tracks_page(
    self,
    access_token: str,
    *,
    url: str | None = None,
    limit: int = 50,
) -> SavedTrackPage:
    """Fetch one page of saved tracks.

    Args:
        access_token: Spotify access token.
        url: Explicit next URL from previous page. If None, fetches first page.
        limit: Number of items per page (max 50).

    Returns:
        A SavedTrackPage with items, total count, and next_url.
    """
    target_url = url or f"{SPOTIFY_API_BASE}/me/tracks"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = await self._request(
        "GET",
        target_url,
        headers=headers,
        params={"limit": limit},
    )
    data = response.json()

    items: list[SavedTrackItem] = []
    for item in data["items"]:
        track = item["track"]
        primary_artist = track["artists"][0]
        items.append(
            SavedTrackItem(
                track=base_module.TrackData(
                    external_id=track["id"],
                    title=track["name"],
                    artist_external_id=primary_artist["id"],
                    artist_name=primary_artist["name"],
                    service=types_module.ServiceType.SPOTIFY,
                ),
                added_at=item["added_at"],
            )
        )

    return SavedTrackPage(
        items=items,
        total=data["total"],
        next_url=data.get("next"),
    )
```

Update `get_followed_artists` to accept `after: str | None = None` (it already uses `after` internally — just expose the initial value as a parameter):

```python
async def get_followed_artists(
    self, access_token: str, *, after: str | None = None
) -> list[base_module.ArtistData]:
```

Keep existing `get_saved_tracks` for backwards compatibility (used by existing sync flow) — it will be replaced in the Spotify execute task.

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_spotify_connector.py -v`
Expected: All pass

**Step 5: Run full suite and type check**

Run: `uv run pytest -x -q && uv run mypy src/`
Expected: All pass

**Step 6: Commit**

```bash
git add src/resonance/connectors/spotify.py tests/test_spotify_connector.py
git commit -m "feat: add incremental support to Spotify connector methods"
```

---

### Task 5: Implement Spotify incremental execute with stop-early and watermark output

**Files:**
- Modify: `src/resonance/sync/spotify.py:95-238`
- Modify: `tests/test_sync_spotify_strategy.py`

This is the largest task. The Spotify `execute()` and its helper functions need to:
1. Use watermarks from task params
2. Return watermark values in the result dict (for the worker to write back)
3. Implement stop-early for saved_tracks (all-duplicates-on-page)
4. Implement fast-finish for saved_tracks (total matches existing count)

**Step 1: Write failing tests for watermark output in results**

Add to `tests/test_sync_spotify_strategy.py`:

```python
class TestSpotifyWatermarkOutput:
    """Tests for watermark values in execute() results."""

    @pytest.mark.asyncio
    async def test_recently_played_returns_watermark(self) -> None:
        """Result includes last_played_at for watermark update."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "recently_played"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        played_items = [
            spotify_module.PlayedTrackItem(
                track=connector_base.TrackData(
                    external_id="t1", title="Song", artist_external_id="a1",
                    artist_name="Artist", service=types_module.ServiceType.SPOTIFY,
                ),
                played_at="2026-04-05T12:00:00Z",
            ),
        ]

        with (
            patch.object(strategy, "_get_access_token", new_callable=AsyncMock, return_value="tok"),
            patch.object(
                sync_spotify_module, "_sync_recently_played",
                new_callable=AsyncMock, return_value=(1, {"last_played_at": "2026-04-05T12:00:00Z"}),
            ),
        ):
            result = await strategy.execute(session, task, connector)

        assert result.get("watermark") == {"last_played_at": "2026-04-05T12:00:00Z"}

    @pytest.mark.asyncio
    async def test_saved_tracks_returns_watermark(self) -> None:
        """Result includes last_saved_at for watermark update."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(strategy, "_get_access_token", new_callable=AsyncMock, return_value="tok"),
            patch.object(
                sync_spotify_module, "_sync_saved_tracks",
                new_callable=AsyncMock, return_value=(5, 2, {"last_saved_at": "2026-04-05T12:00:00Z"}),
            ),
        ):
            result = await strategy.execute(session, task, connector)

        assert result.get("watermark") == {"last_saved_at": "2026-04-05T12:00:00Z"}

    @pytest.mark.asyncio
    async def test_followed_artists_returns_watermark(self) -> None:
        """Result includes after_cursor for watermark update."""
        strategy = sync_spotify_module.SpotifySyncStrategy(_TEST_ENCRYPTION_KEY)
        session = AsyncMock()
        task = _make_task(params={"data_type": "followed_artists"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        with (
            patch.object(strategy, "_get_access_token", new_callable=AsyncMock, return_value="tok"),
            patch.object(
                sync_spotify_module, "_sync_followed_artists",
                new_callable=AsyncMock, return_value=(2, 1, {"after_cursor": "artist123"}),
            ),
        ):
            result = await strategy.execute(session, task, connector)

        assert result.get("watermark") == {"after_cursor": "artist123"}
```

Add tests for stop-early behavior:

```python
class TestSavedTracksStopEarly:
    """Tests for saved_tracks stop-early behavior."""

    @pytest.mark.asyncio
    async def test_stops_when_all_page_items_are_duplicates(self) -> None:
        """Stops pagination when every item on a page already exists."""
        # Test the _sync_saved_tracks function directly
        session = AsyncMock()
        session.no_autoflush = MagicMock()
        session.no_autoflush.__enter__ = MagicMock(return_value=None)
        session.no_autoflush.__exit__ = MagicMock(return_value=False)
        task = _make_task(params={"data_type": "saved_tracks"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        page1_items = [
            spotify_module.SavedTrackItem(
                track=connector_base.TrackData(
                    external_id=f"t{i}", title=f"Song {i}",
                    artist_external_id=f"a{i}", artist_name=f"Artist {i}",
                    service=types_module.ServiceType.SPOTIFY,
                ),
                added_at=f"2026-04-0{i}T12:00:00Z",
            )
            for i in range(1, 4)
        ]
        page1 = spotify_module.SavedTrackPage(items=page1_items, total=100, next_url="next")

        # All items already exist (upsert returns False = not created)
        page2_items = [
            spotify_module.SavedTrackItem(
                track=connector_base.TrackData(
                    external_id=f"t{i}", title=f"Song {i}",
                    artist_external_id=f"a{i}", artist_name=f"Artist {i}",
                    service=types_module.ServiceType.SPOTIFY,
                ),
                added_at=f"2026-03-0{i}T12:00:00Z",
            )
            for i in range(1, 4)
        ]
        page2 = spotify_module.SavedTrackPage(items=page2_items, total=100, next_url="next2")

        connector.get_saved_tracks_page = AsyncMock(side_effect=[page1, page2])

        with (
            patch.object(
                sync_spotify_module.runner_module, "_upsert_artist_from_track",
                new_callable=AsyncMock,
            ),
            patch.object(
                sync_spotify_module.runner_module, "_upsert_track",
                new_callable=AsyncMock,
                # page1: all new (True), page2: all duplicates (False)
                side_effect=[True, True, True, False, False, False],
            ),
            patch.object(
                sync_spotify_module.runner_module, "_upsert_user_track_relation",
                new_callable=AsyncMock,
            ),
        ):
            created, updated, watermark = await sync_spotify_module._sync_saved_tracks(
                session, task, connector, "tok"
            )

        # Only 2 pages fetched — stopped after page 2 was all duplicates
        assert connector.get_saved_tracks_page.call_count == 2
        assert created == 3
        assert updated == 3

    @pytest.mark.asyncio
    async def test_fast_finish_when_total_matches(self) -> None:
        """Skips processing when total matches existing track count."""
        session = AsyncMock()
        task = _make_task(params={"data_type": "saved_tracks"})
        connector = MagicMock(spec=spotify_module.SpotifyConnector)

        # First page returns total=50, and we already have 50 tracks for this connection
        page1 = spotify_module.SavedTrackPage(items=[], total=50, next_url=None)
        connector.get_saved_tracks_page = AsyncMock(return_value=page1)

        # Mock the count query to return 50 existing tracks
        count_result = MagicMock()
        count_result.scalar_one.return_value = 50
        session.execute = AsyncMock(return_value=count_result)

        created, updated, watermark = await sync_spotify_module._sync_saved_tracks(
            session, task, connector, "tok"
        )

        assert created == 0
        assert updated == 0
        # Only one page fetch (to get total), no further processing
        assert connector.get_saved_tracks_page.call_count == 1
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_sync_spotify_strategy.py -v -k "watermark or stop_early or fast_finish"`
Expected: FAIL

**Step 3: Implement updated execute() and helper functions**

Update `execute()` in `src/resonance/sync/spotify.py` to capture watermark from helpers and include in result:

```python
async def execute(
    self,
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: connector_base.BaseConnector,
) -> dict[str, object]:
    sp_connector = _cast_connector(connector)
    data_type = str(task.params.get("data_type", ""))

    if data_type not in _DATA_TYPE_DESCRIPTIONS:
        logger.warning("unknown_spotify_data_type", data_type=data_type)

    access_token = await self._get_access_token(session, task, sp_connector)

    items_created = 0
    items_updated = 0
    watermark: dict[str, object] = {}

    try:
        if data_type == "followed_artists":
            items_created, items_updated, watermark = await _sync_followed_artists(
                session, task, sp_connector, access_token
            )
        elif data_type == "saved_tracks":
            items_created, items_updated, watermark = await _sync_saved_tracks(
                session, task, sp_connector, access_token
            )
        elif data_type == "recently_played":
            items_created, watermark = await _sync_recently_played(
                session, task, sp_connector, access_token
            )
    except connector_base.RateLimitExceededError as exc:
        raise sync_base.DeferRequest(
            retry_after=exc.retry_after,
            resume_params={
                "data_type": data_type,
                "items_created": items_created,
                "items_updated": items_updated,
            },
        ) from exc

    await session.commit()
    result: dict[str, object] = {
        "items_created": items_created,
        "items_updated": items_updated,
        "watermark": watermark,
    }
    logger.info(
        "spotify_range_completed",
        data_type=data_type,
        items_created=items_created,
        items_updated=items_updated,
    )
    return result
```

Rewrite `_sync_followed_artists` to return watermark and accept `after` cursor from params:

```python
async def _sync_followed_artists(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> tuple[int, int, dict[str, object]]:
    """Fetch followed artists and upsert. Returns (created, updated, watermark)."""
    after_cursor = task.params.get("after_cursor")
    after = str(after_cursor) if after_cursor is not None else None
    artists = await connector.get_followed_artists(access_token, after=after)
    logger.info("spotify_artists_fetched", count=len(artists))
    created = 0
    updated = 0
    last_cursor: str | None = None
    for artist_data in artists:
        with session.no_autoflush:
            was_created = await runner_module._upsert_artist(session, artist_data)
            await session.flush()
            if was_created:
                created += 1
            else:
                updated += 1
            await runner_module._upsert_user_artist_relation(
                session, task.user_id, artist_data, task.service_connection_id
            )
        if artist_data.external_id:
            last_cursor = artist_data.external_id
    watermark: dict[str, object] = {}
    if last_cursor is not None:
        watermark["after_cursor"] = last_cursor
    return created, updated, watermark
```

Rewrite `_sync_saved_tracks` with page-by-page fetching, fast-finish, and stop-early:

```python
async def _sync_saved_tracks(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> tuple[int, int, dict[str, object]]:
    """Fetch saved tracks page-by-page with stop-early. Returns (created, updated, watermark)."""
    created = 0
    updated = 0
    watermark: dict[str, object] = {}
    next_url: str | None = None
    first_page = True

    while True:
        page = await connector.get_saved_tracks_page(
            access_token, url=next_url
        )

        # Capture watermark from first item on first page (newest saved)
        if first_page and page.items:
            watermark["last_saved_at"] = page.items[0].added_at

        # Fast-finish: if total matches existing count, nothing new
        if first_page:
            first_page = False
            existing_count_result = await session.execute(
                sa.select(sa.func.count()).where(
                    models_module.UserTrackRelation.source_connection_id == task.service_connection_id,
                    models_module.UserTrackRelation.relation_type == types_module.TrackRelationType.LIKE,
                )
            )
            existing_count: int = existing_count_result.scalar_one()
            if page.total == existing_count:
                logger.info("saved_tracks_fast_finish", total=page.total)
                task.progress_total = page.total
                task.progress_current = page.total
                return created, updated, watermark

            task.progress_total = page.total

        if not page.items:
            break

        # Process page and track duplicates
        page_all_duplicates = True
        for item in page.items:
            with session.no_autoflush:
                await runner_module._upsert_artist_from_track(session, item.track)
                await session.flush()
                was_created = await runner_module._upsert_track(session, item.track)
                await session.flush()
                if was_created:
                    created += 1
                    page_all_duplicates = False
                else:
                    updated += 1
                await runner_module._upsert_user_track_relation(
                    session, task.user_id, item.track, task.service_connection_id
                )

        task.progress_current = created + updated
        await session.commit()

        # Stop-early: all items on this page already existed
        if page_all_duplicates:
            logger.info("saved_tracks_stop_early", created=created, updated=updated)
            break

        next_url = page.next_url
        if next_url is None:
            break

    return created, updated, watermark
```

Rewrite `_sync_recently_played` to accept `after` from params and return watermark:

```python
async def _sync_recently_played(
    session: sa_async.AsyncSession,
    task: task_module.SyncTask,
    connector: spotify_module.SpotifyConnector,
    access_token: str,
) -> tuple[int, dict[str, object]]:
    """Fetch recently played and upsert. Returns (created, watermark)."""
    after_param = task.params.get("last_played_at")
    after = str(after_param) if after_param is not None else None
    played_items = await connector.get_recently_played(access_token, after=after)
    logger.info("spotify_recent_fetched", count=len(played_items))
    created = 0
    watermark: dict[str, object] = {}
    if played_items:
        watermark["last_played_at"] = played_items[0].played_at
    for played_item in played_items:
        with session.no_autoflush:
            await runner_module._upsert_artist_from_track(session, played_item.track)
            await session.flush()
            await runner_module._upsert_track(session, played_item.track)
            await session.flush()
            await runner_module._upsert_listening_event(
                session, task.user_id, played_item.track, played_item.played_at
            )
        created += 1
    return created, watermark
```

Note: `_sync_saved_tracks` needs additional imports at the top of spotify.py:

```python
import resonance.models as models_module
```

**Step 4: Update existing tests for new return signatures**

Update `TestSpotifyExecute.test_followed_artists_dispatches_to_connector` — the mock return value changes from `(2, 1)` to `(2, 1, {"after_cursor": "abc"})`.

Update `TestSpotifyExecute.test_unknown_data_type_returns_zero_counts` — result now has `"watermark"` key.

Update `TestSpotifyExecute.test_rate_limit_raises_defer_request` — mock should use new return signature.

**Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_spotify_strategy.py -v`
Expected: All pass

**Step 6: Run full suite and type check**

Run: `uv run pytest -x -q && uv run mypy src/`
Expected: All pass

**Step 7: Commit**

```bash
git add src/resonance/sync/spotify.py tests/test_sync_spotify_strategy.py
git commit -m "feat: implement incremental Spotify sync with stop-early and watermark output"
```

---

### Task 6: Write watermarks back to connection in worker

**Files:**
- Modify: `src/resonance/worker.py:219-224`
- Modify: `tests/test_worker.py`

**Step 1: Write failing test**

Add to `tests/test_worker.py`:

```python
class TestSyncRangeWatermarkWrite:
    """Tests for watermark write-back after successful sync_range."""

    @pytest.mark.asyncio
    async def test_writes_watermark_to_connection(self) -> None:
        """On success, watermark from result is saved to connection.sync_watermark."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()
        user_id = uuid.uuid4()
        parent_id = uuid.uuid4()

        task = task_module.SyncTask(
            id=task_id,
            user_id=user_id,
            service_connection_id=conn_id,
            parent_id=parent_id,
            task_type=types_module.SyncTaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": "recently_played"},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id
        connection.sync_watermark = {}

        session = AsyncMock()

        # 1. _load_task
        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task

        # 2. Load connection (in sync_range)
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection

        # 3. _check_parent_completion: pending count
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 0

        # 4. Load parent
        parent_task = _make_task(task_id=parent_id, status=types_module.SyncStatus.RUNNING)
        parent_result = MagicMock()
        parent_result.scalar_one_or_none.return_value = parent_task

        # 5. failed count
        failed_result = MagicMock()
        failed_result.scalar_one.return_value = 0

        # 6. children
        children_scalars = MagicMock()
        children_scalars.all.return_value = [task]
        children_result = MagicMock()
        children_result.scalars.return_value = children_scalars

        session.execute.side_effect = [
            task_result, conn_result,
            pending_result, parent_result, failed_result, children_result,
        ]

        # Strategy returns result with watermark
        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.return_value = {
            "items_created": 5,
            "items_updated": 0,
            "watermark": {"last_played_at": "2026-04-05T12:00:00Z"},
        }

        mock_connector = MagicMock()
        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = mock_connector

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.SPOTIFY: mock_strategy},
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        # Watermark should be written to connection
        assert connection.sync_watermark["recently_played"] == {"last_played_at": "2026-04-05T12:00:00Z"}

    @pytest.mark.asyncio
    async def test_no_watermark_in_result_skips_write(self) -> None:
        """When result has no 'watermark' key, connection is not modified."""
        task_id = uuid.uuid4()
        conn_id = uuid.uuid4()

        task = task_module.SyncTask(
            id=task_id,
            user_id=uuid.uuid4(),
            service_connection_id=conn_id,
            parent_id=uuid.uuid4(),
            task_type=types_module.SyncTaskType.TIME_RANGE,
            status=types_module.SyncStatus.PENDING,
            params={"data_type": "followed_artists"},
        )

        connection = MagicMock(spec=user_models.ServiceConnection)
        connection.service_type = types_module.ServiceType.SPOTIFY
        connection.id = conn_id
        connection.sync_watermark = {"existing": {"key": "value"}}

        session = AsyncMock()

        task_result = MagicMock()
        task_result.scalar_one_or_none.return_value = task
        conn_result = MagicMock()
        conn_result.scalar_one.return_value = connection
        pending_result = MagicMock()
        pending_result.scalar_one.return_value = 1
        next_result = MagicMock()
        next_result.scalar_one_or_none.return_value = None

        session.execute.side_effect = [
            task_result, conn_result, pending_result, next_result,
        ]

        mock_strategy = AsyncMock(spec=sync_base.SyncStrategy)
        mock_strategy.concurrency = "sequential"
        mock_strategy.execute.return_value = {"items_created": 0, "items_updated": 0}

        mock_connector_registry = MagicMock()
        mock_connector_registry.get.return_value = MagicMock()

        ctx: dict[str, Any] = {
            "session_factory": _mock_session_factory(session),
            "connector_registry": mock_connector_registry,
            "strategies": {types_module.ServiceType.SPOTIFY: mock_strategy},
            "redis": AsyncMock(),
        }

        await worker_module.sync_range(ctx, str(task_id))

        # Existing watermark should be untouched
        assert connection.sync_watermark == {"existing": {"key": "value"}}
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_worker.py::TestSyncRangeWatermarkWrite -v`
Expected: FAIL

**Step 3: Add watermark write-back to sync_range**

In `src/resonance/worker.py`, after the successful completion block (after line 224 — `log.info("sync_range_completed", result=task.result)`), add watermark write-back:

```python
try:
    result = await strategy.execute(session, task, connector)
    task.status = types_module.SyncStatus.COMPLETED
    task.result = result
    task.completed_at = datetime.datetime.now(datetime.UTC)

    # Write watermark back to connection
    watermark = result.get("watermark")
    if watermark and isinstance(watermark, dict):
        data_type = str(task.params.get("data_type", ""))
        data_type_key = data_type or task.params.get("username", "listens")
        # For ListenBrainz, the key is "listens"
        if connection.service_type == types_module.ServiceType.LISTENBRAINZ:
            data_type_key = "listens"
        updated_watermarks = dict(connection.sync_watermark)
        updated_watermarks[data_type_key] = watermark
        connection.sync_watermark = updated_watermarks

    await session.commit()
    log.info("sync_range_completed", result=task.result)
```

Note: The `connection` variable is already loaded in `sync_range` at line 205 — no additional query needed.

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_worker.py -v`
Expected: All pass

**Step 5: Run full suite and type check**

Run: `uv run pytest -x -q && uv run mypy src/`
Expected: All pass

**Step 6: Commit**

```bash
git add src/resonance/worker.py tests/test_worker.py
git commit -m "feat: write watermarks back to connection on sync completion"
```

---

### Task 7: Update ListenBrainz execute() to return watermark for write-back

**Files:**
- Modify: `src/resonance/sync/listenbrainz.py:192-197`
- Modify: `tests/test_sync_listenbrainz.py`

**Step 1: Write failing test**

Add to `tests/test_sync_listenbrainz.py` in `TestExecute`:

```python
@pytest.mark.asyncio
async def test_result_includes_watermark_dict(self) -> None:
    """Result includes a 'watermark' dict for the worker to write back."""
    strategy = lb_sync_module.ListenBrainzSyncStrategy()
    session = AsyncMock()
    session.no_autoflush = MagicMock()
    session.no_autoflush.__enter__ = MagicMock(return_value=None)
    session.no_autoflush.__exit__ = MagicMock(return_value=False)
    task = _make_task(params={"username": "testuser"})
    connector = _make_lb_connector()

    listen1 = _make_listen(1700000100, "Song A", "Artist A")
    connector.get_listens = AsyncMock(side_effect=[[listen1], []])

    with (
        patch.object(lb_sync_module.runner_module, "_upsert_artist_from_track", new_callable=AsyncMock),
        patch.object(lb_sync_module.runner_module, "_upsert_track", new_callable=AsyncMock),
        patch.object(lb_sync_module.runner_module, "_upsert_listening_event", new_callable=AsyncMock),
    ):
        result = await strategy.execute(session, task, connector)

    assert result["watermark"] == {"last_listened_at": 1700000100}
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_sync_listenbrainz.py::TestExecute::test_result_includes_watermark_dict -v`
Expected: FAIL — no `watermark` key in result

**Step 3: Add watermark to ListenBrainz execute() result**

In `src/resonance/sync/listenbrainz.py`, update the result building at the end of `execute()` (lines 192-197):

```python
result: dict[str, object] = {"items_created": items_created}
if last_listened_at is not None:
    result["last_listened_at"] = last_listened_at
    result["watermark"] = {"last_listened_at": last_listened_at}
if page_limit_reached:
    result["page_limit_reached"] = True
return result
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_sync_listenbrainz.py -v`
Expected: All pass

**Step 5: Run full suite and type check**

Run: `uv run pytest -x -q && uv run mypy src/`
Expected: All pass

**Step 6: Commit**

```bash
git add src/resonance/sync/listenbrainz.py tests/test_sync_listenbrainz.py
git commit -m "feat: include watermark dict in ListenBrainz execute() result"
```

---

### Task 8: Final integration test and cleanup

**Files:**
- Review all modified files for consistency
- Run full test suite, type checking, linting

**Step 1: Run the complete validation suite**

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/
uv run pytest -x -v
```

Expected: All pass with zero errors

**Step 2: Remove unused get_saved_tracks method (optional)**

If the old `get_saved_tracks` in `connectors/spotify.py` is no longer called anywhere (replaced by `get_saved_tracks_page`), remove it. Search first:

```bash
uv run ruff check . && grep -r "get_saved_tracks\b" src/ tests/ --include="*.py" | grep -v "get_saved_tracks_page"
```

If only the old connector method and its test remain, remove both. If anything else references it, keep it.

**Step 3: Commit any cleanup**

```bash
git add -u
git commit -m "chore: remove unused get_saved_tracks after incremental sync migration"
```
