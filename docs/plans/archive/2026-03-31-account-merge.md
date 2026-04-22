# Account Merge Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** When a logged-in user connects a service that already belongs to a different account, offer to merge the two accounts instead of silently switching users.

**Architecture:** The auth callback detects cross-user conflicts and redirects to a merge confirmation page. The merge page shows what will be transferred. On confirmation, all data from the source account is reassigned to the current user in a single transaction, with duplicate relation handling, and the orphan account is deleted.

**Tech Stack:** FastAPI, SQLAlchemy async, Jinja2, HTMX

---

### Task 1: Auth Callback Conflict Detection

**Files:**
- Modify: `src/resonance/api/v1/auth.py`
- Modify: `tests/test_api_auth.py`

**Step 1: Write the failing test**

Add to `tests/test_api_auth.py`:

```python
class TestAuthMergeDetection:
    async def test_conflict_redirects_to_merge_page(self):
        """When a logged-in user connects a service owned by another user,
        redirect to /merge instead of silently switching."""
        # This test needs:
        # 1. A logged-in session (user A)
        # 2. An OAuth callback returning an external_user_id
        #    that belongs to a ServiceConnection owned by user B
        # 3. Verify redirect to /merge
        ...
```

The exact test setup will follow the patterns already established in `test_api_auth.py` (FakeRedis, FakeAsyncSession, mock connectors).

**Step 2: Modify auth callback**

In `auth_callback`, after finding `existing_connection` (line 173), add a conflict check:

```python
if existing_connection is not None:
    current_user_id = session.get("user_id")

    # CONFLICT: connection belongs to a different user
    if (current_user_id is not None
            and existing_connection.user_id != uuid.UUID(current_user_id)):
        # Store merge details in session for the merge page
        session["merge_source_user_id"] = str(existing_connection.user_id)
        session["merge_service_type"] = service_type.value
        session["merge_connection_id"] = str(existing_connection.id)
        # Update tokens on the existing connection so they're fresh
        existing_connection.encrypted_access_token = encrypted_access
        existing_connection.encrypted_refresh_token = encrypted_refresh
        existing_connection.token_expires_at = token_expires_at
        existing_connection.scopes = tokens.scope
        await db.commit()
        # Clear OAuth state
        session["oauth_state"] = None
        session["oauth_service"] = None
        return fastapi_responses.RedirectResponse(
            url="/merge", status_code=307
        )

    # Normal returning user — update tokens
    existing_connection.encrypted_access_token = encrypted_access
    ...
```

The key insight: we update the tokens on the existing connection (so they're fresh after the OAuth flow) but do NOT change the session user_id. The user stays logged in as themselves, and the merge page uses the session merge data.

**Step 3: Run tests, lint, commit**

```bash
git add src/resonance/api/v1/auth.py tests/test_api_auth.py
git commit -m "feat: detect cross-user service conflicts in auth callback"
```

---

### Task 2: Merge Data Function

**Files:**
- Create: `src/resonance/merge.py`
- Test: `tests/test_merge.py`

**Step 1: Write the failing tests**

Create `tests/test_merge.py`:

```python
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import resonance.merge as merge_module


class TestMergeAccounts:
    async def test_reassigns_connections(self) -> None:
        """ServiceConnections should move from source to target user."""
        ...

    async def test_reassigns_listening_events(self) -> None:
        """ListeningEvents should move from source to target user."""
        ...

    async def test_skips_duplicate_artist_relations(self) -> None:
        """Duplicate UserArtistRelations should be deleted, not moved."""
        ...

    async def test_skips_duplicate_track_relations(self) -> None:
        """Duplicate UserTrackRelations should be deleted, not moved."""
        ...

    async def test_reassigns_sync_jobs(self) -> None:
        """SyncJobs should move from source to target user."""
        ...

    async def test_deletes_source_user(self) -> None:
        """Source user should be deleted after merge."""
        ...

    async def test_returns_merge_stats(self) -> None:
        """Should return counts of what was moved."""
        ...
```

**Step 2: Write implementation**

Create `src/resonance/merge.py`:

```python
"""Account merge — reassign all data from one user to another."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

import sqlalchemy as sa

import resonance.models as models_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


@dataclass
class MergeStats:
    """Counts of data moved during a merge."""

    connections_moved: int = 0
    events_moved: int = 0
    artist_relations_moved: int = 0
    artist_relations_skipped: int = 0
    track_relations_moved: int = 0
    track_relations_skipped: int = 0
    sync_jobs_moved: int = 0


async def get_account_summary(
    session: AsyncSession, user_id: uuid.UUID
) -> dict[str, int]:
    """Get data counts for an account (for merge confirmation display)."""
    counts: dict[str, int] = {}

    for label, model, filter_col in [
        ("connections", models_module.ServiceConnection,
         models_module.ServiceConnection.user_id),
        ("listening_events", models_module.ListeningEvent,
         models_module.ListeningEvent.user_id),
        ("artist_relations", models_module.UserArtistRelation,
         models_module.UserArtistRelation.user_id),
        ("track_relations", models_module.UserTrackRelation,
         models_module.UserTrackRelation.user_id),
        ("sync_jobs", models_module.SyncJob,
         models_module.SyncJob.user_id),
    ]:
        result = await session.execute(
            sa.select(sa.func.count()).select_from(model).where(
                filter_col == user_id
            )
        )
        counts[label] = result.scalar_one()

    return counts


async def merge_accounts(
    session: AsyncSession,
    target_user_id: uuid.UUID,
    source_user_id: uuid.UUID,
) -> MergeStats:
    """Merge source_user into target_user.

    Moves all data from source to target, handling duplicate
    relations by deleting the source copy. Deletes the source
    user after all data is reassigned.

    Must be called within a transaction (caller commits).
    """
    stats = MergeStats()

    # 1. Move ServiceConnections
    result = await session.execute(
        sa.update(models_module.ServiceConnection)
        .where(models_module.ServiceConnection.user_id == source_user_id)
        .values(user_id=target_user_id)
    )
    stats.connections_moved = result.rowcount  # type: ignore[assignment]

    # 2. Move ListeningEvents
    result = await session.execute(
        sa.update(models_module.ListeningEvent)
        .where(models_module.ListeningEvent.user_id == source_user_id)
        .values(user_id=target_user_id)
    )
    stats.events_moved = result.rowcount  # type: ignore[assignment]

    # 3. Move UserArtistRelations (skip duplicates)
    # Find source relations that would duplicate target relations
    source_relations = await session.execute(
        sa.select(models_module.UserArtistRelation).where(
            models_module.UserArtistRelation.user_id == source_user_id
        )
    )
    for rel in source_relations.scalars().all():
        # Check if target already has this relation
        dup_check = await session.execute(
            sa.select(models_module.UserArtistRelation).where(
                models_module.UserArtistRelation.user_id == target_user_id,
                models_module.UserArtistRelation.artist_id == rel.artist_id,
                models_module.UserArtistRelation.relation_type == rel.relation_type,
                models_module.UserArtistRelation.source_service == rel.source_service,
            )
        )
        if dup_check.scalar_one_or_none() is not None:
            await session.delete(rel)
            stats.artist_relations_skipped += 1
        else:
            rel.user_id = target_user_id
            stats.artist_relations_moved += 1

    # 4. Move UserTrackRelations (skip duplicates)
    source_track_rels = await session.execute(
        sa.select(models_module.UserTrackRelation).where(
            models_module.UserTrackRelation.user_id == source_user_id
        )
    )
    for rel in source_track_rels.scalars().all():
        dup_check = await session.execute(
            sa.select(models_module.UserTrackRelation).where(
                models_module.UserTrackRelation.user_id == target_user_id,
                models_module.UserTrackRelation.track_id == rel.track_id,
                models_module.UserTrackRelation.relation_type == rel.relation_type,
                models_module.UserTrackRelation.source_service == rel.source_service,
            )
        )
        if dup_check.scalar_one_or_none() is not None:
            await session.delete(rel)
            stats.track_relations_skipped += 1
        else:
            rel.user_id = target_user_id
            stats.track_relations_moved += 1

    # 5. Move SyncJobs
    result = await session.execute(
        sa.update(models_module.SyncJob)
        .where(models_module.SyncJob.user_id == source_user_id)
        .values(user_id=target_user_id)
    )
    stats.sync_jobs_moved = result.rowcount  # type: ignore[assignment]

    # 6. Delete source user
    await session.execute(
        sa.delete(models_module.User).where(
            models_module.User.id == source_user_id
        )
    )

    logger.info(
        "Merged user %s into %s: %s",
        source_user_id, target_user_id, stats,
    )

    return stats
```

**Step 3: Run tests, lint, type check, commit**

```bash
git add src/resonance/merge.py tests/test_merge.py
git commit -m "feat: add account merge function with duplicate relation handling"
```

---

### Task 3: Merge UI Page

**Files:**
- Create: `src/resonance/templates/merge.html`
- Modify: `src/resonance/ui/routes.py`
- Modify: `tests/test_ui.py`

**Step 1: Write the failing test**

Add to `tests/test_ui.py`:

```python
class TestMergePage:
    async def test_merge_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/merge", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"

    async def test_merge_without_session_data_redirects_to_account(
        self, client: httpx.AsyncClient
    ) -> None:
        """If no merge data in session, redirect to account page."""
        # Need an authenticated client without merge session data
        ...
```

**Step 2: Add merge routes**

Add to `src/resonance/ui/routes.py`:

```python
@router.get("/merge", response_model=None)
async def merge_page(request: fastapi.Request):
    session = request.state.session
    user_id = session.get("user_id")
    if not user_id:
        return RedirectResponse(url="/login", status_code=307)

    source_user_id = session.get("merge_source_user_id")
    if not source_user_id:
        return RedirectResponse(url="/account", status_code=307)

    # Get data counts for the source account
    async with request.app.state.session_factory() as db:
        source_summary = await merge_module.get_account_summary(
            db, uuid.UUID(source_user_id)
        )
        service_type = session.get("merge_service_type", "unknown")

    return templates.TemplateResponse(
        request,
        "merge.html",
        {
            "user_id": user_id,
            "source_summary": source_summary,
            "service_type": service_type,
        },
    )


@router.post("/merge", response_model=None)
async def merge_confirm(request: fastapi.Request):
    session = request.state.session
    user_id = session.get("user_id")
    if not user_id:
        return RedirectResponse(url="/login", status_code=307)

    source_user_id = session.get("merge_source_user_id")
    if not source_user_id:
        return RedirectResponse(url="/account", status_code=307)

    async with request.app.state.session_factory() as db:
        stats = await merge_module.merge_accounts(
            db,
            target_user_id=uuid.UUID(user_id),
            source_user_id=uuid.UUID(source_user_id),
        )
        await db.commit()

    # Clear merge session data
    session["merge_source_user_id"] = None
    session["merge_service_type"] = None
    session["merge_connection_id"] = None

    return RedirectResponse(url="/account", status_code=307)
```

**Step 3: Create merge template**

Create `src/resonance/templates/merge.html`:

```html
{% extends "base.html" %}

{% block title %}Merge Accounts — Resonance{% endblock %}

{% block content %}
<h1>Merge Accounts</h1>

<article>
    <header>Account Conflict Detected</header>
    <p>
        The <strong>{{ service_type | capitalize }}</strong> account you just
        connected is already linked to a different Resonance account.
    </p>
    <p>
        You can merge that account's data into your current account.
        The other account will be deleted after the merge.
    </p>
</article>

<h2>Data to be merged</h2>
<table>
    <tbody>
        <tr>
            <td>Service connections</td>
            <td>{{ source_summary.connections }}</td>
        </tr>
        <tr>
            <td>Listening events</td>
            <td>{{ source_summary.listening_events }}</td>
        </tr>
        <tr>
            <td>Artist relations</td>
            <td>{{ source_summary.artist_relations }}</td>
        </tr>
        <tr>
            <td>Track relations</td>
            <td>{{ source_summary.track_relations }}</td>
        </tr>
        <tr>
            <td>Sync jobs</td>
            <td>{{ source_summary.sync_jobs }}</td>
        </tr>
    </tbody>
</table>

<form method="post" action="/merge">
    <div class="grid">
        <button type="submit">Merge into my account</button>
        <a href="/account" role="button" class="secondary outline">Cancel</a>
    </div>
</form>
{% endblock %}
```

**Step 4: Run tests, lint, commit**

```bash
git add src/resonance/templates/merge.html src/resonance/ui/routes.py tests/test_ui.py
git commit -m "feat: add merge confirmation page with data summary"
```

---

## Summary

After all 3 tasks:

| Component | What |
|-----------|------|
| Auth callback | Detects when connecting a service owned by another user, redirects to /merge |
| Merge function | Reassigns all data from source to target user, handles duplicate relations, deletes orphan |
| Merge UI | Confirmation page showing what will be merged, with merge/cancel buttons |

**Flow:**
1. Logged in as User A → Connect ListenBrainz → OAuth succeeds
2. Auth callback finds ListenBrainz connection belongs to User B
3. Stores merge details in session, redirects to /merge
4. Merge page shows User B's data counts
5. Click "Merge" → all data moves to User A, User B deleted
6. Redirect to /account showing both connections

**Not in scope:**
- Email verification (future)
- Undo merge
- Admin merge tools
