# Admin Functionality and Test Connector Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a role-based access control system (owner/admin/user), an admin UI for user management and task debugging, a test service connector, and task cloning with step-through mode.

**Architecture:** Add `UserRole` enum and `role` column to User model. First registered user becomes owner. Admin-gated dependencies enforce access control. Test connector implements `SyncStrategy` with deterministic data generation. Task cloning creates new SyncTasks from existing ones with optional step-through mode.

**Tech Stack:** Python 3.14, FastAPI, SQLAlchemy 2.0 (async), Alembic, Jinja2, HTMX, arq

---

### Task 1: UserRole Enum + User Model Migration

**Files:**
- Modify: `src/resonance/types.py`
- Modify: `src/resonance/models/user.py`
- Create: `alembic/versions/XXXX_add_user_role.py` (via autogenerate)
- Test: `tests/test_models.py`

**Step 1: Write the failing tests**

Add to `tests/test_models.py`:

```python
class TestUserRole:
    def test_user_role_values(self) -> None:
        assert types_module.UserRole.USER == "user"
        assert types_module.UserRole.ADMIN == "admin"
        assert types_module.UserRole.OWNER == "owner"

class TestUserModelRole:
    def test_user_has_role_column(self) -> None:
        user = models_module.User(
            id=uuid.uuid4(),
            display_name="Test",
        )
        assert user.role == types_module.UserRole.USER

    def test_user_role_default_is_user(self) -> None:
        user = models_module.User(
            id=uuid.uuid4(),
            display_name="Test",
        )
        assert user.role == types_module.UserRole.USER
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_models.py::TestUserRole -v`
Expected: FAIL — `AttributeError: module 'resonance.types' has no attribute 'UserRole'`

**Step 3: Add UserRole enum to types.py**

Add after the existing enums:

```python
class UserRole(enum.StrEnum):
    """User authorization roles."""

    USER = "user"
    ADMIN = "admin"
    OWNER = "owner"
```

**Step 4: Add role field to User model**

In `src/resonance/models/user.py`, add after the `timezone` field:

```python
role: orm.Mapped[types_module.UserRole] = orm.mapped_column(
    sa.Enum(types_module.UserRole, native_enum=False),
    nullable=False,
    server_default="user",
    default=types_module.UserRole.USER,
)
```

**Step 5: Run tests to verify they pass**

Run: `uv run pytest tests/test_models.py -v`

**Step 6: Generate Alembic migration**

Port-forward to PostgreSQL and generate:
```bash
kubectl --context=megadoomer-do -n resonance port-forward svc/resonance-postgresql 5432:5432 &
PGHOST=localhost PGPORT=5432 PGUSER=resonance PGDATABASE=resonance \
  PGPASSWORD=$(kubectl --context=megadoomer-do -n resonance get secret resonance-db-credentials -o jsonpath='{.data.password}' | base64 -d) \
  uv run alembic revision --autogenerate -m "add user role column"
kill %1
```

Review the generated migration. It should add a `role` varchar column with default `user`.

After the column is added, update the first user (earliest `created_at`) to `owner`:
```python
# Add to the upgrade() function after the column add:
op.execute("""
    UPDATE users SET role = 'owner'
    WHERE id = (SELECT id FROM users ORDER BY created_at ASC LIMIT 1)
""")
```

**Step 7: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`

**Step 8: Commit**

```bash
git add src/resonance/types.py src/resonance/models/user.py alembic/versions/ tests/test_models.py
git commit -m "feat: add UserRole enum and role column to User model (#30)"
```

---

### Task 2: Auth Dependencies (require_admin, require_owner)

**Files:**
- Modify: `src/resonance/dependencies.py`
- Test: `tests/test_dependencies.py`

**Step 1: Write the failing tests**

Create `tests/test_dependencies.py` (or add to existing):

```python
import uuid
import pytest
import fastapi

import resonance.dependencies as deps_module
import resonance.types as types_module


class TestRequireAdmin:
    def test_admin_passes(self) -> None:
        # Should not raise
        deps_module.require_admin(types_module.UserRole.ADMIN)

    def test_owner_passes(self) -> None:
        deps_module.require_admin(types_module.UserRole.OWNER)

    def test_user_raises_403(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            deps_module.require_admin(types_module.UserRole.USER)
        assert exc_info.value.status_code == 403


class TestRequireOwner:
    def test_owner_passes(self) -> None:
        deps_module.require_owner(types_module.UserRole.OWNER)

    def test_admin_raises_403(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            deps_module.require_owner(types_module.UserRole.ADMIN)
        assert exc_info.value.status_code == 403

    def test_user_raises_403(self) -> None:
        with pytest.raises(fastapi.HTTPException) as exc_info:
            deps_module.require_owner(types_module.UserRole.USER)
        assert exc_info.value.status_code == 403
```

**Step 2: Run tests to verify they fail**

**Step 3: Add dependencies to dependencies.py**

```python
async def get_current_user(
    user_id: Annotated[uuid.UUID, fastapi.Depends(get_current_user_id)],
    db: sa_async.AsyncSession = fastapi.Depends(get_db),
) -> user_models.User:
    """Get the full User object for the authenticated user."""
    result = await db.execute(
        sa.select(user_models.User).where(user_models.User.id == user_id)
    )
    user = result.scalar_one_or_none()
    if user is None:
        raise fastapi.HTTPException(status_code=401, detail="User not found")
    return user


def require_admin(role: types_module.UserRole) -> None:
    """Raise 403 if user is not admin or owner."""
    if role not in (types_module.UserRole.ADMIN, types_module.UserRole.OWNER):
        raise fastapi.HTTPException(
            status_code=403, detail="Admin access required"
        )


def require_owner(role: types_module.UserRole) -> None:
    """Raise 403 if user is not owner."""
    if role != types_module.UserRole.OWNER:
        raise fastapi.HTTPException(
            status_code=403, detail="Owner access required"
        )
```

Add necessary imports: `sa`, `user_models`, `types_module`.

**Step 4: Run tests, lint, commit**

```bash
git add src/resonance/dependencies.py tests/test_dependencies.py
git commit -m "feat: add require_admin and require_owner auth dependencies (#30)"
```

---

### Task 3: First-User-Gets-Owner in Auth Callback

**Files:**
- Modify: `src/resonance/api/v1/auth.py`
- Modify: `tests/test_api_auth.py`

**Step 1: Write the failing test**

Add to `tests/test_api_auth.py`:

```python
class TestFirstUserGetsOwner:
    async def test_first_user_becomes_owner(self):
        """When no users exist, the first registered user gets owner role."""
        # Setup: mock connector, empty DB
        # Trigger: OAuth callback creates new user
        # Assert: user.role == UserRole.OWNER
        ...

    async def test_subsequent_user_is_regular(self):
        """Second and later users get user role."""
        ...
```

**Step 2: Modify auth callback**

In `auth_callback`, find the new user creation block. Add a count check before creating:

```python
# Check if this is the first user
count_result = await db.execute(
    sa.select(sa.func.count()).select_from(user_models.User)
)
is_first_user = count_result.scalar_one() == 0

new_user = user_models.User(
    display_name=display_name,
    role=(
        types_module.UserRole.OWNER
        if is_first_user
        else types_module.UserRole.USER
    ),
)
```

**Step 3: Run tests, lint, commit**

```bash
git add src/resonance/api/v1/auth.py tests/test_api_auth.py
git commit -m "feat: first registered user automatically becomes owner (#30)"
```

---

### Task 4: Admin Nav Link + Role in Templates

**Files:**
- Modify: `src/resonance/templates/base.html`
- Modify: `src/resonance/ui/routes.py`

**Step 1: Pass user_role to all authenticated templates**

In `ui/routes.py`, update each route that renders a template to fetch and pass `user_role`. The pattern:

```python
# In each authenticated route, after getting user_id:
user_role = None
async with _get_db(request) as db:
    user_result = await db.execute(
        sa.select(user_models.User.role).where(
            user_models.User.id == user_uuid
        )
    )
    user_role = user_result.scalar_one_or_none()
```

Then include `"user_role": user_role` in every template context dict.

A better approach: create a helper function `_get_user_role(request, db, user_uuid)` to avoid repeating this in every route. Or add a `user_role` key to the session when the user logs in.

**Best approach: store role in session on login.** Update the auth callback to also store `session["user_role"] = user.role.value` alongside `session["user_id"]`. Then all routes can read it from the session without a DB query:

```python
user_role = request.state.session.get("user_role", "user")
```

Update the auth callback to store the role in the session.

**Step 2: Add admin link to base.html nav**

In `base.html`, add after the Account link:

```html
{% if user_role in ("admin", "owner") %}
<li><a href="/admin">Admin</a></li>
{% endif %}
```

**Step 3: Update all template context dicts to include user_role**

Every `templates.TemplateResponse` call that passes `user_id` should also pass `user_role`.

**Step 4: Run tests, lint, commit**

```bash
git add src/resonance/templates/base.html src/resonance/ui/routes.py src/resonance/api/v1/auth.py
git commit -m "feat: add admin nav link with role-based visibility (#30)"
```

---

### Task 5: Admin Dashboard Page

**Files:**
- Create: `src/resonance/templates/admin.html`
- Create: `src/resonance/templates/admin_users.html`
- Modify: `src/resonance/ui/routes.py`
- Test: `tests/test_ui.py`

**Step 1: Write the failing test**

```python
class TestAdminPage:
    async def test_admin_requires_auth(self, client: httpx.AsyncClient) -> None:
        response = await client.get("/admin", follow_redirects=False)
        assert response.status_code == 307
        assert response.headers["location"] == "/login"
```

**Step 2: Create admin routes**

```python
@router.get("/admin", response_model=None)
async def admin_dashboard(request: fastapi.Request):
    user_id = request.state.session.get("user_id")
    if not user_id:
        return fastapi.responses.RedirectResponse(url="/login", status_code=307)

    user_role = request.state.session.get("user_role", "user")
    if user_role not in ("admin", "owner"):
        return fastapi.responses.RedirectResponse(url="/", status_code=307)

    user_uuid = uuid.UUID(user_id)

    async with _get_db(request) as db:
        # Total users
        user_count = (await db.execute(
            sa.select(sa.func.count()).select_from(user_models.User)
        )).scalar_one()

        # All users with connections count
        users = (await db.execute(
            sa.select(user_models.User).order_by(user_models.User.created_at)
        )).scalars().all()

    return templates.TemplateResponse(
        request,
        "admin.html",
        {
            "user_id": user_id,
            "user_role": user_role,
            "user_tz": _user_tz(request),
            "user_count": user_count,
            "users": users,
        },
    )
```

**Step 3: Create admin.html template**

```html
{% extends "base.html" %}

{% block title %}Admin — resonance{% endblock %}

{% block content %}
<h1>Admin</h1>

<div class="grid">
    <article>
        <header>Users</header>
        <p><strong>{{ user_count }}</strong></p>
    </article>
</div>

<h2>Users</h2>
<figure>
    <table>
        <thead>
            <tr>
                <th>Display Name</th>
                <th>Role</th>
                <th>Member Since</th>
                <th>Actions</th>
            </tr>
        </thead>
        <tbody>
            {% for u in users %}
            <tr>
                <td>{{ u.display_name }}</td>
                <td>{{ u.role.value | capitalize }}</td>
                <td>{{ (u.created_at | localtime(user_tz)).strftime('%Y-%m-%d') }}</td>
                <td>
                    {% if u.id | string != user_id %}
                    <form style="display:inline"
                          hx-post="/admin/users/{{ u.id }}/role"
                          hx-target="closest tr"
                          hx-swap="outerHTML">
                        <select name="role" onchange="this.form.requestSubmit()">
                            <option value="user" {% if u.role.value == 'user' %}selected{% endif %}>User</option>
                            <option value="admin" {% if u.role.value == 'admin' %}selected{% endif %}>Admin</option>
                            {% if user_role == 'owner' %}
                            <option value="owner" {% if u.role.value == 'owner' %}selected{% endif %}>Owner</option>
                            {% endif %}
                        </select>
                    </form>
                    {% else %}
                    <small>(you)</small>
                    {% endif %}
                </td>
            </tr>
            {% endfor %}
        </tbody>
    </table>
</figure>
{% endblock %}
```

**Step 4: Add role change endpoint**

```python
@router.post("/admin/users/{target_user_id}/role", response_model=None)
async def change_user_role(
    target_user_id: uuid.UUID,
    request: fastapi.Request,
):
    user_id = request.state.session.get("user_id")
    user_role = request.state.session.get("user_role", "user")

    if not user_id or user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    form = await request.form()
    new_role = form.get("role", "user")

    # Validate: admin can only set user/admin, owner can set any
    if user_role != "owner" and new_role == "owner":
        raise fastapi.HTTPException(
            status_code=403, detail="Only owner can promote to owner"
        )

    # Cannot change own role
    if str(target_user_id) == user_id:
        raise fastapi.HTTPException(
            status_code=400, detail="Cannot change your own role"
        )

    async with _get_db(request) as db:
        result = await db.execute(
            sa.select(user_models.User).where(
                user_models.User.id == target_user_id
            )
        )
        target_user = result.scalar_one_or_none()
        if target_user is None:
            raise fastapi.HTTPException(status_code=404)

        target_user.role = types_module.UserRole(new_role)
        await db.commit()

        # Return updated table row as HTMX partial
        return fastapi.responses.HTMLResponse(
            f"<tr>"
            f"<td>{target_user.display_name}</td>"
            f"<td>{target_user.role.value.capitalize()}</td>"
            f"<td>{target_user.created_at.strftime('%Y-%m-%d')}</td>"
            f"<td><small>Updated</small></td>"
            f"</tr>"
        )
```

**Step 5: Run tests, lint, commit**

```bash
git add src/resonance/templates/admin.html src/resonance/ui/routes.py tests/test_ui.py
git commit -m "feat: add admin dashboard with user management (#30)"
```

---

### Task 6: CLI Set-Role Command

**Files:**
- Create: `src/resonance/cli.py`
- Modify: `pyproject.toml` (add script entry point)

**Step 1: Create CLI module**

```python
"""CLI commands for Resonance administration."""

import asyncio
import sys
import uuid

import resonance.config as config_module
import resonance.database as database_module
import resonance.models.user as user_models
import resonance.types as types_module

import sqlalchemy as sa


async def _set_role(user_id_str: str, role_str: str) -> None:
    """Set a user's role directly in the database."""
    settings = config_module.Settings()
    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)

    try:
        user_id = uuid.UUID(user_id_str)
    except ValueError:
        print(f"Error: Invalid UUID: {user_id_str}")
        sys.exit(1)

    try:
        role = types_module.UserRole(role_str)
    except ValueError:
        valid = ", ".join(r.value for r in types_module.UserRole)
        print(f"Error: Invalid role '{role_str}'. Valid roles: {valid}")
        sys.exit(1)

    async with session_factory() as db:
        result = await db.execute(
            sa.select(user_models.User).where(user_models.User.id == user_id)
        )
        user = result.scalar_one_or_none()
        if user is None:
            print(f"Error: No user found with ID {user_id}")
            sys.exit(1)

        old_role = user.role
        user.role = role
        await db.commit()
        print(f"Updated {user.display_name}: {old_role.value} → {role.value}")

    await engine.dispose()


def set_role() -> None:
    """Entry point for `resonance set-role <user_id> <role>`."""
    if len(sys.argv) != 3:
        print("Usage: resonance set-role <user_id> <role>")
        print(f"  Roles: {', '.join(r.value for r in types_module.UserRole)}")
        sys.exit(1)

    asyncio.run(_set_role(sys.argv[1], sys.argv[2]))
```

**Step 2: Add script entry point to pyproject.toml**

```toml
[project.scripts]
"resonance-set-role" = "resonance.cli:set_role"
```

**Step 3: Run lint, commit**

```bash
git add src/resonance/cli.py pyproject.toml
git commit -m "feat: add resonance-set-role CLI command (#30)"
```

---

### Task 7: Test Service Type + Test Connector

**Files:**
- Modify: `src/resonance/types.py` (add TEST to ServiceType)
- Create: `src/resonance/connectors/test.py`
- Test: `tests/test_test_connector.py`

**Step 1: Add TEST to ServiceType**

In `types.py`, add to `ServiceType`:
```python
TEST = "test"
```

**Step 2: Create test connector**

Create `src/resonance/connectors/test.py`:

```python
"""Test service connector for admin testing and development."""

import resonance.connectors.base as base_module
import resonance.types as types_module


class TestConnector(base_module.BaseConnector):
    """Fake connector for testing the sync pipeline."""

    service_type = types_module.ServiceType.TEST
    capabilities = frozenset({
        base_module.ConnectorCapability.LISTENING_HISTORY,
    })

    def __init__(self) -> None:
        self._http_client = None
        self._budget = base_module.ratelimit_module.RateLimitBudget(
            default_interval=0.0
        )
```

Note: The test connector doesn't need OAuth methods — it uses instant connect. The sync strategy handles data generation.

**Step 3: Create admin-only connect endpoint**

Add to `src/resonance/api/v1/sync.py` or a new `src/resonance/api/v1/admin.py`:

```python
@router.post("/test/connect")
async def connect_test_service(
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
    # role check happens here
):
    """Admin-only: instantly connect the test service."""
    # Check admin role from session or DB
    # Create ServiceConnection with dummy data
    ...
```

**Step 4: Run tests, lint, commit**

```bash
git add src/resonance/types.py src/resonance/connectors/test.py tests/test_test_connector.py
git commit -m "feat: add TEST service type and test connector (#30)"
```

---

### Task 8: Test Sync Strategy

**Files:**
- Create: `src/resonance/sync/test.py`
- Modify: `src/resonance/worker.py` (register strategy)
- Test: `tests/test_sync_test_strategy.py`

**Step 1: Create test sync strategy**

```python
"""Test sync strategy — generates deterministic fake data."""

import hashlib
import uuid
from typing import Any

import resonance.connectors.base as base_module
import resonance.sync.base as sync_base
import resonance.sync.runner as runner_module
import resonance.types as types_module

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession
    from resonance.models.sync import SyncTask
    from resonance.models.user import ServiceConnection


class TestSyncStrategy(sync_base.SyncStrategy):
    """Generates deterministic fake data for testing."""

    concurrency = "sequential"

    async def plan(
        self,
        session: AsyncSession,
        connection: ServiceConnection,
        connector: base_module.BaseConnector,
    ) -> list[sync_base.SyncTaskDescriptor]:
        # Default counts, can be overridden by task params
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
        session: AsyncSession,
        task: SyncTask,
        connector: base_module.BaseConnector,
        connection: ServiceConnection,
    ) -> dict[str, Any]:
        params = task.params or {}
        num_artists = params.get("artists", 5)
        num_tracks = params.get("tracks", 20)
        num_listens = params.get("listens", 100)
        seed = params.get("seed", "default")

        items_created = 0

        # Generate deterministic artists
        artists = []
        for i in range(num_artists):
            name = f"Test Artist {_seeded_name(seed, 'artist', i)}"
            ext_id = _seeded_id(seed, "artist", i)
            artist_data = base_module.ArtistData(
                external_id=ext_id,
                name=name,
                service=types_module.ServiceType.TEST,
            )
            created = await runner_module._upsert_artist(session, artist_data)
            if created:
                items_created += 1
            artists.append((name, ext_id))

        await session.flush()

        # Generate deterministic tracks
        tracks = []
        for i in range(num_tracks):
            artist_idx = i % num_artists
            artist_name, artist_ext_id = artists[artist_idx]
            track_data = base_module.TrackData(
                external_id=_seeded_id(seed, "track", i),
                title=f"Test Track {_seeded_name(seed, 'track', i)}",
                artist_external_id=artist_ext_id,
                artist_name=artist_name,
                service=types_module.ServiceType.TEST,
            )
            created = await runner_module._upsert_track(session, track_data)
            if created:
                items_created += 1
            tracks.append(track_data)

        await session.flush()

        # Generate deterministic listening events
        import datetime

        base_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)
        for i in range(num_listens):
            track = tracks[i % num_tracks]
            listened_at = base_time + datetime.timedelta(hours=i)
            await runner_module._upsert_listening_event(
                session, connection.user_id, track, listened_at.isoformat()
            )
            items_created += 1

        task.progress_current = 1
        return {"items_created": items_created, "items_updated": 0}


def _seeded_name(seed: str, kind: str, index: int) -> str:
    """Generate a deterministic name from seed."""
    h = hashlib.md5(f"{seed}:{kind}:{index}".encode()).hexdigest()
    return h[:8]


def _seeded_id(seed: str, kind: str, index: int) -> str:
    """Generate a deterministic ID from seed."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{seed}:{kind}:{index}"))
```

**Step 2: Register strategy in worker.py**

Add import and registration in the `startup()` function's strategies dict.

**Step 3: Run tests, lint, commit**

```bash
git add src/resonance/sync/test.py src/resonance/worker.py tests/test_sync_test_strategy.py
git commit -m "feat: add test sync strategy with deterministic data generation (#30)"
```

---

### Task 9: Task Cloning API + UI

**Files:**
- Modify: `src/resonance/ui/routes.py`
- Create: `src/resonance/templates/admin_tasks.html`

**Step 1: Add clone endpoint**

```python
@router.post("/admin/tasks/{task_id}/clone", response_model=None)
async def clone_task(
    task_id: uuid.UUID,
    request: fastapi.Request,
):
    user_id = request.state.session.get("user_id")
    user_role = request.state.session.get("user_role", "user")
    if not user_id or user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    form = await request.form()
    step_mode = form.get("step_mode") == "true"

    async with _get_db(request) as db:
        # Load original task
        original = (await db.execute(
            sa.select(sync_models.SyncTask).where(
                sync_models.SyncTask.id == task_id
            )
        )).scalar_one_or_none()

        if original is None:
            raise fastapi.HTTPException(status_code=404)

        # Clone with admin's user_id
        params = dict(original.params or {})
        if step_mode:
            params["step_mode"] = True

        cloned = sync_models.SyncTask(
            user_id=uuid.UUID(user_id),
            service_connection_id=original.service_connection_id,
            task_type=original.task_type,
            params=params,
            status=types_module.SyncStatus.PENDING,
            progress_total=original.progress_total,
        )
        db.add(cloned)
        await db.commit()

        # Enqueue via arq
        redis = request.app.state.arq_redis
        if redis:
            job_name = (
                "plan_sync" if original.task_type == types_module.SyncTaskType.SYNC_JOB
                else "sync_range"
            )
            await redis.enqueue_job(
                job_name,
                str(cloned.id),
                _job_id=f"{job_name}:{cloned.id}",
            )

    return fastapi.responses.RedirectResponse(
        url="/admin", status_code=303
    )
```

**Step 2: Add step-through resume endpoint**

```python
@router.post("/admin/tasks/{task_id}/resume", response_model=None)
async def resume_task(task_id: uuid.UUID, request: fastapi.Request):
    """Resume a deferred step-mode task for one more page."""
    user_id = request.state.session.get("user_id")
    user_role = request.state.session.get("user_role", "user")
    if not user_id or user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    async with _get_db(request) as db:
        task = (await db.execute(
            sa.select(sync_models.SyncTask).where(
                sync_models.SyncTask.id == task_id
            )
        )).scalar_one_or_none()

        if task is None:
            raise fastapi.HTTPException(status_code=404)

        if task.status != types_module.SyncStatus.DEFERRED:
            raise fastapi.HTTPException(
                status_code=400, detail="Task is not in deferred state"
            )

        task.status = types_module.SyncStatus.PENDING
        await db.commit()

        redis = request.app.state.arq_redis
        if redis:
            await redis.enqueue_job(
                "sync_range",
                str(task.id),
                _job_id=f"sync_range:{task.id}",
            )

    return fastapi.responses.RedirectResponse(
        url="/admin", status_code=303
    )
```

**Step 3: Add step_mode handling in worker sync_range**

In `worker.py`'s `sync_range` function, after processing one page, check for step_mode:

```python
# After each page in execute():
if task.params and task.params.get("step_mode"):
    # Save progress and defer
    task.status = SyncStatus.DEFERRED
    await session.commit()
    return  # Don't process more pages
```

This needs to be integrated into the strategy's execute method or the worker's sync_range function.

**Step 4: Run tests, lint, commit**

```bash
git add src/resonance/ui/routes.py src/resonance/worker.py
git commit -m "feat: add task cloning with step-through mode (#30)"
```

---

### Task 10: Test Connector UI + Connect Button

**Files:**
- Modify: `src/resonance/templates/account.html`
- Modify: `src/resonance/app.py` (register test connector)
- Modify: `src/resonance/ui/routes.py` (pass role to account page)

**Step 1: Add test connector connect button to account page**

In `account.html`, in the "Connect Another Service" section, add conditionally:

```html
{% if user_role in ("admin", "owner") %}
<button
    hx-post="/api/v1/test/connect"
    hx-on::after-request="location.reload()"
>Connect Test Service</button>
{% endif %}
```

**Step 2: Register test connector in app.py**

```python
import resonance.connectors.test as test_connector_module

# In create_app(), after other connector registrations:
connector_registry.register(test_connector_module.TestConnector())
```

**Step 3: Create the instant connect endpoint**

In a new file `src/resonance/api/v1/admin.py` or in `sync.py`:

```python
@router.post("/test/connect")
async def connect_test_service(
    request: fastapi.Request,
    user_id: Annotated[uuid.UUID, fastapi.Depends(deps_module.get_current_user_id)],
    db: Annotated[sa_async.AsyncSession, fastapi.Depends(deps_module.get_db)],
) -> dict[str, str]:
    """Admin-only: instantly connect the test service."""
    # Verify admin role
    user_role = request.state.session.get("user_role", "user")
    if user_role not in ("admin", "owner"):
        raise fastapi.HTTPException(status_code=403)

    # Check if already connected
    existing = (await db.execute(
        sa.select(user_models.ServiceConnection).where(
            user_models.ServiceConnection.user_id == user_id,
            user_models.ServiceConnection.service_type == types_module.ServiceType.TEST,
        )
    )).scalar_one_or_none()

    if existing:
        return {"status": "already_connected"}

    settings = request.app.state.settings

    connection = user_models.ServiceConnection(
        user_id=user_id,
        service_type=types_module.ServiceType.TEST,
        external_user_id="test",
        encrypted_access_token=crypto_module.encrypt_token(
            "test-token", settings.token_encryption_key
        ),
    )
    db.add(connection)
    await db.commit()

    return {"status": "connected"}
```

**Step 4: Run tests, lint, commit**

```bash
git add src/resonance/templates/account.html src/resonance/app.py src/resonance/api/v1/admin.py src/resonance/ui/routes.py
git commit -m "feat: add test service instant connect for admins (#30)"
```

---

## Summary

After all 10 tasks:

| Component | Status |
|-----------|--------|
| UserRole enum | owner, admin, user with DB migration |
| Role dependencies | require_admin, require_owner |
| First-user bootstrap | First registered user gets owner |
| CLI escape hatch | resonance-set-role command |
| Admin nav link | Visible only to admin/owner |
| Admin dashboard | User list with role management |
| Test service type | TEST added to ServiceType |
| Test connector | Instant connect, no OAuth |
| Test sync strategy | Deterministic data generation |
| Task cloning | Clone + step-through mode |

**Not in scope (future):**
- Error simulation in test connector
- Dry-run mode for cloned tasks
- Admin audit log
- Permission-based access control
