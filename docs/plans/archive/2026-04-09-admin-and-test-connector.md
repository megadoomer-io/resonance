# Admin Functionality and Test Connector

## Overview

Add a role-based access control system, an admin UI for user management and debugging, a test service connector for end-to-end testing without external APIs, and task cloning for sync debugging.

## Role System

### Roles

Three roles in ascending privilege order:

- **`user`** (default) — Normal functionality: connect services, sync data, browse own data
- **`admin`** — Everything `user` can do, plus: view all users, grant/revoke admin, use test connector, view all sync jobs, clone tasks
- **`owner`** — Everything `admin` can do, plus: promote users to owner, demote other owners

### User Model Changes

Add `role` column to `users` table:

```python
class UserRole(enum.StrEnum):
    USER = "user"
    ADMIN = "admin"
    OWNER = "owner"

# On User model:
role: orm.Mapped[types_module.UserRole] = orm.mapped_column(
    sa.Enum(types_module.UserRole, native_enum=False),
    nullable=False,
    default=types_module.UserRole.USER,
)
```

### Bootstrap

- First user to register automatically gets `owner` role
- In the auth callback, after creating a new User, check if they're the only user in the DB. If so, set `role = owner`.
- Subsequent users default to `user`

### CLI Escape Hatch

Management command for disaster recovery:

```bash
uv run resonance set-role <user_id> <role>
```

Directly updates the DB, no auth checks. Documented as an escape hatch, not normal workflow.

### Dependencies

```python
async def get_current_user_role(session, user_id) -> UserRole:
    """Get the role of the current user."""

def require_admin(role: UserRole) -> UserRole:
    """Raise 403 if user is not admin or owner."""

def require_owner(role: UserRole) -> UserRole:
    """Raise 403 if user is not owner."""
```

### Alembic Migration

1. Add `role` column with default `user`
2. Update the earliest user (by `created_at`) to `owner`

## Test Connector

### Service Type

Add `TEST` to `ServiceType` enum.

### Connect Flow

Admin-only instant connect. No OAuth, no redirect.

- Admin clicks "Connect Test Service" on account page (button only visible to admin/owner)
- Creates `ServiceConnection` with `service_type=TEST`, `external_user_id="test"`, dummy encrypted tokens
- No connector class needed for auth — just a direct DB insert via a dedicated API endpoint

### Sync Strategy

Implements `SyncStrategy` like Spotify and ListenBrainz:

- **`plan()`** — Creates a single `time_range` child task with generation params
- **`execute()`** — Generates deterministic fake data:
  - Default: 5 artists, 20 tracks, 100 listening events
  - Configurable via sync trigger params: `{"artists": N, "tracks": N, "listens": N}`
  - Seeded random using `user_id` for reproducibility
  - Uses `ServiceType.TEST` in `service_links`
- Respects the same upsert pipeline as real connectors (artists → tracks → events)
- Reports progress like real syncs (`progress_total`, `progress_current`)

### Admin-Only Gating

- Test connector only visible on connect page for admin/owner
- `POST /api/v1/test/connect` endpoint gated by `require_admin`
- Test service sync trigger uses the normal `POST /api/v1/sync/test` endpoint (same as other services)

## Admin UI

### Navigation

Admin link in the nav bar, only visible to admin/owner users.

### Pages

All routes gated by `require_admin` dependency.

**`GET /admin`** — Admin dashboard
- Total users, total connections, active syncs
- Quick links to user management and sync debugging

**`GET /admin/users`** — User list
- Display name, role, connected services, last activity
- Role change buttons (admin can set user/admin, owner can set any role)

**`POST /admin/users/{user_id}/role`** — Change user role
- Validates: admin can only set user/admin, owner can set any role
- Cannot demote self (prevents lockout)
- Returns updated user list (HTMX partial swap)

**Admin sync visibility:**
- Admin can view sync tasks for any user from the admin dashboard
- Uses the existing sync status partial with a user filter parameter

## Task Cloning

### Flow

1. Admin browses sync tasks (own or other users')
2. Clicks "Clone" on any task
3. System creates a new SyncTask with:
   - Same `params` as the original
   - `user_id` set to the admin's own user (data goes into admin's account)
   - `parent_id = None` (top-level task, independent of original hierarchy)
   - Status: `PENDING`
4. Task is enqueued via arq and runs normally

### Step-Through Mode

- Clone with `step_mode: true` in params
- Worker processes one page, then sets task to `DEFERRED` status (reusing existing deferred mechanism)
- UI shows "Next Step" button for deferred tasks with `step_mode: true`
- "Next Step" re-enqueues the task, which processes one more page and defers again
- "Run to completion" button removes `step_mode` from params and re-enqueues — task runs normally from that point

### API

- `POST /admin/tasks/{task_id}/clone` — Clone a task
  - Body: `{"step_mode": false}` (optional, defaults to false)
  - Returns the new task ID
- `POST /admin/tasks/{task_id}/resume` — Resume a deferred step-mode task

## Future Enhancements (not in scope)

- Error simulation in test connector (configurable timeouts, rate limits, malformed data)
- Dry-run mode for cloned tasks (fetch but don't write)
- Permission-based access control (granular permissions instead of roles)
- Admin audit log (who changed what role, when)
