"""CLI commands for Resonance administration."""

import asyncio
import json
import os
import sys
import uuid

import httpx
import sqlalchemy as sa

import resonance.config as config_module
import resonance.database as database_module
import resonance.models.user as user_models
import resonance.types as types_module


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
    """Entry point for `resonance-set-role <user_id> <role>`."""
    if len(sys.argv) != 3:
        print("Usage: resonance-set-role <user_id> <role>")
        print(f"  Roles: {', '.join(r.value for r in types_module.UserRole)}")
        sys.exit(1)

    asyncio.run(_set_role(sys.argv[1], sys.argv[2]))


# ---------------------------------------------------------------------------
# resonance-api: CLI for admin API calls via bearer token
# ---------------------------------------------------------------------------


def _get_api_config() -> tuple[str, str]:
    """Get base URL and API token from env or settings."""
    settings = config_module.Settings()
    base_url = os.environ.get("RESONANCE_URL", settings.base_url)
    token = os.environ.get("RESONANCE_API_TOKEN", settings.admin_api_token)
    if not token:
        print("Error: No API token. Set RESONANCE_API_TOKEN or admin_api_token.")
        sys.exit(1)
    return base_url, token


def _api_request(method: str, path: str, **kwargs: object) -> httpx.Response:
    """Make an authenticated API request."""
    base_url, token = _get_api_config()
    url = f"{base_url}{path}"
    try:
        response = httpx.request(
            method,
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=60.0,
            follow_redirects=True,
            **kwargs,  # type: ignore[arg-type]
        )
    except httpx.ConnectError as exc:
        print(f"Error: Could not connect to {base_url}: {exc}")
        sys.exit(1)
    except httpx.TimeoutException:
        print(f"Error: Request to {url} timed out")
        sys.exit(1)

    if response.status_code >= 400:
        print(f"Error: HTTP {response.status_code}")
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text[:500]
        print(f"  {detail}")
        sys.exit(1)

    return response


def api() -> None:
    """Entry point for `resonance-api <command> [args]`.

    Commands:
        dedup-events     Remove duplicate cross-service listening events
        sync <service>   Trigger a sync for a service
        healthz          Check health and revision
        users            List all users
    """
    if len(sys.argv) < 2:
        print("Usage: resonance-api <command> [args]")
        print()
        print("Commands:")
        print("  healthz          Check health + deployed revision")
        print("  sync <service>   Trigger a sync (spotify, listenbrainz, lastfm)")
        print("  dedup-events     Remove cross-service duplicate listening events")
        print("  dedup-artists    Merge duplicate artist records")
        print("  dedup-tracks     Merge duplicate track records")
        print("  healthz          Check health and deployed revision")
        print("  users            List all users")
        sys.exit(1)

    command = sys.argv[1]

    if command == "healthz":
        resp = _api_request("GET", "/healthz")
        print(json.dumps(resp.json(), indent=2))

    elif command == "dedup-events":
        print("Running dedup...")
        resp = _api_request("POST", "/admin/dedup-events")
        if resp.status_code == 200:
            print(json.dumps(resp.json(), indent=2))
        else:
            print(f"Error {resp.status_code}: {resp.text}")

    elif command == "sync":
        if len(sys.argv) < 3:
            print("Usage: resonance-api sync <service> [--full]")
            sys.exit(1)
        service = sys.argv[2]
        body = None
        if "--full" in sys.argv[3:]:
            body = {"sync_from": "full"}
            print(f"Triggering full re-sync for {service}...")
        else:
            print(f"Triggering incremental sync for {service}...")
        resp = _api_request(
            "POST",
            f"/api/v1/sync/{service}",
            json=body,
        )
        print(json.dumps(resp.json(), indent=2))

    elif command == "dedup-artists":
        print("Deduplicating artists...")
        resp = _api_request("POST", "/admin/dedup-artists")
        print(json.dumps(resp.json(), indent=2))

    elif command == "dedup-tracks":
        print("Deduplicating tracks...")
        resp = _api_request("POST", "/admin/dedup-tracks")
        print(json.dumps(resp.json(), indent=2))

    elif command == "users":
        resp = _api_request("GET", "/admin")
        print("Users page returned (HTML). Use the web UI for user management.")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
