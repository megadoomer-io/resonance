"""CLI commands for Resonance administration."""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import sys
import time
import uuid
from typing import TYPE_CHECKING

import httpx
import sqlalchemy as sa

import resonance.config as config_module
import resonance.database as database_module
import resonance.models.user as user_models
import resonance.types as types_module

if TYPE_CHECKING:
    from collections.abc import Callable


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


# ---------------------------------------------------------------------------
# resonance-api: CLI for admin API calls via bearer token
# ---------------------------------------------------------------------------

_USAGE = """\
Usage: resonance-api <command> [args]

Commands:
  healthz                      Health + deployed revision
  status                       Recent sync job overview
  stats                        Database statistics
  sync <service> [--full]      Trigger a sync
  feeds                        List calendar connections
  feed-add songkick <username>  Add Songkick connection
  feed-add ical <url> [--label] Add generic iCal connection
  feed-sync <conn_id|all>      Sync a calendar connection (or all)
  dedup <type> [--no-wait]     Run deduplication
  task <task_id>               Check task status
  track <query>                Search tracks by title
  profile <subcommand>         Manage generator profiles
  generate <profile-id>        Generate a playlist from a profile
  playlists                    List playlists
  playlist <id> [diff <other>] Show or diff a playlist
  set-role <user_id> <role>    Set user role (direct DB)
"""

_DEDUP_USAGE = """\
Usage: resonance-api dedup <type>

Types:
  events    Remove cross-service duplicate events
  artists   Merge duplicate artist records
  tracks    Merge duplicate track records
  all       Run all three in sequence
"""


def _get_api_config() -> tuple[str, str]:
    """Get base URL and API token from env or settings."""
    settings = config_module.Settings()
    base_url = os.environ.get("RESONANCE_URL", settings.base_url)
    token = os.environ.get("RESONANCE_API_TOKEN", settings.admin_api_token)
    if not token:
        print("Error: No API token. Set RESONANCE_API_TOKEN or admin_api_token.")
        sys.exit(1)
    return base_url, token


def _api_request(
    method: str, path: str, timeout: float = 300.0, **kwargs: object
) -> httpx.Response:
    """Make an authenticated API request."""
    base_url, token = _get_api_config()
    url = f"{base_url}{path}"
    try:
        response = httpx.request(
            method,
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
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


def _cmd_healthz() -> None:
    resp = _api_request("GET", "/healthz")
    print(json.dumps(resp.json(), indent=2))


def _cmd_status() -> None:
    resp = _api_request("GET", "/admin/status")
    data = resp.json()
    for job in data.get("sync_jobs", []):
        status = job["status"].upper()
        svc = job["service"]
        created = job["created_at"][:19].replace("T", " ")
        print(f"[{status}] {svc} sync — {created}")
        for child in job.get("children", []):
            c_status = child["status"].upper()
            desc = child.get("description") or child["type"]
            progress = child.get("progress", 0)
            total = child.get("total")
            p_str = f" {progress}/{total}" if total else ""
            err = f" — {child['error']}" if child.get("error") else ""
            print(f"  {c_status:10s} {desc}{p_str}{err}")
        print()


def _cmd_stats() -> None:
    resp = _api_request("GET", "/admin/stats")
    data = resp.json()
    print(f"Artists:  {data['artists']}")
    print(f"Tracks:  {data['tracks']}")
    dur_with = data["tracks_with_duration"]
    dur_without = data["tracks_without_duration"]
    total = dur_with + dur_without
    pct = (dur_with / total * 100) if total else 0
    print(f"  with duration:    {dur_with} ({pct:.0f}%)")
    print(f"  without duration: {dur_without}")
    print(f"Events:  {data['events_total']}")
    for svc, count in sorted(data.get("events_by_service", {}).items()):
        print(f"  {svc:15s} {count}")
    dup_a = data.get("duplicate_artist_groups", 0)
    dup_t = data.get("duplicate_track_groups", 0)
    if dup_a or dup_t:
        print(f"Duplicate groups: {dup_a} artists, {dup_t} tracks")


def _cmd_sync() -> None:
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
    resp = _api_request("POST", f"/api/v1/sync/{service}", json=body)
    print(json.dumps(resp.json(), indent=2))


def _poll_task(task_id: str, label: str) -> dict[str, object]:
    """Poll a task until completion, showing progress."""
    is_tty = sys.stdout.isatty()
    poll_interval = 3

    while True:
        resp = _api_request("GET", f"/admin/tasks/{task_id}")
        data = resp.json()
        status = data.get("status", "unknown")

        if status in ("completed", "failed"):
            if is_tty:
                # Clear the progress line
                sys.stdout.write("\r" + " " * 60 + "\r")
                sys.stdout.flush()
            if status == "failed":
                error = data.get("error", "Unknown error")
                print(f"FAILED: {error}")
                sys.exit(1)
            result: dict[str, object] = data.get("result") or {}
            return result

        # Show progress
        progress = data.get("progress_current", 0)
        total = data.get("progress_total")
        p_str = f"{progress}/{total}" if total else f"{progress}"
        if is_tty:
            sys.stdout.write(f"\r{label}... {p_str}")
            sys.stdout.flush()
        else:
            now = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
            print(f"{now} {label} {status} {p_str}")

        time.sleep(poll_interval)


def _cmd_dedup() -> None:
    if len(sys.argv) < 3:
        print(_DEDUP_USAGE)
        sys.exit(1)
    dedup_type = sys.argv[2]
    no_wait = "--no-wait" in sys.argv[3:]

    targets: list[tuple[str, str]] = []
    if dedup_type == "events":
        targets = [("events", "/admin/dedup-events")]
    elif dedup_type == "artists":
        targets = [("artists", "/admin/dedup-artists")]
    elif dedup_type == "tracks":
        targets = [("tracks", "/admin/dedup-tracks")]
    elif dedup_type == "all":
        targets = [
            ("artists", "/admin/dedup-artists"),
            ("tracks", "/admin/dedup-tracks"),
            ("events", "/admin/dedup-events"),
        ]
    else:
        print(f"Unknown dedup type: {dedup_type}")
        print(_DEDUP_USAGE)
        sys.exit(1)

    for label, path in targets:
        resp = _api_request("POST", path)
        data = resp.json()
        task_id = data.get("task_id", "")

        if no_wait:
            print(f"{label}: task {task_id}")
            continue

        result = _poll_task(task_id, f"Deduplicating {label}")
        print(json.dumps(result, indent=2))
        if len(targets) > 1:
            print()


def _cmd_task() -> None:
    if len(sys.argv) < 3:
        print("Usage: resonance-api task <task_id>")
        sys.exit(1)
    task_id = sys.argv[2]
    resp = _api_request("GET", f"/admin/tasks/{task_id}")
    print(json.dumps(resp.json(), indent=2))


def _cmd_track() -> None:
    query = " ".join(sys.argv[2:])
    if not query.strip():
        print("Usage: resonance-api track <query>")
        sys.exit(1)
    resp = _api_request("GET", f"/admin/track?q={query}")
    data = resp.json()
    results = data.get("results", [])
    if not results:
        print(f"No tracks found matching '{query}'")
        return
    for t in results:
        dur = t.get("duration") or "no duration"
        print(f"{t['title']} — {t['artist']} ({dur})")
        print(f"  id: {t['id']}")
        links = t.get("service_links") or {}
        if links:
            print(f"  links: {links}")
        evts = t.get("events_by_service", {})
        if evts:
            parts = [f"{s}: {c}" for s, c in sorted(evts.items())]
            print(f"  events: {', '.join(parts)}")
        for ev in t.get("recent_events", []):
            ts = ev["listened_at"][:19].replace("T", " ")
            print(f"    {ts} ({ev['service']})")
        print()


def _cmd_feeds() -> None:
    resp = _api_request("GET", "/api/v1/calendar-feeds")
    connections = resp.json()
    if not connections:
        print("No calendar connections configured.")
        return
    for c in connections:
        synced = c["last_synced_at"] or "never"
        enabled = "" if c["enabled"] else " [DISABLED]"
        label = f" ({c['label']})" if c.get("label") else ""
        ext_id = c.get("external_user_id") or ""
        name = f"{c['service_type']}"
        if ext_id:
            name += f" ({ext_id})"
        print(f"{name}{label}{enabled}")
        print(f"  id:     {c['id']}")
        if c.get("url"):
            print(f"  url:    {c['url']}")
        print(f"  synced: {synced}")
        print()


_FEED_ADD_USAGE = """\
Usage: resonance-api feed-add <type> [args]

Types:
  songkick <username>        Add Songkick connection
  ical <url> [--label NAME]  Add a generic iCal connection
"""


def _cmd_feed_add() -> None:
    if len(sys.argv) < 4:
        print(_FEED_ADD_USAGE)
        sys.exit(1)
    feed_type = sys.argv[2]

    if feed_type == "songkick":
        username = sys.argv[3]
        print(f"Adding Songkick connection for {username}...")
        resp = _api_request(
            "POST",
            "/api/v1/calendar-feeds/songkick",
            json={"username": username},
        )
        c = resp.json()
        print(f"  Created: {c['service_type']} ({c.get('external_user_id', '')})")

    elif feed_type == "ical":
        url = sys.argv[3]
        label = None
        if "--label" in sys.argv[4:]:
            idx = sys.argv.index("--label")
            if idx + 1 < len(sys.argv):
                label = sys.argv[idx + 1]
        body: dict[str, str | None] = {"url": url}
        if label:
            body["label"] = label
        print(f"Adding iCal connection: {url}")
        resp = _api_request(
            "POST",
            "/api/v1/calendar-feeds/ical",
            json=body,
        )
        c = resp.json()
        print(f"  Created: {c['service_type']} — {c.get('url', '')}")

    else:
        print(f"Unknown feed type: {feed_type}")
        print(_FEED_ADD_USAGE)
        sys.exit(1)


def _cmd_feed_sync() -> None:
    if len(sys.argv) < 3:
        print("Usage: resonance-api feed-sync <connection_id|all>")
        sys.exit(1)
    target = sys.argv[2]

    if target == "all":
        feeds_resp = _api_request("GET", "/api/v1/calendar-feeds")
        connections = feeds_resp.json()
        if not connections:
            print("No calendar connections configured.")
            return
        for c in connections:
            if not c["enabled"]:
                print(f"Skipping disabled: {c['service_type']}")
                continue
            name = c["service_type"]
            ext_id = c.get("external_user_id")
            if ext_id:
                name += f" ({ext_id})"
            print(f"Syncing {name}...")
            resp = _api_request(
                "POST",
                f"/api/v1/sync/connection/{c['id']}",
            )
            data = resp.json()
            task_id = data.get("task_id", "")
            result = _poll_task(task_id, f"  {name}")
            print(f"  Done: {json.dumps(result)}")
            print()
    else:
        print(f"Syncing connection {target}...")
        resp = _api_request(
            "POST",
            f"/api/v1/sync/connection/{target}",
        )
        data = resp.json()
        task_id = data.get("task_id", "")
        result = _poll_task(task_id, "Syncing")
        print(json.dumps(result, indent=2))


_PROFILE_USAGE = """\
Usage: resonance-api profile <subcommand> [args]

Subcommands:
  list                              List all profiles
  show <profile-id>                 Show profile with generation history
  create --type <type> --name <n>   Create a profile
           [--input key=val ...]
           [--param key=val ...]
  update <profile-id>               Update a profile
           [--name <n>]
           [--param key=val ...]
           [--input key=val ...]
  delete <profile-id>               Delete a profile
"""


def _parse_key_value_args(args: list[str], flag: str) -> dict[str, str]:
    """Parse repeated --flag key=value pairs from argv.

    Args:
        args: The argument list to scan.
        flag: The flag name (e.g. "--param").

    Returns:
        A dict of parsed key=value pairs.
    """
    result: dict[str, str] = {}
    i = 0
    while i < len(args):
        if args[i] == flag and i + 1 < len(args):
            kv = args[i + 1]
            if "=" in kv:
                k, v = kv.split("=", 1)
                result[k] = v
            i += 2
        else:
            i += 1
    return result


def _cmd_profile() -> None:
    if len(sys.argv) < 3:
        print(_PROFILE_USAGE)
        sys.exit(1)

    subcmd = sys.argv[2]

    if subcmd == "list":
        resp = _api_request("GET", "/api/v1/generator-profiles")
        profiles = resp.json()
        if not profiles:
            print("No generator profiles.")
            return
        for p in profiles:
            params_str = ", ".join(
                f"{k}={v}" for k, v in sorted(p.get("parameter_values", {}).items())
            )
            print(f"{p['name']} ({p['generator_type']})")
            print(f"  id: {p['id']}")
            if params_str:
                print(f"  params: {params_str}")
            created = p["created_at"][:19].replace("T", " ")
            print(f"  created: {created}")
            print()

    elif subcmd == "show":
        if len(sys.argv) < 4:
            print("Usage: resonance-api profile show <profile-id>")
            sys.exit(1)
        profile_id = sys.argv[3]
        resp = _api_request("GET", f"/api/v1/generator-profiles/{profile_id}")
        p = resp.json()
        params_str = ", ".join(
            f"{k}={v}" for k, v in sorted(p.get("parameter_values", {}).items())
        )
        print(f"{p['name']} ({p['generator_type']})")
        print(f"  id: {p['id']}")
        if params_str:
            print(f"  params: {params_str}")
        inputs = p.get("input_references", {})
        if inputs:
            inputs_str = ", ".join(f"{k}={v}" for k, v in sorted(inputs.items()))
            print(f"  inputs: {inputs_str}")
        created = p["created_at"][:19].replace("T", " ")
        updated = p["updated_at"][:19].replace("T", " ")
        print(f"  created: {created}")
        print(f"  updated: {updated}")
        generations = p.get("generations", [])
        if generations:
            print(f"  generations ({len(generations)}):")
            for g in generations:
                gen_date = g["created_at"][:19].replace("T", " ")
                duration = g.get("generation_duration_ms")
                dur_str = f" ({duration}ms)" if duration else ""
                freshness = g.get("freshness_actual")
                fresh_str = f" freshness={freshness}" if freshness is not None else ""
                print(f"    {gen_date} playlist={g['playlist_id']}{fresh_str}{dur_str}")

    elif subcmd == "create":
        remaining = sys.argv[3:]
        name: str | None = None
        gen_type: str | None = None
        # Parse --name and --type
        i = 0
        while i < len(remaining):
            if remaining[i] == "--name" and i + 1 < len(remaining):
                name = remaining[i + 1]
                i += 2
            elif remaining[i] == "--type" and i + 1 < len(remaining):
                gen_type = remaining[i + 1]
                i += 2
            else:
                i += 1
        if not name or not gen_type:
            print("Usage: resonance-api profile create --type <type> --name <name>")
            sys.exit(1)
        inputs = _parse_key_value_args(remaining, "--input")
        params = _parse_key_value_args(remaining, "--param")
        body: dict[str, object] = {
            "name": name,
            "generator_type": gen_type,
            "input_references": inputs,
        }
        if params:
            body["parameter_values"] = {k: int(v) for k, v in params.items()}
        resp = _api_request("POST", "/api/v1/generator-profiles", json=body)
        data = resp.json()
        print(f"Created profile: {data['name']} ({data['generator_type']})")
        print(f"  id: {data['profile_id']}")

    elif subcmd == "update":
        if len(sys.argv) < 4:
            print("Usage: resonance-api profile update <profile-id> [--name ...] ...")
            sys.exit(1)
        profile_id = sys.argv[3]
        remaining = sys.argv[4:]
        body_update: dict[str, object] = {}
        # Parse --name
        i = 0
        while i < len(remaining):
            if remaining[i] == "--name" and i + 1 < len(remaining):
                body_update["name"] = remaining[i + 1]
                i += 2
            else:
                i += 1
        params = _parse_key_value_args(remaining, "--param")
        if params:
            body_update["parameter_values"] = {k: int(v) for k, v in params.items()}
        inputs = _parse_key_value_args(remaining, "--input")
        if inputs:
            body_update["input_references"] = inputs
        if not body_update:
            print("Nothing to update. Use --name, --param, or --input.")
            sys.exit(1)
        resp = _api_request(
            "PATCH",
            f"/api/v1/generator-profiles/{profile_id}",
            json=body_update,
        )
        data = resp.json()
        print(f"Updated profile: {data['name']} ({data['generator_type']})")
        print(f"  id: {data['id']}")

    elif subcmd == "delete":
        if len(sys.argv) < 4:
            print("Usage: resonance-api profile delete <profile-id>")
            sys.exit(1)
        profile_id = sys.argv[3]
        _api_request("DELETE", f"/api/v1/generator-profiles/{profile_id}")
        print(f"Deleted profile {profile_id}")

    else:
        print(f"Unknown profile subcommand: {subcmd}")
        print(_PROFILE_USAGE)
        sys.exit(1)


def _cmd_generate() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: resonance-api generate <profile-id>"
            " [--freshness N] [--max-tracks N]"
        )
        sys.exit(1)
    profile_id = sys.argv[2]
    remaining = sys.argv[3:]

    body: dict[str, int] = {}
    i = 0
    while i < len(remaining):
        if remaining[i] == "--freshness" and i + 1 < len(remaining):
            body["freshness_target"] = int(remaining[i + 1])
            i += 2
        elif remaining[i] == "--max-tracks" and i + 1 < len(remaining):
            body["max_tracks"] = int(remaining[i + 1])
            i += 2
        else:
            i += 1

    print(f"Triggering generation for profile {profile_id}...")
    resp = _api_request(
        "POST",
        f"/api/v1/generator-profiles/{profile_id}/generate",
        json=body if body else None,
    )
    data = resp.json()
    task_id = data.get("task_id", "")
    result = _poll_task(task_id, "Generating playlist")
    playlist_id = result.get("playlist_id", "unknown")
    print(f"Playlist created: {playlist_id}")


def _cmd_playlists() -> None:
    resp = _api_request("GET", "/api/v1/playlists")
    playlists = resp.json()
    if not playlists:
        print("No playlists.")
        return
    for p in playlists:
        count = p.get("track_count", 0)
        pinned = " [pinned]" if p.get("is_pinned") else ""
        print(f"{p['name']} ({count} tracks){pinned}")
        print(f"  id: {p['id']}")
        created = p["created_at"][:19].replace("T", " ")
        print(f"  created: {created}")
        print()


def _cmd_playlist() -> None:
    if len(sys.argv) < 3:
        print("Usage: resonance-api playlist <id> [diff <other-id>]")
        sys.exit(1)

    # Check for diff subcommand
    if sys.argv[2] == "diff":
        if len(sys.argv) < 5:
            print("Usage: resonance-api playlist diff <playlist-id> <other-id>")
            sys.exit(1)
        playlist_id = sys.argv[3]
        other_id = sys.argv[4]
        resp = _api_request("GET", f"/api/v1/playlists/{playlist_id}/diff/{other_id}")
        data = resp.json()
        print(f"Playlist diff: {data['playlist_a_id']} vs {data['playlist_b_id']}")
        print(f"  added:   {data['added_count']}")
        print(f"  removed: {data['removed_count']}")
        print(f"  common:  {data['common_count']}")
        return

    playlist_id = sys.argv[2]

    # Check for 'diff' as third arg: playlist <id> diff <other-id>
    if len(sys.argv) >= 5 and sys.argv[3] == "diff":
        other_id = sys.argv[4]
        resp = _api_request("GET", f"/api/v1/playlists/{playlist_id}/diff/{other_id}")
        data = resp.json()
        print(f"Playlist diff: {data['playlist_a_id']} vs {data['playlist_b_id']}")
        print(f"  added:   {data['added_count']}")
        print(f"  removed: {data['removed_count']}")
        print(f"  common:  {data['common_count']}")
        return

    resp = _api_request("GET", f"/api/v1/playlists/{playlist_id}")
    p = resp.json()
    count = p.get("track_count", 0)
    pinned = " [pinned]" if p.get("is_pinned") else ""
    print(f"{p['name']} ({count} tracks){pinned}")

    gen = p.get("generation")
    if gen:
        profile_name = gen.get("profile_name") or gen["profile_id"]
        freshness = gen.get("freshness_actual")
        fresh_str = f", freshness={freshness}" if freshness is not None else ""
        print(f"  generated from: {profile_name}{fresh_str}")

    tracks = p.get("tracks", [])
    for t in tracks:
        pos = t["position"]
        source_tag = f"[{t['source']}]" if t.get("source") else ""
        score = t.get("score")
        score_str = f" ({score:.2f})" if score is not None else ""
        print(f"  {pos}. {t['title']} -- {t['artist_name']} {source_tag}{score_str}")


def _cmd_set_role() -> None:
    if len(sys.argv) != 4:
        print("Usage: resonance-api set-role <user_id> <role>")
        valid = ", ".join(r.value for r in types_module.UserRole)
        print(f"  Roles: {valid}")
        sys.exit(1)
    asyncio.run(_set_role(sys.argv[2], sys.argv[3]))


_COMMANDS: dict[str, tuple[str, Callable[[], None]]] = {
    "healthz": ("Health + deployed revision", _cmd_healthz),
    "status": ("Recent sync job overview", _cmd_status),
    "stats": ("Database statistics", _cmd_stats),
    "sync": ("Trigger a sync", _cmd_sync),
    "feeds": ("List calendar connections", _cmd_feeds),
    "feed-add": ("Add calendar connection", _cmd_feed_add),
    "feed-sync": ("Sync a calendar connection", _cmd_feed_sync),
    "dedup": ("Run deduplication", _cmd_dedup),
    "task": ("Check task status", _cmd_task),
    "track": ("Search tracks by title", _cmd_track),
    "profile": ("Manage generator profiles", _cmd_profile),
    "generate": ("Generate a playlist", _cmd_generate),
    "playlists": ("List playlists", _cmd_playlists),
    "playlist": ("Show or diff a playlist", _cmd_playlist),
    "set-role": ("Set user role (direct DB)", _cmd_set_role),
}


def api() -> None:
    """Entry point for ``resonance-api <command> [args]``."""
    if len(sys.argv) < 2 or sys.argv[1] in ("--help", "-h"):
        print(_USAGE)
        sys.exit(0 if len(sys.argv) >= 2 else 1)

    command = sys.argv[1]

    handler = _COMMANDS.get(command)
    if handler is None:
        print(f"Unknown command: {command}")
        print(_USAGE)
        sys.exit(1)

    handler[1]()
