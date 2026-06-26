"""CLI commands for Resonance administration."""

from __future__ import annotations

import asyncio
import datetime
import json
import os
import pathlib
import sys
import time
import urllib.parse
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
Usage: resonance-api [--as-user <id>] <command> [args]

Global flags:
  --as-user <id>               Assume a user identity on user-scoped endpoints
                               (admin token only; for agent live testing)

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
  backfill-mbids [opts]        Backfill MusicBrainz MBIDs (#71)
  backfill-popularity [opts]   Backfill ListenBrainz popularity
  task <task_id>               Check task status
  track <query>                Search tracks by title
  profile <subcommand>         Manage generator profiles
  generate <profile-id>        Generate a playlist from a profile
  enrich <profile-id> ...      Add related artists to a profile's pool
  playlists                    List playlists
  playlist <id> [diff <other>] Show or diff a playlist
  watermark <service> [--reset] View or reset sync watermark (direct DB)
  import concert_archives      Import Concert Archives CSV
  api [METHOD] PATH [opts]     Raw API request (like gh api)
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
    headers = {"Authorization": f"Bearer {token}"}
    # Assume a user identity for user-scoped endpoints (#135). Set via the
    # global --as-user flag or RESONANCE_ASSUME_USER. Requires the admin token.
    assume_user = os.environ.get("RESONANCE_ASSUME_USER")
    if assume_user:
        headers["X-Assume-User"] = assume_user
    try:
        response = httpx.request(
            method,
            url,
            headers=headers,
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
    resp = _api_request("GET", "/api/v1/admin/status")
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
    resp = _api_request("GET", "/api/v1/admin/stats")
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
        resp = _api_request("GET", f"/api/v1/admin/tasks/{task_id}")
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
            print(f"{now} {label} {status} {p_str}", flush=True)

        time.sleep(poll_interval)


def _cmd_dedup() -> None:
    if len(sys.argv) < 3:
        print(_DEDUP_USAGE)
        sys.exit(1)
    dedup_type = sys.argv[2]
    no_wait = "--no-wait" in sys.argv[3:]

    targets: list[tuple[str, str]] = []
    if dedup_type == "events":
        targets = [("events", "/api/v1/admin/dedup/events")]
    elif dedup_type == "artists":
        targets = [("artists", "/api/v1/admin/dedup/artists")]
    elif dedup_type == "tracks":
        targets = [("tracks", "/api/v1/admin/dedup/tracks")]
    elif dedup_type == "all":
        targets = [
            ("artists", "/api/v1/admin/dedup/artists"),
            ("tracks", "/api/v1/admin/dedup/tracks"),
            ("events", "/api/v1/admin/dedup/events"),
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
    resp = _api_request("GET", f"/api/v1/admin/tasks/{task_id}")
    print(json.dumps(resp.json(), indent=2))


def _cmd_track() -> None:
    query = " ".join(sys.argv[2:])
    if not query.strip():
        print("Usage: resonance-api track <query>")
        sys.exit(1)
    resp = _api_request("GET", f"/api/v1/admin/tracks?q={urllib.parse.quote(query)}")
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
                print(f"Skipping disabled: {c['service_type']}", flush=True)
                continue
            name = c["service_type"]
            ext_id = c.get("external_user_id")
            if ext_id:
                name += f" ({ext_id})"
            print(f"Syncing {name}...", flush=True)
            resp = _api_request(
                "POST",
                f"/api/v1/sync/connection/{c['id']}",
            )
            data = resp.json()
            task_id = data.get("task_id", "")
            result = _poll_task(task_id, f"  {name}")
            print(f"  Done: {json.dumps(result)}", flush=True)
            print(flush=True)
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
           [--source event:<uuid> ...]    Layered pool sources (#128)
           [--source artist:<uuid> ...]
           [--source related:<n>[:seed] ...]
           [--exclude <artist-uuid> ...]  Drop artists from the pool
           [--param key=val ...]
           [--input key=val ...]          Legacy (e.g. event_id=<uuid>)
  update <profile-id>               Update a profile
           [--name <n>]
           [--param key=val ...]
           [--source ... | --exclude ... | --input key=val ...]
  exclude-track <profile-id> <track-id> [<track-id> ...] [--regenerate]
                                    Add tracks to the recipe's exclude list
                                    (an excluded track is skipped at selection;
                                    --regenerate refills the freed slots)
  delete <profile-id>               Delete a profile

Sources and --input are mutually exclusive: if any --source/--exclude is given,
the layered spec is sent; otherwise --input key=value is used (legacy).
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


def _parse_source_spec(spec: str) -> dict[str, object]:
    """Parse one ``--source`` spec into a layered pool source dict.

    Grammar:
      ``event:<uuid>``                -> an event source
      ``artist:<uuid>``               -> a single artist source
      ``related:<amount>[:<seed>]``   -> fold in N related artists (seed defaults
                                         to "target")

    Exits with a usage error on an unknown kind or a non-integer related amount.
    """
    kind, _, rest = spec.partition(":")
    if kind == "event":
        return {"kind": "event", "event_id": rest, "enabled": True}
    if kind == "artist":
        return {"kind": "artist", "artist_id": rest, "enabled": True}
    if kind == "related":
        amount_str, _, seed = rest.partition(":")
        try:
            amount = int(amount_str)
        except ValueError:
            print(f"Invalid related amount: {amount_str!r} (expected an integer)")
            sys.exit(1)
        return {
            "kind": "related",
            "amount": amount,
            "seed": seed or "target",
            "enabled": True,
        }
    print(f"Unknown source kind: {kind!r} (expected event, artist, or related)")
    sys.exit(1)


def _parse_pool_sources(args: list[str]) -> dict[str, object] | None:
    """Parse ``--source`` / ``--exclude`` flags into a layered input_references dict.

    Grammar (both flags repeatable)::

        --source event:<uuid>
        --source artist:<uuid>
        --source related:<amount>[:<seed>]
        --exclude <artist-uuid>

    Returns the ``{"sources": [...], "exclude_artist_ids": [...]}`` dict, or None
    when neither flag is present so the caller can fall back to the legacy
    ``--input key=value`` form (e.g. ``--input event_id=<uuid>``). This keeps the
    CLI at parity with the API, which accepts both shapes (#128).
    """
    sources: list[dict[str, object]] = []
    excludes: list[str] = []
    saw_flag = False
    i = 0
    while i < len(args):
        if args[i] == "--source" and i + 1 < len(args):
            saw_flag = True
            sources.append(_parse_source_spec(args[i + 1]))
            i += 2
        elif args[i] == "--exclude" and i + 1 < len(args):
            saw_flag = True
            excludes.append(args[i + 1])
            i += 2
        else:
            i += 1
    if not saw_flag:
        return None
    return {"sources": sources, "exclude_artist_ids": excludes}


def _cmd_profile() -> None:
    if len(sys.argv) < 3:
        print(_PROFILE_USAGE)
        sys.exit(1)

    subcmd = sys.argv[2]

    if subcmd == "list":
        resp = _api_request("GET", "/api/v1/generator-profiles/")
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
        # Prefer the layered --source/--exclude grammar; fall back to the legacy
        # --input key=value form (e.g. --input event_id=<uuid>) (#128).
        layered = _parse_pool_sources(remaining)
        input_refs: dict[str, object] = (
            layered
            if layered is not None
            else dict(_parse_key_value_args(remaining, "--input"))
        )
        params = _parse_key_value_args(remaining, "--param")
        body: dict[str, object] = {
            "name": name,
            "generator_type": gen_type,
            "input_references": input_refs,
        }
        if params:
            body["parameter_values"] = {k: int(v) for k, v in params.items()}
        resp = _api_request("POST", "/api/v1/generator-profiles/", json=body)
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
        layered = _parse_pool_sources(remaining)
        if layered is not None:
            body_update["input_references"] = layered
        else:
            legacy_inputs = _parse_key_value_args(remaining, "--input")
            if legacy_inputs:
                body_update["input_references"] = dict(legacy_inputs)
        if not body_update:
            print(
                "Nothing to update. Use --name, --param, --source, --exclude, "
                "or --input."
            )
            sys.exit(1)
        resp = _api_request(
            "PATCH",
            f"/api/v1/generator-profiles/{profile_id}",
            json=body_update,
        )
        data = resp.json()
        print(f"Updated profile: {data['name']} ({data['generator_type']})")
        print(f"  id: {data['id']}")

    elif subcmd == "exclude-track":
        if len(sys.argv) < 5:
            print(
                "Usage: resonance-api profile exclude-track <profile-id> "
                "<track-id> [<track-id> ...] [--regenerate]"
            )
            sys.exit(1)
        profile_id = sys.argv[3]
        rest = sys.argv[4:]
        regenerate = "--regenerate" in rest
        track_ids = [a for a in rest if a != "--regenerate"]
        if not track_ids:
            print("Provide at least one track id to exclude.")
            sys.exit(1)
        # Read the current recipe, append to exclude_track_ids (dedup, preserve
        # order), and PATCH the whole input_references back. The excluded track is
        # skipped at selection time; regenerate refills the freed slot.
        resp = _api_request("GET", f"/api/v1/generator-profiles/{profile_id}")
        refs = dict(resp.json().get("input_references") or {})
        excluded = [str(t) for t in (refs.get("exclude_track_ids") or [])]
        excluded_set = set(excluded)
        for tid in track_ids:
            if tid not in excluded_set:
                excluded.append(tid)
                excluded_set.add(tid)
        refs["exclude_track_ids"] = excluded
        _api_request(
            "PATCH",
            f"/api/v1/generator-profiles/{profile_id}",
            json={"input_references": refs},
        )
        print(
            f"Excluded {len(track_ids)} track(s); {len(excluded)} total on the recipe."
        )
        if regenerate:
            print(f"Triggering regeneration for profile {profile_id}...")
            gen = _api_request(
                "POST",
                f"/api/v1/generator-profiles/{profile_id}/generate",
                json=None,
            )
            task_id = gen.json().get("task_id", "")
            result = _poll_task(task_id, "Regenerating playlist")
            print(f"Playlist regenerated: {result.get('playlist_id', 'unknown')}")

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


def _cmd_enrich() -> None:
    if len(sys.argv) < 3:
        print(
            "Usage: resonance-api enrich <profile-id>"
            " (--lineup | --seed <artist-id> [--seed <artist-id> ...]) [--n N]"
        )
        sys.exit(1)
    profile_id = sys.argv[2]
    remaining = sys.argv[3:]

    seeds: list[str] = []
    lineup = False
    n = 10
    i = 0
    while i < len(remaining):
        if remaining[i] == "--lineup":
            lineup = True
            i += 1
        elif remaining[i] == "--seed" and i + 1 < len(remaining):
            seeds.append(remaining[i + 1])
            i += 2
        elif remaining[i] == "--n" and i + 1 < len(remaining):
            n = int(remaining[i + 1])
            i += 2
        else:
            i += 1

    if lineup and seeds:
        print("Specify either --lineup or --seed, not both.")
        sys.exit(1)
    if not lineup and not seeds:
        print("Specify --lineup or at least one --seed.")
        sys.exit(1)

    body: dict[str, object] = {
        "n": n,
        "seed_artist_ids": "lineup" if lineup else seeds,
    }
    scope_label = "lineup" if lineup else f"{len(seeds)} seed(s)"
    print(f"Triggering enrichment ({scope_label}) for profile {profile_id}...")
    resp = _api_request(
        "POST",
        f"/api/v1/generator-profiles/{profile_id}/enrich",
        json=body,
    )
    data = resp.json()
    task_id = data.get("task_id", "")
    result = _poll_task(task_id, "Enriching pool")
    message = result.get("message")
    if message:
        print(f"Note: {message}")
    found = result.get("found", "?")
    requested = result.get("requested", "?")
    print(f"Added {found} of {requested} related artists to the pool.")


def _cmd_playlists() -> None:
    resp = _api_request("GET", "/api/v1/playlists/")
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


_IMPORT_USAGE = """\
Usage: resonance-api import <type> [args]

Types:
  concert_archives --file PATH  Import a Concert Archives CSV export
      [--export-date YYYY-MM-DD]  Override export date (default: from filename or today)
      [--wait]                    Poll until import completes
"""


def _cmd_import() -> None:
    if len(sys.argv) < 3:
        print(_IMPORT_USAGE)
        sys.exit(1)
    import_type = sys.argv[2]

    if import_type == "concert_archives":
        _import_concert_archives()
    else:
        print(f"Unknown import type: {import_type}")
        print(_IMPORT_USAGE)
        sys.exit(1)


def _import_concert_archives() -> None:
    remaining = sys.argv[3:]

    file_path: str | None = None
    export_date: str | None = None
    wait = "--wait" in remaining

    i = 0
    while i < len(remaining):
        if remaining[i] == "--file" and i + 1 < len(remaining):
            file_path = remaining[i + 1]
            i += 2
        elif remaining[i] == "--export-date" and i + 1 < len(remaining):
            export_date = remaining[i + 1]
            i += 2
        elif remaining[i] == "--wait":
            i += 1
        else:
            i += 1

    if not file_path:
        print("Error: --file is required")
        print(_IMPORT_USAGE)
        sys.exit(1)

    csv_path = pathlib.Path(file_path).expanduser().resolve()
    if not csv_path.is_file():
        print(f"Error: File not found: {csv_path}")
        sys.exit(1)

    print(f"Uploading {csv_path.name}...")

    base_url, token = _get_api_config()
    url = f"{base_url}/api/v1/connections/concert-archives/upload"

    data: dict[str, str] = {}
    if export_date:
        data["export_date"] = export_date

    try:
        with csv_path.open("rb") as f:
            response = httpx.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                files={"file": (csv_path.name, f, "text/csv")},
                data=data,
                timeout=300.0,
                follow_redirects=True,
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

    result = response.json()
    task_id = result.get("task_id", "")
    print(f"Import started: task {task_id}")

    if wait:
        task_result = _poll_task(task_id, "Importing concert archives")
        print(json.dumps(task_result, indent=2))


_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}

_API_USAGE = """\
Usage: resonance-api api [METHOD] PATH [-d DATA] [-H HEADER]...

Make an authenticated API request. Method defaults to GET.

Examples:
  resonance-api api /healthz
  resonance-api api GET /api/v1/calendar-feeds
  resonance-api api POST /api/v1/calendar-feeds/songkick -d '{"username": "mike"}'
  resonance-api api DELETE /api/v1/calendar-feeds/songkick/mike
"""


def _cmd_api() -> None:
    args = sys.argv[2:]
    if not args or args[0] in ("--help", "-h"):
        print(_API_USAGE)
        sys.exit(0)

    if args[0].upper() in _HTTP_METHODS:
        method = args[0].upper()
        if len(args) < 2:
            print("Error: PATH required after METHOD")
            print(_API_USAGE)
            sys.exit(1)
        path = args[1]
        remaining = args[2:]
    else:
        method = "GET"
        path = args[0]
        remaining = args[1:]

    data: str | None = None
    extra_headers: dict[str, str] = {}
    i = 0
    while i < len(remaining):
        if remaining[i] in ("-d", "--data") and i + 1 < len(remaining):
            data = remaining[i + 1]
            i += 2
        elif remaining[i] == "-H" and i + 1 < len(remaining):
            header = remaining[i + 1]
            if ":" in header:
                key, val = header.split(":", 1)
                extra_headers[key.strip()] = val.strip()
            i += 2
        else:
            i += 1

    base_url, token = _get_api_config()
    url = f"{base_url}{path}"
    headers = {"Authorization": f"Bearer {token}"}
    assume_user = os.environ.get("RESONANCE_ASSUME_USER")
    if assume_user:
        headers["X-Assume-User"] = assume_user
    headers.update(extra_headers)

    kwargs: dict[str, object] = {}
    if data is not None:
        try:
            kwargs["json"] = json.loads(data)
        except json.JSONDecodeError:
            kwargs["content"] = data

    try:
        response = httpx.request(
            method,
            url,
            headers=headers,
            timeout=300.0,
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
        print(f"HTTP {response.status_code}", file=sys.stderr)

    try:
        body = response.json()
        print(json.dumps(body, indent=2))
    except Exception:
        text = response.text
        if text:
            print(text)

    if response.status_code >= 400:
        sys.exit(1)


def _cmd_set_role() -> None:
    if len(sys.argv) != 4:
        print("Usage: resonance-api set-role <user_id> <role>")
        valid = ", ".join(r.value for r in types_module.UserRole)
        print(f"  Roles: {valid}")
        sys.exit(1)
    asyncio.run(_set_role(sys.argv[2], sys.argv[3]))


async def _watermark(service: str, *, reset: bool = False) -> None:
    """View or reset a service connection's sync watermark."""
    settings = config_module.Settings()
    engine = database_module.create_async_engine(settings)
    session_factory = database_module.create_session_factory(engine)

    try:
        svc_type = types_module.ServiceType(service)
    except ValueError:
        valid = ", ".join(s.value for s in types_module.ServiceType)
        print(f"Error: Unknown service '{service}'. Valid: {valid}")
        sys.exit(1)

    async with session_factory() as db:
        result = await db.execute(
            sa.select(user_models.ServiceConnection).where(
                user_models.ServiceConnection.service_type == svc_type.name
            )
        )
        conn = result.scalar_one_or_none()
        if conn is None:
            print(f"No connection found for {service}")
            sys.exit(1)

        if reset:
            old = conn.sync_watermark
            conn.sync_watermark = {}
            await db.commit()
            print(f"Watermark reset for {service}")
            print(f"  was: {json.dumps(old, indent=2)}")
        else:
            print(json.dumps(conn.sync_watermark, indent=2))

    await engine.dispose()


def _cmd_watermark() -> None:
    if len(sys.argv) < 3:
        print("Usage: resonance-api watermark <service> [--reset]")
        sys.exit(1)
    service = sys.argv[2]
    reset = "--reset" in sys.argv[3:]
    asyncio.run(_watermark(service, reset=reset))


_BACKFILL_USAGE = """\
Usage: resonance-api backfill-mbids [opts]

Options:
  --status       Show coverage (counts by match status), do not enqueue
  --retry        Re-attempt prior no-match / below-similarity rows
  --tracks       Only backfill track recording MBIDs
  --artists      Only backfill artist MBIDs
  --no-wait      Enqueue and return immediately (don't poll)

With no --tracks/--artists flag, both passes run (tracks first, to harvest
artist MBIDs). Default polls the task to completion and prints per-type counts.
"""


def _cmd_backfill_mbids() -> None:
    args = sys.argv[2:]
    if "--help" in args or "-h" in args:
        print(_BACKFILL_USAGE)
        return
    if "--status" in args:
        resp = _api_request("GET", "/api/v1/admin/backfill-mbids")
        print(json.dumps(resp.json(), indent=2))
        return

    query: list[str] = []
    if "--retry" in args:
        query.append("retry=true")
    want_tracks = "--tracks" in args
    want_artists = "--artists" in args
    if want_tracks and not want_artists:
        query.append("entity_types=track")
    elif want_artists and not want_tracks:
        query.append("entity_types=artist")

    path = "/api/v1/admin/backfill-mbids"
    if query:
        path += "?" + "&".join(query)
    resp = _api_request("POST", path)
    task_id = resp.json().get("task_id", "")

    if "--no-wait" in args:
        print(f"backfill-mbids: task {task_id}")
        return
    result = _poll_task(task_id, "Backfilling MBIDs")
    print(json.dumps(result, indent=2))


_POPULARITY_BACKFILL_USAGE = """\
Usage: resonance-api backfill-popularity [opts]

Options:
  --status       Show coverage (mb-linked vs scored), do not enqueue
  --no-wait      Enqueue and return immediately (don't poll)

Refreshes Track.popularity_score from ListenBrainz recording popularity (global
listen counts, normalized to 0-100) for every track carrying a MusicBrainz recording
MBID, overwriting discovery-sourced synthetic values. Default polls the task to
completion and prints the updated/no-popularity counts.
"""


def _cmd_backfill_popularity() -> None:
    args = sys.argv[2:]
    if "--help" in args or "-h" in args:
        print(_POPULARITY_BACKFILL_USAGE)
        return
    if "--status" in args:
        resp = _api_request("GET", "/api/v1/admin/backfill-popularity")
        print(json.dumps(resp.json(), indent=2))
        return

    resp = _api_request("POST", "/api/v1/admin/backfill-popularity")
    task_id = resp.json().get("task_id", "")

    if "--no-wait" in args:
        print(f"backfill-popularity: task {task_id}")
        return
    result = _poll_task(task_id, "Backfilling popularity")
    print(json.dumps(result, indent=2))


_COMMANDS: dict[str, tuple[str, Callable[[], None]]] = {
    "healthz": ("Health + deployed revision", _cmd_healthz),
    "status": ("Recent sync job overview", _cmd_status),
    "stats": ("Database statistics", _cmd_stats),
    "sync": ("Trigger a sync", _cmd_sync),
    "feeds": ("List calendar connections", _cmd_feeds),
    "feed-add": ("Add calendar connection", _cmd_feed_add),
    "feed-sync": ("Sync a calendar connection", _cmd_feed_sync),
    "dedup": ("Run deduplication", _cmd_dedup),
    "backfill-mbids": ("Backfill MusicBrainz MBIDs", _cmd_backfill_mbids),
    "backfill-popularity": (
        "Backfill ListenBrainz popularity",
        _cmd_backfill_popularity,
    ),
    "task": ("Check task status", _cmd_task),
    "track": ("Search tracks by title", _cmd_track),
    "profile": ("Manage generator profiles", _cmd_profile),
    "generate": ("Generate a playlist", _cmd_generate),
    "enrich": ("Add related artists to a profile's pool", _cmd_enrich),
    "playlists": ("List playlists", _cmd_playlists),
    "playlist": ("Show or diff a playlist", _cmd_playlist),
    "watermark": ("View/reset sync watermark", _cmd_watermark),
    "import": ("Import data from external sources", _cmd_import),
    "api": ("Raw API request", _cmd_api),
    "set-role": ("Set user role (direct DB)", _cmd_set_role),
}


def _extract_as_user() -> None:
    """Strip a global ``--as-user <id>`` flag from argv into the environment.

    Commands parse ``sys.argv`` positionally, so the flag is pulled out before
    dispatch and surfaced to ``_api_request`` via ``RESONANCE_ASSUME_USER``.
    Supports both ``--as-user <id>`` and ``--as-user=<id>``.
    """
    argv = sys.argv
    for i, arg in enumerate(argv[1:], start=1):
        if arg == "--as-user":
            if i + 1 >= len(argv):
                print("Error: --as-user requires a user id")
                sys.exit(1)
            os.environ["RESONANCE_ASSUME_USER"] = argv[i + 1]
            del argv[i : i + 2]
            return
        if arg.startswith("--as-user="):
            os.environ["RESONANCE_ASSUME_USER"] = arg.split("=", 1)[1]
            del argv[i]
            return


def api() -> None:
    """Entry point for ``resonance-api <command> [args]``."""
    _extract_as_user()
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
