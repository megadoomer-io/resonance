# Dev Environments

`make dev-up` gives you one local PostgreSQL + Redis. Add `ENV=<name>` to the
same `dev-*` targets to get a separate, disposable environment instead. Each
one can run several app versions (any git ref) against a single database. Use
them for smoke checks, migration rollout rehearsals and bug reproductions that
unit tests can't model.

With `ENV` unset, every target behaves as it always has: compose project from
the checkout directory, ports 5432/6379, the `<project>_resonance-pgdata18`
volume. `ENV` and the other variables (`REF`, `TO`, `NAME`, `PORT`) count only
when given on the `make` command line, so an `ENV` exported in your shell
changes nothing, and an empty `ENV=` is an error rather than the default stack.

Requires Docker (Colima on macOS) and `docker-compose`.

## Targets

| Target | What it does |
|--------|--------------|
| `make dev-up ENV=x` | Start PostgreSQL + Redis as compose project `resonance-x` on its own ports and volume |
| `make dev-migrate ENV=x [REF=<ref>] [TO=<rev>]` | `alembic upgrade` from REF's worktree (default: the current checkout), to `head` by default |
| `make dev-app ENV=x REF=<ref> [NAME=<app>] [PORT=<port>]` | Start an API (uvicorn) + worker from a worktree at REF, in the background. NAME defaults to `app`. Rerunning with the same NAME restarts it |
| `make dev-stop-app ENV=x NAME=<app>` | Stop one app |
| `make -s dev-env ENV=x [NAME=<app>]` | Print `RESONANCE_URL` / `RESONANCE_API_TOKEN` exports for an app. NAME is optional when only one app is running |
| `make dev-status` | List environments and apps: ref, commit, URL, whether API and worker are running |
| `make dev-down ENV=x` | Stop the env's apps, `docker-compose down -v` its project, remove its worktrees and state. Also cleans up a partly removed env (state dir or containers left over) |

`dev-reset ENV=x` is the same as `dev-down ENV=x`. With `ENV` unset,
`dev-down` / `dev-reset` keep their usual meaning and act on the default stack only.

The logic lives in `scripts/dev-env.sh`.

### What an environment holds

State goes in `~/.local/state/resonance-dev/<env>/` (override with
`RESONANCE_DEV_STATE`):

- `env`: the port block and the env's throwaway secrets, including its
  `ADMIN_API_TOKEN`. Each env gets a block of 10 ports starting from a hash of
  its name (PostgreSQL at +0, Redis at +1, apps from +2). The block is moved if
  it's taken, then recorded, so later commands never need you to remember ports.
- `worktrees/<sha>/`: one detached git worktree, with its own `uv` venv, per
  commit. Apps at the same commit share one.
- `apps/<name>/`: `meta` (ref, commit, port), `api.pid` / `worker.pid`, and
  `api.log` / `worker.log`.

Apps run with `DEBUG=true`, `WORKER_MODE=external`, `DEV_SEED_ENABLED=true`
and the rate limiter off. The worker runs as `python -m resonance.worker`
(the `arq` CLI crashes on Python 3.14).

Both the API and the worker refuse to start when the database isn't at their
code's migration head (`assert_schema_current`). Migrate before `dev-app`,
and expect an app to fail to start if the DB is at a revision its code doesn't know.

## Seeding test data

Create test data through the API, one scenario per environment. There is no
shared base dataset. The admin seed endpoint builds the states the regular API
can't build cheaply:

```bash
eval "$(make -s dev-env ENV=x)"
uv run resonance-api seed scenario.json      # or: ... seed - < scenario.json
```

`POST /api/v1/admin/seed` takes one JSON document:

```json
{
  "user": {"display_name": "Tester", "email": null, "role": "user"},
  "artists": [
    {
      "name": "Converge",
      "service_links": {"musicbrainz": {"id": "8f6bd1e4-fbe1-4f50-aa9b-94c450ec0f11"}},
      "tags": [{"tag": "metalcore", "genre_mbid": "<genre mbid>", "count": 7}],
      "similar": [{"neighbor_name": "Botch", "neighbor_mbid": null, "rank": 1, "connector": "lastfm"}],
      "tracks": [
        {"title": "Jane Doe", "duration_ms": 690000, "popularity_score": 40,
         "service_links": null,
         "listens": [{"listened_at": "2026-01-01T12:00:00Z", "source_service": "listenbrainz"}]}
      ]
    },
    {"name": "Legacy shape", "service_links": {"listenbrainz": "<flat mbid string>"}}
  ]
}
```

- Everything except `name` / `title` / `tag` / `neighbor_name` / `listened_at`
  is optional. Unknown fields are rejected (422).
- `service_links` is stored as given, so legacy shapes work.
- Listens belong to `user` when it's given (a new user is created). Otherwise
  they go to the owner, the user the admin token acts as, who is created
  ("Dev Owner") if the database has none.
- The response holds the user id, each artist's id and track ids, and counts.
- Rows go through the ORM. A constraint violation (for example two artists
  with one MBID once the unique index exists) rolls back the whole scenario
  and returns 409.
- The endpoint returns 404 unless the app runs with `DEV_SEED_ENABLED=true`
  (off by default; `dev-app` turns it on).

### States the current schema forbids

Seeding never bypasses constraints. Data the current code refuses (duplicates
from before a unique index, a legacy column shape) arises in prod by being
written while the schema still allowed it. Build it the same way: run an
**older** app at a ref whose schema allows the state, seed through its API,
then migrate the database forward. That needs a ref with both `/admin/seed`
and the pre-constraint schema, so it works for constraints added after
seeding exists. For older constraints, use whatever write path the older
ref's API offers.

## Worked example: two refs on one database

This runs `origin/main` and your checkout's commit (`HEAD`) side by side,
after a stop at an older migration revision. Only `HEAD` has `/admin/seed`,
so it seeds, and both apps read.

```bash
# 1. An environment, migrated one revision short of head (before the unique
#    index on artists.musicbrainz_id).
make dev-up ENV=demo
make dev-migrate ENV=demo REF=origin/main TO=b8c9d0e1f2a3

# 2. An app refuses to start on a schema behind its code; the log names
#    both revisions.
make dev-app ENV=demo REF=origin/main NAME=old
tail -n 3 ~/.local/state/resonance-dev/demo/apps/old/api.log

# 3. Migrate to head, then run both refs against the one database.
make dev-migrate ENV=demo REF=origin/main
make dev-app ENV=demo REF=origin/main NAME=old
make dev-app ENV=demo REF=HEAD NAME=new
make dev-status

# 4. Seed through the newer app.
eval "$(make -s dev-env ENV=demo NAME=new)"
uv run resonance-api seed - <<'EOF'
{"artists": [
  {"name": "Converge",
   "service_links": {"musicbrainz": {"id": "8f6bd1e4-fbe1-4f50-aa9b-94c450ec0f11"}},
   "tracks": [{"title": "Jane Doe", "listens": [{"listened_at": "2026-01-01T12:00:00Z"}]}]}
]}
EOF

# 5. The unique index holds: a second artist with the same MBID (any case,
#    any service_links shape) is refused with 409, and nothing is kept.
uv run resonance-api seed - <<'EOF'
{"artists": [{"name": "Converge", "service_links": {"listenbrainz": "8F6BD1E4-FBE1-4F50-AA9B-94C450EC0F11"}}]}
EOF

# 6. The older app reads the same rows.
eval "$(make -s dev-env ENV=demo NAME=old)"
uv run resonance-api stats
uv run resonance-api artists mbid-duplicates
```

To rehearse a migration a later change adds, swap the roles: seed through the
app at the older ref while the database is at that ref's head, stop it, run
`make dev-migrate ENV=demo REF=<newer ref>`, and start the newer app. A
migration that refuses the data leaves the database at the older revision, so
the older app can still start and fix the data through its API.

## Tear down

```bash
make dev-down ENV=demo    # apps, containers, volume, worktrees, state
make dev-status           # confirm it's gone
```

### Leftover Docker volumes

Every Docker volume these targets, the default `make dev-*` stack, test runs or
agent worktrees create holds disposable dev data, so deleting it is always
safe. That includes the default stack's `<project>_resonance-pgdata18` and
volumes from older compose files (`*_resonance-pgdata`). Real data lives only in
the cluster, never in a local volume.

Volumes outlive the checkout or worktree that created them. To find and remove
the unused ones:

```bash
for v in $(docker volume ls -q | grep resonance-pgdata); do
  [ -z "$(docker ps -aq --filter volume="$v")" ] && echo "$v"
done                       # unused volumes
docker volume rm <name>... # remove them
```
