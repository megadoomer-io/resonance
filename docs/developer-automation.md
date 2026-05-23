# Developer Automation Patterns

Patterns built into Resonance that accelerate development, especially with
AI-assisted workflows. These are designed so that an automated agent (or a
developer with a CLI) can build, deploy, verify, operate, and debug the
application without opening a browser or reading logs manually.

For architecture details, see [architecture.md](architecture.md).
For deployment setup, see [self-hosting.md](self-hosting.md).

---

## 1. Deployment Verification: `/healthz` with Git SHA

The app exposes a health endpoint that returns the exact git commit running in
production. This closes the "what's actually deployed?" question instantly.

**How it works:**

1. `Dockerfile` accepts a build arg and exports it to the runtime environment:
   ```dockerfile
   ARG GIT_SHA=unknown
   ENV GIT_SHA=${GIT_SHA}
   ```

2. CI passes the commit SHA at build time:
   ```yaml
   # .github/workflows/publish.yaml
   build-args: |
     GIT_SHA=${{ github.sha }}
   ```

3. The app reads it at request time:
   ```python
   @application.get("/healthz")
   async def healthz() -> dict[str, str]:
       return {"status": "ok", "revision": os.environ.get("GIT_SHA", "dev")}
   ```

4. After a deploy, verify with: `curl https://app.example.com/healthz`

**Why it matters:** Without this, verifying a deploy means checking ArgoCD,
cross-referencing image tags, or hoping the change is visible in the UI. With
it, `resonance-api healthz` confirms the running revision in one call.

**Bootstrapping in a new project:** Add the three pieces above. Takes ~5
minutes and saves hours of "is my change live?" confusion.

---

## 2. CLI Tool with Bearer Token Auth

`resonance-api` is a standalone CLI that talks to the running application over
HTTP using bearer token authentication. Every admin operation available in the
UI is also available via CLI.

**Auth pattern:**

- Server side (`dependencies.py`): a `verify_admin_access` dependency checks
  the `Authorization: Bearer <token>` header against the `ADMIN_API_TOKEN`
  config value. Falls back to session-based auth for browser requests.
- Client side (`cli.py`): reads `RESONANCE_API_TOKEN` env var (or falls back
  to the app's config file), attaches it to every request.
- Same token, same endpoints, two access paths (browser session vs bearer
  token).

**Key CLI capabilities:**

| Command | Purpose |
|---------|---------|
| `healthz` | Deployment verification (git SHA) |
| `status` | Recent sync job overview |
| `stats` | Database statistics |
| `sync <service>` | Trigger a data sync |
| `dedup <type>` | Run deduplication jobs |
| `task <id>` | Poll a background task's progress |
| `api METHOD /path` | Raw API passthrough (like `gh api`) |

**Design decisions:**

- **No framework** — the CLI is a single `cli.py` file using `sys.argv`
  parsing and `httpx` for HTTP calls. No Click, Typer, or argparse. This keeps
  the dependency footprint minimal and the code easy to read.
- **Task polling** — long-running operations return a task ID. The CLI polls
  with TTY-aware progress display and supports `--wait`/`--no-wait` modes.
- **Raw API passthrough** — `resonance-api api GET /api/v1/whatever` sends an
  authenticated request and pretty-prints the response. Useful for ad-hoc
  queries without crafting curl commands.
- **Direct DB escape hatch** — `set-role` and `watermark` bypass the API and
  connect directly to PostgreSQL. These are disaster recovery commands for when
  the app is down.

**Bootstrapping in a new project:** Start with `healthz`, `status`, and `api`
commands. Add domain-specific commands as the project grows. The bearer token
auth pattern is the foundation — get that right first.

---

## 3. GitOps Deployment Pipeline

Pushing to `main` triggers a fully automated build-tag-deploy pipeline with no
manual steps.

**Pipeline flow:**

```
push to main
  → GitHub Actions: build Docker image
  → Tag with YYYYMMDDTHHmmss-<shortsha> + latest
  → Push to GHCR (ghcr.io/megadoomer-io/resonance)
  → Check out megadoomer-config repo
  → Run kustomize edit set image to update the tag
  → Commit and push config change
  → ArgoCD detects config change and auto-syncs
```

**Key implementation details:**

- **Image tag format** (`YYYYMMDDTHHmmss-<shortsha>`): sortable by time,
  traceable to commit. Better than `:latest` alone because you can see when an
  image was built.
- **Cross-repo config update**: the `update-config` job checks out the
  deployment config repo (`megadoomer-config`) using a fine-grained PAT, runs
  `kustomize edit set image`, and pushes. This keeps deployment state in git
  (true GitOps) rather than using imperative `kubectl set image`.
- **Idempotent**: if the image tag hasn't changed, `git diff --cached --quiet`
  skips the commit.

**CI workflows:**

| Workflow | Trigger | What it does |
|----------|---------|--------------|
| `lint-and-test.yaml` | PR + push to main | ruff lint, ruff format check, mypy, pytest |
| `publish.yaml` | push to main | Build, tag, push image, update config repo |

**Bootstrapping in a new project:** Copy `publish.yaml` and adapt the image
name, config repo path, and PAT secret name. The `lint-and-test.yaml` is
generic for any Python/uv project.

---

## 4. Database Migrations as Init Container

Alembic migrations are baked into the Docker image and run on every deploy.

**How it works:**

- `alembic.ini` and `alembic/` directory are copied into the image at build
  time
- The Kubernetes deployment runs `alembic upgrade head` as an init container
  before the app starts
- Schema always matches the code in the deployed image

**Safety conventions:**

- Never drop a table in the same migration that creates its replacement. Use
  multi-step migrations: create new → migrate data → update code → drop old.
- Each migration is independently deployable and rollback-safe.

---

## 5. Background Task Infrastructure

Long-running operations (data sync, dedup, playlist generation) run as
background tasks via arq (Redis-backed job queue).

**Patterns worth replicating:**

- **Task lifecycle helpers** (`sync/lifecycle.py`): `complete_task()` and
  `fail_task()` ensure consistent state transitions. No inline status updates
  scattered across the codebase.
- **Parent-child task hierarchy**: a sync job spawns child tasks for parallel
  or sequential work. Parent completion checker aggregates child results.
- **Orphan recovery on startup**: when the worker starts, it scans for
  PENDING/RUNNING tasks that lost their arq job (e.g., after a crash) and
  re-enqueues them. No lost work across restarts.
- **Watermark-based resume**: streaming syncs checkpoint progress. On crash,
  the next run resumes from the watermark instead of restarting from scratch.
- **Graceful shutdown**: 30-second timeout for in-flight jobs before worker
  termination (`--timeout-graceful-shutdown 30` in the uvicorn CMD).

**Bootstrapping in a new project:** Start with arq + Redis, add lifecycle
helpers early. Orphan recovery and watermarks can wait until the project has
long-running jobs that justify the complexity.

---

## 6. Local Development Loop

### Makefile

```makefile
run:        uv run uvicorn app:create_app --factory --reload
test:       uv run pytest
lint:       uv run ruff check .
format:     uv run ruff format .
typecheck:  uv run mypy src/
check:      lint typecheck test    # one command before pushing
clean:      rm -rf .mypy_cache .pytest_cache .ruff_cache dist
```

`make check` runs the same gates as CI. If it passes locally, CI will pass.

### Pre-commit hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: local
    hooks:
      - id: mypy
        entry: uv run mypy src/
        language: system
        types: [python]
        pass_filenames: false
```

Three checks on every commit: lint (with auto-fix), format, and strict type
checking. Catches issues before they reach CI.

### Component playground

`/dev/components` (admin-only) renders every UI macro in every state. A
metadata-driven catalog defined in `ui/playground.py`:

```python
COMPONENT_ENTRIES = [
    {"name": "entity_list", "description": "...", "states": ["populated", "empty"]},
    {"name": "action_button", "description": "...", "states": ["default", "danger"]},
    # ...
]
```

Useful for isolated component development and design system documentation.

---

## 7. Dual Auth: Session + Bearer Token

The app supports two authentication paths through the same endpoints:

1. **Browser sessions** — cookie-based, backed by Redis. Used by the web UI.
2. **Bearer tokens** — `Authorization: Bearer <token>` header. Used by the CLI
   and programmatic access.

The `get_current_user_id` dependency tries session first, then bearer token.
The `verify_admin_access` dependency does the same for admin routes. This means
every endpoint works for both humans and automation without separate API
versions.

**Implementation** (`dependencies.py`):

```python
async def get_current_user_id(request, session) -> uuid.UUID:
    # 1. Try session auth
    user_id = session.get("user_id")
    if user_id is not None:
        return uuid.UUID(user_id)

    # 2. Try bearer token → resolve to owner user
    auth_header = request.headers.get("authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        if settings.admin_api_token and token == settings.admin_api_token:
            # Look up owner user in DB
            ...
```

---

## 8. Docker Image Optimization

The Dockerfile uses a multi-stage build with uv for fast, reproducible builds:

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev    # deps layer (cached)
COPY src/ src/
RUN uv sync --frozen --no-dev                          # project install

FROM python:3.14-slim-bookworm
COPY --from=builder /app/.venv /app/.venv
# ... copy source, alembic, set GIT_SHA
USER nobody
CMD ["uvicorn", "resonance.app:create_app", "--factory", ...]
```

**Key decisions:**

- **Dependency layer caching**: `pyproject.toml` + `uv.lock` are copied before
  source code. Dependencies only rebuild when they change.
- **`--frozen`**: uses the lockfile exactly as committed. No version resolution
  at build time.
- **Non-root user**: runs as `nobody` for security.
- **Slim base image**: minimal attack surface.

---

## Summary: Bootstrap Priority

When starting a new project, add these in order of effort-to-value ratio:

| Priority | Pattern | Effort | Value |
|----------|---------|--------|-------|
| 1 | `/healthz` with git SHA | ~5 min | Answers "what's deployed?" instantly |
| 2 | Makefile with `check` target | ~5 min | One command to validate locally |
| 3 | Pre-commit hooks (ruff + mypy) | ~10 min | Catches issues before CI |
| 4 | CI lint-and-test workflow | ~15 min | Same checks in CI as locally |
| 5 | Bearer token auth + CLI skeleton | ~30 min | Programmatic access from day one |
| 6 | GitOps publish workflow | ~30 min | Push-to-deploy pipeline |
| 7 | Docker multi-stage build | ~15 min | Fast, reproducible images |
| 8 | Task lifecycle helpers | ~1 hr | Only when background jobs arrive |
| 9 | Component playground | ~30 min | Only when UI component library exists |
