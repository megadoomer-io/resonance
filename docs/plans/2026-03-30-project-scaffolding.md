# Project Scaffolding Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a deployable skeleton — a minimal FastAPI app with health endpoint, containerized and built by CI, so every future feature is immediately shippable.

**Architecture:** Minimal FastAPI app factory with a `/healthz` endpoint, packaged via uv and multi-stage Docker build. GitHub Actions runs lint/test on PRs and builds/pushes to GHCR on merge to main. No database or Redis wiring yet — just the app process.

**Tech Stack:** Python 3.13, FastAPI, uvicorn, uv, ruff, mypy, pytest, Docker, GitHub Actions, GHCR

---

### Task 1: pyproject.toml

**Files:**
- Create: `pyproject.toml`

**Step 1: Create pyproject.toml**

```toml
[project]
name = "resonance"
version = "0.1.0"
description = "Personal media discovery and playlist generation platform"
requires-python = ">=3.13"
dependencies = [
    "fastapi>=0.115",
    "uvicorn[standard]>=0.34",
    "pydantic-settings>=2.7",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.25",
    "httpx>=0.28",
    "mypy>=1.15",
    "ruff>=0.11",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/resonance"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"

[tool.ruff]
target-version = "py313"
src = ["src", "tests"]

[tool.ruff.lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "N",   # pep8-naming
    "UP",  # pyupgrade
    "B",   # flake8-bugbear
    "SIM", # flake8-simplify
    "TCH", # flake8-type-checking
    "RUF", # ruff-specific
]

[tool.mypy]
strict = true
warn_return_any = true
disallow_untyped_defs = true
plugins = ["pydantic.mypy"]
```

**Step 2: Initialize uv and lock dependencies**

Run: `uv sync`
Expected: Creates `uv.lock` and `.venv/`

**Step 3: Add .python-version**

Create `.python-version` with content `3.13`.

**Step 4: Create .gitignore**

Standard Python gitignore: `.venv/`, `__pycache__/`, `*.pyc`, `.mypy_cache/`, `.pytest_cache/`, `.ruff_cache/`, `dist/`, `*.egg-info/`.

**Step 5: Commit**

```bash
git add pyproject.toml uv.lock .python-version .gitignore
git commit -m "chore: add pyproject.toml with dependencies and tool config"
```

---

### Task 2: Minimal App with Health Endpoint

**Files:**
- Create: `src/resonance/__init__.py`
- Create: `src/resonance/app.py`
- Create: `src/resonance/config.py`

**Step 1: Write the failing test**

Create `tests/__init__.py` (empty) and `tests/test_health.py`:

```python
import httpx
import pytest
import resonance.app as app_module


@pytest.fixture
def client() -> httpx.AsyncClient:
    application = app_module.create_app()
    transport = httpx.ASGITransport(app=application)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


async def test_healthz_returns_ok(client: httpx.AsyncClient) -> None:
    response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_health.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'resonance'`

**Step 3: Write minimal implementation**

`src/resonance/__init__.py`:
```python
```
(empty file)

`src/resonance/config.py`:
```python
import pydantic_settings


class Settings(pydantic_settings.BaseSettings):
    """Application settings loaded from environment variables."""

    app_name: str = "resonance"
    debug: bool = False
```

`src/resonance/app.py`:
```python
import fastapi

import resonance.config as config_module


def create_app() -> fastapi.FastAPI:
    """Create and configure the FastAPI application."""
    settings = config_module.Settings()
    application = fastapi.FastAPI(
        title=settings.app_name,
        docs_url="/docs" if settings.debug else None,
        redoc_url=None,
    )

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return application
```

**Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_health.py -v`
Expected: PASS

**Step 5: Run lint and type check**

Run: `uv run ruff check . && uv run ruff format --check . && uv run mypy src/`
Expected: All pass. Fix any issues before proceeding.

**Step 6: Commit**

```bash
git add src/resonance/ tests/
git commit -m "feat: minimal FastAPI app with /healthz endpoint"
```

---

### Task 3: Makefile

**Files:**
- Create: `Makefile`

**Step 1: Create Makefile**

```makefile
.PHONY: run test lint format typecheck check clean

run:
	uv run uvicorn resonance.app:create_app --factory --reload

test:
	uv run pytest

lint:
	uv run ruff check .

format:
	uv run ruff format .

typecheck:
	uv run mypy src/

check: lint typecheck test

clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache dist
```

**Step 2: Verify targets work**

Run: `make check`
Expected: lint, typecheck, and test all pass.

**Step 3: Commit**

```bash
git add Makefile
git commit -m "chore: add Makefile with dev command targets"
```

---

### Task 4: Dockerfile

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

**Step 1: Create .dockerignore**

```
.venv/
.git/
.github/
.mypy_cache/
.pytest_cache/
.ruff_cache/
__pycache__/
*.pyc
dist/
tests/
docs/
*.md
!README.md
Makefile
```

**Step 2: Create multi-stage Dockerfile**

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

WORKDIR /app

# Install dependencies first (cache-friendly layer ordering)
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

# Copy source and install the project
COPY src/ src/
RUN uv sync --frozen --no-dev

FROM python:3.13-slim-bookworm

WORKDIR /app

# Copy the virtual environment from builder
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
USER nobody

CMD ["uvicorn", "resonance.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
```

**Step 3: Build and test locally**

Run: `docker build -t resonance:test .`
Expected: Build succeeds.

Run: `docker run --rm -p 8000:8000 resonance:test &` then `curl http://localhost:8000/healthz` then stop the container.
Expected: `{"status":"ok"}`

**Step 4: Commit**

```bash
git add Dockerfile .dockerignore
git commit -m "chore: add multi-stage Dockerfile with uv"
```

---

### Task 5: GitHub Actions — Lint & Test

**Files:**
- Create: `.github/workflows/lint-and-test.yaml`

**Step 1: Create the workflow**

```yaml
name: Lint & Test

on:
  pull_request:
  push:
    branches: [main]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: astral-sh/setup-uv@v4
        with:
          enable-cache: true

      - name: Install dependencies
        run: uv sync

      - name: Ruff lint
        run: uv run ruff check .

      - name: Ruff format check
        run: uv run ruff format --check .

      - name: Mypy
        run: uv run mypy src/

      - name: Tests
        run: uv run pytest --tb=short
```

**Step 2: Commit**

```bash
git add .github/workflows/lint-and-test.yaml
git commit -m "ci: add lint and test workflow"
```

---

### Task 6: GitHub Actions — Build & Publish

**Files:**
- Create: `.github/workflows/publish.yaml`

**Step 1: Create the workflow**

This follows the pattern from argocd-bot, pushing to GHCR with the timestamp-sha tag format.

```yaml
name: Build & Publish

on:
  push:
    branches: [main]

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write
    steps:
      - uses: actions/checkout@v4

      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - uses: docker/metadata-action@v5
        id: meta
        with:
          images: ghcr.io/${{ github.repository }}
          tags: |
            type=raw,value=latest
            type=raw,value={{commit_date 'YYYYMMDDTHHmmss' tz='UTC'}}-{{sha}}

      - uses: docker/setup-buildx-action@v3

      - uses: docker/build-push-action@v6
        with:
          context: .
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

**Step 2: Commit**

```bash
git add .github/workflows/publish.yaml
git commit -m "ci: add build and publish workflow to GHCR"
```

---

### Task 7: Alembic Initialization

**Files:**
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/versions/` (empty directory with `.gitkeep`)
- Create: `alembic/script.py.mako`

**Step 1: Initialize alembic**

Run: `uv run alembic init alembic`

This creates the standard alembic scaffolding. The generated files need minor edits:

**Step 2: Edit alembic.ini**

Set `sqlalchemy.url` to empty (will be overridden by env.py from app config at runtime):
```ini
sqlalchemy.url =
```

**Step 3: Edit alembic/env.py**

Replace the `sqlalchemy.url` config with a placeholder comment noting it will be wired to `Settings.database_url` when the database layer is added. For now, leave the generated file mostly as-is — it won't be runnable until we add SQLAlchemy models and a database URL.

**Step 4: Add .gitkeep to versions directory**

Run: `touch alembic/versions/.gitkeep`

**Step 5: Commit**

```bash
git add alembic.ini alembic/
git commit -m "chore: initialize alembic migration scaffolding"
```

---

## Summary

After all 7 tasks, the repo will have:

| Component | Status |
|-----------|--------|
| pyproject.toml + uv.lock | Dependencies locked, ruff/mypy/pytest configured |
| src/resonance/ | Minimal app factory with /healthz |
| tests/ | Health endpoint test |
| Makefile | `make check` runs lint + typecheck + test |
| Dockerfile | Multi-stage build, runs on port 8000 |
| CI — lint & test | Runs on PRs and pushes to main |
| CI — build & publish | Builds and pushes to GHCR on merge to main |
| Alembic | Initialized, no migrations yet |

**Not in scope (next steps):**
- megadoomer-config ArgoCD application definition (separate repo)
- Cross-repo image promotion workflow
- Database models and migrations
- Redis session/cache wiring
- OAuth connector implementations
