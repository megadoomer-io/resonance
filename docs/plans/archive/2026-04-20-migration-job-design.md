# Alembic Migrations as ArgoCD PreSync Hook Job

**Date**: 2026-04-20
**Issue**: [#34](https://github.com/megadoomer-io/resonance/issues/34)
**Status**: Accepted

## Context

Alembic migrations currently run as an init container on the main deployment pod.
If ArgoCD deploys a newer image while a migration is running, the init container
is killed mid-transaction. While Alembic's transaction rollback handles this
safely, the migration must restart from scratch on the next pod.

Issue #28 added a generous `terminationGracePeriodSeconds` (300s) as an interim
fix. This design replaces the init container with a standalone Kubernetes Job.

## Decision

Replace the init container with a Kubernetes Job that runs as an ArgoCD PreSync
hook. The Job uses the same container image and `alembic upgrade head` command.

## Design

### Job configuration (megadoomer-config helm-values.yaml)

Add a new `migrations` controller of type `job`:

- **Image**: Same `app` image used by main and worker controllers
- **Command**: `alembic upgrade head`
- **Env**: Database credentials only (PGHOST, PGDATABASE, PGUSER, PGPASSWORD, PGPORT)
- **Resources**: 100m/256Mi limits, 10m/128Mi requests (same as current init container)
- **restartPolicy**: `OnFailure`
- **backoffLimit**: 3

### ArgoCD annotations

- `argocd.argoproj.io/hook: PreSync` — runs before the Deployment is updated
- `argocd.argoproj.io/hook-delete-policy: BeforeHookCreation` — deletes the
  previous Job when a new sync begins

### Deployment lifecycle

1. CI pushes a new image tag to megadoomer-config
2. ArgoCD detects drift and starts a sync
3. PreSync hook runs the migration Job against PostgreSQL
4. On success: ArgoCD proceeds to update the Deployment and Worker
5. On failure (after 3 retries): sync fails, Deployment stays on current version
6. On next sync: old Job is deleted before creating the new one

### What changes

- **megadoomer-config**: Add `migrations` job controller, remove
  `initContainers.migrations` from the main controller
- **resonance repo**: No changes needed — Dockerfile already includes Alembic

### What doesn't change

- The migration command (`alembic upgrade head`)
- The container image (same as app)
- The environment variables (database credentials)
- The resource limits

## Alternatives considered

**Sync Waves** (Job in wave -1, Deployment in wave 0): Adds complexity with no
benefit over PreSync hooks. Requires `ttlSecondsAfterFinished` for cleanup and
ArgoCD health assessment configuration for Jobs.

## Risks

- **First deploy**: The PreSync Job and init container removal happen in the same
  sync. If the Job fails, ArgoCD won't update the Deployment, so the old init
  container remains — safe rollback.
- **Concurrent syncs**: ArgoCD serializes syncs per application, so only one
  migration Job runs at a time.
