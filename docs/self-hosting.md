# Self-Hosting Guide

How to run your own Resonance instance from source or Docker.

## Prerequisites

- **Python 3.14+** (with [uv](https://docs.astral.sh/uv/) for dependency management)
- **PostgreSQL 15+**
- **Redis 7+**
- **Docker** (optional, if you prefer a containerized deployment)

## OAuth App Registration

Resonance connects to external music services via OAuth. Register an app with
each service you want to use. You can start with one and add more later.

### Spotify

1. Go to [developer.spotify.com](https://developer.spotify.com/) and sign in.
2. Open the Dashboard and click **Create App**.
3. Set the redirect URI to `{BASE_URL}/api/v1/auth/spotify/callback`
   (e.g., `http://localhost:8000/api/v1/auth/spotify/callback` for local dev).
4. Copy the **Client ID** and **Client Secret**.

Spotify Development Mode limits your app to 5 authorized users and imposes
aggressive rate limits. See [spotify-api-constraints.md](spotify-api-constraints.md)
for details and sync strategy implications.

### Last.fm

1. Go to [last.fm/api/account/create](https://www.last.fm/api/account/create) and
   create an API account.
2. Note the **API Key** and **Shared Secret**.
3. No redirect URI is required -- Last.fm uses a web auth token flow.

### ListenBrainz (via MusicBrainz)

1. Go to [musicbrainz.org/account/applications](https://musicbrainz.org/account/applications)
   and register a new OAuth application.
2. Set the redirect URI to `{BASE_URL}/api/v1/auth/listenbrainz/callback`
   (e.g., `http://localhost:8000/api/v1/auth/listenbrainz/callback`).
3. Copy the **Client ID** and **Client Secret**.

## Environment Variables

Create a `.env` file (or export these in your shell). All variables map to
uppercase environment variable names.

### Database

| Variable     | Description                | Default     |
|--------------|----------------------------|-------------|
| `PGHOST`     | PostgreSQL host            | `localhost` |
| `PGPORT`     | PostgreSQL port            | `5432`      |
| `PGUSER`     | PostgreSQL user            | `resonance` |
| `PGPASSWORD` | PostgreSQL password        | `resonance` |
| `PGDATABASE` | PostgreSQL database name   | `resonance` |

### Redis

| Variable         | Description         | Default     |
|------------------|----------------------|-------------|
| `REDIS_HOST`     | Redis host           | `localhost` |
| `REDIS_PORT`     | Redis port           | `6379`      |
| `REDIS_PASSWORD` | Redis password       | (empty)     |

### Security

| Variable               | Description                              | How to generate |
|------------------------|------------------------------------------|-----------------|
| `SESSION_SECRET_KEY`   | Signing key for session cookies          | `python -c "import secrets; print(secrets.token_urlsafe(32))"` |
| `TOKEN_ENCRYPTION_KEY` | Fernet key for encrypting stored OAuth tokens | `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |

Both of these must be changed from their defaults before running in any
non-throwaway environment.

### Application

| Variable   | Description                                      | Default                  |
|------------|--------------------------------------------------|--------------------------|
| `BASE_URL` | Public URL of your instance (used for OAuth redirect URIs) | `http://localhost:8000` |
| `LOG_LEVEL` | Logging level                                   | `INFO`                   |

### Spotify

| Variable                | Description            |
|-------------------------|------------------------|
| `SPOTIFY_CLIENT_ID`     | Spotify OAuth client ID |
| `SPOTIFY_CLIENT_SECRET` | Spotify OAuth client secret |

### MusicBrainz / ListenBrainz

| Variable                    | Description                       |
|-----------------------------|-----------------------------------|
| `MUSICBRAINZ_CLIENT_ID`     | MusicBrainz OAuth client ID       |
| `MUSICBRAINZ_CLIENT_SECRET` | MusicBrainz OAuth client secret   |

### Last.fm

| Variable             | Description           |
|----------------------|-----------------------|
| `LASTFM_API_KEY`     | Last.fm API key       |
| `LASTFM_SHARED_SECRET` | Last.fm shared secret |

### Admin

| Variable           | Description                                | How to generate |
|--------------------|--------------------------------------------|-----------------|
| `ADMIN_API_TOKEN`  | Bearer token for admin API / CLI access    | `python -c "import secrets; print(secrets.token_urlsafe(32))"` |

### Worker

| Variable      | Description                                      | Default    |
|---------------|--------------------------------------------------|------------|
| `WORKER_MODE` | Task execution mode: `external` (arq) or `inline` (dev) | `external` |

Set to `inline` during local development if you do not want to run a separate
arq worker process. In `inline` mode, background tasks execute in the web
process. Use `external` (the default) for production, which requires the arq
worker to be running separately.

## Database Setup

Create the database and run migrations:

```bash
createdb resonance
uv run alembic upgrade head
```

If your PostgreSQL connection parameters differ from the defaults above, set the
`PG*` environment variables before running the migration.

## Running the App

### Option A: Direct (recommended for development)

Install dependencies and start the web server:

```bash
uv sync --all-extras
uv run uvicorn resonance.app:create_app --factory --reload
```

If using `WORKER_MODE=external` (the default), start the arq worker in a
separate terminal:

```bash
uv run arq resonance.worker.WorkerSettings
```

The app will be available at `http://localhost:8000`.

### Option B: Docker

Build and run the container:

```bash
docker build -t resonance .
docker run -p 8000:8000 --env-file .env resonance
```

The Docker image runs only the web server. If you need the arq worker, run a
second container with an overridden command:

```bash
docker run --env-file .env resonance arq resonance.worker.WorkerSettings
```

Both containers need access to the same PostgreSQL and Redis instances.

## First Login and Setup

1. Open your instance in a browser (e.g., `http://localhost:8000`).
2. Log in via any configured service (Spotify, Last.fm, or ListenBrainz).
3. The very first user to log in is automatically assigned the **owner** role.
   If you need to adjust roles later, use the CLI:

   ```bash
   uv run resonance-api set-role <your-user-id> owner
   ```

   The `set-role` command connects directly to the database and does not require
   the web server or admin token.

4. Once logged in as owner, you can trigger syncs, run dedup jobs, and access
   admin features from the dashboard or CLI. See the project README for CLI
   usage.
