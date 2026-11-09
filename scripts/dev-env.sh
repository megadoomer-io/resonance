#!/usr/bin/env bash
# Named dev environments: see docs/dev-environments.md. Driven by the dev-* Make
# targets when ENV is set. Written for macOS's bash 3.2.
set -euo pipefail

REPO=$(cd "$(dirname "$0")/.." && pwd)
STATE_ROOT=${RESONANCE_DEV_STATE:-$HOME/.local/state/resonance-dev}

die() { echo "dev-env: $*" >&2; exit 1; }

check_name() {
  [[ $2 =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "invalid $1 '$2' (use a-z, 0-9, -)"
}

env_dir() { echo "$STATE_ROOT/$1"; }

load_env() {
  local file
  file="$(env_dir "$1")/env"
  [[ -f $file ]] || die "environment '$1' is not up (make dev-up ENV=$1)"
  # shellcheck source=/dev/null
  . "$file"
}

compose() {
  local name=$1; shift
  RESONANCE_PG_PORT=${PG_PORT:-} RESONANCE_REDIS_PORT=${REDIS_PORT:-} \
    docker-compose -f "$REPO/docker-compose.yml" -p "resonance-$name" "$@"
}

port_busy() { lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1; }

# grep, not cat | sed: an unmatched glob (an env with no apps) must not fail
# the pipeline under pipefail.
recorded_ports() {
  grep -hs '^[A-Z_]*PORT=' "$STATE_ROOT"/*/env "$STATE_ROOT"/*/apps/*/meta | cut -d= -f2 || true
}

port_taken() {
  local ports
  port_busy "$1" && return 0
  ports=$(recorded_ports)
  grep -qx "$1" <<<"$ports"
}

# Each env gets a block of 10 ports, PG at +0, Redis at +1, apps from +2. The
# block starts at a hash of the name so the same env usually lands on the same
# ports; the chosen block is recorded, so later commands never recompute it.
allocate_base() {
  local base tries=0
  base=$(( 20000 + ($(printf %s "$1" | cksum | cut -d' ' -f1) % 900) * 10 ))
  while port_taken "$base" || port_taken $((base + 1)); do
    base=$(( base + 10 )); (( base < 29990 )) || base=20000
    tries=$((tries + 1)); (( tries < 900 )) || die "no free port block"
  done
  echo "$base"
}

cmd_up() {
  local name=$1 dir
  dir=$(env_dir "$name")
  if [[ ! -f $dir/env ]]; then
    local base
    base=$(allocate_base "$name")
    mkdir -p "$dir/apps" "$dir/worktrees"
    # Throwaway local secrets; DEBUG=true already tolerates the defaults.
    cat > "$dir/env" <<EOF
PORT_BASE=$base
PG_PORT=$base
REDIS_PORT=$((base + 1))
ADMIN_API_TOKEN=dev-$name-$(openssl rand -hex 16)
SESSION_SECRET_KEY=$(openssl rand -hex 32)
TOKEN_ENCRYPTION_KEY=$(openssl rand -base64 32 | tr '+/' '-_')
EOF
  fi
  load_env "$name"
  compose "$name" up -d
  # Over TCP: the image's first-run init server listens on the socket only.
  echo "Waiting for PostgreSQL..."
  until compose "$name" exec -T postgres pg_isready -h 127.0.0.1 -U resonance >/dev/null 2>&1; do sleep 1; done
  echo "Environment '$name' ready: PostgreSQL localhost:$PG_PORT, Redis localhost:$REDIS_PORT"
}

# Prints the worktree for a ref, creating it (and its venv) on first use.
# Worktrees are keyed by commit, so apps at the same commit share one.
worktree_for() {
  local name=$1 ref=$2 sha wt
  sha=$(git -C "$REPO" rev-parse --verify --quiet "$ref^{commit}") || die "unknown ref '$ref'"
  wt="$(env_dir "$name")/worktrees/$sha"
  if [[ ! -d $wt ]]; then
    git -C "$REPO" worktree add --detach "$wt" "$sha" >&2
  fi
  # Also retries a sync that failed last time.
  if [[ ! -x $wt/.venv/bin/python ]]; then
    if ! (cd "$wt" && env -u VIRTUAL_ENV uv sync --all-extras -q) >&2; then
      git -C "$REPO" worktree remove --force "$wt" >&2 || rm -rf "$wt"
      git -C "$REPO" worktree prune
      die "uv sync failed for $ref; worktree removed"
    fi
  fi
  echo "$wt"
}

# The environment shared by the API, worker and migrations.
app_env_args() {
  APP_ENV=(
    -u VIRTUAL_ENV
    PGHOST=localhost PGPORT="$PG_PORT" PGUSER=resonance PGPASSWORD=resonance PGDATABASE=resonance
    REDIS_HOST=localhost REDIS_PORT="$REDIS_PORT" REDIS_PASSWORD=
    ADMIN_API_TOKEN="$ADMIN_API_TOKEN" SESSION_SECRET_KEY="$SESSION_SECRET_KEY"
    TOKEN_ENCRYPTION_KEY="$TOKEN_ENCRYPTION_KEY"
    DEBUG=true WORKER_MODE=external DEV_SEED_ENABLED=true RATE_LIMIT_ENABLED=false
  )
}

alive() { [[ -n ${1:-} ]] && kill -0 "$1" 2>/dev/null; }

# A pid file can outlive its process and the pid be reused, so a pid counts
# as the app's only while its command line runs the app's module and its cwd
# is the app's worktree. (The command line alone can't carry the worktree:
# framework Python on macOS re-execs as Python.app, dropping the venv path.)
owned() {
  local pid=$1 module=$2 wt=${3:-} cmdline cwd
  alive "$pid" || return 1
  cmdline=$(ps -p "$pid" -o command= 2>/dev/null) || return 1
  [[ $cmdline == *"$module"* ]] || return 1
  [[ -z $wt ]] && return 0
  cwd=$(lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | sed -n 's/^n//p')
  [[ $cwd == "$wt" ]]
}

# owned()'s module and worktree arguments for an app's api or worker.
owner_args() {
  local dir=$1 role=$2 wt=""
  [[ -f $dir/meta ]] && wt=$(sed -n 's/^WORKTREE=//p' "$dir/meta")
  if [[ $role == api ]]; then echo "resonance.app:create_app"; else echo "resonance.worker"; fi
  echo "$wt"
}

app_alive() {
  local dir=$1 role=$2 args
  args=$(owner_args "$dir" "$role")
  owned "$(cat "$dir/$role.pid" 2>/dev/null)" "$(sed -n 1p <<<"$args")" "$(sed -n 2p <<<"$args")"
}

stop_pid() {
  local dir=$1 role=$2 pid i
  pid=$(cat "$dir/$role.pid" 2>/dev/null)
  app_alive "$dir" "$role" || return 0
  kill "$pid" 2>/dev/null || true
  for i in 1 2 3 4 5 6 7 8 9 10; do alive "$pid" || return 0; sleep 1; done
  kill -9 "$pid" 2>/dev/null || true
}

cmd_stop_app() {
  local name=$1 app=$2 dir role
  dir="$(env_dir "$name")/apps/$app"
  [[ -d $dir ]] || return 0
  for role in api worker; do
    if [[ -f $dir/$role.pid ]]; then
      stop_pid "$dir" "$role"
      rm -f "$dir/$role.pid"
    fi
  done
  rm -f "$dir/meta"
  echo "Stopped app '$app' in '$name'."
}

cmd_app() {
  local name=$1 ref=$2 app=$3 port=${4:-} dir wt sha i
  check_name app "$app"
  load_env "$name"
  dir="$(env_dir "$name")/apps/$app"
  cmd_stop_app "$name" "$app" >/dev/null
  if [[ -z $port ]]; then
    for i in 2 3 4 5 6 7 8 9; do
      port_taken $((PORT_BASE + i)) || { port=$((PORT_BASE + i)); break; }
    done
    [[ -n $port ]] || die "no free app port in this env's block; pass PORT="
  elif port_busy "$port"; then
    die "port $port is in use"
  fi
  wt=$(worktree_for "$name" "$ref")
  sha=$(git -C "$wt" rev-parse HEAD)
  mkdir -p "$dir"
  printf 'REF=%s\nSHA=%s\nAPP_PORT=%s\nWORKTREE=%s\n' "$ref" "$sha" "$port" "$wt" > "$dir/meta"

  cd "$wt"
  app_env_args
  # nohup and env exec, and the venv's python runs directly (not `uv run`), so
  # the recorded pid is the server process itself.
  nohup env "${APP_ENV[@]}" BASE_URL="http://localhost:$port" GIT_SHA="$sha" \
    .venv/bin/python -m uvicorn resonance.app:create_app --factory \
    --host 127.0.0.1 --port "$port" > "$dir/api.log" 2>&1 &
  echo $! > "$dir/api.pid"
  # python -m resonance.worker, not the arq CLI, which crashes on Python 3.14.
  nohup env "${APP_ENV[@]}" .venv/bin/python -m resonance.worker \
    > "$dir/worker.log" 2>&1 &
  echo $! > "$dir/worker.pid"

  for i in $(seq 1 60); do
    if curl -fsS "http://localhost:$port/healthz" >/dev/null 2>&1; then
      # The worker checks the schema at startup too; give it a moment to fail.
      sleep 2
      if ! app_alive "$dir" worker; then
        echo "App '$app': API is up but the worker exited; last worker log lines ($dir/worker.log):" >&2
        tail -n 15 "$dir/worker.log" >&2
        return 1
      fi
      echo "App '$app' ($ref @ ${sha:0:9}) serving on http://localhost:$port"
      echo "  logs: $dir/api.log $dir/worker.log"
      echo "  eval \"\$(make -s dev-env ENV=$name NAME=$app)\""
      return 0
    fi
    app_alive "$dir" api || break
    sleep 1
  done
  echo "App '$app' did not come up; last API log lines ($dir/api.log):" >&2
  tail -n 15 "$dir/api.log" >&2
  return 1
}

cmd_migrate() {
  local name=$1 ref=${2:-} to=${3:-head} dir=$REPO
  load_env "$name"
  [[ -n $ref ]] && dir=$(worktree_for "$name" "$ref")
  cd "$dir"
  app_env_args
  env "${APP_ENV[@]}" uv run alembic upgrade "$to"
}

resolve_app() {
  local name=$1 app=${2:-} apps
  if [[ -z $app ]]; then
    apps=$(cd "$(env_dir "$name")/apps" && for m in */meta; do [[ -f $m ]] && dirname "$m"; done || true)
    [[ $(echo "$apps" | grep -c .) -eq 1 ]] || die "pass NAME= (apps in '$name': $(echo "$apps" | tr '\n' ' '))"
    app=$apps
  fi
  [[ -f "$(env_dir "$name")/apps/$app/meta" ]] || die "no app '$app' in '$name'"
  echo "$app"
}

cmd_env() {
  local name=$1 app
  load_env "$name"
  app=$(resolve_app "$name" "${2:-}")
  # shellcheck source=/dev/null
  . "$(env_dir "$name")/apps/$app/meta"
  echo "export RESONANCE_URL=http://localhost:$APP_PORT"
  echo "export RESONANCE_API_TOKEN=$ADMIN_API_TOKEN"
}

cmd_status() {
  local dir name app meta pid state
  [[ -d $STATE_ROOT ]] || { echo "No dev environments."; return 0; }
  for dir in "$STATE_ROOT"/*/; do
    [[ -f $dir/env ]] || continue
    name=$(basename "$dir")
    load_env "$name"
    echo "$name  pg=localhost:$PG_PORT redis=localhost:$REDIS_PORT"
    for meta in "$dir"apps/*/meta; do
      [[ -f $meta ]] || continue
      app=$(basename "$(dirname "$meta")")
      # shellcheck source=/dev/null
      . "$meta"
      state=""
      for pid in api worker; do
        if app_alive "$(dirname "$meta")" "$pid"; then
          state="$state $pid=up"
        else
          state="$state $pid=DOWN"
        fi
      done
      echo "  $app  $REF (${SHA:0:9})  http://localhost:$APP_PORT $state"
    done
  done
}

# Tolerates partial state (no env file, a worktree git won't remove) and
# keeps going, so nothing is left orphaned.
cmd_down() {
  local name=$1 dir app wt
  dir=$(env_dir "$name")
  if [[ ! -d $dir && -z $(compose "$name" ps -q) \
        && -z $(docker volume ls -q --filter "name=^resonance-${name}_") ]]; then
    die "environment '$name' does not exist"
  fi
  for app in "$dir"/apps/*/; do
    if [[ -d $app ]]; then cmd_stop_app "$name" "$(basename "$app")"; fi
  done
  compose "$name" down -v
  for wt in "$dir"/worktrees/*/; do
    if [[ -d $wt ]] && ! git -C "$REPO" worktree remove --force "$wt"; then
      echo "dev-env: could not remove worktree $wt; deleting it" >&2
    fi
  done
  rm -rf "$dir"
  git -C "$REPO" worktree prune
  echo "Environment '$name' removed."
}

usage() {
  cat >&2 <<EOF
Usage: $0 <command> <env> [args]
  up <env>                         start PostgreSQL + Redis
  app <env> <ref> <name> [port]    start API + worker at a git ref
  stop-app <env> <name>            stop one app
  migrate <env> [ref] [revision]   alembic upgrade (default: current checkout, head)
  env <env> [name]                 print RESONANCE_URL / RESONANCE_API_TOKEN exports
  status                           list environments and apps
  down <env>                       stop apps, remove containers, volume and worktrees
EOF
  exit 1
}

cmd=${1:-}; shift || true
case $cmd in
  status) cmd_status; exit 0 ;;
  up|app|stop-app|migrate|env|down) ;;
  *) usage ;;
esac
[[ $# -ge 1 ]] || usage
check_name env "$1"
case $cmd in
  up) cmd_up "$1" ;;
  app) [[ $# -ge 3 && -n $2 ]] || die "dev-app needs REF= (and NAME=)"; cmd_app "$@" ;;
  stop-app) [[ $# -ge 2 && -n $2 ]] || die "pass NAME="; check_name app "$2"; cmd_stop_app "$1" "$2" ;;
  migrate) cmd_migrate "$@" ;;
  env) if [[ -n ${2:-} ]]; then check_name app "$2"; fi; cmd_env "$@" ;;
  down) cmd_down "$1" ;;
esac
