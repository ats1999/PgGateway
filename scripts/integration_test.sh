#!/usr/bin/env bash
# Integration tests for pg-bouncer-rs against local PostgreSQL.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/target/release/pg-bouncer-rs"
POOL_PORT=6432
PG_HOST=127.0.0.1
PG_PORT=5432
PG_USER=postgres
PG_PASS=postgres
PG_DB=postgres
export PGPASSWORD="$PG_PASS"

log() { echo "==> $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

if ! command -v psql >/dev/null; then
  fail "psql not found"
fi

if ! pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -t 3 >/dev/null 2>&1; then
  fail "PostgreSQL not reachable at ${PG_HOST}:${PG_PORT}"
fi

log "build release binary"
cargo build --release --manifest-path "$ROOT/Cargo.toml"

kill_pooler() {
  if [[ -n "${POOLER_PID:-}" ]] && kill -0 "$POOLER_PID" 2>/dev/null; then
    kill "$POOLER_PID" 2>/dev/null || true
    wait "$POOLER_PID" 2>/dev/null || true
  fi
}
trap kill_pooler EXIT

start_pooler() {
  local mode="$1"
  kill_pooler
  log "start pooler mode=$mode"
  RUST_LOG=info "$BIN" \
    --listen-addr "127.0.0.1:${POOL_PORT}" \
    --backend-host "$PG_HOST" \
    --backend-port "$PG_PORT" \
    --pool-mode "$mode" \
    >/tmp/pg-bouncer-rs.log 2>&1 &
  POOLER_PID=$!
  sleep 0.5
  for _ in $(seq 1 30); do
    if psql -h 127.0.0.1 -p "$POOL_PORT" -U "$PG_USER" -d "$PG_DB" -c 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  cat /tmp/pg-bouncer-rs.log >&2 || true
  fail "pooler did not become ready (mode=$mode)"
}

query() {
  psql -h 127.0.0.1 -p "$POOL_PORT" -U "$PG_USER" -d "$PG_DB" -Atc "$1"
}

admin_query() {
  psql -h 127.0.0.1 -p "$POOL_PORT" -U "$PG_USER" -d pgbouncer -Atc "$1"
}

log "test session mode"
start_pooler session
[[ "$(query 'SELECT 42')" == "42" ]] || fail "session mode query"
[[ "$(query 'SELECT current_database()')" == "postgres" ]] || fail "session mode db"

log "test transaction mode"
start_pooler transaction
[[ "$(query 'SELECT 1')" == "1" ]] || fail "transaction mode query"
psql -h 127.0.0.1 -p "$POOL_PORT" -U "$PG_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
SELECT 2;
COMMIT;
SQL

log "test statement mode"
start_pooler statement
[[ "$(query 'SELECT 3')" == "3" ]] || fail "statement mode query"
[[ "$(query 'SELECT 4')" == "4" ]] || fail "statement mode second query"

log "test admin SHOW STATS"
start_pooler transaction
admin_query 'SHOW STATS;' | grep -q total_queries || fail "SHOW STATS"
admin_query 'SHOW POOLS;' | grep -q postgres || fail "SHOW POOLS"
admin_query 'SHOW CONFIG;' | grep -q pool_mode || fail "SHOW CONFIG"

log "test PAUSE / RESUME"
admin_query 'PAUSE;' >/dev/null
sleep 0.3
if psql -h 127.0.0.1 -p "$POOL_PORT" -U "$PG_USER" -d "$PG_DB" --connect-timeout=3 -c 'SELECT 1' >/dev/null 2>&1; then
  fail "expected pause to block or fail new work"
fi
admin_query 'RESUME;' >/dev/null
[[ "$(query 'SELECT 5')" == "5" ]] || fail "after resume"

log "test concurrent clients"
start_pooler transaction
pids=()
for i in $(seq 1 10); do
  query "SELECT $i" &
  pids+=($!)
done
for pid in "${pids[@]}"; do
  wait "$pid"
done

log "all tests passed"
