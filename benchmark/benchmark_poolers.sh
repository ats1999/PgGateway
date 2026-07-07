#!/usr/bin/env bash
# Benchmark direct PostgreSQL, PgBouncer, and pg-bouncer-rs using pgbench.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN_DIR="$(cd "$ROOT/.." && pwd)"
PG_BOUNCER_RS="$ROOT/target/release/pg-bouncer-rs"
PGBOUNCER_BIN="${PGBOUNCER_BIN:-$(command -v pgbouncer)}"

PG_HOST="${PG_HOST:-127.0.0.1}"
PG_PORT="${PG_PORT:-5432}"
PG_USER="${PG_USER:-postgres}"
PG_PASS="${PG_PASS:-postgres}"
BENCH_DB="${BENCH_DB:-pgbench}"

PGBOUNCER_PORT="${PGBOUNCER_PORT:-6433}"
PGBOUNCER_RS_PORT="${PGBOUNCER_RS_PORT:-6432}"
POOL_MODE="${POOL_MODE:-transaction}"
POOL_SIZE="${POOL_SIZE:-10}"

CLIENTS="${CLIENTS:-20}"
JOBS="${JOBS:-4}"
DURATION="${DURATION:-15}"
SCALE="${SCALE:-1}"

RUN_DIR="${RUN_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/pooler-bench.XXXXXX")}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/benchmark-results}"
TIMESTAMP="$(date -u +"%Y-%m-%dT%H-%M-%SZ")"
RESULTS_FILE="${RESULTS_FILE:-$RESULTS_DIR/benchmark_${TIMESTAMP}.txt}"

PGBOUNCER_PID=""
PGBOUNCER_RS_PID=""

export PGPASSWORD="$PG_PASS"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$RESULTS_FILE"; }
fail() { log "ERROR: $*"; cleanup; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "required command not found: $1"
}

cleanup() {
  if [[ -n "$PGBOUNCER_RS_PID" ]] && kill -0 "$PGBOUNCER_RS_PID" 2>/dev/null; then
    kill "$PGBOUNCER_RS_PID" 2>/dev/null || true
    wait "$PGBOUNCER_RS_PID" 2>/dev/null || true
  fi
  if [[ -n "$PGBOUNCER_PID" ]] && kill -0 "$PGBOUNCER_PID" 2>/dev/null; then
    kill "$PGBOUNCER_PID" 2>/dev/null || true
    wait "$PGBOUNCER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

wait_for_pooler() {
  local port="$1"
  local label="$2"
  for _ in $(seq 1 50); do
    if PGCONNECT_TIMEOUT=2 psql -h "$PG_HOST" -p "$port" -U "$PG_USER" -d "$BENCH_DB" -c 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  if [[ "$label" == "PgBouncer" && -f "$RUN_DIR/pgbouncer.log" ]]; then
    log "PgBouncer log:"
    tail -20 "$RUN_DIR/pgbouncer.log" >>"$RESULTS_FILE" || true
  fi
  if [[ "$label" == "pg-bouncer-rs" && -f "$RUN_DIR/pg-bouncer-rs.log" ]]; then
    log "pg-bouncer-rs log:"
    tail -20 "$RUN_DIR/pg-bouncer-rs.log" >>"$RESULTS_FILE" || true
  fi
  fail "$label did not become ready on port $port"
}

prepare_database() {
  log "Preparing benchmark database '$BENCH_DB' on ${PG_HOST}:${PG_PORT}"
  if ! psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d postgres -Atqc \
    "SELECT 1 FROM pg_database WHERE datname = '$BENCH_DB'" | grep -q 1; then
    createdb -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" "$BENCH_DB"
  fi
  pgbench -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -i -s "$SCALE" -q "$BENCH_DB"
}

write_pgbouncer_config() {
  local ini="$RUN_DIR/pgbouncer.ini"
  local auth_file="$RUN_DIR/userlist.txt"
  printf '"%s" "%s"\n' "$PG_USER" "$PG_PASS" >"$auth_file"
  cat >"$ini" <<EOF
[databases]
${BENCH_DB} = host=${PG_HOST} port=${PG_PORT} dbname=${BENCH_DB} user=${PG_USER} password=${PG_PASS}

[pgbouncer]
listen_addr = ${PG_HOST}
listen_port = ${PGBOUNCER_PORT}
unix_socket_dir = ${RUN_DIR}
auth_type = trust
auth_file = ${auth_file}
pool_mode = ${POOL_MODE}
default_pool_size = ${POOL_SIZE}
max_client_conn = 200
admin_users = ${PG_USER}
ignore_startup_parameters = extra_float_digits
EOF
  echo "$ini"
}

start_pgbouncer() {
  local ini
  ini="$(write_pgbouncer_config)"
  if [[ -n "$PGBOUNCER_PID" ]] && kill -0 "$PGBOUNCER_PID" 2>/dev/null; then
    kill "$PGBOUNCER_PID" 2>/dev/null || true
    wait "$PGBOUNCER_PID" 2>/dev/null || true
    PGBOUNCER_PID=""
  fi
  rm -f "$RUN_DIR/.s.PGSQL.${PGBOUNCER_PORT}" 2>/dev/null || true
  log "Starting PgBouncer on ${PG_HOST}:${PGBOUNCER_PORT} (pool_mode=${POOL_MODE}, pool_size=${POOL_SIZE})"
  "$PGBOUNCER_BIN" "$ini" >"$RUN_DIR/pgbouncer.log" 2>&1 &
  PGBOUNCER_PID=$!
  wait_for_pooler "$PGBOUNCER_PORT" "PgBouncer"
}

start_pg_bouncer_rs() {
  if [[ -n "$PGBOUNCER_RS_PID" ]] && kill -0 "$PGBOUNCER_RS_PID" 2>/dev/null; then
    kill "$PGBOUNCER_RS_PID" 2>/dev/null || true
    wait "$PGBOUNCER_RS_PID" 2>/dev/null || true
    PGBOUNCER_RS_PID=""
  fi
  log "Building pg-bouncer-rs"
  cargo build --release --manifest-path "$ROOT/Cargo.toml" >/dev/null 2>&1

  log "Starting pg-bouncer-rs on ${PG_HOST}:${PGBOUNCER_RS_PORT} (pool_mode=${POOL_MODE}, pool_size=${POOL_SIZE})"
  "$PG_BOUNCER_RS" \
    --listen-addr "${PG_HOST}:${PGBOUNCER_RS_PORT}" \
    --backend-host "$PG_HOST" \
    --backend-port "$PG_PORT" \
    --pool-mode "$POOL_MODE" \
    >"$RUN_DIR/pg-bouncer-rs.log" 2>&1 &
  PGBOUNCER_RS_PID=$!
  wait_for_pooler "$PGBOUNCER_RS_PORT" "pg-bouncer-rs"
}

extract_metric() {
  local file="$1"
  local key="$2"
  case "$key" in
    tps)
      grep -E '^tps = ' "$file" | tail -1 | awk '{print $3}'
      ;;
    latency\ average)
      grep -E '^latency average = ' "$file" | awk '{print $4}'
      ;;
    latency\ stddev)
      grep -E '^latency stddev = ' "$file" | awk '{print $4}'
      ;;
    transactions)
      grep -E '^number of transactions actually processed: ' "$file" | awk '{print $6}'
      ;;
    failed)
      grep -E '^number of failed transactions: ' "$file" | awk '{print $6}'
      ;;
    *)
      grep -F "$key" "$file" | tail -1
      ;;
  esac
}

run_pgbench() {
  local label="$1"
  local port="$2"
  local raw="$RUN_DIR/${label// /_}.txt"

  log "Running pgbench for $label (${PG_HOST}:${port})"
  pgbench \
    -h "$PG_HOST" \
    -p "$port" \
    -U "$PG_USER" \
    -d "$BENCH_DB" \
    -c "$CLIENTS" \
    -j "$JOBS" \
    -T "$DURATION" \
    --no-vacuum \
    >"$raw" 2>/dev/null

  if [[ ! -s "$raw" ]]; then
    fail "pgbench produced no output for $label"
  fi

  {
    echo
    echo "=== $label ==="
    echo "host=${PG_HOST} port=${port}"
    cat "$raw"
    echo "tps_without_init=$(extract_metric "$raw" "tps")"
    echo "latency_avg_ms=$(extract_metric "$raw" "latency average")"
    echo "latency_stddev_ms=$(extract_metric "$raw" "latency stddev")"
    echo "transactions=$(extract_metric "$raw" "transactions")"
    echo "failed=$(extract_metric "$raw" "failed")"
  } >>"$RESULTS_FILE"
}

write_header() {
  mkdir -p "$RESULTS_DIR"
  {
    echo "PostgreSQL pooler benchmark"
    echo "timestamp_utc=${TIMESTAMP}"
    echo "postgres=${PG_HOST}:${PG_PORT}"
    echo "database=${BENCH_DB}"
    echo "pool_mode=${POOL_MODE}"
    echo "pool_size=${POOL_SIZE}"
    echo "pgbench_clients=${CLIENTS}"
    echo "pgbench_jobs=${JOBS}"
    echo "pgbench_duration_sec=${DURATION}"
    echo "pgbench_scale=${SCALE}"
    echo "pgbouncer_bin=${PGBOUNCER_BIN}"
    echo "pg_bouncer_rs_bin=${PG_BOUNCER_RS}"
    echo "run_dir=${RUN_DIR}"
    echo
  } >"$RESULTS_FILE"
}

write_summary_table() {
  {
    echo
    echo "=== SUMMARY ==="
    printf "%-20s %12s %16s %12s\n" "target" "tps" "latency_avg_ms" "transactions"
    for label in "direct-postgres" "pgbouncer" "pg-bouncer-rs"; do
      local raw="$RUN_DIR/${label}.txt"
      [[ -f "$raw" ]] || continue
      local tps latency txns
      tps="$(grep -E '^tps = ' "$raw" | tail -1 | awk '{print $3}')"
      latency="$(grep -E '^latency average = ' "$raw" | awk '{print $4}')"
      txns="$(grep -E '^number of transactions actually processed: ' "$raw" | awk '{print $6}')"
      printf "%-20s %12s %16s %12s\n" "$label" "$tps" "$latency" "$txns"
    done
    echo
    echo "full_results_file=${RESULTS_FILE}"
  } >>"$RESULTS_FILE"

  echo
  echo "Benchmark complete. Results written to:"
  echo "  $RESULTS_FILE"
  echo
  tail -20 "$RESULTS_FILE"
}

main() {
  require_cmd psql
  require_cmd pgbench
  require_cmd createdb
  require_cmd cargo
  require_cmd pg_isready
  [[ -x "$PGBOUNCER_BIN" ]] || fail "pgbouncer binary not found (set PGBOUNCER_BIN)"
  [[ -f "$ROOT/Cargo.toml" ]] || fail "pg-bouncer-rs project not found at $ROOT"

  pkill pgbouncer 2>/dev/null || true
  pkill -f "pg-bouncer-rs" 2>/dev/null || true
  sleep 0.3

  if ! pg_isready -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -t 3 >/dev/null 2>&1; then
    fail "PostgreSQL not reachable at ${PG_HOST}:${PG_PORT}"
  fi

  write_header
  prepare_database

  run_pgbench "direct-postgres" "$PG_PORT"

  start_pgbouncer
  run_pgbench "pgbouncer" "$PGBOUNCER_PORT"
  kill "$PGBOUNCER_PID" 2>/dev/null || true
  wait "$PGBOUNCER_PID" 2>/dev/null || true
  PGBOUNCER_PID=""

  start_pg_bouncer_rs
  run_pgbench "pg-bouncer-rs" "$PGBOUNCER_RS_PORT"

  write_summary_table
}

main "$@"
