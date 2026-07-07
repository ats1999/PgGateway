#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  pooler_control.sh start-pgbouncer --run-dir DIR --listen-addr ADDR --listen-port PORT --pg-host HOST --pg-port PORT --pg-user USER --pg-pass PASS --db-name NAME --pool-mode MODE --pool-size N
  pooler_control.sh stop-pgbouncer  --run-dir DIR

  pooler_control.sh start-pg-bouncer-rs --run-dir DIR --listen-addr ADDR --listen-port PORT --pg-host HOST --pg-port PORT --pool-mode MODE --pool-size N [--bin PATH]
  pooler_control.sh stop-pg-bouncer-rs  --run-dir DIR

Outputs:
  Prints "pid=<PID>" on success.
EOF
}

fail() { echo "ERROR: $*" >&2; exit 1; }
command_exists() { command -v "$1" >/dev/null 2>&1; }

subcommand="${1:-}"
shift || true

run_dir=""
listen_addr="0.0.0.0"
listen_port=""
pg_host=""
pg_port="5432"
pg_user="postgres"
pg_pass="postgres"
db_name="pgbench"
pool_mode="transaction"
pool_size="10"
pg_bouncer_rs_bin="${PG_BOUNCER_RS_BIN:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-dir) run_dir="${2:-}"; shift 2 ;;
    --listen-addr) listen_addr="${2:-}"; shift 2 ;;
    --listen-port) listen_port="${2:-}"; shift 2 ;;
    --pg-host) pg_host="${2:-}"; shift 2 ;;
    --pg-port) pg_port="${2:-}"; shift 2 ;;
    --pg-user) pg_user="${2:-}"; shift 2 ;;
    --pg-pass) pg_pass="${2:-}"; shift 2 ;;
    --db-name) db_name="${2:-}"; shift 2 ;;
    --pool-mode) pool_mode="${2:-}"; shift 2 ;;
    --pool-size) pool_size="${2:-}"; shift 2 ;;
    --bin) pg_bouncer_rs_bin="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "Unknown arg: $1" ;;
  esac
done

[[ -n "$run_dir" ]] || fail "Missing --run-dir"
mkdir -p "$run_dir"

pgbouncer_pid_file="$run_dir/pgbouncer.pid"
pgbouncer_log="$run_dir/pgbouncer.log"
pgbouncer_ini="$run_dir/pgbouncer.ini"
pgbouncer_userlist="$run_dir/userlist.txt"

rs_pid_file="$run_dir/pg-bouncer-rs.pid"
rs_log="$run_dir/pg-bouncer-rs.log"

stop_pid_file() {
  local pid_file="$1"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" || true)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_file" || true
  fi
}

wait_for_ready() {
  local host="$1"
  local port="$2"
  local timeout_sec="${3:-10}"
  local start
  start="$(date +%s)"
  while true; do
    if command_exists psql; then
      if PGPASSWORD="$pg_pass" PGCONNECT_TIMEOUT=2 psql -h "$host" -p "$port" -U "$pg_user" -d "$db_name" -c 'SELECT 1' >/dev/null 2>&1; then
        return 0
      fi
    else
      if command_exists nc && nc -z "$host" "$port" >/dev/null 2>&1; then
        return 0
      fi
    fi
    if (( $(date +%s) - start > timeout_sec )); then
      return 1
    fi
    sleep 0.2
  done
}

case "$subcommand" in
  start-pgbouncer)
    [[ -n "$listen_port" ]] || fail "Missing --listen-port"
    [[ -n "$pg_host" ]] || fail "Missing --pg-host"
    stop_pid_file "$pgbouncer_pid_file"

    command_exists pgbouncer || fail "pgbouncer not found in PATH"

    printf '"%s" "%s"\n' "$pg_user" "$pg_pass" >"$pgbouncer_userlist"
    cat >"$pgbouncer_ini" <<EOF
[databases]
${db_name} = host=${pg_host} port=${pg_port} dbname=${db_name} user=${pg_user} password=${pg_pass}

[pgbouncer]
listen_addr = ${listen_addr}
listen_port = ${listen_port}
auth_type = trust
auth_file = ${pgbouncer_userlist}
pool_mode = ${pool_mode}
default_pool_size = ${pool_size}
max_client_conn = 2000
ignore_startup_parameters = extra_float_digits
EOF

    pgbouncer "$pgbouncer_ini" >"$pgbouncer_log" 2>&1 &
    echo $! >"$pgbouncer_pid_file"

    if ! wait_for_ready "127.0.0.1" "$listen_port" 15; then
      tail -200 "$pgbouncer_log" >&2 || true
      fail "pgbouncer did not become ready on port $listen_port"
    fi

    echo "pid=$(cat "$pgbouncer_pid_file")"
    ;;

  stop-pgbouncer)
    stop_pid_file "$pgbouncer_pid_file"
    echo "ok"
    ;;

  start-pg-bouncer-rs)
    [[ -n "$listen_port" ]] || fail "Missing --listen-port"
    [[ -n "$pg_host" ]] || fail "Missing --pg-host"
    stop_pid_file "$rs_pid_file"

    if [[ -z "$pg_bouncer_rs_bin" ]]; then
      if command_exists pg-bouncer-rs; then
        pg_bouncer_rs_bin="$(command -v pg-bouncer-rs)"
      else
        fail "pg-bouncer-rs binary not found (set --bin or PG_BOUNCER_RS_BIN)"
      fi
    fi
    [[ -x "$pg_bouncer_rs_bin" ]] || fail "pg-bouncer-rs not executable: $pg_bouncer_rs_bin"

    "$pg_bouncer_rs_bin" \
      --listen-addr "${listen_addr}:${listen_port}" \
      --backend-host "$pg_host" \
      --backend-port "$pg_port" \
      --pool-mode "$pool_mode" \
      >"$rs_log" 2>&1 &
    echo $! >"$rs_pid_file"

    if ! wait_for_ready "127.0.0.1" "$listen_port" 15; then
      tail -200 "$rs_log" >&2 || true
      fail "pg-bouncer-rs did not become ready on port $listen_port"
    fi

    echo "pid=$(cat "$rs_pid_file")"
    ;;

  stop-pg-bouncer-rs)
    stop_pid_file "$rs_pid_file"
    echo "ok"
    ;;

  *)
    usage >&2
    exit 2
    ;;
esac
