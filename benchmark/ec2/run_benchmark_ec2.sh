#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Run pgbench from a "client" VM against a "pooler" VM and a "pg" VM.

Example:
  scripts/ec2/run_benchmark_ec2.sh \
    --ssh-user ubuntu \
    --ssh-key ~/.ssh/bench.pem \
    --pooler-host 10.0.1.10 \
    --pg-host 10.0.2.10 \
    --pg-user postgres --pg-pass postgres \
    --db-name pgbench \
    --clients 64 --jobs 8 --duration 60 --scale 10

Required:
  --pooler-host HOST
  --pg-host HOST

Notes:
  - Requires SSH access from client -> pooler/pg.
  - Requires ports: client->pg:5432, client->pooler:6432/6433, pooler->pg:5432.
EOF
}

fail() { echo "ERROR: $*" >&2; exit 1; }
command_exists() { command -v "$1" >/dev/null 2>&1; }

require_cmd() { command_exists "$1" || fail "missing required command: $1"; }

ssh_user="${SSH_USER:-ubuntu}"
ssh_key="${SSH_KEY:-}"
ssh_port="${SSH_PORT:-22}"

pooler_host=""
pg_host=""

pg_port="5432"
pg_user="postgres"
pg_pass="postgres"
db_name="pgbench"

pool_mode="transaction"
pool_size="10"
pgbouncer_port="6433"
pg_bouncer_rs_port="6432"

clients="64"
jobs="8"
duration="60"
scale="10"

results_root="${RESULTS_DIR:-$(cd "$(dirname "$0")/../.." && pwd)/benchmark-results}"
run_id="${RUN_ID:-$(date -u +"ec2-%Y-%m-%dT%H-%M-%SZ")}"

pg_bouncer_rs_remote_bin="${PG_BOUNCER_RS_REMOTE_BIN:-pg-bouncer-rs}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ssh-user) ssh_user="${2:-}"; shift 2 ;;
    --ssh-key) ssh_key="${2:-}"; shift 2 ;;
    --ssh-port) ssh_port="${2:-}"; shift 2 ;;
    --pooler-host) pooler_host="${2:-}"; shift 2 ;;
    --pg-host) pg_host="${2:-}"; shift 2 ;;
    --pg-port) pg_port="${2:-}"; shift 2 ;;
    --pg-user) pg_user="${2:-}"; shift 2 ;;
    --pg-pass) pg_pass="${2:-}"; shift 2 ;;
    --db-name) db_name="${2:-}"; shift 2 ;;
    --pool-mode) pool_mode="${2:-}"; shift 2 ;;
    --pool-size) pool_size="${2:-}"; shift 2 ;;
    --pgbouncer-port) pgbouncer_port="${2:-}"; shift 2 ;;
    --pg-bouncer-rs-port) pg_bouncer_rs_port="${2:-}"; shift 2 ;;
    --clients) clients="${2:-}"; shift 2 ;;
    --jobs) jobs="${2:-}"; shift 2 ;;
    --duration) duration="${2:-}"; shift 2 ;;
    --scale) scale="${2:-}"; shift 2 ;;
    --results-dir) results_root="${2:-}"; shift 2 ;;
    --run-id) run_id="${2:-}"; shift 2 ;;
    --pg-bouncer-rs-remote-bin) pg_bouncer_rs_remote_bin="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "unknown arg: $1" ;;
  esac
done

[[ -n "$pooler_host" ]] || fail "missing --pooler-host"
[[ -n "$pg_host" ]] || fail "missing --pg-host"

require_cmd ssh
require_cmd scp
require_cmd psql
require_cmd pgbench

ssh_opts=(
  -p "$ssh_port"
  -o StrictHostKeyChecking=accept-new
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=3
)
if [[ -n "$ssh_key" ]]; then
  ssh_opts+=(-i "$ssh_key")
fi

ssh_run() {
  local host="$1"
  shift
  ssh "${ssh_opts[@]}" "${ssh_user}@${host}" "$@"
}

scp_put() {
  local local_path="$1"
  local host="$2"
  local remote_path="$3"
  scp "${ssh_opts[@]}" "$local_path" "${ssh_user}@${host}:${remote_path}"
}

scp_get() {
  local host="$1"
  local remote_path="$2"
  local local_path="$3"
  scp "${ssh_opts[@]}" "${ssh_user}@${host}:${remote_path}" "$local_path"
}

local_run_dir="${results_root}/ec2/${run_id}"
mkdir -p "$local_run_dir"

remote_base_dir="/tmp/pg-bouncer-rs-bench/${run_id}"
remote_pooler_dir="${remote_base_dir}/pooler"
remote_pg_dir="${remote_base_dir}/pg"

metrics_script_local="$(cd "$(dirname "$0")" && pwd)/remote/metrics.sh"
pooler_control_local="$(cd "$(dirname "$0")" && pwd)/remote/pooler_control.sh"

require_cmd bash
[[ -f "$metrics_script_local" ]] || fail "missing metrics script: $metrics_script_local"
[[ -f "$pooler_control_local" ]] || fail "missing pooler control script: $pooler_control_local"

echo "run_id=${run_id}" | tee "$local_run_dir/run_meta.txt" >/dev/null
{
  echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "client_host=$(hostname)"
  echo "pooler_host=${pooler_host}"
  echo "pg_host=${pg_host}"
  echo "pg_port=${pg_port}"
  echo "db_name=${db_name}"
  echo "pool_mode=${pool_mode}"
  echo "pool_size=${pool_size}"
  echo "clients=${clients}"
  echo "jobs=${jobs}"
  echo "duration=${duration}"
  echo "scale=${scale}"
} >>"$local_run_dir/run_meta.txt"

echo "==> prepare remote dirs"
ssh_run "$pooler_host" "mkdir -p '$remote_pooler_dir' '$remote_pooler_dir/tools'"
ssh_run "$pg_host" "mkdir -p '$remote_pg_dir' '$remote_pg_dir/tools'"

scp_put "$metrics_script_local" "$pooler_host" "$remote_pooler_dir/tools/metrics.sh"
scp_put "$pooler_control_local" "$pooler_host" "$remote_pooler_dir/tools/pooler_control.sh"
ssh_run "$pooler_host" "chmod +x '$remote_pooler_dir/tools/metrics.sh' '$remote_pooler_dir/tools/pooler_control.sh'"

scp_put "$metrics_script_local" "$pg_host" "$remote_pg_dir/tools/metrics.sh"
ssh_run "$pg_host" "chmod +x '$remote_pg_dir/tools/metrics.sh'"

echo "==> start PG metrics (process-name=postgres)"
ssh_run "$pg_host" "bash '$remote_pg_dir/tools/metrics.sh' start --run-dir '$remote_pg_dir/metrics' --process-name postgres --interval 1" \
  | tee "$local_run_dir/pg_metrics_start.txt" >/dev/null

export PGPASSWORD="$pg_pass"
psql_base=(psql -v ON_ERROR_STOP=1 -h "$pg_host" -p "$pg_port" -U "$pg_user")

echo "==> ensure database exists: ${db_name}"
db_exists="$("${psql_base[@]}" -d postgres -Atqc "SELECT 1 FROM pg_database WHERE datname='${db_name}'" || true)"
if [[ "$db_exists" != "1" ]]; then
  "${psql_base[@]}" -d postgres -c "CREATE DATABASE \"${db_name}\";"
fi

echo "==> pgbench init scale=${scale}"
pgbench -h "$pg_host" -p "$pg_port" -U "$pg_user" -i -s "$scale" -q "$db_name"

run_pgbench() {
  local label="$1"
  local host="$2"
  local port="$3"
  local out_file="$local_run_dir/pgbench_${label}.txt"

  echo "==> pgbench ${label} ${host}:${port}"
  {
    echo "label=${label}"
    echo "timestamp_utc=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "host=${host}"
    echo "port=${port}"
    echo "clients=${clients}"
    echo "jobs=${jobs}"
    echo "duration=${duration}"
    echo
    pgbench -h "$host" -p "$port" -U "$pg_user" -d "$db_name" -c "$clients" -j "$jobs" -T "$duration" --no-vacuum
  } >"$out_file"
}

echo "==> benchmark: direct postgres"
run_pgbench "direct_postgres" "$pg_host" "$pg_port"

start_pooler_and_metrics() {
  local target="$1" # pgbouncer | pg-bouncer-rs
  local listen_port="$2"

  local remote_target_dir="$remote_pooler_dir/$target"
  ssh_run "$pooler_host" "mkdir -p '$remote_target_dir'"

  if [[ "$target" == "pgbouncer" ]]; then
    pooler_pid_line="$(ssh_run "$pooler_host" \
      "bash '$remote_pooler_dir/tools/pooler_control.sh' start-pgbouncer \
        --run-dir '$remote_target_dir' \
        --listen-addr 0.0.0.0 --listen-port '$listen_port' \
        --pg-host '$pg_host' --pg-port '$pg_port' \
        --pg-user '$pg_user' --pg-pass '$pg_pass' \
        --db-name '$db_name' \
        --pool-mode '$pool_mode' --pool-size '$pool_size'")"
  else
    pooler_pid_line="$(ssh_run "$pooler_host" \
      "bash '$remote_pooler_dir/tools/pooler_control.sh' start-pg-bouncer-rs \
        --run-dir '$remote_target_dir' \
        --listen-addr 0.0.0.0 --listen-port '$listen_port' \
        --pg-host '$pg_host' --pg-port '$pg_port' \
        --pool-mode '$pool_mode' --pool-size '$pool_size' \
        --bin '$pg_bouncer_rs_remote_bin'")"
  fi

  echo "$pooler_pid_line" | tee "$local_run_dir/${target}_start.txt" >/dev/null
  pooler_pid="${pooler_pid_line#pid=}"
  [[ "$pooler_pid" =~ ^[0-9]+$ ]] || fail "could not parse pooler pid from: $pooler_pid_line"

  ssh_run "$pooler_host" \
    "bash '$remote_pooler_dir/tools/metrics.sh' start --run-dir '$remote_target_dir/metrics' --pid '$pooler_pid' --interval 1" \
    | tee "$local_run_dir/${target}_metrics_start.txt" >/dev/null
}

stop_pooler_and_metrics() {
  local target="$1"
  local remote_target_dir="$remote_pooler_dir/$target"
  ssh_run "$pooler_host" "bash '$remote_pooler_dir/tools/metrics.sh' stop --run-dir '$remote_target_dir/metrics'" >/dev/null || true
  if [[ "$target" == "pgbouncer" ]]; then
    ssh_run "$pooler_host" "bash '$remote_pooler_dir/tools/pooler_control.sh' stop-pgbouncer --run-dir '$remote_target_dir'" >/dev/null || true
  else
    ssh_run "$pooler_host" "bash '$remote_pooler_dir/tools/pooler_control.sh' stop-pg-bouncer-rs --run-dir '$remote_target_dir'" >/dev/null || true
  fi
}

echo "==> benchmark: pgbouncer"
start_pooler_and_metrics "pgbouncer" "$pgbouncer_port"
run_pgbench "pgbouncer" "$pooler_host" "$pgbouncer_port"
stop_pooler_and_metrics "pgbouncer"

echo "==> benchmark: pg-bouncer-rs"
start_pooler_and_metrics "pg-bouncer-rs" "$pg_bouncer_rs_port"
run_pgbench "pg_bouncer_rs" "$pooler_host" "$pg_bouncer_rs_port"
stop_pooler_and_metrics "pg-bouncer-rs"

echo "==> stop PG metrics"
ssh_run "$pg_host" "bash '$remote_pg_dir/tools/metrics.sh' stop --run-dir '$remote_pg_dir/metrics'" \
  | tee "$local_run_dir/pg_metrics_stop.txt" >/dev/null || true

echo "==> collect remote artifacts"
ssh_run "$pooler_host" "tar -C '$remote_pooler_dir' -czf '$remote_base_dir/pooler.tar.gz' ."
ssh_run "$pg_host" "tar -C '$remote_pg_dir' -czf '$remote_base_dir/pg.tar.gz' ."

scp_get "$pooler_host" "$remote_base_dir/pooler.tar.gz" "$local_run_dir/pooler.tar.gz"
scp_get "$pg_host" "$remote_base_dir/pg.tar.gz" "$local_run_dir/pg.tar.gz"

echo "==> done"
echo "results_dir=$local_run_dir"
