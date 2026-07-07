#!/usr/bin/env bash
# benchmark_aws.sh — Automated EC2 benchmark for PgGateway
#
# Provisions 3 EC2 instances (client, pooler, postgres) via Terraform
# (terraform/main.tf), installs all deps, runs benchmark scenarios across
# multiple targets (pggateway, pgbouncer, direct), collects OS + app metrics,
# computes p50/p95/p99, and generates a report. Requires `terraform` and `jq`
# on PATH plus AWS credentials in the environment (same as the AWS CLI would use).
#
# Usage:
#   ./benchmark_aws.sh --key-name my-key --key-file ~/.ssh/my-key.pem [options]
#
# Required:
#   --key-name      EC2 key pair name
#   --key-file      Path to private key (.pem)
#
# Optional:
#   --targets       Comma-separated: pggateway,pgbouncer,direct   (default: all three)
#   --queries       Comma-separated: tpcb,simple,sleep,large,write,lookup  (default: tpcb,simple,sleep,large,write,lookup)
#   --pool-mode     session|transaction|statement  (default: transaction)
#   --pool-size     Connections per pool           (default: 10)
#   --clients       pgbench concurrency            (default: 64)
#   --jobs          pgbench worker threads          (default: 8)
#   --duration      Seconds per scenario           (default: 60)
#   --scale         pgbench scale factor           (default: 10)
#   --sleep-ms      ms for sleep query             (default: 100)
#   --instance-type EC2 instance type              (default: c5.xlarge)
#   --region        AWS region                     (default: us-east-1)
#   --ssh-user      SSH username on instances      (default: ubuntu)
#   --results-dir   Local output dir               (default: ./benchmark-results/aws)
#   --run-id        Custom run ID                  (default: aws-TIMESTAMP)
#   --no-teardown   Keep instances after run       (default: terminate)
#   --use-existing  Skip provisioning; requires --client-ip --pooler-ip --pg-ip
#   --client-ip     Existing client instance IP
#   --pooler-ip     Existing pooler instance IP
#   --pg-ip         Existing postgres instance IP

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_SRC="$(cd "$SCRIPT_DIR/../.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────────────────────
TARGETS="pggateway,pgbouncer,direct"
QUERIES="tpcb,simple,sleep,large,write,lookup"
POOL_MODE="transaction"
POOL_SIZE="10"
CLIENTS="64"
JOBS="8"
DURATION="60"
SCALE="10"
SLEEP_MS="100"
INSTANCE_TYPE="c5.xlarge"
REGION="us-east-1"
SSH_USER="ubuntu"
RESULTS_DIR="./benchmark-results/aws"
NO_TEARDOWN="false"
USE_EXISTING="false"
KEY_NAME=""
KEY_FILE=""
CLIENT_IP=""
POOLER_IP=""
PG_IP=""
RUN_ID="aws-$(date -u +%Y%m%dT%H%M%SZ)"

# EC2 internal
CLIENT_INSTANCE_ID=""
POOLER_INSTANCE_ID=""
PG_INSTANCE_ID=""
SG_ID=""
TF_DIR=""

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --key-name)      KEY_NAME="$2";      shift 2 ;;
    --key-file)      KEY_FILE="$2";      shift 2 ;;
    --targets)       TARGETS="$2";       shift 2 ;;
    --queries)       QUERIES="$2";       shift 2 ;;
    --pool-mode)     POOL_MODE="$2";     shift 2 ;;
    --pool-size)     POOL_SIZE="$2";     shift 2 ;;
    --clients)       CLIENTS="$2";       shift 2 ;;
    --jobs)          JOBS="$2";          shift 2 ;;
    --duration)      DURATION="$2";      shift 2 ;;
    --scale)         SCALE="$2";         shift 2 ;;
    --sleep-ms)      SLEEP_MS="$2";      shift 2 ;;
    --instance-type) INSTANCE_TYPE="$2"; shift 2 ;;
    --region)        REGION="$2";        shift 2 ;;
    --ssh-user)      SSH_USER="$2";      shift 2 ;;
    --results-dir)   RESULTS_DIR="$2";   shift 2 ;;
    --run-id)        RUN_ID="$2";        shift 2 ;;
    --no-teardown)   NO_TEARDOWN="true"; shift ;;
    --use-existing)  USE_EXISTING="true"; shift ;;
    --client-ip)     CLIENT_IP="$2";     shift 2 ;;
    --pooler-ip)     POOLER_IP="$2";     shift 2 ;;
    --pg-ip)         PG_IP="$2";         shift 2 ;;
    -h|--help)
      head -35 "$0" | tail -30
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Validation ────────────────────────────────────────────────────────────────
if [[ "$USE_EXISTING" == "false" ]]; then
  [[ -z "$KEY_NAME" ]] && { echo "ERROR: --key-name required"; exit 1; }
  [[ -z "$KEY_FILE" ]] && { echo "ERROR: --key-file required"; exit 1; }
  [[ ! -f "$KEY_FILE" ]] && { echo "ERROR: key file not found: $KEY_FILE"; exit 1; }
else
  [[ -z "$CLIENT_IP" || -z "$POOLER_IP" || -z "$PG_IP" ]] && {
    echo "ERROR: --use-existing requires --client-ip, --pooler-ip, --pg-ip"
    exit 1
  }
fi

if [[ "$USE_EXISTING" == "false" ]]; then
  command -v terraform &>/dev/null || { echo "ERROR: terraform not found"; exit 1; }
  command -v jq        &>/dev/null || { echo "ERROR: jq not found (needed to parse terraform output)"; exit 1; }
fi
command -v psql &>/dev/null || { echo "ERROR: psql not found (needed for admin queries)"; exit 1; }

RUN_DIR="$RESULTS_DIR/$RUN_ID"
mkdir -p "$RUN_DIR"
TF_DIR="$RUN_DIR/terraform"

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_FILE="$RUN_DIR/benchmark.log"
log()  { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG_FILE"; }
die()  { log "ERROR: $*"; exit 1; }
step() { log ""; log "══ $* ══"; }

# ── SSH helpers ───────────────────────────────────────────────────────────────
SSH_OPTS="-i $KEY_FILE -o StrictHostKeyChecking=no -o ConnectTimeout=15 -o ServerAliveInterval=30"

ssh_cmd() {
  local host="$1"; shift
  ssh $SSH_OPTS "$SSH_USER@$host" "$@"
}

ssh_bg() {
  # Run command in background on remote, return PID
  local host="$1"; shift
  ssh $SSH_OPTS "$SSH_USER@$host" "nohup $* >/dev/null 2>&1 &; echo \$!"
}

scp_put() {
  local src="$1" host="$2" dest="$3"
  scp -r $SSH_OPTS "$src" "$SSH_USER@$host:$dest"
}

scp_get() {
  local host="$1" src="$2" dest="$3"
  scp -r $SSH_OPTS "$SSH_USER@$host:$src" "$dest"
}

rsync_to() {
  local src="$1" host="$2" dest="$3"
  rsync -avz -e "ssh $SSH_OPTS" --exclude target/ --exclude '*.pyc' "$src" "$SSH_USER@$host:$dest"
}

wait_for_ssh() {
  local host="$1" label="$2"
  log "Waiting for SSH on $label ($host)..."
  local attempts=0
  until ssh_cmd "$host" "echo ok" &>/dev/null; do
    (( attempts++ )) || true
    [[ $attempts -gt 60 ]] && die "SSH timeout on $label"
    sleep 5
  done
  log "  SSH ready: $label"
}

# ── EC2 provisioning (Terraform) ──────────────────────────────────────────────
tf() {
  terraform -chdir="$TF_DIR" "$@" \
    -var "region=$REGION" \
    -var "instance_type=$INSTANCE_TYPE" \
    -var "key_name=$KEY_NAME" \
    -var "run_id=$RUN_ID"
}

provision() {
  step "Provisioning EC2 instances (Terraform)"

  mkdir -p "$TF_DIR"
  cp "$SCRIPT_DIR/terraform/"*.tf "$TF_DIR/"

  log "Running terraform init..."
  terraform -chdir="$TF_DIR" init -input=false >> "$LOG_FILE" 2>&1

  log "Launching 3 $INSTANCE_TYPE instances via terraform apply..."
  tf apply -auto-approve -input=false >> "$LOG_FILE" 2>&1

  local outputs
  outputs=$(terraform -chdir="$TF_DIR" output -json)

  CLIENT_INSTANCE_ID=$(echo "$outputs" | jq -r '.instance_ids.value.client')
  POOLER_INSTANCE_ID=$(echo "$outputs" | jq -r '.instance_ids.value.pooler')
  PG_INSTANCE_ID=$(echo "$outputs" | jq -r '.instance_ids.value.postgres')
  SG_ID=$(echo "$outputs" | jq -r '.security_group_id.value')

  CLIENT_IP=$(echo "$outputs" | jq -r '.public_ips.value.client')
  POOLER_IP=$(echo "$outputs" | jq -r '.public_ips.value.pooler')
  PG_IP=$(echo "$outputs" | jq -r '.public_ips.value.postgres')

  POOLER_PRIVATE_IP=$(echo "$outputs" | jq -r '.private_ips.value.pooler')
  PG_PRIVATE_IP=$(echo "$outputs" | jq -r '.private_ips.value.postgres')

  log "  client:  $CLIENT_INSTANCE_ID  public=$CLIENT_IP"
  log "  pooler:  $POOLER_INSTANCE_ID  public=$POOLER_IP  private=$POOLER_PRIVATE_IP"
  log "  postgres: $PG_INSTANCE_ID  public=$PG_IP  private=$PG_PRIVATE_IP"
  log "  security group: $SG_ID"

  # Wait for SSH
  wait_for_ssh "$CLIENT_IP"  "client"
  wait_for_ssh "$POOLER_IP"  "pooler"
  wait_for_ssh "$PG_IP"      "postgres"
}

# ── Machine setup ─────────────────────────────────────────────────────────────
setup_all() {
  step "Setting up machines (parallel)"

  # Upload scripts + project source to all machines
  log "Uploading scripts to all machines..."
  rsync_to "$SCRIPT_DIR/" "$CLIENT_IP"  "~/benchmark/"  &
  rsync_to "$SCRIPT_DIR/" "$POOLER_IP"  "~/benchmark/"  &
  rsync_to "$SCRIPT_DIR/" "$PG_IP"      "~/benchmark/"  &
  wait

  # Upload project source to pooler for building
  log "Uploading project source to pooler..."
  rsync_to "$PROJECT_SRC/" "$POOLER_IP" "~/pg-bouncer-rs/"

  # Run setup scripts in parallel
  log "Running setup scripts in parallel..."
  ssh_cmd "$CLIENT_IP"  "bash ~/benchmark/setup/setup_client.sh"   > "$RUN_DIR/setup_client.log"  2>&1 &
  ssh_cmd "$POOLER_IP"  "bash ~/benchmark/setup/setup_pooler.sh"   > "$RUN_DIR/setup_pooler.log"  2>&1 &
  ssh_cmd "$PG_IP"      "bash ~/benchmark/setup/setup_postgres.sh" > "$RUN_DIR/setup_postgres.log" 2>&1 &
  wait

  log "All machines ready."

  # Initialize pgbench schema on postgres machine
  log "Initializing pgbench schema (scale=$SCALE)..."
  ssh_cmd "$CLIENT_IP" "PGPASSWORD=postgres pgbench \
    -h $PG_PRIVATE_IP -p 5432 -U postgres -d pgbench \
    -i -s $SCALE 2>&1" | tee -a "$LOG_FILE"
  log "Schema initialized."
}

# ── Metrics helpers ───────────────────────────────────────────────────────────
start_metrics() {
  local label="$1" remote_dir="$2"
  # Start on all 3 machines
  ssh_cmd "$CLIENT_IP" "bash ~/benchmark/remote/metrics.sh start \
    --run-dir $remote_dir/client_metrics --interval 1 --process-name pgbench 2>&1 || true" &
  ssh_cmd "$POOLER_IP" "bash ~/benchmark/remote/metrics.sh start \
    --run-dir $remote_dir/pooler_metrics --interval 1 --process-name pg-bouncer-rs 2>&1 || true" &
  ssh_cmd "$PG_IP" "bash ~/benchmark/remote/metrics.sh start \
    --run-dir $remote_dir/pg_metrics --interval 1 --process-name postgres 2>&1 || true" &
  wait
}

stop_metrics() {
  local remote_dir="$1"
  ssh_cmd "$CLIENT_IP" "bash ~/benchmark/remote/metrics.sh stop --run-dir $remote_dir/client_metrics 2>&1 || true" &
  ssh_cmd "$POOLER_IP" "bash ~/benchmark/remote/metrics.sh stop --run-dir $remote_dir/pooler_metrics 2>&1 || true" &
  ssh_cmd "$PG_IP"     "bash ~/benchmark/remote/metrics.sh stop --run-dir $remote_dir/pg_metrics    2>&1 || true" &
  wait
}

collect_pooler_stats() {
  local target="$1" out_file="$2"
  [[ "$target" == "direct" ]] && return
  local port=6432
  [[ "$target" == "pgbouncer" ]] && port=6433
  PGPASSWORD=postgres psql -h "$POOLER_IP" -p "$port" -U postgres -d pgbouncer \
    -c "SHOW STATS;" -c "SHOW POOLS;" --no-psqlrc -A 2>/dev/null >> "$out_file" || true
}

collect_pg_stats() {
  local out_file="$1"
  PGPASSWORD=postgres psql -h "$PG_IP" -p 5432 -U postgres -d pgbench \
    --no-psqlrc -A -c "
SELECT query, calls, total_exec_time::bigint AS total_ms,
       mean_exec_time::bigint AS mean_ms,
       rows
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 20;" 2>/dev/null >> "$out_file" || true
}

collect_artifacts() {
  local remote_dir="$1" local_dir="$2"
  scp_get "$CLIENT_IP" "$remote_dir/" "$local_dir/client_data/" &
  scp_get "$POOLER_IP" "$remote_dir/" "$local_dir/pooler_data/" &
  scp_get "$PG_IP"     "$remote_dir/" "$local_dir/pg_data/"     &
  wait
}

# ── Percentile computation ────────────────────────────────────────────────────
compute_percentiles() {
  # pgbench -l produces: client_id txn_no time_us script_no epoch_s epoch_us
  # Collect all log files from remote and compute locally
  local log_dir="$1" out_file="$2"
  local combined="$log_dir/all_latencies.txt"

  # Extract 3rd field (time in microseconds) from all pgbench_log.* files
  cat "$log_dir"/pgbench_log.* 2>/dev/null | awk '{print $3}' | sort -n > "$combined" || true

  local count
  count=$(wc -l < "$combined" 2>/dev/null || echo 0)

  if [[ "$count" -eq 0 ]]; then
    echo "p50_ms=N/A p95_ms=N/A p99_ms=N/A p999_ms=N/A" > "$out_file"
    return
  fi

  awk -v count="$count" '
    { a[NR] = $1 }
    END {
      printf "p50_ms=%.2f\n",  a[int(count * 0.500)] / 1000
      printf "p95_ms=%.2f\n",  a[int(count * 0.950)] / 1000
      printf "p99_ms=%.2f\n",  a[int(count * 0.990)] / 1000
      printf "p999_ms=%.2f\n", a[int(count * 0.999)] / 1000
      printf "sample_count=%d\n", count
    }' "$combined" > "$out_file"
}

# ── Single scenario ───────────────────────────────────────────────────────────
run_scenario() {
  local target="$1" query="$2"
  local scenario_id="${target}_${query}"
  local local_dir="$RUN_DIR/$scenario_id"
  local remote_dir="~/benchmark_run/$RUN_ID/$scenario_id"
  mkdir -p "$local_dir"

  log "  Running: target=$target  query=$query  clients=$CLIENTS  duration=${DURATION}s"

  # Create remote work dir
  ssh_cmd "$CLIENT_IP" "mkdir -p $remote_dir"
  ssh_cmd "$POOLER_IP" "mkdir -p $remote_dir"
  ssh_cmd "$PG_IP"     "mkdir -p $remote_dir"

  # Determine connection endpoint
  local conn_host conn_port
  case "$target" in
    pggateway)  conn_host="$POOLER_PRIVATE_IP"; conn_port=6432 ;;
    pgbouncer)  conn_host="$POOLER_PRIVATE_IP"; conn_port=6433 ;;
    direct)     conn_host="$PG_PRIVATE_IP";     conn_port=5432 ;;
  esac

  # Start metrics on all 3 machines
  start_metrics "$scenario_id" "$remote_dir"

  # Build pgbench command
  local pgbench_cmd="PGPASSWORD=postgres pgbench \
    -h $conn_host -p $conn_port -U postgres -d pgbench \
    -c $CLIENTS -j $JOBS -T $DURATION \
    -l --log-prefix=$remote_dir/pgbench_log \
    --progress=5"

  case "$query" in
    tpcb)    pgbench_cmd="$pgbench_cmd" ;;  # default TPC-B built-in
    simple)  pgbench_cmd="$pgbench_cmd -f ~/benchmark/queries/simple_select.pgbench" ;;
    sleep)   pgbench_cmd="$pgbench_cmd -f ~/benchmark/queries/sleep.pgbench \
               --define sleep_ms=$SLEEP_MS" ;;
    large)   pgbench_cmd="$pgbench_cmd -f ~/benchmark/queries/large_result.pgbench" ;;
    write)   pgbench_cmd="$pgbench_cmd -f ~/benchmark/queries/write_heavy.pgbench" ;;
    lookup)  pgbench_cmd="$pgbench_cmd -f ~/benchmark/queries/point_lookup.pgbench" ;;
    *)       die "Unknown query type: $query" ;;
  esac

  # Run pgbench on CLIENT machine, capture output
  local pgbench_out="$local_dir/pgbench.txt"
  ssh_cmd "$CLIENT_IP" "$pgbench_cmd 2>&1" | tee "$pgbench_out" || true

  # Stop metrics
  stop_metrics "$remote_dir"

  # Collect pooler admin stats
  collect_pooler_stats "$target" "$local_dir/pooler_stats.txt"

  # Collect pg_stat_statements
  collect_pg_stats "$local_dir/pg_stats.txt"

  # Collect latency log files from client machine
  local log_dir="$local_dir/latency_logs"
  mkdir -p "$log_dir"
  scp_get "$CLIENT_IP" "$remote_dir/pgbench_log.*" "$log_dir/" 2>/dev/null || true

  # Collect OS metrics from all 3 machines
  collect_artifacts "$remote_dir" "$local_dir"

  # Extract summary metrics from pgbench output
  local tps lat_avg lat_stddev txns failed
  tps=$(grep -E '^tps = '                                      "$pgbench_out" | awk '{print $3}' || echo 0)
  lat_avg=$(grep -E '^latency average = '                      "$pgbench_out" | awk '{print $4}' || echo 0)
  lat_stddev=$(grep -E '^latency stddev = '                    "$pgbench_out" | awk '{print $4}' || echo 0)
  txns=$(grep -E '^number of transactions actually processed:' "$pgbench_out" | awk '{print $NF}' || echo 0)
  failed=$(grep -E '^number of failed transactions:'           "$pgbench_out" | awk '{print $NF}' || echo 0)

  # Compute percentiles from latency logs
  compute_percentiles "$log_dir" "$local_dir/percentiles.txt"

  # Write summary
  cat > "$local_dir/summary.txt" <<EOF
target=$target
query=$query
clients=$CLIENTS
jobs=$JOBS
duration_sec=$DURATION
pool_mode=$POOL_MODE
pool_size=$POOL_SIZE
tps=$tps
latency_avg_ms=$lat_avg
latency_stddev_ms=$lat_stddev
transactions=$txns
failed=$failed
$(cat "$local_dir/percentiles.txt" 2>/dev/null)
EOF

  log "    tps=$tps  avg=${lat_avg}ms  txns=$txns  failed=$failed"
  local p95 p99
  p95=$(grep p95_ms "$local_dir/percentiles.txt" 2>/dev/null | cut -d= -f2 || echo N/A)
  p99=$(grep p99_ms "$local_dir/percentiles.txt" 2>/dev/null | cut -d= -f2 || echo N/A)
  log "    p95=${p95}ms  p99=${p99}ms"
}

# ── Pooler lifecycle ──────────────────────────────────────────────────────────
start_target() {
  local target="$1"
  case "$target" in
    pggateway)
      log "  Starting pggateway..."
      ssh_cmd "$POOLER_IP" "bash ~/benchmark/remote/pooler_control.sh start-pg-bouncer-rs \
        --run-dir ~/pooler_run \
        --listen-addr 0.0.0.0 --listen-port 6432 \
        --pg-host $PG_PRIVATE_IP --pg-port 5432 \
        --pool-mode $POOL_MODE --pool-size $POOL_SIZE \
        --bin ~/pg-bouncer-rs/target/release/pg-bouncer-rs 2>&1" | tee -a "$LOG_FILE"
      ;;
    pgbouncer)
      log "  Starting pgbouncer..."
      ssh_cmd "$POOLER_IP" "bash ~/benchmark/remote/pooler_control.sh start-pgbouncer \
        --run-dir ~/pooler_run \
        --listen-addr 0.0.0.0 --listen-port 6433 \
        --pg-host $PG_PRIVATE_IP --pg-port 5432 \
        --pg-user postgres --pg-pass postgres \
        --db-name pgbench \
        --pool-mode $POOL_MODE --pool-size $POOL_SIZE 2>&1" | tee -a "$LOG_FILE"
      ;;
    direct)
      log "  Direct mode: no pooler"
      ;;
  esac
  sleep 2
}

stop_target() {
  local target="$1"
  case "$target" in
    pggateway) ssh_cmd "$POOLER_IP" "bash ~/benchmark/remote/pooler_control.sh stop-pg-bouncer-rs --run-dir ~/pooler_run 2>&1 || true" ;;
    pgbouncer) ssh_cmd "$POOLER_IP" "bash ~/benchmark/remote/pooler_control.sh stop-pgbouncer --run-dir ~/pooler_run 2>&1 || true" ;;
    direct)    true ;;
  esac
}

# ── Full benchmark run ────────────────────────────────────────────────────────
run_benchmarks() {
  step "Running benchmarks"

  IFS=',' read -ra target_list <<< "$TARGETS"
  IFS=',' read -ra query_list  <<< "$QUERIES"

  for target in "${target_list[@]}"; do
    log "Target: $target"
    start_target "$target"
    for query in "${query_list[@]}"; do
      run_scenario "$target" "$query"
    done
    stop_target "$target"
    sleep 3  # let connections drain before next target
  done
}

# ── Teardown ──────────────────────────────────────────────────────────────────
teardown() {
  if [[ "$NO_TEARDOWN" == "true" ]]; then
    log "Skipping teardown (--no-teardown). Instances still running:"
    log "  client:   $CLIENT_IP  ($CLIENT_INSTANCE_ID)"
    log "  pooler:   $POOLER_IP  ($POOLER_INSTANCE_ID)"
    log "  postgres: $PG_IP      ($PG_INSTANCE_ID)"
    log "  terraform state: $TF_DIR"
    return
  fi

  step "Tearing down"

  if [[ -d "$TF_DIR" ]]; then
    tf destroy -auto-approve -input=false >> "$LOG_FILE" 2>&1 || true
    log "  Terraform resources destroyed (instances + security group)"
  fi
}

# ── Error cleanup ─────────────────────────────────────────────────────────────
cleanup_on_error() {
  log "ERROR: benchmark aborted. Cleaning up..."
  teardown
}
trap cleanup_on_error ERR

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  log "PgGateway EC2 Benchmark"
  log "  run_id=$RUN_ID"
  log "  targets=$TARGETS"
  log "  queries=$QUERIES"
  log "  clients=$CLIENTS  duration=${DURATION}s  scale=$SCALE  pool_mode=$POOL_MODE"
  log "  results=$RUN_DIR"
  echo ""

  # Save run metadata
  cat > "$RUN_DIR/run_meta.txt" <<EOF
run_id=$RUN_ID
timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)
targets=$TARGETS
queries=$QUERIES
pool_mode=$POOL_MODE
pool_size=$POOL_SIZE
clients=$CLIENTS
jobs=$JOBS
duration_sec=$DURATION
scale=$SCALE
sleep_ms=$SLEEP_MS
instance_type=$INSTANCE_TYPE
region=$REGION
EOF

  if [[ "$USE_EXISTING" == "false" ]]; then
    provision
    # Save IPs to metadata
    echo "client_ip=$CLIENT_IP" >> "$RUN_DIR/run_meta.txt"
    echo "pooler_ip=$POOLER_IP" >> "$RUN_DIR/run_meta.txt"
    echo "pg_ip=$PG_IP"         >> "$RUN_DIR/run_meta.txt"
    setup_all
  else
    log "Using existing instances: client=$CLIENT_IP pooler=$POOLER_IP pg=$PG_IP"
    # For existing instances, still need private IPs for inter-instance traffic
    POOLER_PRIVATE_IP=$(ssh_cmd "$POOLER_IP" "hostname -I | awk '{print \$1}'")
    PG_PRIVATE_IP=$(ssh_cmd "$PG_IP" "hostname -I | awk '{print \$1}'")
    log "  pooler_private=$POOLER_PRIVATE_IP  pg_private=$PG_PRIVATE_IP"
  fi

  run_benchmarks

  step "Generating report"
  bash "$SCRIPT_DIR/report/generate_report.sh" "$RUN_DIR" | tee "$RUN_DIR/report.md"
  log "Report: $RUN_DIR/report.md"

  teardown

  step "Done"
  log "Results: $RUN_DIR"
  log "Report:  $RUN_DIR/report.md"
}

main "$@"
