#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${ROOT}/test-logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/concurrent-$(date +%Y%m%d-%H%M%S).log"

export PGPASSWORD=postgres
export PG_GATEWAY_CONFIG="${ROOT}/scripts/test-local-rocketdb-transaction.hcl"
export PG_GATEWAY_LOG_FILE="$LOG_FILE"
export RUST_LOG=trace

if ! pg_isready -h 127.0.0.1 -p 5432 -U postgres >/dev/null 2>&1; then
  echo "rocketdb (127.0.0.1:5432) not ready" >&2
  exit 1
fi

pkill -f "target/debug/pg-gateway" 2>/dev/null || true
sleep 0.3

cd "$ROOT"
cargo build -p pg-gateway -q
cargo run -p pg-gateway &
GW_PID=$!
trap 'kill "$GW_PID" 2>/dev/null || true' EXIT

for i in $(seq 1 40); do
  nc -z 127.0.0.1 6432 && break
  sleep 0.15
done

run_client() {
  local id=$1
  psql -h 127.0.0.1 -p 6432 -U postgres -d postgres -v ON_ERROR_STOP=1 -v client_id="$id" <<SQL
BEGIN;
SELECT :client_id AS client_id, pg_sleep(0.05), 1;
COMMIT;
SELECT :client_id * 10 AS after_commit;
SQL
}

echo "=== 3 concurrent clients, explicit transactions ===" | tee -a "$LOG_FILE"
run_client 1 &
run_client 2 &
run_client 3 &
wait

sleep 0.5
kill "$GW_PID" 2>/dev/null || true
wait "$GW_PID" 2>/dev/null || true

echo "=== analyse: reusable after COMMIT ===" | tee -a "$LOG_FILE"
REUSABLE=$(rg -c 'pool_release_reusable' "$LOG_FILE" 2>/dev/null || echo 0)
MID=$(rg -c 'pool_release_mid_session' "$LOG_FILE" 2>/dev/null || echo 0)
ACQUIRE=$(rg -c 'pool_acquire_reuse' "$LOG_FILE" 2>/dev/null || echo 0)
PINNED=$(rg -c 'pool_release_pinned' "$LOG_FILE" 2>/dev/null || echo 0)
DECISION_IDLE=$(rg 'pool_release_decision' "$LOG_FILE" | rg 'release_after_forward=true' | wc -l | tr -d ' ')

echo "log_file=$LOG_FILE"
echo "pool_release_mid_session=$MID"
echo "pool_release_reusable=$REUSABLE"
echo "pool_acquire_reuse=$ACQUIRE"
echo "pool_release_pinned=$PINNED"
echo "release_decisions_true=$DECISION_IDLE"

echo "--- sample trace (release path) ---" | tee -a "$LOG_FILE"
rg 'pool_release_decision|pool_release_mid_session|pool_release_reusable|pool_acquire_reuse' "$LOG_FILE" | head -35