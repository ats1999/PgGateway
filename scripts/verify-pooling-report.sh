#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${ROOT}/test-logs"
mkdir -p "$LOG_DIR"
LOG_FILE="${LOG_DIR}/verify-$(date +%Y%m%d-%H%M%S).log"
export PG_GATEWAY_LOG_FILE="$LOG_FILE"
export RUST_LOG="${RUST_LOG:-info}"

if ! pg_isready -h 127.0.0.1 -p 5432 -U postgres >/dev/null 2>&1; then
  echo "Postgres not on 5432; starting pg-gateway-test-pg (tmpfs)..." | tee -a "$LOG_FILE"
  docker rm -f pg-gateway-test-pg 2>/dev/null || true
  docker run -d --name pg-gateway-test-pg \
    -e POSTGRES_PASSWORD=postgres -e POSTGRES_HOST_AUTH_METHOD=trust \
    -p 5432:5432 --tmpfs /var/lib/postgresql/data:rw,size=256m postgres:16-alpine
  for i in $(seq 1 40); do
    pg_isready -h 127.0.0.1 -p 5432 -U postgres && break
    sleep 0.5
  done
fi

cd "$ROOT"
echo "=== cargo test ===" | tee -a "$LOG_FILE"
if cargo test -p pg-gateway 2>&1 | tee -a "$LOG_FILE"; then
  UNIT_OK=1
else
  UNIT_OK=0
fi

for mode in session transaction statement; do
  echo "=== feature: $mode ===" | tee -a "$LOG_FILE"
  ./scripts/run-feature-tests.sh "$mode" 2>&1 | tee -a "$LOG_FILE" || true
done

echo "=== mid-session transaction (one psql, multiple queries) ===" | tee -a "$LOG_FILE"
pkill -f "target/debug/pg-gateway" 2>/dev/null || true
sleep 0.3
export PG_GATEWAY_CONFIG="${ROOT}/scripts/test-local-transaction.hcl"
cargo build -p pg-gateway -q
cargo run -p pg-gateway >>"$LOG_FILE" 2>&1 &
GW_PID=$!
trap 'kill "$GW_PID" 2>/dev/null || true' EXIT
sleep 0.6
psql -h 127.0.0.1 -p 6432 -U postgres -d postgres -v ON_ERROR_STOP=1 \
  -c "SELECT 100;" -c "SELECT 101;" \
  -c "BEGIN; SELECT 102; COMMIT;" -c "SELECT 103;"
kill "$GW_PID" 2>/dev/null || true

echo "=== log events ===" | tee -a "$LOG_FILE"
rg 'event=' "$LOG_FILE" | tail -40 || true

MID=$(rg -c 'pool_release_mid_session' "$LOG_FILE" 2>/dev/null || echo 0)
CHECKOUT=$(rg -c 'pool_checkout' "$LOG_FILE" 2>/dev/null || echo 0)

echo "=== SUMMARY ===" | tee -a "$LOG_FILE"
echo "log_file=$LOG_FILE"
echo "unit_tests_ok=$UNIT_OK"
echo "mid_session_releases=$MID"
echo "checkouts=$CHECKOUT"
if [[ "$UNIT_OK" == "1" && "$MID" -ge 3 ]]; then
  echo "RESULT=PASS (transaction pooling mid-session releases observed)"
else
  echo "RESULT=FAIL or INCOMPLETE (see log)"
fi