#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-transaction}"
CONFIG="${ROOT}/scripts/test-local-${MODE}.hcl"

if [[ ! -f "$CONFIG" ]]; then
  echo "missing config $CONFIG" >&2
  exit 1
fi

if ! pg_isready -h 127.0.0.1 -p 5432 -U postgres >/dev/null 2>&1; then
  echo "Postgres not ready on 127.0.0.1:5432" >&2
  exit 1
fi

pkill -f "target/debug/pg-gateway" 2>/dev/null || true
sleep 0.5

export PG_GATEWAY_CONFIG="$CONFIG"

cd "$ROOT"
cargo build -p pg-gateway -q

if [[ -n "${PG_GATEWAY_LOG_FILE:-}" ]]; then
  cargo run -p pg-gateway >>"$PG_GATEWAY_LOG_FILE" 2>&1 &
else
  cargo run -p pg-gateway >/dev/null 2>&1 &
fi
GW_PID=$!
trap 'kill "$GW_PID" 2>/dev/null || true' EXIT

for i in $(seq 1 30); do
  if nc -z 127.0.0.1 6432 2>/dev/null; then
    sleep 0.2
    break
  fi
  sleep 0.2
done

PSQL="psql -h 127.0.0.1 -p 6432 -U postgres -d postgres -v ON_ERROR_STOP=1"

echo "=== pool_mode=$MODE ==="

$PSQL -c "SELECT 1 AS first_session;"
$PSQL -c "SELECT 2 AS second_session_should_reuse_if_unpinned;"

if [[ "$MODE" == "transaction" ]]; then
  $PSQL <<'SQL'
BEGIN;
SELECT 3 AS in_txn;
COMMIT;
SELECT 4 AS after_commit;
SQL
fi

if [[ "$MODE" == "statement" ]]; then
  $PSQL -c "SELECT 5;"
  $PSQL -c "SELECT 6;"
fi

if [[ "$MODE" == "userlist" ]]; then
  if psql -h 127.0.0.1 -p 6432 -U baduser -d postgres -c "SELECT 1" 2>/dev/null; then
    echo "userlist test FAILED: baduser should be denied" >&2
    exit 1
  fi
  echo "userlist denied baduser as expected"
  $PSQL -c "SELECT 7 AS allowed_user;"
fi

sleep 0.5
kill "$GW_PID" 2>/dev/null || true
wait "$GW_PID" 2>/dev/null || true
