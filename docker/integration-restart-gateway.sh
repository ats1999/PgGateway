#!/usr/bin/env bash
# Start or restart the pg-gateway binary (out-of-process) for integration tests.
set -euo pipefail

POOL_MODE="${1:?usage: integration-restart-gateway.sh <session|transaction|statement>}"

LISTEN_PORT="${PG_GATEWAY_TEST_LISTEN_PORT:-6432}"
PG_PORT="${PG_GATEWAY_TEST_PG_PORT:-5432}"
HCL_PATH="${PG_GATEWAY_CONFIG_PATH:-/tmp/pg-gateway-integration.hcl}"
TEMPLATE="${PG_GATEWAY_CONFIG_TEMPLATE:-/app/docker/integration-gateway.hcl.tpl}"
BIN="${PG_GATEWAY_BIN:-pg-gateway}"

pkill -x "$(basename "$BIN")" 2>/dev/null || true
sleep 0.2

export LISTEN_PORT PG_PORT POOL_MODE
envsubst '${LISTEN_PORT} ${PG_PORT} ${POOL_MODE}' <"$TEMPLATE" >"$HCL_PATH"

PG_GATEWAY_CONFIG="$HCL_PATH" "$BIN" &
echo $! >/tmp/pg-gateway.pid

for _ in $(seq 1 80); do
  if nc -z 127.0.0.1 "$LISTEN_PORT" 2>/dev/null; then
    exit 0
  fi
  sleep 0.1
done

echo "pg-gateway did not listen on 127.0.0.1:${LISTEN_PORT}" >&2
exit 1