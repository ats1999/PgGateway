#!/usr/bin/env bash
set -euo pipefail

PGDATA="${PGDATA:-/tmp/pgdata-integration}"
export PG_GATEWAY_INTEGRATION=1
export PG_GATEWAY_CONFIG_TEMPLATE="${PG_GATEWAY_CONFIG_TEMPLATE:-/app/docker/integration-gateway.hcl.tpl}"
export PG_GATEWAY_TEST_PG_PORT="${PG_GATEWAY_TEST_PG_PORT:-5432}"
export PG_GATEWAY_TEST_LISTEN_PORT="${PG_GATEWAY_TEST_LISTEN_PORT:-6432}"
export PG_GATEWAY_BIN="${PG_GATEWAY_BIN:-/app/target/release/pg-gateway}"
export PG_GATEWAY_RESTART_SCRIPT="${PG_GATEWAY_RESTART_SCRIPT:-/app/docker/integration-restart-gateway.sh}"

PG_BIN="${PG_BIN:-/usr/lib/postgresql/15/bin}"
if [[ ! -x "${PG_BIN}/pg_ctl" ]]; then
  PG_BIN="$(dirname "$(command -v pg_ctl)")"
fi

setup_postgres() {
  if [[ ! -f "$PGDATA/PG_VERSION" ]]; then
    install -d -o postgres -g postgres "$PGDATA"
    su postgres -s /bin/bash -c "'${PG_BIN}/initdb' -D '${PGDATA}'"
    {
      echo "local all all trust"
      echo "host all all 127.0.0.1/32 trust"
      echo "host all all ::1/128 trust"
    } >>"$PGDATA/pg_hba.conf"
  fi

  if ! su postgres -s /bin/bash -c "'${PG_BIN}/pg_ctl' -D '${PGDATA}' status" >/dev/null 2>&1; then
    su postgres -s /bin/bash -c "'${PG_BIN}/pg_ctl' -D '${PGDATA}' -o '-p ${PG_GATEWAY_TEST_PG_PORT}' -w start"
  fi

  if ! psql -h 127.0.0.1 -p "$PG_GATEWAY_TEST_PG_PORT" -U postgres -d postgres -tAc \
    "SELECT 1 FROM pg_database WHERE datname = 'pg-gateway'" | grep -q 1; then
    psql -h 127.0.0.1 -p "$PG_GATEWAY_TEST_PG_PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 \
      -c 'CREATE DATABASE "pg-gateway"'
  fi
}

cleanup() {
  if [[ -f /tmp/pg-gateway.pid ]]; then
    kill "$(cat /tmp/pg-gateway.pid)" 2>/dev/null || true
  fi
  pkill -x pg-gateway 2>/dev/null || true
  su postgres -s /bin/bash -c "'${PG_BIN}/pg_ctl' -D '${PGDATA}' stop -m fast" 2>/dev/null || true
}

trap cleanup EXIT

setup_postgres

cd /app

"$PG_GATEWAY_RESTART_SCRIPT" transaction

exec cargo test -p pg-gateway --test integration_pool --test integration_admin -- --test-threads=1