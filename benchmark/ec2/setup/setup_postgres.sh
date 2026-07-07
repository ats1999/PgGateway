#!/usr/bin/env bash
# Runs on the Postgres EC2 machine.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "==> Installing PostgreSQL 15..."
apt-get update -qq
apt-get install -y postgresql-15 postgresql-contrib-15 sysstat

PG_CONF=/etc/postgresql/15/main/postgresql.conf
PG_HBA=/etc/postgresql/15/main/pg_hba.conf

echo "==> Configuring PostgreSQL..."
# Allow remote connections
sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" "$PG_CONF"

# Performance tuning for benchmarks
cat >> "$PG_CONF" <<'EOF'

# benchmark tuning
max_connections          = 500
shared_buffers           = 256MB
effective_cache_size     = 1GB
work_mem                 = 4MB
maintenance_work_mem     = 64MB
checkpoint_completion_target = 0.9
wal_buffers              = 16MB
synchronous_commit       = off
fsync                    = off
full_page_writes         = off
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = all
EOF

# Allow password auth from any host
echo "host  all  all  0.0.0.0/0  md5" >> "$PG_HBA"

systemctl restart postgresql

echo "==> Creating benchmark user and database..."
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'postgres';"
sudo -u postgres psql -c "CREATE DATABASE pgbench;" 2>/dev/null || true
sudo -u postgres psql -d pgbench -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"

# Table for write_heavy query
sudo -u postgres psql -d pgbench -c "
  DROP TABLE IF EXISTS bench_writes;
  CREATE UNLOGGED TABLE bench_writes (
    id  BIGSERIAL PRIMARY KEY,
    val TEXT      NOT NULL,
    ts  TIMESTAMPTZ DEFAULT NOW()
  );"

echo "==> PostgreSQL ready."
