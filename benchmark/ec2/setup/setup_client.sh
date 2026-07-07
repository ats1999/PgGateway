#!/usr/bin/env bash
# Runs on the Client EC2 machine.
# Installs pgbench and monitoring tools.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "==> Installing pgbench and tools..."
apt-get update -qq
apt-get install -y postgresql-client-15 postgresql-contrib-15 sysstat bc

# Verify pgbench is available
pgbench --version

echo "==> Client machine ready."
