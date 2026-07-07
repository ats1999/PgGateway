#!/usr/bin/env bash
# Runs on the Pooler EC2 machine.
# Installs Rust, builds pg-bouncer-rs, installs PgBouncer.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive

echo "==> Installing build deps and PgBouncer..."
apt-get update -qq
apt-get install -y build-essential git curl sysstat pgbouncer pkg-config libssl-dev

echo "==> Installing Rust..."
if ! command -v cargo &>/dev/null; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --no-modify-path
fi
source "$HOME/.cargo/env"

echo "==> Building pg-bouncer-rs (this takes ~3-5 min)..."
cd ~/pg-bouncer-rs
cargo build --release 2>&1
echo "==> Build complete: $(ls -lh target/release/pg-bouncer-rs)"

# Verify PgBouncer is installed
pgbouncer --version

mkdir -p ~/pooler_run
echo "==> Pooler machine ready."
