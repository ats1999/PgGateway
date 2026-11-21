# PgGateway

PgGateway is a PostgreSQL-aware proxy that sits between applications and Postgres (primary + replicas). Clients connect to the gateway; it speaks the Postgres wire protocol and manages upstream connections, routing, and policy.

> ## 🚧 Work in Progress - PgGateway is not yet ready for use.

## Features

- **Connection pooler** — reuse upstream connections to many client sessions
- **Read/write split** — send writes to the primary and reads to replica nodes
- **Load balancing** — distribute read traffic across read replicas
- **Query rate limiting** — cap query throughput per user, database, or client
- **Multiple databases** — route many logical databases / upstream clusters from one gateway
- **Query blocking** — deny or allow SQL by policy (blocklist / allowlist)
- **Prepared statement support** — correct behavior across pool modes and route changes
- **Connection pooling modes** — **session**, **transaction**, and **statement** pooling with explicit per-database configuration
- **Health checks** — readiness/liveness for the gateway and upstream nodes

## Crates

- **`pg-protocol`** — wire framing, startup packets, typed client/server streams, session relay.
- **`pg-gateway`** — pass-through proxy with session pooling using `pg-protocol`.

## Run

```bash
cargo run -p pg-gateway
```

### Configuration (HCL)

Set `PG_GATEWAY_CONFIG` to an HCL file, or rely on defaults (listen `127.0.0.1:6432`, database `postgres` → `127.0.0.1:5432`).

Example (`pg-gateway.example.hcl`); full field reference: **[docs/configuration.md](docs/configuration.md)**.

```hcl
listen = "127.0.0.1:6432"

pool_defaults = {
  pool_size          = 100
  server_reset_query = "DISCARD ALL"
}

databases = {
  postgres = {
    primary = { host = "127.0.0.1", port = 5432 }
    replicas = [{ host = "127.0.0.1", port = 5433 }]

    pool_config = {
      pool_mode  = "session"   # session | transaction | statement
      pool_size  = 50          # max_connections is an accepted alias
      min_pool_size = 2
      max_db_connections = 200
    }

    userlist = [{ name = "postgres", password = "postgres" }]
  }
}
```

**Merge order** for pool settings: `pool_defaults` → database `pool_config` → `primary.pool_config` or `replicas[i].pool_config` → `userlist[].pool_config`. `SHOW POOL_CONFIG` on the admin database shows the winning value and layer for every setting. All settings: [docs/configuration.md](docs/configuration.md).

Client startup **`database`** must match a key under `databases`. Pooling uses each database’s **primary** today; **replicas** are configured but not routed yet. If **`userlist`** is non-empty, only listed users may connect; an empty list allows any user (dev default).

```bash
PG_GATEWAY_CONFIG=pg-gateway.example.hcl cargo run -p pg-gateway
```

**Connection Pooling Modes:**

- **`session`** (default) — Upstream connection stays with one client for the whole session; after disconnect it is reset and returned to the pool for the next client.
- **`transaction`** — Server connection only for the current transaction (or single autocommit statement). Returned to the pool on `ReadyForQuery` idle; the client may get another server connection for the next transaction while staying connected.
- **`statement`** — Same release timing as transaction (idle after each completed query). Treat workloads as stateless (no session-pinned prepared statements / `SET`).

**How it works**: one idle queue per `(user, database)`. Acquire reuses idle connections matching the pool mode rules, or opens a new connection to the database’s primary; release resets connection state and decides whether to pin/unpin based on pool mode and transaction status.

### Library

```rust
use pg_gateway::{Gateway, GatewayConfig};

let config = GatewayConfig::from_hcl_file("pg-gateway.hcl")?;
let gateway = Gateway::new(config)?;
gateway.run().await?;
```

Environment:

- `PG_GATEWAY_CONFIG` — path to HCL config (optional)

### Admin console (SQLite-backed)

Connect to the virtual database **`pg_gateway`** (override with `admin_database` in HCL).

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW POOLS;"
```

Configuration reference: **[docs/configuration.md](docs/configuration.md)**.  
Admin console: **[docs/admin-console.md](docs/admin-console.md)**.  
PgBouncer vs PgGateway admin: **[docs/pgbouncer-admin-comparison.md](docs/pgbouncer-admin-comparison.md)**.

Connect with `psql` through the gateway:

```bash
psql "host=127.0.0.1 port=6432 user=… dbname=…"
```

## Debug

Build with `cargo build -p pg-gateway` (dev profile), install the **CodeLLDB** extension in Cursor/VS Code, set breakpoints, and launch **Debug pg-gateway** from Run and Debug (or run `rust-lldb target/debug/pg-gateway` and use `b`, `run`, `n`, `c`).
