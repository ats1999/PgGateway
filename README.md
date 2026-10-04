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

Example (`pg-gateway.example.hcl`):

```hcl
listen = "127.0.0.1:6432"

databases = {
  postgres = {
    primary = {
      host = "127.0.0.1"
      port = 5432
    }

    replicas = [{ host = "127.0.0.1", port = 5433 }]

    # Pool configuration with explicit pooling mode
    # pool_mode options: "session" (default), "transaction", "statement"
    #   - session: Pin for entire session (never release to pool)
    #   - transaction: Pin until COMMIT/ROLLBACK
    #   - statement: Unpin after each statement completes
    pool_config = {
      max_connections = 50
      pool_mode = "session"
    }

    userlist = [{
      name     = "postgres"
      password = "postgres"
    }]
  }
}
```

Client startup **`database`** must match a key under `databases`. Pooling uses each database’s **primary** today; **replicas** are configured but not routed yet. If **`userlist`** is non-empty, only listed `(name, database)` pairs may connect; an empty list allows any user (dev default).

```bash
PG_GATEWAY_CONFIG=pg-gateway.example.hcl cargo run -p pg-gateway
```

**Connection Pooling Modes:**

- **`session`** (default) — Connection pinned to client for entire session. Safest option, minimal connection reuse.
- **`transaction`** — Connection pinned during transactions (BEGIN...COMMIT/ROLLBACK). Released after transaction ends. Best for OLTP workloads.
- **`statement`** — Connection released after each statement. Maximum reuse but requires stateless workloads (no session variables, prepared statements).

**How it works**: one idle queue per `(user, database)`. Acquire reuses idle connections matching the pool mode rules, or opens a new connection to the database’s primary; release resets connection state and decides whether to pin/unpin based on pool mode and transaction status.

### Library

```rust
use pg_gateway::{Gateway, GatewayConfig};

let config = GatewayConfig::from_hcl_file("pg-gateway.hcl")?;
let gateway = Gateway::new(config)?;
gateway.run().await?;
```

Environment:

- `PG_GATEWAY_CONFIG` — path to YAML config (optional)

Connect with `psql` through the gateway:

```bash
psql "host=127.0.0.1 port=6432 user=… dbname=…"
```

## Debug

Build with `cargo build -p pg-gateway` (dev profile), install the **CodeLLDB** extension in Cursor/VS Code, set breakpoints, and launch **Debug pg-gateway** from Run and Debug (or run `rust-lldb target/debug/pg-gateway` and use `b`, `run`, `n`, `c`).
