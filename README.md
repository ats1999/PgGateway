# PgGateway

PgGateway is a PostgreSQL-aware proxy that sits between applications and Postgres (primary + replicas). Clients connect to the gateway; it speaks the Postgres wire protocol and manages upstream connections, routing, and policy.

## Features

- **Connection pooler** — reuse upstream connections to many client sessions
- **Read/write split** — send writes to the primary and reads to replica nodes
- **Load balancing** — distribute read traffic across read replicas
- **Query rate limiting** — cap query throughput per user, database, or client
- **Multiple databases** — route many logical databases / upstream clusters from one gateway
- **Query blocking** — deny or allow SQL by policy (blocklist / allowlist)
- **Prepared statement support** — correct behavior across pool modes and route changes
- **Connection pooling modes** — **session**, **transaction**, and **statement** pooling, plus **`mode=auto`** to pick a mode per connection from usage (e.g. enter transaction pooling when a transaction starts; requires query parsing)
- **Health checks** — readiness/liveness for the gateway and upstream nodes

## Crates

- **`pg-protocol`** — wire framing, startup packets, typed client/server streams, session relay.
- **`pg-gateway`** — pass-through proxy with session pooling using `pg-protocol`.

## Run

```bash
cargo run -p pg-gateway
```

### Configuration (YAML)

Set `PG_GATEWAY_CONFIG` to a YAML file, or rely on defaults (listen `127.0.0.1:6432`, database `postgres` → `127.0.0.1:5432`).

Example (`pg-gateway.example.yaml`):

```yaml
listen: "127.0.0.1:6432"

databases:
  postgres:
    primary:
      host: 127.0.0.1
      port: 5432
    replicas:
      - host: 127.0.0.1
        port: 5433
    pool:
      max_connections: 50   # reserved for future enforcement

users:
  - name: postgres
    database: postgres
    password: postgres      # reserved for future pooler auth
```

Client startup **`database`** must match a key under `databases`. Pooling uses each database’s **primary** today; **replicas** are configured but not routed yet. If **`users`** is non-empty, only listed `(name, database)` pairs may connect; an empty list allows any user (dev default).

```bash
PG_GATEWAY_CONFIG=pg-gateway.example.yaml cargo run -p pg-gateway
```

**Pooling** is always on: one idle queue per `(user, database)`. Acquire reuses idle or opens a new connection to that database’s primary; release runs `DISCARD ALL`.

### Library

```rust
use pg_gateway::{Gateway, GatewayConfig};

let config = GatewayConfig::from_yaml_file("pg-gateway.yaml")?;
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
