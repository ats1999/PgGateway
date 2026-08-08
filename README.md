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
- **`pg-gateway`** — minimal pass-through proxy using `pg-protocol`.

## Run

```bash
cargo run -p pg-gateway
```

Environment:

- `PG_GATEWAY_LISTEN` — default `127.0.0.1:6432`
- `PG_GATEWAY_UPSTREAM` — default `127.0.0.1:5432`

Connect with `psql` through the gateway:

```bash
psql "host=127.0.0.1 port=6432 user=… dbname=…"
```

## Debug

Build with `cargo build -p pg-gateway` (dev profile), install the **CodeLLDB** extension in Cursor/VS Code, set breakpoints, and launch **Debug pg-gateway** from Run and Debug (or run `rust-lldb target/debug/pg-gateway` and use `b`, `run`, `n`, `c`).
