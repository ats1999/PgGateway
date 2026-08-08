# PgGateway

Multi-crate workspace for a PostgreSQL proxy.

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
