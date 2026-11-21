# Gateway configuration (HCL)

PgGateway reads a single HashiCorp HCL file (path from `PG_GATEWAY_CONFIG`). Top-level keys:

| Key | Default | Description |
|-----|---------|-------------|
| `listen` | `127.0.0.1:6432` | Gateway listen address. |
| `admin_database` | `pg_gateway` | Virtual admin database name (see [admin-console.md](admin-console.md)). |
| `pool_defaults` | empty | Global pool/upstream defaults (lowest merge priority). |
| `databases` | one `postgres` entry | Logical databases clients connect to. |

## Logical database (`databases.<name>`)

```hcl
databases = {
  myapp = {
    primary = {
      host = "db-primary.example.com"
      port = 5432
      # Optional overrides for this upstream only (merged after database pool_config).
      pool_config = { server_connect_timeout_seconds = 10 }
    }

    replicas = [{
      host = "db-replica.example.com"
      port = 5432
      pool_config = { pool_size = 30 }   # Overrides for pools on this replica.
    }]

    pool_config = { pool_mode = "transaction", pool_size = 50 }

    userlist = [
      { name = "app", password = "…", pool_config = { pool_size = 20 } },
      { name = "report", password = "…", pool_config = { pool_size = 5 } },
    ]
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `dbname` | No | Actual database name on the primary and every replica (PgBouncer `dbname`). Clients connect with the logical name (the `databases` key); the gateway sends `dbname` upstream. Unset = the logical name. |
| `primary` | Yes | `host`, `port`, optional nested `pool_config`. |
| `replicas` | No | Same shape as `primary`. Each replica's `pool_config` applies to pools on that replica. Clients are not routed to replicas yet (no read routing), so today these pools are only visible in the admin `pool_config` table. |
| `pool_config` | No | Settings for this logical database (every user, every instance). |
| `userlist` | Yes (to connect) | The only users that may connect; an empty userlist admits nobody. Every entry **must** have a `password` (config error otherwise) and may set `pool_config`. pg-gateway only supports password authentication: every client login, first or later, is checked against this password (md5 challenge) before a pooled connection is handed over, and every upstream login (`min_pool_size` warm-up, new connections, transaction/statement re-checkout) uses it, answering cleartext, md5 or SCRAM-SHA-256 (trust needs none). Client passwords are never passed through to Postgres. |

Client startup parameter **`database`** must match a key under `databases`. Traffic uses **`primary`** today.

## Config merge order

Every pool is identified by `(user, database, instance)`, where instance is the primary or one replica. Each setting is resolved by overlaying layers (**later wins**):

1. built-in default
2. `pool_defaults`
3. `databases.<db>.pool_config`
4. `databases.<db>.primary.pool_config` **or** `databases.<db>.replicas[i].pool_config` (whichever instance the pool talks to)
5. the `userlist` entry's `pool_config` for that user

The same chain is used everywhere a setting is read: when connecting, when checking out a connection, while relaying queries, and in the admin database. User-level settings apply on every instance. There is no per-user-per-replica layer.

**Database-wide settings** (`max_db_connections`, `max_db_client_connections`) describe the whole logical database, so they may only be set in `pool_defaults` or `databases.<db>.pool_config`. Setting them on an instance or a user is a config error rather than being silently ignored.

Validation also checks the merged result: a `min_pool_size` from one layer may not exceed a `pool_size` from another.

To see which layer supplied each effective value, query `SHOW POOL_CONFIG` on the admin database ([admin-console.md](admin-console.md)).

## `pool_config` / `pool_defaults` fields

PgBouncer names, with `_seconds` on durations. Unset fields inherit from the previous layer. For every timeout, `0` means disabled, and unset at every layer means disabled. Unknown keys are rejected, so a typo (or `reserve_pool_size`) fails at startup.

| Field | Default | Behaviour |
|-------|---------|-----------|
| `pool_size` (alias `max_connections`) | unlimited | Max upstream connections per pool. When the pool is full, clients **wait** for a connection (see `query_wait_timeout_seconds`). |
| `min_pool_size` | 0 | Keep at least this many upstream connections per pool. The background task (every 1s) opens them, also for `userlist` users before their first client connects. Counts idle and in-use connections. Logs in with the user's userlist password. |
| `max_idle` | unlimited | Max idle connections kept per pool; extra ones are closed on release. |
| `max_db_connections` | unlimited | *Database-wide.* Cap on upstream connections across every pool of the database. When reached, an idle connection of another pool in the same database is closed to make room (as PgBouncer does); otherwise clients wait. |
| `max_db_client_connections` | unlimited | *Database-wide.* Cap on client connections. Extra clients get `FATAL 53300`. |
| `pool_mode` | `session` | `session`, `transaction`, or `statement`. |
| `connect_query` | none | Run once on every new upstream connection after login. Errors are logged and ignored, and the connection stays usable. Empty string disables. |
| `query_wait_timeout_seconds` | disabled | Max time a client waits for an upstream connection. On expiry the client gets `FATAL 08P01 query_wait_timeout`. **Unset means clients wait indefinitely** (PgBouncer's default is 120). |
| `idle_transaction_timeout_seconds` | disabled | Max time a client may sit idle inside an open transaction. The client gets `FATAL 25P03`; the upstream (mid-transaction) is closed. |
| `transaction_timeout_seconds` | disabled | Max time a transaction may stay open, measured from `BEGIN`, even while a query is running. The client gets `FATAL 25P04`; the upstream is closed. Autocommit statements are not affected. |
| `client_idle_timeout_seconds` | disabled | Max time a client may stay connected without sending anything while not in a transaction (and not waiting on a query). The client gets `FATAL 57P05`; the upstream goes back to the pool. |
| `client_login_timeout_seconds` | disabled | Max time from TCP accept to the end of login. The startup packet is bounded by `pool_defaults` (the user and database aren't known yet); the rest of login uses the user's merged value. |
| `server_reset_query` | `DISCARD ALL` | Run when a connection goes back to the pool (session mode, or every mode with `_always`). A failing reset closes the connection. Empty string disables. |
| `server_reset_query_always` | false | Run `server_reset_query` in transaction and statement modes too. |
| `server_check_query` | disabled | Run on an idle connection before handing it out, if it has been idle at least `server_check_delay_seconds`. A failing check closes the connection and the client gets another one. Empty string disables. |
| `server_check_delay_seconds` | 30 | See above. `0` checks on every reuse. |
| `server_idle_timeout_seconds` | disabled | Close connections idle longer than this. Reaped every second by the background task and also checked at checkout. |
| `server_lifetime_seconds` | — | Legacy alias, used only when `server_idle_timeout_seconds` is unset at every layer. |
| `server_connect_timeout_seconds` | disabled | Max time for TCP connect **and** login to an upstream. |
| `server_login_retry_seconds` (alias `server_login_retry`) | disabled | After a failed upstream connect or gateway-side login, wait this long before that pool tries again. Waiting clients stay queued (bounded by `query_wait_timeout_seconds`). A wrong password from a client in pass-through auth does **not** trigger it. PgBouncer's default is 15. |

`reserve_pool_size` is **not supported** and is rejected; use `pool_size`.

### Limitations

- **Password authentication only.** Clients are checked with an md5 challenge (no SCRAM towards clients yet) and there is no TLS, so passwords cross the network unencrypted. No `auth_query`: every user and password lives in the userlist.
- **No read routing:** replica pools resolve their own settings and appear in `SHOW POOL_CONFIG`, but clients always use the primary.

## Example

See **[pg-gateway.example.hcl](../pg-gateway.example.hcl)** at the repo root.

```bash
PG_GATEWAY_CONFIG=pg-gateway.example.hcl cargo run -p pg-gateway
```

## Environment

| Variable | Description |
|----------|-------------|
| `PG_GATEWAY_CONFIG` | Path to HCL file. If unset, built-in defaults apply (single `postgres` → `127.0.0.1:5432`). |

## Pool modes (behavior summary)

| Mode | Upstream pinning |
|------|------------------|
| `session` | One upstream per client session; reusable after disconnect with `server_reset_query` when applicable. |
| `transaction` | Released to the pool on an idle `ReadyForQuery` once every request the client sent has been answered. Between transactions the client holds **no** upstream: the next one is checked out when the client sends its next message. |
| `statement` | Same release timing as `transaction`; treat as stateless across statements. |

A connection released while still inside a transaction (the client disconnected or timed out mid-transaction) is closed, never pooled.

Details: [README.md](../README.md#connection-pooling-modes).

## Admin visibility

Configured limits, live counts, per-pool counters, client states, and the effective value and source layer of every setting are exposed via **`pg_gateway`** (`SHOW DATABASES`, `SHOW POOLS`, `SHOW CLIENTS`, `SHOW POOL_CONFIG`). Column list: [admin-console.md](admin-console.md).

PgBouncer parity for admin columns: [pgbouncer-admin-comparison.md](pgbouncer-admin-comparison.md).