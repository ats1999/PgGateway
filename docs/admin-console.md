# Admin console (`pg_gateway`)

The admin interface is a **virtual database** on the gateway listen address. It does not connect to Postgres upstream; each query refreshes an in-memory snapshot and returns answers over the normal Postgres wire protocol.

## Connect

Default database name: **`pg_gateway`** (override with top-level `admin_database` in HCL).

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway
```

Use a **postgres** user allowed for the logical `postgres` database in your config (`allows_admin_client` checks the `postgres` entry’s `userlist`, and the login is checked against that entry's `password`).

## Protocol and rules

| Rule | Detail |
|------|--------|
| Simple query only | One `Q` message per statement. Extended query (`Parse` / `Bind` / `Execute`) is rejected. |
| Read-only | `SELECT`, `WITH … SELECT`, and `PRAGMA` only. No `INSERT` / `UPDATE` / `DELETE`. |
| Fresh snapshot | Each admin query reloads `databases`, `pools`, `clients`, and `pool_config` from live gateway state before running SQL. |
| No upstream | Admin traffic never opens a pool connection to real Postgres. |

## `SHOW` commands (PgBouncer-style)

These are rewritten to `SELECT` on the admin catalog. Trailing `;` is optional; matching is case-insensitive.

### `SHOW VERSION`

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW VERSION;"
```

| Column | Description |
|--------|-------------|
| `version` | `PgGateway <semver>` |

---

### `SHOW DATABASES`

Configured proxy databases (excluding the admin database itself). Values reflect **database-level** merged config (`pool_defaults` + `databases.*.pool_config`), not per-user overrides.

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW DATABASES;"
```

| Column | Description |
|--------|-------------|
| `name` | Logical database key from HCL `databases`. |
| `primary_host` | Primary upstream host. |
| `primary_port` | Primary upstream port. |
| `pool_mode` | `session`, `transaction`, or `statement`. |
| `pool_size` | Database-level per-pool cap (NULL = unlimited). |
| `min_pool_size` | Target minimum idle upstream connections (NULL = 0). |
| `max_idle` | Max idle upstream connections in a pool (NULL = unlimited). |
| `max_db_connections` | Cap on total upstream connections for this DB (NULL = none). |
| `max_db_client_connections` | Cap on client sessions to this DB (NULL = none). |
| `server_idle_timeout_seconds` | Recycle idle upstream conns older than this (NULL = no limit). |
| `current_connections` | Upstream connections for this logical database (idle + in use + connecting), as counted for `max_db_connections`. |
| `current_client_connections` | Client connections to this database, as counted for `max_db_client_connections`. |
| `replica_count` | Number of configured replicas. |

Equivalent SQL: `SELECT * FROM databases ORDER BY name`.

---

### `SHOW POOLS`

One row per pool `(database, user, target)` that exists at runtime: created when the first client connects, or for `min_pool_size` before any client does. Limit columns are **fully merged** for that pool: global → database → instance → user.

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW POOLS;"
```

| Column | Description |
|--------|-------------|
| `database` | Logical database name. |
| `user` | Client user for the pool key. |
| `pool_mode` | Effective pool mode for this key. |
| `upstream` | Primary address (`host:port`). |
| `idle_count` | Upstream connections idle in the pool for this key. |
| `active_count` | Upstream connections checked out to clients. |
| `pool_size` | Effective per-pool upstream cap (NULL = unlimited). |
| `min_pool_size` | Configured minimum idle target. |
| `max_idle` | Configured max idle queue depth. |
| `max_db_connections` | Database-wide upstream cap from merged config (NULL = none). |
| `server_idle_timeout_seconds` | Effective idle connection max age (seconds). |
| `target` | `primary` or `replica[i]`. |
| `connecting_count` | Connections reserved and currently being opened. |
| `waiting_count` | Clients waiting for a connection right now. |
| `login_backoff_seconds` | Seconds left in the `server_login_retry` backoff (NULL = none). |
| `checkouts` | Connections handed to clients (cumulative). |
| `waits` | Checkouts that had to wait. |
| `wait_timeouts` | Clients disconnected by `query_wait_timeout`. |
| `connections_opened` | New upstream connections. |
| `login_failures` | Failed upstream connects or gateway-side logins. |
| `closed_idle_timeout` | Closed by `server_idle_timeout`. |
| `closed_max_idle` | Closed on release because the idle list was at `max_idle`. |
| `closed_check_failed` | Closed because `server_check_query` failed. |
| `closed_reset_failed` | Closed because `server_reset_query` failed. |
| `closed_in_transaction` | Released mid-transaction (client left or timed out) and closed. |
| `closed_evicted` | Closed to make room under `max_db_connections`. |

Every `closed_*` and count column is cumulative since gateway start.

Equivalent SQL: `SELECT * FROM pools ORDER BY database, user`.

---

### `SHOW CLIENTS`

Clients currently connected to the gateway (proxy or admin).

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW CLIENTS;"
```

| Column | Description |
|--------|-------------|
| `client_id` | Monotonic gateway-assigned id for this session. |
| `peer` | Client socket (`ip:port`). |
| `user` | Startup `user` parameter. |
| `database` | Startup `database` parameter (e.g. `postgres` or `pg_gateway` for admin). |
| `state` | `login`, `waiting` (for an upstream), `active` (query running), `idle`, or `idle_in_transaction`. |
| `connected_at` | Connect time, Unix seconds. |
| `state_seconds` | Seconds spent in the current state. |

Equivalent SQL: `SELECT * FROM clients ORDER BY client_id`.

---

### `SHOW POOL_CONFIG`

The effective value of **every** pool setting, and the config layer it came from, for each `database × instance × configured user`. A userlist-less database shows the user `*`. Use it to answer "why is this pool using that value?".

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW POOL_CONFIG;"
```

| Column | Description |
|--------|-------------|
| `database` | Logical database. |
| `target` | `primary` or `replica[i]`. |
| `user` | Userlist user, or `*`. |
| `setting` | Setting name (as in HCL). |
| `value` | Effective value (`unlimited` / `disabled` for unset caps and timeouts). |
| `source` | Winning layer: `default`, `pool_defaults`, `database`, `primary`, `replica[i]`, or `user`. |

Equivalent SQL: `SELECT * FROM pool_config ORDER BY database, target, user, setting`. `SHOW CONFIG` is an alias.

## Direct `SELECT` examples

```sql
SELECT name, pool_mode, pool_size FROM databases ORDER BY name;
SELECT database, user, idle_count, active_count, pool_size FROM pools;
SELECT client_id, peer, user, database FROM clients;
-- Who is holding transactions open?
SELECT user, database, state_seconds FROM clients WHERE state = 'idle_in_transaction';
-- Which layer set pool_size for each pool?
SELECT database, target, user, value, source FROM pool_config WHERE setting = 'pool_size';
-- Pools under pressure
SELECT database, user, waiting_count, wait_timeouts FROM pools WHERE waits > 0;
```

## HCL configuration

See **[configuration.md](configuration.md)** for merge order, all `pool_config` fields, and the full example file **`pg-gateway.example.hcl`**.

Minimal admin-related snippet:

```hcl
admin_database = "pg_gateway"   # optional; default

databases = {
  postgres = {
    primary = { host = "127.0.0.1", port = 5432 }
    userlist = [{ name = "postgres", password = "…" }]   # admin users and their passwords
  }
}
```

Do not define a `databases` entry whose key equals `admin_database`; that name is reserved for the admin console.

## Errors you may see

| Message | Cause |
|---------|--------|
| `only simple query protocol is supported on admin db` | Client used prepared statements / extended protocol. Use `psql` default or simple query. |
| `only read-only SQL is allowed on admin database` | Non-`SELECT` / non-`WITH` statement. |
| `user … is not allowed for admin database` | `userlist` on `postgres` does not include your user. |