# PgBouncer vs PgGateway admin console

PgGateway uses database **`pg_gateway`** by default. PgBouncer uses **`pgbouncer`**. Both speak simple SQL over the Postgres wire protocol on the pooler listen port.

This compares **read-only `SHOW` / catalog views** that exist in both (or only one). PgBouncer also has **control commands** (`PAUSE`, `RELOAD`, `KILL`, …); PgGateway does not implement those on the admin console.

Sources: [PgBouncer usage — Admin console](https://www.pgbouncer.org/usage.html#admin-console), PgGateway `admin/store.rs`.

## Commands overview

| PgBouncer | PgGateway | Notes |
|-----------|-----------|--------|
| `SHOW VERSION` | `SHOW VERSION` | PgBouncer: upstream version string. PgGateway: `PgGateway <semver>` from crate version. |
| `SHOW DATABASES` | `SHOW DATABASES` | Overlapping columns only; PgBouncer has many more. |
| `SHOW POOLS` | `SHOW POOLS` | PgBouncer: client/server pool metrics. PgGateway: minimal idle snapshot. |
| `SHOW CLIENTS` | `SHOW CLIENTS` | PgBouncer: rich connection state. PgGateway: four columns. |
| `SHOW SERVERS` | — | Not implemented. |
| `SHOW STATS` | — | Not implemented. |
| `SHOW STATS_TOTALS` | — | Not implemented. |
| `SHOW STATS_AVERAGES` | — | Not implemented. |
| `SHOW ACTIVE_SOCKETS` | — | Not implemented. |
| `SHOW LISTS` | — | Not implemented. |
| `SHOW CONFIG` | — | Not implemented. |
| `SHOW MEM` | — | Not implemented. |
| `SHOW FDS` | — | Not implemented. |
| `SHOW DNS_HOSTS` | — | Not implemented. |
| `SHOW DNS_ZONES` | — | Not implemented. |
| `SHOW PEERS` | — | Not implemented. |
| `SHOW PEER_POOLS` | — | Not implemented. |
| `SHOW STATE` | — | Not implemented. |
| `SHOW HELP` | — | Not implemented. |

## `SHOW VERSION`

| Column | PgBouncer | PgGateway |
|--------|-----------|-----------|
| `version` | PgBouncer release string (e.g. `PgBouncer 1.24.0`) | `PgGateway 0.1.0` (package semver from build) |

**Difference:** Same shape (one text column). Meaning is product name + release, not Postgres server version.

---

## `SHOW DATABASES`

### Columns PgBouncer exposes (PgGateway subset marked)

| Column | PgBouncer | PgGateway | PgGateway column / behavior |
|--------|-----------|-----------|-----------------------------|
| `name` | Yes | Yes | `name` |
| `host` | Yes | Yes | `primary_host` (primary upstream host) |
| `port` | Yes | Yes | `primary_port` |
| `database` | Physical DB on server | **No** | Logical name equals client `database`; no separate physical name in admin catalog. |
| `force_user` | Yes | **No** | — |
| `pool_size` | Yes | Yes | HCL `pool_size` (`max_connections` alias); per pool key on `SHOW POOLS`, database-level on `SHOW DATABASES`. |
| `min_pool_size` | Yes | Yes | HCL `min_pool_size`; kept up by the background task every second (also before the first client for `userlist` users). |
| `reserve_pool_size` | Yes | **No** | Not supported, and rejected in config; use `pool_size` only. |
| `server_lifetime` / `server_idle_timeout` | Yes | Partial | HCL `server_idle_timeout_seconds` (or legacy `server_lifetime_seconds`), reaped every second; admin column `server_idle_timeout_seconds`. No separate max-age `server_lifetime`. |
| `pool_mode` | Yes | Yes | `pool_mode` (`session` / `transaction` / `statement`), merged through `userlist`. |
| `load_balance_hosts` | Yes | **No** | — |
| `max_db_connections` | Yes | Yes | HCL + admin `max_db_connections`; enforced across all users per logical DB. |
| `current_connections` | Yes | Yes | `current_connections` on `SHOW DATABASES` (sum per logical DB). |
| `max_client_connections` | Yes | Yes | HCL `max_db_client_connections`; enforced on new client sessions (`FATAL 53300`). |
| `current_client_connections` | Yes | Yes | `current_client_connections` on `SHOW DATABASES`. |
| `paused` | Yes | **No** | — |
| `disabled` | Yes | **No** | — |

**Difference:** PgGateway lists logical databases, primary target, merged database-level pool settings, and live upstream connection totals. No pausing/disabling or PgBouncer-only sizing columns.

---

## `SHOW POOLS`

One row per `(database, user)` in both (PgBouncer definition).

### PgBouncer columns vs PgGateway

| Column | PgBouncer | PgGateway | Notes |
|--------|-----------|-----------|--------|
| `database` | Yes | Yes | Logical database name. |
| `user` | Yes | Yes | Pool key user. |
| `pool_mode` | Yes | Yes | Per-database mode. |
| `cl_active` | Yes | **No** | — |
| `cl_waiting` | Yes | **No** | — |
| `cl_active_cancel_req` | Yes | **No** | — |
| `cl_waiting_cancel_req` | Yes | **No** | — |
| `sv_active` | Yes | **No** | — |
| `sv_active_cancel` | Yes | **No** | — |
| `sv_being_canceled` | Yes | **No** | — |
| `sv_idle` | Yes | Partial | PgGateway **`idle_count`**: count of idle upstream connections in pool (similar intent, different name and no `sv_*` breakdown). |
| `sv_used` | Yes | **No** | — |
| `sv_tested` | Yes | **No** | — |
| `sv_login` | Yes | **No** | — |
| `maxwait` | Yes | **No** | — |
| `maxwait_us` | Yes | **No** | — |
| `load_balance_hosts` | Yes | **No** | — |
| — | **No** | Yes | **`upstream`**: `host:port` of primary for that database. |
| — | **No** | Yes | **`active_count`**, **`pool_size`**, **`min_pool_size`**, **`max_idle`**, **`max_db_connections`**, **`server_idle_timeout_seconds`**: configured/effective limits and checkout count. |

**Difference:** PgBouncer is a full pool pressure dashboard (clients waiting, servers active/idle/login, queue time). PgGateway exposes **idle/active upstream counts**, **merged limits**, **pool mode**, and **upstream address** — not client wait queues or server login state.

---

## `SHOW CLIENTS`

| Column | PgBouncer | PgGateway | PgGateway column |
|--------|-----------|-----------|------------------|
| `id` | Yes | Yes | `client_id` |
| `type` | Yes (`C`) | **No** | — |
| `user` | Yes | Yes | `user` |
| `database` | Yes | Yes | `database` |
| `replication` | Yes | **No** | — |
| `state` | Yes | **No** | — |
| `addr` | Yes | Partial | **`peer`**: `ip:port` (combined, not split addr/port). |
| `port` | Yes | **No** | Included in `peer`. |
| `local_addr` | Yes | **No** | — |
| `local_port` | Yes | **No** | — |
| `connect_time` | Yes | **No** | — |
| `request_time` | Yes | **No** | — |
| `wait` | Yes | **No** | — |
| `wait_us` | Yes | **No** | — |
| `close_needed` | Yes | **No** | — |
| `ptr` / `link` | Yes | **No** | — |
| `remote_pid` | Yes | **No** | — |
| `tls` | Yes | **No** | — |
| `application_name` | Yes | **No** | — |
| `prepared_statements` | Yes | **No** | — |

**Difference:** PgGateway only tracks **who is connected** (id, peer, user, database). No wait time, pairing to server, TLS, or cancel state.

---

## PgBouncer-only `SHOW` commands (not in PgGateway)

Reference: [PgBouncer admin console](https://www.pgbouncer.org/usage.html#admin-console). Connect with `psql -p <listen> pgbouncer` (or your admin DB name).

### `SHOW SERVERS`

One row per **server** (upstream) connection PgBouncer holds. Columns mirror much of `SHOW CLIENTS`, but from the server side (`type` = `S`).

| Column | Meaning |
|--------|---------|
| `type` | `S` for server. |
| `user` | User PgBouncer uses to connect to Postgres. |
| `database` | Database name on the server. |
| `replication` | `none`, `logical`, or `physical`. |
| `state` | `active`, `idle`, `used`, `tested`, `new`, `active_cancel`, `being_canceled`. |
| `addr` | Postgres server IP. |
| `port` | Postgres server port. |
| `local_addr` | Local end of the TCP connection. |
| `local_port` | Local port. |
| `connect_time` | When the server connection was opened. |
| `request_time` | Timestamp of the last request on this link. |
| `wait` | Unused for servers. |
| `wait_us` | Unused for servers. |
| `close_needed` | `1` if the connection will close soon (config/DNS change or `RECONNECT`). |
| `ptr` | Opaque internal object address. |
| `link` | Address of the paired **client** connection, if any. |
| `remote_pid` | Backend PID when available (OS or cancel packet). |
| `tls` | TLS details string, or empty. |
| `application_name` | `application_name` from the linked client, if set. |
| `prepared_statements` | Prepared statements on this server link (`max_prepared_statements` cap). |
| `id` | Unique server connection id. |

---

### `SHOW STATS`

One row per **database**. Cumulative totals since process start plus **averages over the last `stats_period`** (suffix `avg_*`).

| Column | Meaning |
|--------|---------|
| `database` | Database name. |
| `total_xact_count` | Transactions pooled. |
| `total_query_count` | Queries/commands pooled. |
| `total_server_assignment_count` | Times a server was assigned to a client. |
| `total_received` | Bytes received from clients. |
| `total_sent` | Bytes sent to clients. |
| `total_xact_time` | µs connected to Postgres while in a transaction (idle-in-txn + executing). |
| `total_query_time` | µs actively executing queries on the server. |
| `total_wait_time` | µs clients spent waiting for a server assignment. |
| `total_client_parse_count` | Client prepared statements created (named PS mode). |
| `total_server_parse_count` | Server prepared statements created by PgBouncer. |
| `total_bind_count` | Binds forwarded to Postgres. |
| `total_client_login_count` | Successful client logins. |
| `avg_xact_count` | Transactions/s in last stats period. |
| `avg_query_count` | Queries/s in last stats period. |
| `avg_server_assignment_count` | Server assignments/s. |
| `avg_recv` | Bytes received/s from clients. |
| `avg_sent` | Bytes sent/s to clients. |
| `avg_xact_time` | Average transaction duration (µs). |
| `avg_query_time` | Average query duration (µs). |
| `avg_wait_time` | Average client wait for a server (µs). |
| `avg_client_parse_count` | Avg client parses/s (named PS mode). |
| `avg_server_parse_count` | Avg server parses/s. |
| `avg_bind_count` | Avg binds/s. |
| `avg_client_login_count` | Avg logins/s. |

---

### `SHOW STATS_TOTALS` / `SHOW STATS_AVERAGES` / `SHOW TOTALS`

| Command | What it returns |
|---------|-----------------|
| `SHOW STATS_TOTALS` | Same rows as `SHOW STATS`, **only `total_*` columns**. |
| `SHOW STATS_AVERAGES` | Same rows, **only `avg_*` columns**. |
| `SHOW TOTALS` | Like `SHOW STATS` but **one row aggregated across all databases** (no per-DB breakdown). |

---

### `SHOW LISTS`

Single result row, **one column per metric** (not normalized rows). Internal inventory counts.

| Column | Meaning |
|--------|---------|
| `databases` | Configured databases. |
| `users` | Known users. |
| `pools` | Pool entries. |
| `free_clients` | Disconnected client structs kept for reuse. |
| `used_clients` | Active client structs. |
| `login_clients` | Clients in login handshake. |
| `free_servers` | Disconnected server structs kept for reuse. |
| `used_servers` | Active server structs. |
| `dns_names` | Hostnames in DNS cache. |
| `dns_zones` | Zones in DNS cache. |
| `dns_queries` | In-flight DNS lookups. |
| `dns_pending` | Reserved (not used). |

---

### `SHOW CONFIG`

One row per **configuration parameter** (effective config at runtime).

| Column | Meaning |
|--------|---------|
| `key` | Setting name (e.g. `pool_mode`, `max_client_conn`). |
| `value` | Current value. |
| `default` | Built-in default. |
| `changeable` | `yes` if `SET` can change at runtime; `no` if restart required. |

---

### `SHOW MEM`

Low-level **allocator / memory pool sizes** (layout can change between PgBouncer versions). Use for debugging memory growth, not a stable API. Run on the admin DB: `SHOW MEM;`.

---

### `SHOW FDS`

Lists **open file descriptors** the process is using (sockets, etc.). Layout is version-specific; used to debug fd leaks and high connection counts. Not documented column-by-column in the user guide; treat as diagnostic output.

---

### `SHOW DNS_HOSTS`

Rows from the **DNS cache** (multi-host / `load_balance_hosts`).

| Column | Meaning |
|--------|---------|
| `hostname` | Cached hostname. |
| `ttl` | Seconds until next lookup. |
| `addrs` | Comma-separated resolved addresses. |

---

### `SHOW DNS_ZONES`

DNS **zone** cache metadata.

| Column | Meaning |
|--------|---------|
| `zonename` | Zone name. |
| `serial` | Current serial. |
| `count` | Hostnames in this zone. |

---

### `SHOW PEERS`

Configured **peer** poolers (PgBouncer clustering). One row per peer entry.

| Column | Meaning |
|--------|---------|
| `peer_id` | Peer id from config. |
| `host` | Peer host. |
| `port` | Peer port. |
| `pool_size` | Max server connections to this peer. |

---

### `SHOW PEER_POOLS`

Cancel/query forwarding pools toward **peers** (not local Postgres). One row per peer.

| Column | Meaning |
|--------|---------|
| `peer_id` | Peer id. |
| `cl_active_cancel_req` | Clients waiting on cancel forwarded to peer. |
| `cl_waiting_cancel_req` | Clients not yet forwarded cancel. |
| `sv_active_cancel` | Server-side cancel in flight. |
| `sv_login` | Server connections logging in to peer. |

---

### `SHOW STATE`

Global process **run mode** (single value / small row set).

| Value | Meaning |
|-------|---------|
| `active` | Normal operation. |
| `paused` | After `PAUSE` (or equivalent); new client work restricted per pause scope. |

(Exact columns depend on version; output is the high-level state name.)

---

### Other PgBouncer `SHOW` commands (also not in PgGateway)

| Command | What it shows |
|---------|----------------|
| `SHOW USERS` | Per auth user: `name`, `pool_size`, `reserve_pool_size`, `pool_mode` overrides, `max_user_connections`, `current_connections`, `max_user_client_connections`, `current_client_connections`. |
| `SHOW SOCKETS` | All sockets: superset of client/server socket details. |
| `SHOW ACTIVE_SOCKETS` | Same as `SHOW SOCKETS`, only active connections. |
| `SHOW HELP` | Lists available console commands (admin + control). |

---

## Operational commands (PgBouncer only)

PgGateway admin does **not** support: `PAUSE`, `RESUME`, `DISABLE`, `ENABLE`, `RECONNECT`, `KILL`, `KILL_CLIENT`, `RELOAD`, `WAIT_CLOSE`, `SHUTDOWN`, `SET`, etc.

## Protocol

| | PgBouncer | PgGateway |
|---|-----------|-----------|
| Admin DB name | `pgbouncer` | `pg_gateway` (configurable `admin_database`) |
| Simple query | Yes | Yes |
| Extended query | Limited | **Admin: simple only** |
| Writes on admin | Control commands | **Read-only** (`SELECT` / `WITH` / `SHOW`) |

See also [admin-console.md](admin-console.md) for PgGateway connection examples and SQL equivalents.