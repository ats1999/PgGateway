# Admin console (`pg_gateway`)

The admin interface is a **virtual database** on the gateway listen address. It does not connect to Postgres upstream; each query refreshes an in-memory snapshot and returns answers over the normal Postgres wire protocol.

## Connect

Default database name: **`pg_gateway`** (override with top-level `admin_database` in HCL).

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway
```

Use a **postgres** user allowed for the logical `postgres` database in your config (`allows_admin_client` checks the `postgres` entry’s `userlist`; empty userlist allows any user).

## Protocol and rules

| Rule | Detail |
|------|--------|
| Simple query only | One `Q` message per statement. Extended query (`Parse` / `Bind` / `Execute`) is rejected. |
| Read-only | `SELECT`, `WITH … SELECT`, and `PRAGMA` only. No `INSERT` / `UPDATE` / `DELETE`. |
| Fresh snapshot | Each admin query reloads `databases`, `pools`, and `clients` from live gateway state before running SQL. |
| No upstream | Admin traffic never opens a pool connection to real Postgres. |

## `SHOW` commands (PgBouncer-style)

These are rewritten to `SELECT` on the admin catalog. Trailing `;` is optional; matching is case-insensitive.

### `SHOW VERSION`

Gateway build identity (fixed string today).

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW VERSION;"
```

| Column | Description |
|--------|-------------|
| `version` | Always `pg-gateway`. |

Equivalent SQL: `SELECT 'pg-gateway' AS version`.

---

### `SHOW DATABASES`

Configured proxy databases (excluding the admin database itself).

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW DATABASES;"
```

| Column | Description |
|--------|-------------|
| `name` | Logical database key from HCL `databases`. |
| `primary_host` | Primary upstream host. |
| `primary_port` | Primary upstream port. |
| `pool_mode` | `session`, `transaction`, or `statement`. |

Equivalent SQL: `SELECT * FROM databases ORDER BY name`.

---

### `SHOW POOLS`

One row per `(database, user)` pool key with current idle upstream count.

```bash
psql -h 127.0.0.1 -p 6432 -U postgres -d pg_gateway -c "SHOW POOLS;"
```

| Column | Description |
|--------|-------------|
| `database` | Logical database name. |
| `user` | Client user for the pool key. |
| `pool_mode` | Pool mode for that database. |
| `upstream` | Primary address (`host:port`). |
| `idle_count` | Upstream connections idle in the pool for this key. |

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
| `database` | Startup `database` parameter (e.g. `postgres`, `pg-gateway`, or `pg_gateway` for admin). |

Equivalent SQL: `SELECT * FROM clients ORDER BY client_id`.

## Direct `SELECT` examples

Any read-only query against the same three logical tables works:

```sql
SELECT name, pool_mode FROM databases ORDER BY name;
SELECT database, user, idle_count FROM pools WHERE database = 'postgres';
SELECT client_id, peer, user, database FROM clients;
```

`SELECT *` on each table matches the corresponding `SHOW` output (modulo column order for `SHOW VERSION`).

## Configuration

In HCL:

```hcl
admin_database = "pg_gateway"   # optional; this is the default

databases = {
  postgres = {
    primary = { host = "127.0.0.1", port = 5432 }
    userlist = [{ name = "postgres" }]   # required for admin if userlist is used
  }
  # ... proxy databases ...
}
```

Do not define a `databases` entry whose `name` equals `admin_database`; it is reserved for the admin console only.

## Errors you may see

| Message | Cause |
|---------|--------|
| `only simple query protocol is supported on admin db` | Client used prepared statements / extended protocol. Use `psql` default or `simple_query`. |
| `only read-only SQL is allowed on admin database` | Non-`SELECT` / non-`WITH` statement. |
| `user … is not allowed for admin database` | `userlist` on `postgres` does not include your user. |