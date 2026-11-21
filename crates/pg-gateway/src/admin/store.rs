use std::sync::Mutex;
use std::time::Instant;

use anyhow::{bail, Context};
use rusqlite::{Connection, OpenFlags, Row};
use tracing::trace;

use crate::admin::registry::ClientRecord;
use crate::admin::wire::ColumnMeta;
use crate::admin::wire::FieldFormat;
use crate::config::GatewayConfig;
use crate::pool::PoolManager;
use crate::pool_config::PoolSettings;

/// SQLite tables behind the admin database. Leading columns of `databases`, `pools`, and
/// `clients` keep their original order; new columns are appended.
const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS databases (
    name TEXT PRIMARY KEY,
    primary_host TEXT NOT NULL,
    primary_port INTEGER NOT NULL,
    pool_mode TEXT NOT NULL,
    pool_size INTEGER,
    min_pool_size INTEGER,
    max_idle INTEGER,
    max_db_connections INTEGER,
    max_db_client_connections INTEGER,
    server_idle_timeout_seconds INTEGER,
    current_connections INTEGER NOT NULL DEFAULT 0,
    current_client_connections INTEGER NOT NULL DEFAULT 0,
    replica_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS pools (
    database TEXT NOT NULL,
    user TEXT NOT NULL,
    pool_mode TEXT NOT NULL,
    upstream TEXT NOT NULL,
    idle_count INTEGER NOT NULL,
    active_count INTEGER NOT NULL,
    pool_size INTEGER,
    min_pool_size INTEGER,
    max_idle INTEGER,
    max_db_connections INTEGER,
    server_idle_timeout_seconds INTEGER,
    target TEXT NOT NULL,
    connecting_count INTEGER NOT NULL,
    waiting_count INTEGER NOT NULL,
    login_backoff_seconds REAL,
    checkouts INTEGER NOT NULL,
    waits INTEGER NOT NULL,
    wait_timeouts INTEGER NOT NULL,
    connections_opened INTEGER NOT NULL,
    login_failures INTEGER NOT NULL,
    closed_idle_timeout INTEGER NOT NULL,
    closed_max_idle INTEGER NOT NULL,
    closed_check_failed INTEGER NOT NULL,
    closed_reset_failed INTEGER NOT NULL,
    closed_in_transaction INTEGER NOT NULL,
    closed_evicted INTEGER NOT NULL,
    PRIMARY KEY (database, user, target)
);
CREATE TABLE IF NOT EXISTS clients (
    client_id INTEGER PRIMARY KEY,
    peer TEXT NOT NULL,
    user TEXT NOT NULL,
    database TEXT NOT NULL,
    state TEXT NOT NULL,
    connected_at INTEGER NOT NULL,
    state_seconds REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS pool_config (
    database TEXT NOT NULL,
    target TEXT NOT NULL,
    user TEXT NOT NULL,
    setting TEXT NOT NULL,
    value TEXT NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (database, target, user, setting)
);
"#;

pub struct AdminStore {
    conn: Mutex<Connection>,
}

impl AdminStore {
    pub fn new() -> anyhow::Result<Self> {
        let conn = Connection::open_with_flags(
            ":memory:",
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_URI,
        )?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub async fn refresh(
        &self,
        config: &GatewayConfig,
        pools: &PoolManager,
        clients: &[ClientRecord],
    ) -> anyhow::Result<()> {
        let snapshots = pools.admin_pool_snapshots().await;
        let db_totals = pools.admin_database_totals().await;
        let now = Instant::now();

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        for table in ["databases", "pools", "clients", "pool_config"] {
            tx.execute(&format!("DELETE FROM {table}"), [])?;
        }

        for (name, cluster) in config.databases.iter() {
            if name == &config.admin_database {
                continue;
            }
            let pc = config.database_server_settings(name)?;
            let resolved = pc.resolve();
            let current = db_totals.get(name).map(|t| t.current_connections).unwrap_or(0);
            let current_clients = clients.iter().filter(|c| &c.database == name).count();
            tx.execute(
                "INSERT INTO databases (
                    name, primary_host, primary_port, pool_mode,
                    pool_size, min_pool_size, max_idle, max_db_connections,
                    max_db_client_connections, server_idle_timeout_seconds, current_connections,
                    current_client_connections, replica_count
                ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
                rusqlite::params![
                    name,
                    cluster.primary.host,
                    cluster.primary.port,
                    resolved.pool_mode.as_str(),
                    opt_u32(pc.pool_size),
                    opt_u32(pc.min_pool_size),
                    opt_u32(pc.max_idle),
                    opt_u32(pc.max_db_connections),
                    opt_u32(pc.max_db_client_connections),
                    server_idle_seconds(&pc),
                    i64::from(current),
                    current_clients as i64,
                    cluster.replicas.len() as i64,
                ],
            )?;

            // Effective settings with provenance for every instance × configured user.
            for target in cluster.targets() {
                for user in cluster.configured_users() {
                    let sources =
                        config.pool_settings_with_sources(user.unwrap_or(""), name, target)?;
                    for (setting, source) in sources {
                        tx.execute(
                            "INSERT INTO pool_config (database, target, user, setting, value, source)
                             VALUES (?1,?2,?3,?4,?5,?6)",
                            rusqlite::params![
                                name,
                                target.to_string(),
                                user.unwrap_or("*"),
                                setting,
                                source.value,
                                source.source.to_string(),
                            ],
                        )?;
                    }
                }
            }
        }

        for snap in &snapshots {
            let settings = config
                .pool_settings_for(&snap.user, &snap.database, snap.target)
                .unwrap_or_default();
            tx.execute(
                "INSERT INTO pools (
                    database, user, pool_mode, upstream, idle_count, active_count,
                    pool_size, min_pool_size, max_idle, max_db_connections,
                    server_idle_timeout_seconds, target, connecting_count, waiting_count,
                    login_backoff_seconds, checkouts, waits, wait_timeouts, connections_opened,
                    login_failures, closed_idle_timeout, closed_max_idle, closed_check_failed,
                    closed_reset_failed, closed_in_transaction, closed_evicted
                ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,
                          ?21,?22,?23,?24,?25,?26)",
                rusqlite::params![
                    snap.database,
                    snap.user,
                    snap.pool_mode.as_str(),
                    snap.upstream,
                    i64::from(snap.idle_count),
                    i64::from(snap.active_count),
                    opt_u32(settings.pool_size),
                    opt_u32(settings.min_pool_size),
                    opt_u32(settings.max_idle),
                    opt_u32(config
                        .database_server_settings(&snap.database)
                        .ok()
                        .and_then(|d| d.max_db_connections)),
                    server_idle_seconds(&settings),
                    snap.target.to_string(),
                    i64::from(snap.connecting_count),
                    i64::from(snap.waiting_count),
                    snap.login_backoff_seconds,
                    count(snap.checkouts),
                    count(snap.waits),
                    count(snap.wait_timeouts),
                    count(snap.connections_opened),
                    count(snap.login_failures),
                    count(snap.closed_idle_timeout),
                    count(snap.closed_max_idle),
                    count(snap.closed_check_failed),
                    count(snap.closed_reset_failed),
                    count(snap.closed_in_transaction),
                    count(snap.closed_evicted),
                ],
            )?;
        }

        for c in clients {
            tx.execute(
                "INSERT INTO clients (client_id, peer, user, database, state, connected_at,
                                      state_seconds)
                 VALUES (?1,?2,?3,?4,?5,?6,?7)",
                rusqlite::params![
                    c.client_id as i64,
                    c.peer.to_string(),
                    c.user,
                    c.database,
                    c.state.as_str(),
                    c.connected_at as i64,
                    now.saturating_duration_since(c.state_since).as_secs_f64(),
                ],
            )?;
        }
        tx.commit()?;

        trace!(
            databases = config.databases.len(),
            pools = snapshots.len(),
            clients = clients.len(),
            "admin store refreshed"
        );
        Ok(())
    }

    pub fn query(
        &self,
        sql: &str,
    ) -> anyhow::Result<(Vec<ColumnMeta>, Vec<Vec<Option<String>>>)> {
        let sql = normalize_admin_sql(sql);
        if !is_read_only(&sql) {
            bail!("only read-only SQL is allowed on admin database");
        }

        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql).context("prepare admin sql")?;
        let col_count = stmt.column_count();

        let mut columns = Vec::with_capacity(col_count);
        for i in 0..col_count {
            let name = stmt.column_name(i)?.to_string();
            columns.push(ColumnMeta {
                name,
                format: FieldFormat::Text,
            });
        }

        let mut rows = Vec::new();
        let mut rows_cursor = stmt.query([])?;
        while let Some(row) = rows_cursor.next()? {
            rows.push(row_to_cells(row, col_count)?);
        }

        Ok((columns, rows))
    }
}

fn server_idle_seconds(pc: &PoolSettings) -> Option<i64> {
    pc.server_idle_timeout_seconds
        .or(pc.server_lifetime_seconds)
        .and_then(|v| i64::try_from(v).ok())
}

fn opt_u32(value: Option<u32>) -> Option<i64> {
    value.map(i64::from)
}

fn count(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn is_read_only(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("SELECT") || upper.starts_with("WITH") || upper.starts_with("PRAGMA")
}

/// Semver string for `SHOW VERSION` (matches `Cargo.toml` / `CARGO_PKG_VERSION`).
pub fn admin_version_string() -> String {
    format!("PgGateway {}", env!("CARGO_PKG_VERSION"))
}

/// PgBouncer-style `SHOW` → SQLite `SELECT` for psql users.
pub fn normalize_admin_sql(sql: &str) -> String {
    let s = sql.trim().trim_end_matches(';').trim();
    match s.to_ascii_uppercase().as_str() {
        "SHOW POOLS" => "SELECT * FROM pools ORDER BY database, user".to_string(),
        "SHOW CLIENTS" => "SELECT * FROM clients ORDER BY client_id".to_string(),
        "SHOW DATABASES" => "SELECT * FROM databases ORDER BY name".to_string(),
        "SHOW POOL_CONFIG" | "SHOW CONFIG" => {
            "SELECT * FROM pool_config ORDER BY database, target, user, setting".to_string()
        }
        "SHOW VERSION" => {
            let version = admin_version_string().replace('\'', "''");
            format!("SELECT '{version}' AS version")
        }
        _ => s.to_string(),
    }
}

/// Render every SQLite value as text. Reading as `Option<String>` directly fails for INTEGER and
/// REAL columns (rusqlite won't convert them), which used to turn every number into NULL.
fn row_to_cells(row: &Row<'_>, col_count: usize) -> anyhow::Result<Vec<Option<String>>> {
    use rusqlite::types::ValueRef;
    let mut cells = Vec::with_capacity(col_count);
    for i in 0..col_count {
        let cell = match row.get_ref(i)? {
            ValueRef::Null => None,
            ValueRef::Integer(v) => Some(v.to_string()),
            ValueRef::Real(v) => Some(v.to_string()),
            ValueRef::Text(v) | ValueRef::Blob(v) => Some(String::from_utf8_lossy(v).into_owned()),
        };
        cells.push(cell);
    }
    Ok(cells)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn show_pools_maps_to_select() {
        assert_eq!(
            normalize_admin_sql("SHOW POOLS;"),
            "SELECT * FROM pools ORDER BY database, user"
        );
    }

    #[test]
    fn integer_and_real_columns_render_as_text() {
        let store = AdminStore::new().unwrap();
        let (_, rows) = store
            .query("SELECT 42 AS i, 1.5 AS r, 'x' AS t, NULL AS n")
            .unwrap();
        assert_eq!(
            rows,
            vec![vec![Some("42".into()), Some("1.5".into()), Some("x".into()), None]]
        );
    }

    #[test]
    fn show_version_includes_semver() {
        let sql = normalize_admin_sql("SHOW VERSION");
        assert!(sql.contains(env!("CARGO_PKG_VERSION")));
        assert!(sql.contains("PgGateway"));
    }
}