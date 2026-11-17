use std::sync::Mutex;

use anyhow::{bail, Context};
use rusqlite::{Connection, OpenFlags, Row};
use tracing::trace;

use crate::admin::registry::ClientRecord;
use crate::admin::wire::FieldFormat;
use crate::admin::wire::ColumnMeta;
use crate::config::{GatewayConfig, PoolMode};
use crate::pool::PoolManager;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS databases (
    name TEXT PRIMARY KEY,
    primary_host TEXT NOT NULL,
    primary_port INTEGER NOT NULL,
    pool_mode TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pools (
    database TEXT NOT NULL,
    user TEXT NOT NULL,
    pool_mode TEXT NOT NULL,
    upstream TEXT NOT NULL,
    idle_count INTEGER NOT NULL,
    PRIMARY KEY (database, user)
);
CREATE TABLE IF NOT EXISTS clients (
    client_id INTEGER PRIMARY KEY,
    peer TEXT NOT NULL,
    user TEXT NOT NULL,
    database TEXT NOT NULL
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
        let config = config.clone();
        let clients = clients.to_vec();

        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM databases", [])?;
        conn.execute("DELETE FROM pools", [])?;
        conn.execute("DELETE FROM clients", [])?;

        for (name, cluster) in config.databases.iter() {
            if name == &config.admin_database {
                continue;
            }
            conn.execute(
                "INSERT INTO databases (name, primary_host, primary_port, pool_mode) VALUES (?1,?2,?3,?4)",
                rusqlite::params![
                    name,
                    cluster.primary.host,
                    cluster.primary.port,
                    pool_mode_label(cluster.pool_config.pool_mode),
                ],
            )?;
        }

        let pool_rows = snapshots.len();
        for snap in &snapshots {
            conn.execute(
                "INSERT INTO pools (database, user, pool_mode, upstream, idle_count) VALUES (?1,?2,?3,?4,?5)",
                rusqlite::params![
                    snap.database,
                    snap.user,
                    pool_mode_label(snap.pool_mode),
                    snap.upstream,
                    snap.idle_count as i64,
                ],
            )?;
        }

        let client_rows = clients.len();
        for c in &clients {
            conn.execute(
                "INSERT INTO clients (client_id, peer, user, database) VALUES (?1,?2,?3,?4)",
                rusqlite::params![
                    c.client_id as i64,
                    c.peer.to_string(),
                    c.user,
                    c.database
                ],
            )?;
        }

        trace!(
            databases = config.databases.len(),
            pools = pool_rows,
            clients = client_rows,
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

fn pool_mode_label(mode: PoolMode) -> &'static str {
    match mode {
        PoolMode::Session => "session",
        PoolMode::Transaction => "transaction",
        PoolMode::Statement => "statement",
    }
}

fn is_read_only(sql: &str) -> bool {
    let upper = sql.trim().to_ascii_uppercase();
    upper.starts_with("SELECT") || upper.starts_with("WITH") || upper.starts_with("PRAGMA")
}

/// PgBouncer-style `SHOW` → SQLite `SELECT` for psql users.
pub fn normalize_admin_sql(sql: &str) -> String {
    let s = sql.trim().trim_end_matches(';').trim();
    match s.to_ascii_uppercase().as_str() {
        "SHOW POOLS" => "SELECT * FROM pools ORDER BY database, user".to_string(),
        "SHOW CLIENTS" => "SELECT * FROM clients ORDER BY client_id".to_string(),
        "SHOW DATABASES" => "SELECT * FROM databases ORDER BY name".to_string(),
        "SHOW VERSION" => "SELECT 'pg-gateway' AS version".to_string(),
        _ => s.to_string(),
    }
}

fn row_to_cells(row: &Row<'_>, col_count: usize) -> anyhow::Result<Vec<Option<String>>> {
    let mut cells = Vec::with_capacity(col_count);
    for i in 0..col_count {
        let value: Option<String> = row.get(i).ok();
        cells.push(value);
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
}