#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use anyhow::Context;

use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_postgres::{NoTls, SimpleQueryMessage};

pub const DEFAULT_PG_PORT: u16 = 5432;
pub const DEFAULT_GATEWAY_PORT: u16 = 6432;

pub struct IntegrationTarget {
    pub gateway_port: u16,
    pub pg_port: u16,
}

pub fn integration_enabled() -> bool {
    std::env::var("PG_GATEWAY_INTEGRATION")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// DB e2e tests always use an out-of-process pg-gateway when `PG_GATEWAY_INTEGRATION` is set.
pub fn external_gateway_required() -> bool {
    integration_enabled()
}

pub fn skip_unless_db_e2e() -> bool {
    if !integration_enabled() {
        eprintln!("skip: run via scripts/run-integration-tests.sh (Docker)");
        return false;
    }
    true
}

pub fn pg_port() -> u16 {
    std::env::var("PG_GATEWAY_TEST_PG_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_PG_PORT)
}

pub fn gateway_port() -> u16 {
    std::env::var("PG_GATEWAY_TEST_LISTEN_PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(DEFAULT_GATEWAY_PORT)
}

fn gateway_host() -> String {
    std::env::var("PG_GATEWAY_TEST_LISTEN_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn restart_script() -> PathBuf {
    std::env::var("PG_GATEWAY_RESTART_SCRIPT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/app/docker/integration-restart-gateway.sh"))
}

/// Extra pool settings rendered into the integration config template, one `key = value` per
/// line, for `pool_defaults`, the `pg-gateway` database, and its `postgres` userlist entry.
#[derive(Default, Clone, Debug)]
pub struct PoolOverrides {
    pub defaults: String,
    pub database: String,
    pub user: String,
}

impl PoolOverrides {
    pub fn database(lines: &str) -> Self {
        Self {
            database: lines.to_string(),
            ..Self::default()
        }
    }
}

/// Restart the pg-gateway OS process with the given pool mode (see `integration-restart-gateway.sh`).
pub fn attach_gateway(pool_mode: &str) -> anyhow::Result<IntegrationTarget> {
    attach_gateway_with(pool_mode, &PoolOverrides::default())
}

/// Restart pg-gateway with extra pool settings.
pub fn attach_gateway_with(
    pool_mode: &str,
    overrides: &PoolOverrides,
) -> anyhow::Result<IntegrationTarget> {
    if !integration_enabled() {
        anyhow::bail!("PG_GATEWAY_INTEGRATION is not set");
    }

    let script = restart_script();
    let status = Command::new(&script)
        .arg(pool_mode)
        .env("DEFAULTS_POOL_CONFIG", &overrides.defaults)
        .env("DB_POOL_CONFIG", &overrides.database)
        .env("USER_POOL_CONFIG", &overrides.user)
        .status()
        .with_context(|| format!("run {}", script.display()))?;
    if !status.success() {
        anyhow::bail!("{} failed with {}", script.display(), status);
    }

    Ok(IntegrationTarget {
        gateway_port: gateway_port(),
        pg_port: pg_port(),
    })
}

impl IntegrationTarget {
    pub fn proxy_dsn(&self) -> String {
        format!(
            "host={} port={} user=postgres password=postgres dbname=pg-gateway",
            gateway_host(),
            self.gateway_port
        )
    }

    pub fn admin_dsn(&self) -> String {
        format!(
            "host={} port={} user=postgres password=postgres dbname=pg_gateway",
            gateway_host(),
            self.gateway_port
        )
    }
}

async fn wait_for_tcp(addr: SocketAddr) -> anyhow::Result<()> {
    let deadline = Duration::from_secs(30);
    timeout(deadline, async {
        loop {
            if TcpStream::connect(addr).await.is_ok() {
                return Ok(());
            }
            sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for {addr}"))?
}

pub async fn connect_proxy(
    dsn: &str,
) -> anyhow::Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let handle = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("proxy connection: {e}");
        }
    });
    Ok((client, handle))
}

/// Like [`connect_proxy`], but the returned task yields the connection's own error — that is
/// where gateway-sent FATAL errors (timeouts, limits) surface.
pub async fn connect_capturing(
    dsn: &str,
) -> Result<
    (
        tokio_postgres::Client,
        tokio::task::JoinHandle<Result<(), tokio_postgres::Error>>,
    ),
    tokio_postgres::Error,
> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    Ok((client, tokio::spawn(connection)))
}

/// Server-side message of a tokio-postgres error (`FATAL` text sent by the gateway).
pub fn error_text(error: &tokio_postgres::Error) -> String {
    match error.as_db_error() {
        Some(db) => format!("{} {}", db.code().code(), db.message()),
        None => error.to_string(),
    }
}

/// Direct connection to Postgres, bypassing the gateway.
pub fn direct_dsn(target: &IntegrationTarget) -> String {
    format!(
        "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
        target.pg_port
    )
}

/// One integer cell from the admin database.
pub async fn admin_scalar_i64(target: &IntegrationTarget, sql: &str) -> anyhow::Result<i64> {
    let rows = admin_query_rows(&target.admin_dsn(), sql).await?;
    let cell = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|c| c.clone())
        .ok_or_else(|| anyhow::anyhow!("no value for `{sql}`: {rows:?}"))?;
    Ok(cell.parse()?)
}

pub async fn admin_query_rows(
    dsn: &str,
    sql: &str,
) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    let (client, _connection_task) = connect_proxy(dsn).await?;
    simple_query_rows(&client, sql).await
}

pub async fn simple_query_rows(
    client: &tokio_postgres::Client,
    sql: &str,
) -> anyhow::Result<Vec<Vec<Option<String>>>> {
    let mut rows = Vec::new();
    for msg in client.simple_query(sql).await? {
        if let SimpleQueryMessage::Row(row) = msg {
            let mut cells = Vec::with_capacity(row.len());
            for i in 0..row.len() {
                cells.push(row.get(i).map(|cell| cell.to_string()));
            }
            rows.push(cells);
        }
    }
    Ok(rows)
}

pub async fn simple_query_one_i32(
    client: &tokio_postgres::Client,
    sql: &str,
) -> anyhow::Result<i32> {
    let rows = simple_query_rows(client, sql).await?;
    let first = rows
        .first()
        .and_then(|r| r.first())
        .and_then(|c| c.as_deref())
        .ok_or_else(|| anyhow::anyhow!("no scalar in simple query result"))?;
    Ok(first.parse()?)
}

pub async fn simple_query_one_string(
    client: &tokio_postgres::Client,
    sql: &str,
) -> anyhow::Result<String> {
    let rows = simple_query_rows(client, sql).await?;
    rows.first()
        .and_then(|r| r.first())
        .and_then(|c| c.clone())
        .ok_or_else(|| anyhow::anyhow!("no string in simple query result"))
}