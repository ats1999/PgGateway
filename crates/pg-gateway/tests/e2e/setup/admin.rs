//! The gateway's admin console (`dbname=pg_gateway`): its own view of pools and clients.

use anyhow::{Context, Result};
use tokio_postgres::{NoTls, SimpleQueryMessage};

use super::gateway::TestGateway;

/// Rows of one admin query (simple protocol only, like PgBouncer's console).
pub async fn rows(gateway: &TestGateway, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
    let (client, connection) = tokio_postgres::connect(&gateway.admin_connection_string(), NoTls)
        .await
        .context("connect to admin console")?;
    let task = tokio::spawn(connection);
    let mut out = Vec::new();
    for message in client.simple_query(sql).await.with_context(|| format!("admin: {sql}"))? {
        if let SimpleQueryMessage::Row(row) = message {
            out.push((0..row.len()).map(|i| row.get(i).map(str::to_string)).collect());
        }
    }
    drop(client);
    task.abort();
    Ok(out)
}

/// Server connections of the `pg_gateway_test` primary pool, as the gateway counts them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolState {
    pub active: i64,
    pub idle: i64,
    pub waiting: i64,
    pub wait_timeouts: i64,
    pub connections_opened: i64,
}

pub async fn pool_state(gateway: &TestGateway) -> Result<PoolState> {
    let rows = rows(
        gateway,
        "SELECT active_count, idle_count, waiting_count, wait_timeouts, connections_opened \
         FROM pools WHERE database = 'pg_gateway_test' AND target = 'primary'",
    )
    .await?;
    let Some(row) = rows.first() else {
        // The pool is created on first use.
        return Ok(PoolState::default());
    };
    let n = |i: usize| -> Result<i64> {
        row[i].as_deref().unwrap_or("0").parse().context("numeric pool column")
    };
    Ok(PoolState {
        active: n(0)?,
        idle: n(1)?,
        waiting: n(2)?,
        wait_timeouts: n(3)?,
        connections_opened: n(4)?,
    })
}

/// States of the clients connected to `pg_gateway_test` (`idle`, `active`, `waiting`,
/// `idle_in_transaction`), sorted.
pub async fn client_states(gateway: &TestGateway) -> Result<Vec<String>> {
    let rows = rows(
        gateway,
        "SELECT state FROM clients WHERE database = 'pg_gateway_test' ORDER BY state",
    )
    .await?;
    Ok(rows.into_iter().filter_map(|r| r.into_iter().next().flatten()).collect())
}
