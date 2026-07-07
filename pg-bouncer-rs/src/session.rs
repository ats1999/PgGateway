use crate::config::{Config, PoolMode};
use crate::pool::{ConnectionPool, PoolKey};
use crate::protocol::{
    self, auth_ok, backend_key_data, is_begin_query, is_commit_or_rollback, is_query, is_terminate,
    parameter_status, read_message, ready_for_query_idle, ready_for_query_status, write_message,
};
use crate::stats::StatsHandle;
use anyhow::Result;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::net::TcpStream;

pub async fn handle_client(
    mut client: TcpStream,
    pool: Arc<ConnectionPool>,
    cfg: Arc<Config>,
    stats: StatsHandle,
    client_id: u64,
) -> Result<()> {
    stats
        .global
        .client_connections
        .fetch_add(1, Ordering::Relaxed);

    let result = handle_client_inner(&mut client, pool.clone(), cfg, stats.clone(), client_id).await;
    stats
        .global
        .client_connections
        .fetch_sub(1, Ordering::Relaxed);
    result
}

async fn handle_client_inner(
    client: &mut TcpStream,
    pool: Arc<ConnectionPool>,
    cfg: Arc<Config>,
    stats: StatsHandle,
    client_id: u64,
) -> Result<()> {
    let startup = protocol::read_startup(client).await?;

    if startup.database() == cfg.admin_database {
        return crate::admin::serve_admin(client, &cfg, stats, client_id).await;
    }

    if stats.server_state.is_paused() {
        write_message(client, &protocol::error_response("pooler is paused")).await?;
        return Ok(());
    }

    write_message(client, &auth_ok()).await?;
    write_message(client, &parameter_status("server_version", "14.0")).await?;
    write_message(
        client,
        &parameter_status("client_encoding", "UTF8"),
    )
    .await?;
    write_message(client, &backend_key_data(client_id as i32, client_id as i32)).await?;
    write_message(client, &ready_for_query_idle()).await?;

    let key = PoolKey::from_startup(&startup);
    let mut backend = if cfg.pool_mode == PoolMode::Session {
        Some(pool.acquire(&key, &startup).await?)
    } else {
        None
    };
    let mut explicit_tx = false;

    loop {
        let Some(client_msg) = read_message(client).await? else {
            break;
        };

        if is_terminate(&client_msg) {
            break;
        }

        if backend.is_none() {
            backend = Some(pool.acquire(&key, &startup).await?);
        }
        let conn = backend.as_mut().expect("backend acquired");

        if is_query(&client_msg) {
            stats.global.queries.fetch_add(1, Ordering::Relaxed);
            if is_begin_query(&client_msg) {
                explicit_tx = true;
                stats.global.transactions.fetch_add(1, Ordering::Relaxed);
            }
            if is_commit_or_rollback(&client_msg) {
                explicit_tx = false;
            }
        }

        write_message(&mut conn.stream, &client_msg).await?;

        loop {
            let Some(server_msg) = read_message(&mut conn.stream).await? else {
                anyhow::bail!("backend closed");
            };
            write_message(client, &server_msg).await?;

            if server_msg.tag == b'Z' {
                let status = ready_for_query_status(&server_msg.body);
                conn.in_transaction = matches!(status, Some(b'T' | b'E')) || explicit_tx;

                if should_release(pool.pool_mode(), status, explicit_tx) {
                    if let Some(released) = backend.take() {
                        pool.release(&key, released, false).await;
                    }
                    explicit_tx = false;
                }
                break;
            }
        }
    }

    if let Some(conn) = backend.take() {
        pool.release(&key, conn, false).await;
    }
    Ok(())
}

fn should_release(mode: PoolMode, status: Option<u8>, explicit_tx: bool) -> bool {
    match mode {
        PoolMode::Session => false,
        PoolMode::Transaction => status == Some(b'I') && !explicit_tx,
        PoolMode::Statement => status == Some(b'I'),
    }
}
