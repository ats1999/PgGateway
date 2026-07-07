use crate::config::Config;
use crate::pool::ConnectionPool;
use crate::session;
use crate::stats::{ServerState, StatsHandle};
use anyhow::{Context, Result};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn run(
    cfg: Arc<Config>,
    pool: Arc<ConnectionPool>,
    stats: StatsHandle,
    _server_state: Arc<ServerState>,
) -> Result<()> {
    let listener = TcpListener::bind(&cfg.listen_addr)
        .await
        .with_context(|| format!("bind {}", cfg.listen_addr))?;
    tracing::info!(
        "pg-bouncer-rs listening on {} -> {}:{} mode={:?}",
        cfg.listen_addr,
        cfg.backend_host,
        cfg.backend_port,
        cfg.pool_mode
    );

    let client_seq = AtomicU64::new(1);

    loop {
        let (socket, addr) = listener.accept().await.context("accept")?;
        let pool = pool.clone();
        let cfg = cfg.clone();
        let stats = stats.clone();
        let id = client_seq.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            tracing::debug!("client {id} connected from {addr}");
            if let Err(e) = session::handle_client(socket, pool, cfg, stats, id).await {
                tracing::warn!("client {id} error: {e:#}");
            }
        });
    }
}
