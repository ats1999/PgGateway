mod admin;
mod config;
mod pool;
mod protocol;
mod scram;
mod server;
mod session;
mod stats;

use anyhow::Result;
use clap::Parser;
use config::{Cli, Config};
use pool::ConnectionPool;
use stats::{ServerState, StatsHandle};
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("pg_bouncer_rs=info".parse()?))
        .init();

    let cli = Cli::parse();
    let cfg = Arc::new(Config::load(&cli)?);
    let server_state = ServerState::new();
    let stats = StatsHandle::new(server_state.clone());
    let pool = ConnectionPool::new(cfg.clone(), stats.clone());

    server::run(cfg, pool, stats, server_state).await
}
