use anyhow::{Context, Result};
use clap::Parser;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum PoolMode {
    Session,
    Transaction,
    Statement,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub listen_addr: String,
    pub backend_host: String,
    pub backend_port: u16,
    pub backend_user: String,
    pub backend_password: String,
    pub pool_mode: PoolMode,
    pub default_pool_size: usize,
    pub max_client_conn: usize,
    pub worker_threads: usize,
    pub admin_database: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:6432".into(),
            backend_host: "127.0.0.1".into(),
            backend_port: 5432,
            backend_user: "postgres".into(),
            backend_password: "postgres".into(),
            pool_mode: PoolMode::Transaction,
            default_pool_size: 10,
            max_client_conn: 200,
            worker_threads: num_cpus(),
            admin_database: "pgbouncer".into(),
        }
    }
}

#[derive(Parser, Debug)]
#[command(name = "pg-bouncer-rs", about = "Multi-threaded PostgreSQL pooler")]
pub struct Cli {
    #[arg(long, short = 'c')]
    pub config: Option<PathBuf>,
    #[arg(long)]
    pub listen_addr: Option<String>,
    #[arg(long)]
    pub backend_host: Option<String>,
    #[arg(long)]
    pub backend_port: Option<u16>,
    #[arg(long)]
    pub pool_mode: Option<PoolMode>,
}

impl Config {
    pub fn load(cli: &Cli) -> Result<Self> {
        let mut cfg = if let Some(path) = &cli.config {
            let raw = std::fs::read_to_string(path)
                .with_context(|| format!("read config {}", path.display()))?;
            toml::from_str(&raw).context("parse config toml")?
        } else {
            Config::default()
        };

        if let Some(v) = &cli.listen_addr {
            cfg.listen_addr = v.clone();
        }
        if let Some(v) = &cli.backend_host {
            cfg.backend_host = v.clone();
        }
        if let Some(v) = cli.backend_port {
            cfg.backend_port = v;
        }
        if let Some(v) = cli.pool_mode {
            cfg.pool_mode = v;
        }

        Ok(cfg)
    }
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(2)
}
