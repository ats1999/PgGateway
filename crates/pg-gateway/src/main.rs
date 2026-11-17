use std::fs::File;
use std::sync::Mutex;

use pg_gateway::{Gateway, GatewayConfig};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info"));

    if let Ok(path) = std::env::var("PG_GATEWAY_LOG_FILE") {
        let file = File::create(&path)?;
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(Mutex::new(file))
            .with_ansi(false)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let config = GatewayConfig::load()?;
    info!(
        listen = %config.listen,
        databases = config.databases.len(),
        "starting pg-gateway"
    );

    let gateway = Gateway::new(config)?;
    gateway.run().await
}