use pg_gateway::{Gateway, GatewayConfig};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let config = GatewayConfig::load()?;
    info!(
        listen = %config.listen,
        databases = config.databases.len(),
        users = config.users.len(),
        "starting pg-gateway"
    );

    let gateway = Gateway::new(config)?;
    gateway.run().await
}
