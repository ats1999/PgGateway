use pg_gateway::run_listener;
use tracing::info;

const DEFAULT_LISTEN: &str = "127.0.0.1:6432";
const DEFAULT_UPSTREAM: &str = "127.0.0.1:5432";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let listen = std::env::var("PG_GATEWAY_LISTEN").unwrap_or_else(|_| DEFAULT_LISTEN.to_string());
    let upstream =
        std::env::var("PG_GATEWAY_UPSTREAM").unwrap_or_else(|_| DEFAULT_UPSTREAM.to_string());

    info!(%listen, %upstream, "pg-gateway listening");
    run_listener(&listen, &upstream).await
}
