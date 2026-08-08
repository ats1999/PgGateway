use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Context};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use crate::config::GatewayConfig;
use crate::connection::{ClientConnection, ServerConnection};
use crate::pool::PoolManager;

/// Running gateway instance (library entry point).
pub struct Gateway {
    config: Arc<GatewayConfig>,
    pools: Arc<PoolManager>,
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        let config = Arc::new(config);
        let pools = PoolManager::new(Arc::clone(&config));
        Ok(Arc::new(Self { config, pools }))
    }

    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    pub fn pools(&self) -> &Arc<PoolManager> {
        &self.pools
    }

    pub async fn run(self: &Arc<Self>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.listen)
            .await
            .with_context(|| format!("bind {}", self.config.listen))?;
        let database_names: Vec<_> = self.config.databases.keys().cloned().collect();
        info!(
            listen = %self.config.listen,
            ?database_names,
            users = self.config.users.len(),
            "pg-gateway listening"
        );

        loop {
            let (client, peer) = listener.accept().await.context("accept")?;
            let gateway = Arc::clone(self);
            tokio::spawn(async move {
                if let Err(error) = gateway.serve(client, peer).await {
                    warn!(%peer, "{error:#}");
                }
            });
        }
    }

    pub async fn serve(&self, stream: TcpStream, peer: SocketAddr) -> anyhow::Result<()> {
        serve_connection(stream, peer, self).await
    }
}

pub async fn serve_connection(
    stream: TcpStream,
    peer: SocketAddr,
    gateway: &Gateway,
) -> anyhow::Result<()> {
    let mut client = ClientConnection::new(stream, peer);
    let identity = client.complete_startup().await.context("client startup")?;

    if !gateway.config.allows_client(&identity.user, &identity.database) {
        bail!(
            "user `{}` is not allowed for database `{}`",
            identity.user,
            identity.database
        );
    }

    let primary = gateway
        .config
        .primary_upstream(&identity.database)
        .with_context(|| format!("resolve primary for database `{}`", identity.database))?;

    let (upstream, checkout) = if let Some(mut checkout) = gateway
        .pools
        .try_acquire_idle(&identity.user, &identity.database)
        .await?
    {
        client
            .replay_auth_flight(checkout.auth_flight())
            .await?;
        let stream = checkout.take_stream();
        (stream, checkout)
    } else {
        let mut upstream = ServerConnection::connect_pass_through(&primary, identity.raw.as_ref())
            .await?
            .into_tcp();
        let auth_flight = client.authenticate_upstream(&mut upstream).await?;
        let mut checkout = gateway
            .pools
            .attach_checked_out(
                &identity.user,
                &identity.database,
                auth_flight,
                upstream,
            )
            .await?;
        let stream = checkout.take_stream();
        (stream, checkout)
    };

    let upstream = client.relay_with_upstream(upstream).await?;

    checkout.release(upstream).await;

    Ok(())
}
