use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, Context};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::config::GatewayConfig;
use crate::connection::{ClientConnection, ServerConnection};
use crate::pool::{ConnectionState, PoolManager, PooledServerConnection};
use crate::statement_detector::{detect_transaction_status, TransactionStatus};
use pg_protocol::{read_backend, write_backend, ProtocolError};

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
        info!(
            listen = %self.config.listen,
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

/// Relay client↔server traffic while tracking transaction state via protocol-level detection.
///
/// Monitors ReadyForQuery (Z) messages from server to detect transaction status:
/// - 'I' (Idle) → ConnectionState::Idle
/// - 'T' (In Transaction) → ConnectionState::InTransaction
/// - 'E' (Failed Transaction) → ConnectionState::InTransaction (keep pinned)
async fn relay_with_transaction_tracking(
    client: ClientConnection,
    upstream: TcpStream,
    checkout: &mut PooledServerConnection,
) -> anyhow::Result<TcpStream> {
    use pg_protocol::{read_frontend, write_frontend};

    let (client_read, client_write) = client.into_split();
    let (server_read, server_write) = upstream.into_split();

    let mut client_read = client_read;
    let mut client_write = client_write;
    let mut server_read = server_read;
    let mut server_write = server_write;

    loop {
        tokio::select! {
            // Client to server
            client_result = async {
                match read_frontend(&mut client_read).await {
                    Ok(msg) => {
                        write_frontend(&mut server_write, &msg).await?;
                        if msg.tag() == b'X' {
                            Ok::<_, anyhow::Error>(Some(()))  // Terminate
                        } else {
                            Ok(None)
                        }
                    }
                    Err(ProtocolError::UnexpectedEof) => Ok(Some(())),
                    Err(e) => Err(e.into()),
                }
            } => {
                if client_result?.is_some() {
                    let _ = server_write.shutdown().await;
                    break;
                }
            }

            // Server to client - detect transaction status from ReadyForQuery
            server_result = async {
                match read_backend(&mut server_read).await {
                    Ok(msg) => {
                        // Track transaction status from ReadyForQuery messages
                        if msg.tag() == b'Z' {
                            if let Some(status) = detect_transaction_status(&msg.raw) {
                                let new_state = match status {
                                    TransactionStatus::Idle => ConnectionState::Idle,
                                    TransactionStatus::InTransaction => ConnectionState::InTransaction,
                                    TransactionStatus::FailedTransaction => ConnectionState::InTransaction,
                                };

                                if new_state != checkout.state {
                                    debug!(
                                        from = ?checkout.state,
                                        to = ?new_state,
                                        "connection state changed"
                                    );
                                    checkout.state = new_state;
                                }
                            }
                        }

                        write_backend(&mut client_write, &msg).await?;
                        Ok::<_, anyhow::Error>(false)
                    }
                    Err(ProtocolError::UnexpectedEof) => Ok(true),
                    Err(e) => Err(e.into()),
                }
            } => {
                if server_result? {
                    let _ = client_write.shutdown().await;
                    break;
                }
            }
        }
    }

    server_read
        .reunite(server_write)
        .map_err(|_| anyhow::anyhow!("reunite upstream after relay"))
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

    let (upstream, mut checkout) = if let Some(mut checkout) = gateway
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

    let upstream = relay_with_transaction_tracking(client, upstream, &mut checkout).await?;
    checkout.release(upstream).await;
    Ok(())
}
