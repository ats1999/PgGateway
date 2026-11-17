use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context};
use tokio::io::AsyncWriteExt;
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tracing::{info, trace, warn};

use crate::admin::{serve_admin_connection, AdminStore, ClientRegistry};
use crate::config::{GatewayConfig, PoolMode};
use crate::connection::{ClientConnection, ServerConnection, StartupIdentity};
use crate::pool::{ConnectionState, PoolManager, PooledServerConnection};
use crate::statement_detector::{detect_transaction_status, TransactionStatus};
use pg_protocol::{read_backend, write_backend, ProtocolError};

/// Running gateway instance (library entry point).
pub struct Gateway {
    config: Arc<GatewayConfig>,
    pools: Arc<PoolManager>,
    admin: Arc<AdminStore>,
    clients: Arc<ClientRegistry>,
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> anyhow::Result<Arc<Self>> {
        config.validate()?;
        let config = Arc::new(config);
        let pools = PoolManager::new(Arc::clone(&config));
        let admin = Arc::new(AdminStore::new()?);
        let clients = ClientRegistry::new();
        Ok(Arc::new(Self {
            config,
            pools,
            admin,
            clients,
        }))
    }

    pub fn config(&self) -> &GatewayConfig {
        &self.config
    }

    pub fn pools(&self) -> &Arc<PoolManager> {
        &self.pools
    }

    pub fn admin_store(&self) -> &AdminStore {
        &self.admin
    }

    pub fn clients(&self) -> &ClientRegistry {
        &self.clients
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

/// Why the relay loop stopped — client hung up or we put the upstream back early.
enum RelayOutcome {
    ClientDone(TcpStream),
    ReleaseToPool(TcpStream),
}

/// One iteration of reading from Postgres; pool release happens outside the select arm.
enum ServerTick {
    Continue,
    ReleaseAfterForward,
    ServerEof,
}

/// Client socket shape: whole connection (session mode) or already split for the txn/statement loop.
enum ClientEndpoint<'a> {
    Whole(&'a mut ClientConnection),
    Split(&'a mut OwnedReadHalf, &'a mut OwnedWriteHalf),
}

/// Proxy bytes both ways and watch `ReadyForQuery` so the pool knows txn state.
/// Does not forward client `Terminate` so we can reuse the upstream socket.
async fn relay_with_transaction_tracking(
    client_read: &mut OwnedReadHalf,
    client_write: &mut OwnedWriteHalf,
    upstream: TcpStream,
    checkout: &mut PooledServerConnection,
    pool_mode: PoolMode,
) -> anyhow::Result<RelayOutcome> {
    use pg_protocol::{read_frontend, write_frontend};

    let (server_read, server_write) = upstream.into_split();

    let mut server_read = server_read;
    let mut server_write = server_write;
    let client_sent_since_checkout = AtomicBool::new(false);

    loop {
        tokio::select! {
            client_result = async {
                match read_frontend(client_read).await {
                    Ok(msg) => {
                        if msg.tag() == b'X' {
                            Ok::<_, anyhow::Error>(Some(()))
                        } else {
                            client_sent_since_checkout.store(true, Ordering::Relaxed);
                            write_frontend(&mut server_write, &msg).await?;
                            Ok(None)
                        }
                    }
                    Err(ProtocolError::UnexpectedEof) => Ok(Some(())),
                    Err(e) => Err(e.into()),
                }
            } => {
                if client_result?.is_some() {
                    break;
                }
            }

            server_tick = async {
                match read_backend(&mut server_read).await {
                    Ok(msg) => {
                        let mut release_after_forward = false;
                        if msg.tag() == b'Z' {
                            if let Some(status) = detect_transaction_status(&msg.raw) {
                                let new_state = match status {
                                    TransactionStatus::Idle => ConnectionState::Idle,
                                    TransactionStatus::InTransaction => ConnectionState::InTransaction,
                                    TransactionStatus::FailedTransaction => ConnectionState::InTransaction,
                                };

                                if new_state != checkout.state {
                                    trace!(
                                        event = "pool_conn_state_change",
                                        from = ?checkout.state,
                                        to = ?new_state,
                                        txn_status = ?status,
                                        "ReadyForQuery"
                                    );
                                    checkout.state = new_state;
                                }

                                release_after_forward = should_release_upstream(
                                    pool_mode,
                                    status,
                                    client_sent_since_checkout.load(Ordering::Relaxed),
                                );
                                trace!(
                                    event = "pool_release_decision",
                                    txn_status = ?status,
                                    pool_mode = ?pool_mode,
                                    release_after_forward,
                                    conn_state = ?checkout.state,
                                );
                            }
                        }

                        write_backend(client_write, &msg).await?;

                        Ok::<_, anyhow::Error>(if release_after_forward {
                            ServerTick::ReleaseAfterForward
                        } else {
                            ServerTick::Continue
                        })
                    }
                    Err(ProtocolError::UnexpectedEof) => {
                        let _ = client_write.shutdown().await;
                        Ok(ServerTick::ServerEof)
                    }
                    Err(e) => Err(e.into()),
                }
            } => {
                match server_tick? {
                    ServerTick::Continue => {}
                    ServerTick::ReleaseAfterForward => {
                        trace!(
                            event = "pool_release_mid_session",
                            pool_mode = ?pool_mode,
                            conn_state = ?checkout.state,
                            "upstream returned after query/transaction idle"
                        );
                        let upstream = server_read
                            .reunite(server_write)
                            .map_err(|_| anyhow::anyhow!("reunite upstream after pool release"))?;
                        return Ok(RelayOutcome::ReleaseToPool(upstream));
                    }
                    ServerTick::ServerEof => {
                        let upstream = server_read
                            .reunite(server_write)
                            .map_err(|_| anyhow::anyhow!("reunite upstream after server eof"))?;
                        return Ok(RelayOutcome::ClientDone(upstream));
                    }
                }
            }
        }
    }

    let upstream = server_read
        .reunite(server_write)
        .map_err(|_| anyhow::anyhow!("reunite upstream after relay"))?;
    Ok(RelayOutcome::ClientDone(upstream))
}

/// PgBouncer-style release: transaction/statement modes drop the server link on idle `Z`
/// after the client has actually sent a query (skip auth's first `Z`).
fn should_release_upstream(
    pool_mode: PoolMode,
    status: TransactionStatus,
    client_sent_since_checkout: bool,
) -> bool {
    if !client_sent_since_checkout {
        return false;
    }
    match pool_mode {
        PoolMode::Session => false,
        PoolMode::Statement | PoolMode::Transaction => {
            matches!(status, TransactionStatus::Idle)
        }
    }
}

/// Grab an idle upstream or open a new one.
/// `send_auth_to_client` is only true on the first leg of a multiplexed session — after that
/// the client is already past auth and must not get a replayed auth flight.
async fn checkout_upstream(
    client: ClientEndpoint<'_>,
    gateway: &Gateway,
    identity: &StartupIdentity,
    primary: &str,
    send_auth_to_client: bool,
) -> anyhow::Result<(TcpStream, PooledServerConnection)> {
    if let Some(mut checkout) = gateway
        .pools
        .try_acquire_idle(&identity.user, &identity.database)
        .await?
    {
        trace!(
            event = "pool_checkout",
            user = %identity.user,
            database = %identity.database,
            source = "idle_pool",
            send_auth_to_client,
        );
        if send_auth_to_client {
            match client {
                ClientEndpoint::Whole(c) => c.replay_auth_flight(checkout.auth_flight()).await?,
                ClientEndpoint::Split(_, client_write) => {
                    ClientConnection::replay_auth_flight_to_client_write(
                        client_write,
                        checkout.auth_flight(),
                    )
                    .await?;
                }
            }
        }
        let stream = checkout.take_stream();
        return Ok((stream, checkout));
    }

    trace!(
        event = "pool_checkout",
        user = %identity.user,
        database = %identity.database,
        source = "new_connect",
        send_auth_to_client,
        primary,
    );
    if send_auth_to_client {
        let upstream =
            ServerConnection::connect_pass_through(primary, identity.raw.as_ref()).await?;
        let mut upstream_tcp = upstream.into_tcp();

        let auth_flight = match client {
            ClientEndpoint::Whole(c) => c.authenticate_upstream(&mut upstream_tcp).await?,
            ClientEndpoint::Split(client_read, client_write) => {
                ClientConnection::authenticate_upstream_halves(
                    client_read,
                    client_write,
                    &mut upstream_tcp,
                )
                .await?
            }
        };

        let mut checkout = gateway
            .pools
            .attach_checked_out(
                &identity.user,
                &identity.database,
                auth_flight,
                upstream_tcp,
            )
            .await?;

        let stream = checkout.take_stream();
        return Ok((stream, checkout));
    }

    let (server, auth_flight) = ServerConnection::connect_and_authenticate(
        primary,
        &identity.user,
        &identity.database,
    )
    .await?;

    let mut checkout = gateway
        .pools
        .attach_checked_out(
            &identity.user,
            &identity.database,
            auth_flight,
            server.into_tcp(),
        )
        .await?;

    let stream = checkout.take_stream();
    Ok((stream, checkout))
}

pub async fn serve_connection(
    stream: TcpStream,
    peer: SocketAddr,
    gateway: &Gateway,
) -> anyhow::Result<()> {
    let mut client = ClientConnection::new(stream, peer);
    let identity = client.complete_startup().await.context("client startup")?;

    if gateway.config.is_admin_database(&identity.database) {
        if !gateway.config.allows_admin_client(&identity.user) {
            bail!(
                "user `{}` is not allowed for admin database",
                identity.user
            );
        }
        let client_guard = gateway
            .clients
            .register(
                peer,
                identity.user.clone(),
                identity.database.clone(),
            )
            .await;
        let res =
            serve_admin_connection(client, peer, gateway, identity, client_guard).await;
        return res;
    }

    if !gateway.config.allows_client(&identity.user, &identity.database) {
        bail!(
            "user `{}` is not allowed for database `{}`",
            identity.user,
            identity.database
        );
    }

    let client_guard = gateway
        .clients
        .register(
            peer,
            identity.user.clone(),
            identity.database.clone(),
        )
        .await;

    let primary = gateway
        .config
        .primary_upstream(&identity.database)
        .with_context(|| format!("resolve primary for database `{}`", identity.database))?;

    let pool_mode = gateway
        .config
        .cluster(&identity.database)?
        .pool_config
        .pool_mode;

    match pool_mode {
        PoolMode::Session => {
            let (upstream, mut checkout) = checkout_upstream(
                ClientEndpoint::Whole(&mut client),
                gateway,
                &identity,
                &primary,
                true,
            )
            .await?;
            let (mut client_read, mut client_write) = client.into_split();
            match relay_with_transaction_tracking(
                &mut client_read,
                &mut client_write,
                upstream,
                &mut checkout,
                pool_mode,
            )
            .await?
            {
                RelayOutcome::ClientDone(stream) | RelayOutcome::ReleaseToPool(stream) => {
                    checkout.release(stream).await;
                }
            }
        }
        PoolMode::Transaction | PoolMode::Statement => {
            let (mut client_read, mut client_write) = client.into_split();
            let mut send_auth_to_client = true;

            loop {
                let (upstream, mut checkout) = checkout_upstream(
                    ClientEndpoint::Split(&mut client_read, &mut client_write),
                    gateway,
                    &identity,
                    &primary,
                    send_auth_to_client,
                )
                .await?;
                send_auth_to_client = false;

                match relay_with_transaction_tracking(
                    &mut client_read,
                    &mut client_write,
                    upstream,
                    &mut checkout,
                    pool_mode,
                )
                .await?
                {
                    RelayOutcome::ClientDone(stream) => {
                        checkout.release(stream).await;
                        break;
                    }
                    RelayOutcome::ReleaseToPool(stream) => {
                        checkout.release(stream).await;
                    }
                }
            }
        }
    };

    client_guard.unregister().await;
    Ok(())
}