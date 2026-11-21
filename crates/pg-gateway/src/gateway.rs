use std::future::Future;
use std::net::SocketAddr;
use std::println;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use bytes::Bytes;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{debug, info, trace, warn};

use crate::admin::{serve_admin_connection, AdminStore, ClientGuard, ClientRegistry, ClientState};
use crate::config::{GatewayConfig, PoolMode, UpstreamTarget};
use crate::connection::{ClientConnection, ServerConnection, StartupIdentity};
use crate::framed::FrameReader;
use crate::pool::{Checkout, ConnectionState, PoolKey, PoolManager, PooledServerConnection};
use crate::pool_config::ResolvedPoolSettings;
use crate::statement_detector::{detect_transaction_status, TransactionStatus};

/// How often idle connections are reaped and `min_pool_size` is topped up.
pub const MAINTENANCE_INTERVAL: Duration = Duration::from_secs(1);

/// Running gateway instance (library entry point).
pub struct Gateway {
    config: Arc<GatewayConfig>,
    pools: Arc<PoolManager>,
    admin: Arc<AdminStore>,
    clients: Arc<ClientRegistry>,
    maintenance: StdMutex<Option<JoinHandle<()>>>,
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
            maintenance: StdMutex::new(None),
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

    /// Start background pool upkeep (`server_idle_timeout`, `min_pool_size`). Idempotent;
    /// [`Gateway::run`] calls it, embedders that call [`Gateway::serve`] directly may too.
    pub fn start_maintenance(&self, interval: Duration) {
        let mut slot = self.maintenance.lock().unwrap();
        if slot.is_none() {
            *slot = Some(self.pools.spawn_maintenance(interval));
        }
    }

    pub async fn run(self: &Arc<Self>) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.config.listen)
            .await
            .with_context(|| format!("bind {}", self.config.listen))?;
        info!(listen = %self.config.listen, "pg-gateway listening");
        self.start_maintenance(MAINTENANCE_INTERVAL);

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

impl Drop for Gateway {
    fn drop(&mut self) {
        if let Some(task) = self.maintenance.lock().unwrap().take() {
            task.abort();
        }
    }
}

/// SQLSTATE codes sent with gateway-generated FATAL errors.
mod sqlstate {
    pub const PROTOCOL_VIOLATION: &str = "08P01";
    pub const TOO_MANY_CONNECTIONS: &str = "53300";
    pub const INVALID_AUTHORIZATION: &str = "28000";
    pub const INVALID_PASSWORD: &str = "28P01";
    pub const IDLE_IN_TRANSACTION_TIMEOUT: &str = "25P03";
    pub const TRANSACTION_TIMEOUT: &str = "25P04";
    pub const IDLE_SESSION_TIMEOUT: &str = "57P05";
    pub const CANNOT_CONNECT_NOW: &str = "57P03";
}

/// Check the client's password (md5 challenge) against `password`; on failure send the FATAL
/// and return an error.
async fn check_client_password(
    client: &mut ClientConnection,
    user: &str,
    password: &str,
    deadline: Option<Instant>,
) -> anyhow::Result<()> {
    match until(deadline, client.verify_password(user, password)).await {
        Some(Ok(true)) => Ok(()),
        Some(Ok(false)) => {
            let message = format!("password authentication failed for user \"{user}\"");
            send_fatal(client.stream_mut(), sqlstate::INVALID_PASSWORD, &message).await;
            bail!("{message}");
        }
        Some(Err(error)) => Err(error.context("client password check")),
        None => {
            let message = "client_login_timeout: password not received in time";
            send_fatal(client.stream_mut(), sqlstate::PROTOCOL_VIOLATION, message).await;
            bail!("{message}");
        }
    }
}

/// Send a FATAL `ErrorResponse`; the caller closes the connection afterwards. Best effort.
async fn send_fatal(writer: &mut (impl AsyncWrite + Unpin), code: &str, message: &str) {
    let mut body = Vec::new();
    for (field, value) in [
        (b'S', "FATAL"),
        (b'V', "FATAL"),
        (b'C', code),
        (b'M', message),
    ] {
        body.push(field);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut out = vec![b'E'];
    out.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    out.extend_from_slice(&body);
    let _ = writer.write_all(&out).await;
    let _ = writer.flush().await;
}

/// Await `fut`, giving up at `deadline` (if any).
async fn until<T>(deadline: Option<Instant>, fut: impl Future<Output = T>) -> Option<T> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline.into(), fut).await.ok(),
        None => Some(fut.await),
    }
}

/// Which session timeout fired in the relay loop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionTimeout {
    ClientIdle,
    IdleInTransaction,
    Transaction,
}

impl SessionTimeout {
    fn report(self, limits: &ResolvedPoolSettings) -> (&'static str, String) {
        let secs = |d: Option<Duration>| d.map(|d| d.as_secs()).unwrap_or(0);
        match self {
            SessionTimeout::ClientIdle => (
                sqlstate::IDLE_SESSION_TIMEOUT,
                format!(
                    "client_idle_timeout: client idle for more than {}s",
                    secs(limits.client_idle_timeout)
                ),
            ),
            SessionTimeout::IdleInTransaction => (
                sqlstate::IDLE_IN_TRANSACTION_TIMEOUT,
                format!(
                    "idle_transaction_timeout: transaction idle for more than {}s",
                    secs(limits.idle_transaction_timeout)
                ),
            ),
            SessionTimeout::Transaction => (
                sqlstate::TRANSACTION_TIMEOUT,
                format!(
                    "transaction_timeout: transaction open for more than {}s",
                    secs(limits.transaction_timeout)
                ),
            ),
        }
    }
}

/// Why one relay leg ended.
enum RelayOutcome {
    /// Client sent Terminate or hung up; upstream can go back to the pool.
    ClientDone(TcpStream),
    /// Transaction/statement mode: upstream is idle and goes back to the pool mid-session.
    ReleaseToPool(TcpStream),
    /// A session timeout fired; the client was told and must be disconnected.
    TimedOut(TcpStream),
    /// The upstream closed; nothing to release.
    ServerClosed,
}

/// Per-client session context.
struct Session<'a> {
    gateway: &'a Gateway,
    identity: StartupIdentity,
    key: PoolKey,
    limits: ResolvedPoolSettings,
    upstream_addr: String,
    /// Actual database name upstream (`dbname`); `identity.database` is the logical name.
    upstream_database: String,
    /// Userlist password the client was checked against; every upstream login uses it.
    gateway_password: String,
    guard: ClientGuard,
}

pub async fn serve_connection(
    stream: TcpStream,
    peer: SocketAddr,
    gateway: &Gateway,
) -> anyhow::Result<()> {
    let started = Instant::now();
    let mut client = ClientConnection::new(stream, peer);

    // Before startup we don't know the user/database yet: only `pool_defaults` applies.
    let startup_limit = gateway.config.startup_settings().client_login_timeout;
    let identity = match until(
        startup_limit.map(|d| started + d),
        client.complete_startup(),
    )
    .await
    {
        Some(result) => result.context("client startup")?,
        None => {
            let message = "client_login_timeout: startup not completed in time";
            send_fatal(client.stream_mut(), sqlstate::PROTOCOL_VIOLATION, message).await;
            bail!("{message}");
        }
    };

    let login_deadline_at = startup_limit.map(|d| started + d);
    println!("id.db {}, id.user {}", &identity.database, &identity.user);
    if gateway.config.is_admin_database(&identity.database) {
        if !gateway.config.allows_admin_client(&identity.user) {
            send_fatal(
                client.stream_mut(),
                sqlstate::INVALID_AUTHORIZATION,
                "user is not allowed on the admin database",
            )
            .await;
            bail!("user `{}` is not allowed for admin database", identity.user);
        }
        // Admin users are the `postgres` database's userlist (see `allows_admin_client`).
        let password = gateway
            .config
            .user_password("postgres", &identity.user)
            .context("admin user has no password")?
            .to_string();
        check_client_password(&mut client, &identity.user, &password, login_deadline_at).await?;
        let client_guard = gateway
            .clients
            .register(peer, identity.user.clone(), identity.database.clone())
            .await;
        client_guard.set_state(ClientState::Active);
        return serve_admin_connection(client, peer, gateway, identity, client_guard).await;
    }

    if !gateway
        .config
        .allows_client(&identity.user, &identity.database)
    {
        send_fatal(
            client.stream_mut(),
            sqlstate::INVALID_AUTHORIZATION,
            &format!(
                "user \"{}\" is not allowed for database \"{}\"",
                identity.user, identity.database
            ),
        )
        .await;
        bail!(
            "user `{}` is not allowed for database `{}`",
            identity.user,
            identity.database
        );
    }

    // Every login is checked against the userlist password (PgBouncer auth_file semantics)
    // before any pooled connection is handed over; upstream logins use the same password.
    let gateway_password = gateway
        .config
        .user_password(&identity.database, &identity.user)
        .context("allowed user has no password")?
        .to_string();
    check_client_password(
        &mut client,
        &identity.user,
        &gateway_password,
        login_deadline_at,
    )
    .await?;

    let max_clients = gateway
        .config
        .database_server_settings(&identity.database)?
        .max_db_client_connections;
    let Some(guard) = gateway.clients.try_register(
        peer,
        identity.user.clone(),
        identity.database.clone(),
        max_clients,
    ) else {
        let message = format!(
            "max_db_client_connections ({}) reached for database \"{}\"",
            max_clients.unwrap_or(0),
            identity.database
        );
        send_fatal(
            client.stream_mut(),
            sqlstate::TOO_MANY_CONNECTIONS,
            &message,
        )
        .await;
        bail!("{message}");
    };

    // Clients are routed to the primary; replica pools resolve their own settings once read
    // routing exists.
    let target = UpstreamTarget::Primary;
    let session = Session {
        gateway,
        key: PoolKey::for_target(identity.user.clone(), identity.database.clone(), target),
        limits: gateway
            .config
            .resolved_pool_for(&identity.user, &identity.database, target)?,
        upstream_addr: gateway
            .config
            .upstream_addr(&identity.database, target)
            .with_context(|| format!("resolve upstream for `{}`", identity.database))?,
        upstream_database: gateway.config.upstream_dbname(&identity.database)?,
        gateway_password,
        identity,
        guard,
    };

    // The user-level `client_login_timeout` covers the rest of login (upstream auth).
    let login_deadline = session.limits.client_login_timeout.map(|d| started + d);
    let (mut client_read, mut client_write) = client.into_split();
    let mut client_frames = FrameReader::new();

    let first = until(login_deadline, first_checkout(&mut client_write, &session)).await;
    let (upstream, mut checkout) = match first {
        Some(Ok(pair)) => pair,
        Some(Err(error)) => return Err(error),
        None => {
            let message = "client_login_timeout: login not completed in time";
            send_fatal(&mut client_write, sqlstate::PROTOCOL_VIOLATION, message).await;
            bail!("{message}");
        }
    };

    // Login is done (the client has its ReadyForQuery). Session mode keeps the upstream for the
    // whole session; transaction/statement mode hands it back right away, so a connected but
    // idle client holds no server connection.
    let mut outcome = match session.limits.pool_mode {
        PoolMode::Session => {
            relay(
                &mut client_read,
                &mut client_write,
                &mut client_frames,
                upstream,
                &mut checkout,
                &session,
                None,
            )
            .await?
        }
        PoolMode::Transaction | PoolMode::Statement => RelayOutcome::ReleaseToPool(upstream),
    };

    loop {
        match outcome {
            RelayOutcome::ClientDone(stream) | RelayOutcome::TimedOut(stream) => {
                checkout.release(stream).await;
                break;
            }
            RelayOutcome::ServerClosed => {
                drop(checkout);
                break;
            }
            RelayOutcome::ReleaseToPool(stream) => {
                checkout.release(stream).await;
            }
        }

        // Transaction/statement mode: hold no upstream until the client sends something.
        session.guard.set_state(ClientState::Idle);
        let idle_deadline = session
            .limits
            .client_idle_timeout
            .map(|d| Instant::now() + d);
        let pending = match until(idle_deadline, client_frames.next(&mut client_read)).await {
            None => {
                let (code, message) = SessionTimeout::ClientIdle.report(&session.limits);
                send_fatal(&mut client_write, code, &message).await;
                debug!(user = %session.identity.user, "{message}");
                break;
            }
            Some(Ok(Some(frame))) if frame[0] != b'X' => frame,
            Some(Ok(_)) => break,
            Some(Err(error)) => {
                debug!(user = %session.identity.user, "client read failed: {error:?}");
                break;
            }
        };

        let (upstream, next) = match next_checkout(&session).await {
            Ok(pair) => pair,
            Err(error) => {
                let code = if error.is::<crate::pool::CheckoutError>() {
                    sqlstate::PROTOCOL_VIOLATION
                } else {
                    sqlstate::CANNOT_CONNECT_NOW
                };
                send_fatal(&mut client_write, code, &format!("{error:#}")).await;
                return Err(error);
            }
        };
        checkout = next;
        outcome = relay(
            &mut client_read,
            &mut client_write,
            &mut client_frames,
            upstream,
            &mut checkout,
            &session,
            Some(pending),
        )
        .await?;
    }

    Ok(())
}

/// First upstream for a session: the client still needs its login. An idle upstream replays
/// its recorded auth flight; a new one runs the real auth exchange with the client.
async fn first_checkout(
    client_write: &mut OwnedWriteHalf,
    session: &Session<'_>,
) -> anyhow::Result<(TcpStream, PooledServerConnection)> {
    session.guard.set_state(ClientState::Waiting);
    let checkout = match session.gateway.pools.checkout(&session.key).await {
        Ok(checkout) => checkout,
        Err(error) => {
            send_fatal(
                client_write,
                sqlstate::PROTOCOL_VIOLATION,
                &format!("{error:#}"),
            )
            .await;
            return Err(error);
        }
    };

    match checkout {
        Checkout::Idle(mut conn) => {
            trace!(event = "pool_checkout", user = %session.identity.user, source = "idle_pool");
            ClientConnection::replay_auth_flight_to_client_write(client_write, conn.auth_flight())
                .await?;
            let stream = conn.take_stream();
            Ok((stream, conn))
        }
        Checkout::Connect(slot) => {
            trace!(event = "pool_checkout", user = %session.identity.user, source = "gateway_login");
            // The client was checked by the gateway; log in upstream with the userlist password.
            match ServerConnection::connect_and_authenticate(
                &session.upstream_addr,
                &session.identity.user,
                &session.upstream_database,
                Some(&session.gateway_password),
                session.limits.server_connect_timeout,
                session.limits.connect_query.as_deref(),
            )
            .await
            {
                Ok((server, auth_flight)) => {
                    ClientConnection::replay_auth_flight_to_client_write(
                        client_write,
                        &auth_flight,
                    )
                    .await?;
                    let mut conn = slot.attach(server.into_tcp(), auth_flight);
                    let stream = conn.take_stream();
                    Ok((stream, conn))
                }
                Err(error) => {
                    slot.login_failed();
                    send_fatal(
                        client_write,
                        sqlstate::CANNOT_CONNECT_NOW,
                        &format!("{error:#}"),
                    )
                    .await;
                    Err(error)
                }
            }
        }
    }
}

/// Later upstreams in transaction/statement mode: the client is already logged in, so new
/// connections use the gateway's own login.
async fn next_checkout(
    session: &Session<'_>,
) -> anyhow::Result<(TcpStream, PooledServerConnection)> {
    session.guard.set_state(ClientState::Waiting);
    match session.gateway.pools.checkout(&session.key).await? {
        Checkout::Idle(mut conn) => {
            let stream = conn.take_stream();
            Ok((stream, conn))
        }
        Checkout::Connect(slot) => {
            match ServerConnection::connect_and_authenticate(
                &session.upstream_addr,
                &session.identity.user,
                &session.upstream_database,
                Some(&session.gateway_password),
                session.limits.server_connect_timeout,
                session.limits.connect_query.as_deref(),
            )
            .await
            {
                Ok((server, auth_flight)) => {
                    let mut conn = slot.attach(server.into_tcp(), auth_flight);
                    let stream = conn.take_stream();
                    Ok((stream, conn))
                }
                Err(error) => {
                    slot.login_failed();
                    Err(error)
                }
            }
        }
    }
}

/// Relay one leg between client and upstream, tracking transaction state from
/// `ReadyForQuery` and enforcing `client_idle_timeout`, `idle_transaction_timeout`, and
/// `transaction_timeout`. Client `Terminate` is not forwarded so the upstream can be reused.
async fn relay(
    client_read: &mut OwnedReadHalf,
    client_write: &mut OwnedWriteHalf,
    client_frames: &mut FrameReader,
    upstream: TcpStream,
    checkout: &mut PooledServerConnection,
    session: &Session<'_>,
    pending: Option<Bytes>,
) -> anyhow::Result<RelayOutcome> {
    let limits = &session.limits;
    let pool_mode = limits.pool_mode;
    let (mut server_read, mut server_write) = upstream.into_split();
    let mut server_frames = FrameReader::new();

    // Requests the server still owes a `ReadyForQuery` for (Query, Sync, FunctionCall).
    let mut outstanding: u32 = 0;
    // Client sent extended-protocol messages not yet followed by Sync.
    let mut unsynced = false;
    let mut client_sent = false;
    let mut quiet_since = Instant::now();
    let mut txn_started: Option<Instant> =
        (checkout.state == ConnectionState::InTransaction).then(Instant::now);

    if let Some(frame) = pending {
        server_write
            .write_all(&frame)
            .await
            .context("forward to upstream")?;
        note_client_message(frame[0], &mut outstanding, &mut unsynced);
        client_sent = true;
    }

    loop {
        let busy = outstanding > 0 || unsynced;
        let in_txn = checkout.state == ConnectionState::InTransaction;
        session.guard.set_state(match (busy, in_txn) {
            (true, _) => ClientState::Active,
            (false, true) => ClientState::IdleInTransaction,
            (false, false) => ClientState::Idle,
        });
        let deadline = next_deadline(limits, busy, in_txn, quiet_since, txn_started);

        tokio::select! {
            frame = client_frames.next(client_read) => {
                match frame {
                    Ok(Some(frame)) if frame[0] != b'X' => {
                        server_write.write_all(&frame).await.context("forward to upstream")?;
                        note_client_message(frame[0], &mut outstanding, &mut unsynced);
                        client_sent = true;
                    }
                    // Terminate, EOF, or a broken client socket all end the session.
                    Ok(_) | Err(_) => {
                        let upstream = reunite(server_read, server_write)?;
                        return Ok(RelayOutcome::ClientDone(upstream));
                    }
                }
            }

            frame = server_frames.next(&mut server_read) => {
                let frame = match frame {
                    Ok(Some(frame)) => frame,
                    Ok(None) | Err(_) => {
                        let _ = client_write.shutdown().await;
                        return Ok(RelayOutcome::ServerClosed);
                    }
                };
                let mut release = false;
                if frame[0] == b'Z' {
                    outstanding = outstanding.saturating_sub(1);
                    quiet_since = Instant::now();
                    if let Some(status) = detect_transaction_status(&frame) {
                        let new_state = match status {
                            TransactionStatus::Idle => ConnectionState::Idle,
                            TransactionStatus::InTransaction
                            | TransactionStatus::FailedTransaction => ConnectionState::InTransaction,
                        };
                        if new_state != checkout.state {
                            trace!(event = "pool_conn_state_change", from = ?checkout.state, to = ?new_state);
                            txn_started = match new_state {
                                ConnectionState::InTransaction => Some(Instant::now()),
                                ConnectionState::Idle => None,
                            };
                            checkout.state = new_state;
                        }
                        release = client_sent
                            && outstanding == 0
                            && !unsynced
                            && should_release_upstream(pool_mode, status);
                    }
                }
                client_write.write_all(&frame).await.context("forward to client")?;
                if release {
                    trace!(event = "pool_release_mid_session", pool_mode = ?pool_mode);
                    let upstream = reunite(server_read, server_write)?;
                    return Ok(RelayOutcome::ReleaseToPool(upstream));
                }
            }

            _ = sleep_until_opt(deadline.map(|(at, _)| at)), if deadline.is_some() => {
                let kind = deadline.map(|(_, kind)| kind).expect("guarded by if");
                let (code, message) = kind.report(limits);
                debug!(user = %session.identity.user, "{message}");
                send_fatal(client_write, code, &message).await;
                let upstream = reunite(server_read, server_write)?;
                return Ok(RelayOutcome::TimedOut(upstream));
            }
        }
    }
}

fn note_client_message(tag: u8, outstanding: &mut u32, unsynced: &mut bool) {
    match tag {
        b'Q' | b'F' => *outstanding += 1,
        b'S' => {
            *outstanding += 1;
            *unsynced = false;
        }
        // Flush asks for output but still expects a later Sync.
        b'H' => {}
        _ => *unsynced = true,
    }
}

/// Earliest session timeout that applies right now.
fn next_deadline(
    limits: &ResolvedPoolSettings,
    busy: bool,
    in_txn: bool,
    quiet_since: Instant,
    txn_started: Option<Instant>,
) -> Option<(Instant, SessionTimeout)> {
    let mut candidates = Vec::with_capacity(2);
    if in_txn {
        if let (Some(limit), Some(started)) = (limits.transaction_timeout, txn_started) {
            candidates.push((started + limit, SessionTimeout::Transaction));
        }
        if !busy {
            if let Some(limit) = limits.idle_transaction_timeout {
                candidates.push((quiet_since + limit, SessionTimeout::IdleInTransaction));
            }
        }
    } else if !busy {
        if let Some(limit) = limits.client_idle_timeout {
            candidates.push((quiet_since + limit, SessionTimeout::ClientIdle));
        }
    }
    candidates.into_iter().min_by_key(|(at, _)| *at)
}

async fn sleep_until_opt(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at.into()).await,
        None => std::future::pending().await,
    }
}

fn reunite(
    read: tokio::net::tcp::OwnedReadHalf,
    write: tokio::net::tcp::OwnedWriteHalf,
) -> anyhow::Result<TcpStream> {
    read.reunite(write)
        .map_err(|_| anyhow::anyhow!("reunite upstream halves"))
}

/// Transaction/statement modes give the upstream back on an idle `ReadyForQuery`.
fn should_release_upstream(pool_mode: PoolMode, status: TransactionStatus) -> bool {
    match pool_mode {
        PoolMode::Session => false,
        PoolMode::Statement | PoolMode::Transaction => matches!(status, TransactionStatus::Idle),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool_config::PoolSettings;

    fn limits(hcl: &str) -> ResolvedPoolSettings {
        hcl::from_str::<PoolSettings>(hcl).unwrap().resolve()
    }

    #[test]
    fn client_idle_applies_only_outside_transactions_and_when_quiet() {
        let l = limits("client_idle_timeout_seconds = 5\nidle_transaction_timeout_seconds = 2");
        let now = Instant::now();
        let (_, kind) = next_deadline(&l, false, false, now, None).unwrap();
        assert_eq!(kind, SessionTimeout::ClientIdle);
        assert!(
            next_deadline(&l, true, false, now, None).is_none(),
            "busy: no idle timeout"
        );
        let (_, kind) = next_deadline(&l, false, true, now, Some(now)).unwrap();
        assert_eq!(kind, SessionTimeout::IdleInTransaction);
    }

    #[test]
    fn transaction_timeout_applies_even_while_busy_and_earliest_wins() {
        let l = limits("transaction_timeout_seconds = 3\nidle_transaction_timeout_seconds = 10");
        let now = Instant::now();
        let (at, kind) = next_deadline(&l, true, true, now, Some(now)).unwrap();
        assert_eq!(kind, SessionTimeout::Transaction);
        assert_eq!(at, now + Duration::from_secs(3));
        let (_, kind) = next_deadline(&l, false, true, now, Some(now)).unwrap();
        assert_eq!(kind, SessionTimeout::Transaction);
    }

    #[test]
    fn no_timeouts_configured_means_no_deadline() {
        let l = limits("");
        assert!(next_deadline(&l, false, true, Instant::now(), Some(Instant::now())).is_none());
    }

    #[test]
    fn sync_and_query_are_tracked_until_ready_for_query() {
        let (mut outstanding, mut unsynced) = (0, false);
        for tag in [b'P', b'B', b'E'] {
            note_client_message(tag, &mut outstanding, &mut unsynced);
        }
        assert!(unsynced);
        assert_eq!(outstanding, 0);
        note_client_message(b'S', &mut outstanding, &mut unsynced);
        assert!(!unsynced);
        assert_eq!(outstanding, 1);
        note_client_message(b'Q', &mut outstanding, &mut unsynced);
        assert_eq!(outstanding, 2);
    }

    #[test]
    fn release_rules_per_mode() {
        assert!(!should_release_upstream(
            PoolMode::Session,
            TransactionStatus::Idle
        ));
        assert!(should_release_upstream(
            PoolMode::Transaction,
            TransactionStatus::Idle
        ));
        assert!(!should_release_upstream(
            PoolMode::Transaction,
            TransactionStatus::InTransaction
        ));
        assert!(should_release_upstream(
            PoolMode::Statement,
            TransactionStatus::Idle
        ));
    }
}
