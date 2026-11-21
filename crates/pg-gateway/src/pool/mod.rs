//! Upstream connection pools.
//!
//! One [`UserDatabasePool`] per [`PoolKey`] (`user × database × upstream instance`), plus one
//! [`DatabaseState`] per logical database for the database-wide `max_db_connections` cap.
//!
//! Every upstream connection is counted from the moment its slot is reserved (before TCP
//! connect) until the socket is dropped, both in its pool (`idle + active + connecting`) and in
//! its database. Reserving the slot under the lock is what keeps concurrent clients from
//! overshooting `pool_size` / `max_db_connections`.

mod key;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, trace, warn};

pub use key::PoolKey;

use crate::config::{GatewayConfig, PoolMode, UpstreamTarget};
use crate::connection::ServerConnection;
use crate::pool_config::ResolvedPoolSettings;

/// How long a waiting client re-checks pool state even without a wake-up (safety net).
const WAIT_RECHECK: Duration = Duration::from_millis(250);
/// Upper bound for one `server_check_query` round trip.
const CHECK_QUERY_LIMIT: Duration = Duration::from_secs(5);

/// Connection state for pool management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is idle and can be reused
    Idle,
    /// Connection is inside a transaction (or a failed one)
    InTransaction,
}

/// Why a client could not get an upstream connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckoutError {
    /// `query_wait_timeout` expired while waiting for a free connection.
    QueryWaitTimeout { waited: Duration },
}

impl std::fmt::Display for CheckoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckoutError::QueryWaitTimeout { waited } => write!(
                f,
                "query_wait_timeout: no server connection available after {:.1}s",
                waited.as_secs_f64()
            ),
        }
    }
}

impl std::error::Error for CheckoutError {}

struct IdleEntry {
    stream: TcpStream,
    auth_flight: Bytes,
    /// When the connection went idle; drives `server_idle_timeout` and `server_check_delay`.
    idle_since: Instant,
}

/// Cumulative per-pool counters, exposed through the admin database.
#[derive(Default)]
pub struct PoolStats {
    pub checkouts: AtomicU64,
    pub waits: AtomicU64,
    pub wait_timeouts: AtomicU64,
    pub connections_opened: AtomicU64,
    pub login_failures: AtomicU64,
    pub closed_idle_timeout: AtomicU64,
    pub closed_max_idle: AtomicU64,
    pub closed_check_failed: AtomicU64,
    pub closed_reset_failed: AtomicU64,
    pub closed_in_transaction: AtomicU64,
    pub closed_evicted: AtomicU64,
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

#[derive(Default)]
struct PoolState {
    idle: Vec<IdleEntry>,
    active: u32,
    connecting: u32,
    waiting: u32,
    login_failed_at: Option<Instant>,
}

impl PoolState {
    fn total(&self) -> u32 {
        self.active + self.connecting + self.idle.len() as u32
    }
}

/// Database-wide server connection accounting for `max_db_connections`.
pub struct DatabaseState {
    name: String,
    max_connections: Option<u32>,
    server_connections: StdMutex<u32>,
    notify: Notify,
}

impl DatabaseState {
    fn new(name: String, max_connections: Option<u32>) -> Self {
        Self {
            name,
            max_connections,
            server_connections: StdMutex::new(0),
            notify: Notify::new(),
        }
    }

    fn try_reserve(&self) -> bool {
        let mut count = self.server_connections.lock().unwrap();
        if self.max_connections.is_some_and(|max| *count >= max) {
            return false;
        }
        *count += 1;
        true
    }

    fn release(&self) {
        let mut count = self.server_connections.lock().unwrap();
        *count = count.saturating_sub(1);
        drop(count);
        self.notify.notify_waiters();
    }

    pub fn server_connections(&self) -> u32 {
        *self.server_connections.lock().unwrap()
    }

}

/// What a checkout attempt produced.
pub enum Checkout {
    /// Reused an idle upstream (already authenticated; replay its auth flight to new clients).
    Idle(PooledServerConnection),
    /// A slot was reserved; the caller must connect and then [`ConnectSlot::attach`].
    Connect(ConnectSlot),
}

enum TryCheckout {
    Ready(Checkout),
    /// Nothing available right now; `retry_at` is set while login backoff is active.
    Wait {
        database_full: bool,
        retry_at: Option<Instant>,
    },
}

/// One upstream pool for a `(user, database, instance)` key.
pub struct UserDatabasePool {
    key: PoolKey,
    upstream: String,
    /// Actual database name upstream (`dbname`); `key.database` is the logical name.
    upstream_database: String,
    /// Userlist password for gateway-initiated logins (warm-up); `None` = trust auth only.
    password: Option<String>,
    limits: ResolvedPoolSettings,
    state: StdMutex<PoolState>,
    notify: Notify,
    database: Arc<DatabaseState>,
    stats: PoolStats,
}

impl UserDatabasePool {
    fn new(
        key: PoolKey,
        upstream: String,
        upstream_database: String,
        password: Option<String>,
        limits: ResolvedPoolSettings,
        database: Arc<DatabaseState>,
    ) -> Self {
        Self {
            key,
            upstream,
            upstream_database,
            password,
            limits,
            state: StdMutex::new(PoolState::default()),
            notify: Notify::new(),
            database,
            stats: PoolStats::default(),
        }
    }

    pub fn key(&self) -> &PoolKey {
        &self.key
    }

    pub fn pool_mode(&self) -> PoolMode {
        self.limits.pool_mode
    }

    pub fn upstream(&self) -> &str {
        &self.upstream
    }

    pub fn upstream_database(&self) -> &str {
        &self.upstream_database
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn limits(&self) -> &ResolvedPoolSettings {
        &self.limits
    }

    pub fn stats(&self) -> &PoolStats {
        &self.stats
    }

    pub fn idle_count(&self) -> usize {
        self.state.lock().unwrap().idle.len()
    }

    pub fn active_count(&self) -> u32 {
        self.state.lock().unwrap().active
    }

    pub fn total_count(&self) -> u32 {
        self.state.lock().unwrap().total()
    }

    fn is_idle_expired(&self, entry: &IdleEntry) -> bool {
        self.limits
            .server_idle_timeout
            .is_some_and(|limit| entry.idle_since.elapsed() > limit)
    }

    fn login_retry_at(&self, state: &PoolState) -> Option<Instant> {
        let retry = self.limits.server_login_retry?;
        let at = state.login_failed_at? + retry;
        (Instant::now() < at).then_some(at)
    }

    /// Drop one upstream connection that is no longer counted anywhere else.
    fn close(&self, stream: TcpStream, reason: &AtomicU64) {
        drop(stream);
        bump(reason);
        self.database.release();
        self.notify.notify_waiters();
    }

    fn try_checkout(self: &Arc<Self>) -> TryCheckout {
        let mut expired = Vec::new();
        let outcome = {
            let mut state = self.state.lock().unwrap();
            // Most recently used first; expired entries found on the way are closed.
            let mut picked = None;
            while let Some(entry) = state.idle.pop() {
                if self.is_idle_expired(&entry) {
                    expired.push(entry.stream);
                    continue;
                }
                picked = Some(entry);
                break;
            }
            if let Some(entry) = picked {
                state.active += 1;
                TryCheckout::Ready(Checkout::Idle(self.checked_out(
                    entry.stream,
                    entry.auth_flight,
                    Some(entry.idle_since),
                )))
            } else if let Some(retry_at) = self.login_retry_at(&state) {
                TryCheckout::Wait {
                    database_full: false,
                    retry_at: Some(retry_at),
                }
            } else if state.total() >= self.limits.pool_size {
                TryCheckout::Wait {
                    database_full: false,
                    retry_at: None,
                }
            } else if !self.database.try_reserve() {
                TryCheckout::Wait {
                    database_full: true,
                    retry_at: None,
                }
            } else {
                state.connecting += 1;
                TryCheckout::Ready(Checkout::Connect(ConnectSlot {
                    pool: Some(Arc::clone(self)),
                }))
            }
        };
        for stream in expired {
            self.close(stream, &self.stats.closed_idle_timeout);
        }
        outcome
    }

    fn checked_out(
        self: &Arc<Self>,
        stream: TcpStream,
        auth_flight: Bytes,
        idle_since: Option<Instant>,
    ) -> PooledServerConnection {
        PooledServerConnection {
            stream: Some(stream),
            auth_flight,
            state: ConnectionState::Idle,
            idle_since,
            pool: Some(Arc::clone(self)),
        }
    }

    /// Close one idle connection to make room elsewhere in the database. Returns whether one
    /// was closed.
    fn evict_one_idle(&self) -> bool {
        let entry = {
            let mut state = self.state.lock().unwrap();
            if state.idle.is_empty() {
                return false;
            }
            // Oldest idle first.
            state.idle.remove(0)
        };
        self.close(entry.stream, &self.stats.closed_evicted);
        true
    }

    /// Close idle connections past `server_idle_timeout`.
    pub fn prune_idle(&self) -> usize {
        let expired: Vec<TcpStream> = {
            let mut state = self.state.lock().unwrap();
            let mut keep = Vec::with_capacity(state.idle.len());
            let mut expired = Vec::new();
            for entry in state.idle.drain(..) {
                if self.is_idle_expired(&entry) {
                    expired.push(entry.stream);
                } else {
                    keep.push(entry);
                }
            }
            state.idle = keep;
            expired
        };
        let pruned = expired.len();
        for stream in expired {
            self.close(stream, &self.stats.closed_idle_timeout);
        }
        if pruned > 0 {
            debug!(
                user = %self.key.user,
                database = %self.key.database,
                target = %self.key.target,
                pruned,
                "pool: closed idle connections past server_idle_timeout"
            );
        }
        pruned
    }

    /// Open connections until the pool holds `min_pool_size` (respecting every cap and the
    /// login backoff). Uses the gateway-owned login with the user's userlist password.
    pub async fn maintain_min_pool_size(self: &Arc<Self>) {
        let target = self.limits.min_pool_size;
        if target == 0 {
            return;
        }
        loop {
            let slot = {
                let mut state = self.state.lock().unwrap();
                if state.total() >= target
                    || state.total() >= self.limits.pool_size
                    || self.login_retry_at(&state).is_some()
                    || !self.database.try_reserve()
                {
                    return;
                }
                state.connecting += 1;
                ConnectSlot {
                    pool: Some(Arc::clone(self)),
                }
            };
            match ServerConnection::connect_and_authenticate(
                &self.upstream,
                &self.key.user,
                &self.upstream_database,
                self.password.as_deref(),
                self.limits.server_connect_timeout,
                self.limits.connect_query.as_deref(),
            )
            .await
            {
                Ok((server, auth_flight)) => {
                    slot.attach_idle(server.into_tcp(), auth_flight);
                    trace!(
                        user = %self.key.user,
                        database = %self.key.database,
                        "pool: opened connection for min_pool_size"
                    );
                }
                Err(error) => {
                    warn!(
                        user = %self.key.user,
                        database = %self.key.database,
                        "pool: min_pool_size connect failed: {error:#}"
                    );
                    slot.login_failed();
                    return;
                }
            }
        }
    }

    /// Put a released connection back, or close it. The connection stays counted as active
    /// until it is back in the idle list or closed, so nobody can claim its slot while the
    /// reset query is still running.
    async fn release(&self, stream: TcpStream, auth_flight: Bytes, state: ConnectionState) {
        // A client that left mid-transaction leaves server state nobody can clean up safely.
        if state == ConnectionState::InTransaction {
            trace!(user = %self.key.user, "pool: closing connection released inside a transaction");
            self.close_active(stream, &self.stats.closed_in_transaction);
            return;
        }

        let mut server = ServerConnection::from_tcp(stream);
        if self.limits.needs_server_reset() {
            if let Err(error) = server
                .prepare_for_reuse(&self.limits.server_reset_query)
                .await
            {
                warn!(
                    user = %self.key.user,
                    database = %self.key.database,
                    "pool: server_reset_query failed, closing connection: {error:?}"
                );
                self.close_active(server.into_tcp(), &self.stats.closed_reset_failed);
                return;
            }
        }

        let stream = server.into_tcp();
        let surplus = {
            let mut pool_state = self.state.lock().unwrap();
            pool_state.active = pool_state.active.saturating_sub(1);
            if pool_state.idle.len() >= self.limits.max_idle as usize {
                Some(stream)
            } else {
                pool_state.idle.push(IdleEntry {
                    stream,
                    auth_flight,
                    idle_since: Instant::now(),
                });
                None
            }
        };
        match surplus {
            Some(stream) => self.close(stream, &self.stats.closed_max_idle),
            None => self.notify.notify_waiters(),
        }
    }

    /// Close a connection that is still counted as active.
    fn close_active(&self, stream: TcpStream, reason: &AtomicU64) {
        {
            let mut state = self.state.lock().unwrap();
            state.active = state.active.saturating_sub(1);
        }
        self.close(stream, reason);
    }

    /// The checkout handle was dropped without [`PooledServerConnection::release`]: the
    /// connection is gone, free its slot.
    fn forget_active(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.active = state.active.saturating_sub(1);
        }
        self.database.release();
        self.notify.notify_waiters();
    }

    /// `server_check_query` on a connection idle for at least `server_check_delay`.
    async fn check_reused(&self, conn: &mut PooledServerConnection) -> bool {
        let (Some(sql), Some(idle_since)) = (&self.limits.server_check_query, conn.idle_since)
        else {
            return true;
        };
        if idle_since.elapsed() < self.limits.server_check_delay {
            return true;
        }
        let mut server = ServerConnection::from_tcp(conn.take_stream());
        let limit = self
            .limits
            .server_connect_timeout
            .unwrap_or(CHECK_QUERY_LIMIT)
            .min(CHECK_QUERY_LIMIT);
        match server.check(sql, limit).await {
            Ok(()) => {
                conn.stream = Some(server.into_tcp());
                true
            }
            Err(error) => {
                debug!(
                    user = %self.key.user,
                    database = %self.key.database,
                    "pool: {error:#}; closing connection"
                );
                bump(&self.stats.closed_check_failed);
                // `conn` has no stream any more; dropping it frees the slot.
                drop(server);
                false
            }
        }
    }
}

/// A reserved-but-not-yet-connected slot. Dropping it without [`ConnectSlot::attach`] frees
/// the slot again.
pub struct ConnectSlot {
    pool: Option<Arc<UserDatabasePool>>,
}

impl ConnectSlot {
    pub fn pool(&self) -> &Arc<UserDatabasePool> {
        self.pool.as_ref().expect("slot already consumed")
    }

    /// The connection is up and authenticated; hand it to the caller as checked out.
    pub fn attach(mut self, stream: TcpStream, auth_flight: Bytes) -> PooledServerConnection {
        let pool = self.pool.take().expect("slot already consumed");
        {
            let mut state = pool.state.lock().unwrap();
            state.connecting -= 1;
            state.active += 1;
            state.login_failed_at = None;
        }
        bump(&pool.stats.connections_opened);
        pool.checked_out(stream, auth_flight, None)
    }

    fn attach_idle(mut self, stream: TcpStream, auth_flight: Bytes) {
        let pool = self.pool.take().expect("slot already consumed");
        {
            let mut state = pool.state.lock().unwrap();
            state.connecting -= 1;
            state.login_failed_at = None;
            state.idle.push(IdleEntry {
                stream,
                auth_flight,
                idle_since: Instant::now(),
            });
        }
        bump(&pool.stats.connections_opened);
        pool.notify.notify_waiters();
    }

    /// Connect or gateway-side login failed: start the `server_login_retry` backoff.
    /// Not used for client-supplied credentials failing in pass-through auth.
    pub fn login_failed(self) {
        let pool = self.pool.as_ref().expect("slot already consumed");
        bump(&pool.stats.login_failures);
        pool.state.lock().unwrap().login_failed_at = Some(Instant::now());
        // Drop frees the slot.
    }
}

impl Drop for ConnectSlot {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            {
                let mut state = pool.state.lock().unwrap();
                state.connecting = state.connecting.saturating_sub(1);
            }
            pool.database.release();
            pool.notify.notify_waiters();
        }
    }
}

/// Checkout handle; call [`PooledServerConnection::release`] when the client is done with it.
/// Dropping it without releasing closes the connection and frees its slot.
pub struct PooledServerConnection {
    stream: Option<TcpStream>,
    auth_flight: Bytes,
    pub state: ConnectionState,
    /// Set when reused from the idle list.
    idle_since: Option<Instant>,
    pool: Option<Arc<UserDatabasePool>>,
}

impl PooledServerConnection {
    pub fn auth_flight(&self) -> &Bytes {
        &self.auth_flight
    }

    pub fn take_stream(&mut self) -> TcpStream {
        self.stream.take().expect("pooled stream already taken")
    }

    pub async fn release(mut self, stream: TcpStream) {
        if let Some(pool) = self.pool.take() {
            let auth_flight = std::mem::take(&mut self.auth_flight);
            pool.release(stream, auth_flight, self.state).await;
        }
    }

    pub fn update_state(&mut self, new_state: ConnectionState) {
        self.state = new_state;
    }
}

impl Drop for PooledServerConnection {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.forget_active();
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub user: String,
    pub database: String,
    pub target: UpstreamTarget,
    pub pool_mode: PoolMode,
    pub upstream: String,
    pub idle_count: u32,
    pub active_count: u32,
    pub connecting_count: u32,
    pub waiting_count: u32,
    pub login_backoff_seconds: Option<f64>,
    pub checkouts: u64,
    pub waits: u64,
    pub wait_timeouts: u64,
    pub connections_opened: u64,
    pub login_failures: u64,
    pub closed_idle_timeout: u64,
    pub closed_max_idle: u64,
    pub closed_check_failed: u64,
    pub closed_reset_failed: u64,
    pub closed_in_transaction: u64,
    pub closed_evicted: u64,
}

#[derive(Debug, Clone, Default)]
pub struct DatabasePoolTotals {
    pub current_connections: u32,
}

/// Lazily creates one [`UserDatabasePool`] per [`PoolKey`] and one [`DatabaseState`] per
/// database.
pub struct PoolManager {
    config: Arc<GatewayConfig>,
    pools: Mutex<HashMap<PoolKey, Arc<UserDatabasePool>>>,
    databases: StdMutex<HashMap<String, Arc<DatabaseState>>>,
}

impl PoolManager {
    pub fn new(config: Arc<GatewayConfig>) -> Arc<Self> {
        Arc::new(Self {
            config,
            pools: Mutex::new(HashMap::new()),
            databases: StdMutex::new(HashMap::new()),
        })
    }

    fn database_state(&self, database: &str) -> anyhow::Result<Arc<DatabaseState>> {
        let mut guard = self.databases.lock().unwrap();
        if let Some(state) = guard.get(database) {
            return Ok(Arc::clone(state));
        }
        let max = self
            .config
            .database_server_settings(database)?
            .max_db_connections;
        let state = Arc::new(DatabaseState::new(database.to_string(), max));
        guard.insert(database.to_string(), Arc::clone(&state));
        Ok(state)
    }

    pub async fn pool_for(&self, key: PoolKey) -> anyhow::Result<Arc<UserDatabasePool>> {
        let mut guard = self.pools.lock().await;
        if let Some(pool) = guard.get(&key) {
            return Ok(Arc::clone(pool));
        }
        let upstream = self.config.upstream_addr(&key.database, key.target)?;
        let upstream_database = self.config.upstream_dbname(&key.database)?;
        let password = self
            .config
            .user_password(&key.database, &key.user)
            .map(str::to_string);
        let resolved = self
            .config
            .resolved_pool_for(&key.user, &key.database, key.target)?;
        let database = self.database_state(&key.database)?;
        let pool = Arc::new(UserDatabasePool::new(
            key.clone(),
            upstream,
            upstream_database,
            password,
            resolved,
            database,
        ));
        guard.insert(key, Arc::clone(&pool));
        drop(guard);

        let warm = Arc::clone(&pool);
        tokio::spawn(async move {
            warm.maintain_min_pool_size().await;
        });

        Ok(pool)
    }

    /// Get an upstream for `key`: an idle one, or a reserved slot to connect. Waits while the
    /// pool, the database, or the login backoff say no — at most until `deadline`
    /// (`query_wait_timeout`).
    pub async fn checkout(&self, key: &PoolKey) -> anyhow::Result<Checkout> {
        let pool = self.pool_for(key.clone()).await?;
        let started = Instant::now();
        let deadline = pool.limits.query_wait_timeout.map(|limit| started + limit);
        let mut counted_wait = false;

        loop {
            // Register for wake-ups before looking, so a release in between isn't missed.
            let pool_notified = pool.notify.notified();
            let db_notified = pool.database.notify.notified();
            tokio::pin!(pool_notified);
            tokio::pin!(db_notified);
            pool_notified.as_mut().enable();
            db_notified.as_mut().enable();

            match pool.try_checkout() {
                TryCheckout::Ready(Checkout::Idle(mut conn)) => {
                    if !pool.check_reused(&mut conn).await {
                        continue;
                    }
                    bump(&pool.stats.checkouts);
                    return Ok(Checkout::Idle(conn));
                }
                TryCheckout::Ready(ready) => {
                    bump(&pool.stats.checkouts);
                    return Ok(ready);
                }
                TryCheckout::Wait {
                    database_full,
                    retry_at,
                } => {
                    if database_full && self.evict_idle_elsewhere(&pool).await {
                        continue;
                    }
                    if !counted_wait {
                        bump(&pool.stats.waits);
                        counted_wait = true;
                    }
                    let mut wake_at = Instant::now() + WAIT_RECHECK;
                    if let Some(retry_at) = retry_at {
                        wake_at = wake_at.min(retry_at);
                    }
                    if let Some(deadline) = deadline {
                        if Instant::now() >= deadline {
                            bump(&pool.stats.wait_timeouts);
                            return Err(CheckoutError::QueryWaitTimeout {
                                waited: started.elapsed(),
                            }
                            .into());
                        }
                        wake_at = wake_at.min(deadline);
                    }

                    pool.state.lock().unwrap().waiting += 1;
                    tokio::select! {
                        _ = &mut pool_notified => {}
                        _ = &mut db_notified => {}
                        _ = tokio::time::sleep_until(wake_at.into()) => {}
                    }
                    pool.state.lock().unwrap().waiting -= 1;
                }
            }
        }
    }

    /// `max_db_connections` reached: close an idle connection in another pool of the same
    /// database so this one can open its own (PgBouncer does the same).
    async fn evict_idle_elsewhere(&self, pool: &Arc<UserDatabasePool>) -> bool {
        let candidates: Vec<Arc<UserDatabasePool>> = {
            let guard = self.pools.lock().await;
            guard
                .values()
                .filter(|p| p.key.database == pool.key.database && p.key != pool.key)
                .cloned()
                .collect()
        };
        candidates.iter().any(|candidate| candidate.evict_one_idle())
    }

    /// Background upkeep: close idle connections past `server_idle_timeout` and top pools up
    /// to `min_pool_size` (also for configured users that have not connected yet).
    pub async fn maintain(&self) {
        for (database, cluster) in &self.config.databases {
            if database == &self.config.admin_database {
                continue;
            }
            for user in cluster.userlist.iter().map(|u| u.name.as_str()) {
                let wants_min = self
                    .config
                    .resolved_pool_for(user, database, UpstreamTarget::Primary)
                    .map(|r| r.min_pool_size > 0)
                    .unwrap_or(false);
                if wants_min {
                    let _ = self.pool_for(PoolKey::new(user, database.as_str())).await;
                }
            }
        }

        let pools: Vec<Arc<UserDatabasePool>> =
            self.pools.lock().await.values().cloned().collect();
        for pool in pools {
            pool.prune_idle();
            pool.maintain_min_pool_size().await;
        }
    }

    /// Run [`PoolManager::maintain`] every `interval` until the returned task is aborted.
    pub fn spawn_maintenance(self: &Arc<Self>, interval: Duration) -> tokio::task::JoinHandle<()> {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                manager.maintain().await;
            }
        })
    }

    pub async fn admin_pool_snapshots(&self) -> Vec<PoolSnapshot> {
        let guard = self.pools.lock().await;
        let mut out = Vec::with_capacity(guard.len());
        for (key, pool) in guard.iter() {
            let (idle, active, connecting, waiting, backoff) = {
                let state = pool.state.lock().unwrap();
                let backoff = pool
                    .login_retry_at(&state)
                    .map(|at| at.saturating_duration_since(Instant::now()).as_secs_f64());
                (
                    state.idle.len() as u32,
                    state.active,
                    state.connecting,
                    state.waiting,
                    backoff,
                )
            };
            let stats = &pool.stats;
            let load = |c: &AtomicU64| c.load(Ordering::Relaxed);
            out.push(PoolSnapshot {
                user: key.user.clone(),
                database: key.database.clone(),
                target: key.target,
                pool_mode: pool.pool_mode(),
                upstream: pool.upstream().to_string(),
                idle_count: idle,
                active_count: active,
                connecting_count: connecting,
                waiting_count: waiting,
                login_backoff_seconds: backoff,
                checkouts: load(&stats.checkouts),
                waits: load(&stats.waits),
                wait_timeouts: load(&stats.wait_timeouts),
                connections_opened: load(&stats.connections_opened),
                login_failures: load(&stats.login_failures),
                closed_idle_timeout: load(&stats.closed_idle_timeout),
                closed_max_idle: load(&stats.closed_max_idle),
                closed_check_failed: load(&stats.closed_check_failed),
                closed_reset_failed: load(&stats.closed_reset_failed),
                closed_in_transaction: load(&stats.closed_in_transaction),
                closed_evicted: load(&stats.closed_evicted),
            });
        }
        out
    }

    pub async fn admin_database_totals(&self) -> HashMap<String, DatabasePoolTotals> {
        self.databases
            .lock()
            .unwrap()
            .iter()
            .map(|(name, state)| {
                (
                    name.clone(),
                    DatabasePoolTotals {
                        current_connections: state.server_connections(),
                    },
                )
            })
            .collect()
    }
}

impl std::fmt::Debug for DatabaseState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseState")
            .field("name", &self.name)
            .field("max_connections", &self.max_connections)
            .field("server_connections", &self.server_connections())
            .finish()
    }
}

#[cfg(test)]
mod tests;
