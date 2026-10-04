mod key;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, warn};

pub use key::PoolKey;

use crate::connection::ServerConnection;
use crate::config::PoolMode;

/// Connection state for pool management.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is idle and can be reused
    Idle,
    /// Connection is in an active transaction (pinned in transaction mode)
    InTransaction,
}

struct IdleEntry {
    stream: TcpStream,
    auth_flight: Bytes,
    state: ConnectionState,
}

/// One mutex-protected idle list for a single `(user, database)` pair.
pub struct UserDatabasePool {
    key: PoolKey,
    upstream: String,
    idle: Arc<Mutex<Vec<IdleEntry>>>,
    pool_mode: PoolMode,
}

impl UserDatabasePool {
    fn new(key: PoolKey, upstream: String, pool_mode: PoolMode) -> Self {
        Self {
            key,
            upstream,
            idle: Arc::new(Mutex::new(Vec::new())),
            pool_mode,
        }
    }

    pub fn key(&self) -> &PoolKey {
        &self.key
    }

    pub async fn try_acquire_idle(&self) -> Option<PooledServerConnection> {
        let mut idle_list = self.idle.lock().await;

        // Find an unpinned connection
        for i in (0..idle_list.len()).rev() {
            let should_pin = self.should_pin_connection(idle_list[i].state);
            if !should_pin {
                let entry = idle_list.remove(i);
                let return_target = Arc::new(PoolReturnTarget {
                    key: self.key.clone(),
                    idle: Arc::clone(&self.idle),
                    pool_mode: self.pool_mode,
                });
                debug!(
                    user = %self.key.user,
                    database = %self.key.database,
                    idle_connections_remaining = idle_list.len(),
                    "connection unpinned (acquired from pool)"
                );
                return Some(PooledServerConnection {
                    stream: Some(entry.stream),
                    auth_flight: entry.auth_flight,
                    state: entry.state,
                    return_target: Some(return_target),
                });
            }
        }
        None
    }

    fn should_pin_connection(&self, state: ConnectionState) -> bool {
        match self.pool_mode {
            PoolMode::Session => true,
            PoolMode::Statement => false,
            PoolMode::Transaction => state == ConnectionState::InTransaction,
        }
    }

    pub fn attach_checked_out(
        &self,
        auth_flight: Bytes,
        stream: TcpStream,
    ) -> PooledServerConnection {
        PooledServerConnection {
            stream: Some(stream),
            auth_flight,
            state: ConnectionState::Idle,
            return_target: Some(Arc::new(PoolReturnTarget {
                key: self.key.clone(),
                idle: Arc::clone(&self.idle),
                pool_mode: self.pool_mode,
            })),
        }
    }

    #[allow(dead_code)]
    pub async fn acquire(&self) -> anyhow::Result<PooledServerConnection> {
        if let Some(conn) = self.try_acquire_idle().await {
            return Ok(conn);
        }

        debug!(
            user = %self.key.user,
            database = %self.key.database,
            "pool: opening new upstream connection"
        );
        let (server, auth_flight) = ServerConnection::connect_and_authenticate(
            &self.upstream,
            &self.key.user,
            &self.key.database,
        )
        .await?;

        Ok(PooledServerConnection {
            stream: Some(server.into_tcp()),
            auth_flight,
            state: ConnectionState::Idle,
            return_target: Some(Arc::new(PoolReturnTarget {
                key: self.key.clone(),
                idle: Arc::clone(&self.idle),
                pool_mode: self.pool_mode,
            })),
        })
    }
}

struct PoolReturnTarget {
    key: PoolKey,
    idle: Arc<Mutex<Vec<IdleEntry>>>,
    pool_mode: PoolMode,
}

impl PoolReturnTarget {
    async fn release(
        &self,
        stream: TcpStream,
        auth_flight: Bytes,
        state: ConnectionState,
    ) {
        // Determine whether to pin the connection based on pool mode and state
        let should_pin = self.should_pin(state);

        let mut server = ServerConnection::from_tcp(stream);

        // Only run DISCARD ALL if not pinned (unpinned connections are reset)
        let prepare_result = if should_pin {
            Ok(())
        } else {
            server.prepare_for_reuse().await
        };

        match prepare_result {
            Ok(()) => {
                if should_pin {
                    debug!(
                        user = %self.key.user,
                        database = %self.key.database,
                        state = ?state,
                        "connection pinned (not released to pool)"
                    );
                } else {
                    debug!(
                        user = %self.key.user,
                        database = %self.key.database,
                        "connection unpinned (released to pool)"
                    );
                }
                self.idle.lock().await.push(IdleEntry {
                    stream: server.into_tcp(),
                    auth_flight,
                    state: if should_pin { state } else { ConnectionState::Idle },
                });
            }
            Err(error) => {
                warn!(
                    user = %self.key.user,
                    database = %self.key.database,
                    "pool: discard failed, dropping connection: {error}"
                );
            }
        }
    }

    fn should_pin(&self, state: ConnectionState) -> bool {
        match self.pool_mode {
            PoolMode::Session => true,
            PoolMode::Statement => false,
            PoolMode::Transaction => state == ConnectionState::InTransaction,
        }
    }
}

/// Checkout handle; call [`PooledServerConnection::release`] after the client session ends.
pub struct PooledServerConnection {
    stream: Option<TcpStream>,
    auth_flight: Bytes,
    pub state: ConnectionState,
    return_target: Option<Arc<PoolReturnTarget>>,
}

impl PooledServerConnection {
    pub fn auth_flight(&self) -> &Bytes {
        &self.auth_flight
    }

    pub fn take_stream(&mut self) -> TcpStream {
        self.stream.take().expect("pooled stream already taken")
    }

    pub async fn release(self, stream: TcpStream) {
        if let Some(target) = self.return_target {
            target.release(stream, self.auth_flight, self.state).await;
        }
    }

    /// Update the connection state based on executed statements
    pub fn update_state(&mut self, new_state: ConnectionState) {
        self.state = new_state;
    }
}

/// Lazily creates one [`UserDatabasePool`] per [`PoolKey`].
pub struct PoolManager {
    config: Arc<crate::config::GatewayConfig>,
    pools: Mutex<HashMap<PoolKey, Arc<UserDatabasePool>>>,
}

impl PoolManager {
    pub fn new(config: Arc<crate::config::GatewayConfig>) -> Arc<Self> {
        Arc::new(Self {
            config,
            pools: Mutex::new(HashMap::new()),
        })
    }

    pub async fn pool_for(&self, key: PoolKey) -> anyhow::Result<Arc<UserDatabasePool>> {
        let mut guard = self.pools.lock().await;
        if let Some(pool) = guard.get(&key) {
            return Ok(Arc::clone(pool));
        }
        let upstream = self.config.primary_upstream(&key.database)?;
        let cluster = self.config.cluster(&key.database)?;
        let pool_mode = cluster.pool_config.pool_mode;
        let pool = Arc::new(UserDatabasePool::new(key.clone(), upstream, pool_mode));
        guard.insert(key.clone(), Arc::clone(&pool));
        Ok(pool)
    }

    pub async fn try_acquire_idle(
        &self,
        user: &str,
        database: &str,
    ) -> anyhow::Result<Option<PooledServerConnection>> {
        let pool = self.pool_for(PoolKey::new(user, database)).await?;
        Ok(pool.try_acquire_idle().await)
    }

    pub async fn attach_checked_out(
        &self,
        user: &str,
        database: &str,
        auth_flight: Bytes,
        stream: TcpStream,
    ) -> anyhow::Result<PooledServerConnection> {
        let pool = self.pool_for(PoolKey::new(user, database)).await?;
        Ok(pool.attach_checked_out(auth_flight, stream))
    }

    pub async fn acquire(
        &self,
        user: &str,
        database: &str,
    ) -> anyhow::Result<PooledServerConnection> {
        let pool = self.pool_for(PoolKey::new(user, database)).await?;
        pool.acquire().await
    }
}
