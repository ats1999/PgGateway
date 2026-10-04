mod key;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{debug, warn};

pub use key::PoolKey;

use crate::connection::ServerConnection;

struct IdleEntry {
    stream: TcpStream,
    auth_flight: Bytes,
}

/// One mutex-protected idle list for a single `(user, database)` pair.
pub struct UserDatabasePool {
    key: PoolKey,
    upstream: String,
    idle: Arc<Mutex<Vec<IdleEntry>>>,
}

impl UserDatabasePool {
    fn new(key: PoolKey, upstream: String) -> Self {
        Self {
            key,
            upstream,
            idle: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn key(&self) -> &PoolKey {
        &self.key
    }

    pub async fn try_acquire_idle(&self) -> Option<PooledServerConnection> {
        let return_target = Arc::new(PoolReturnTarget {
            key: self.key.clone(),
            idle: Arc::clone(&self.idle),
        });

        let entry = self.idle.lock().await.pop()?;
        debug!(
            user = %self.key.user,
            database = %self.key.database,
            "pool: reused idle connection"
        );
        Some(PooledServerConnection {
            stream: Some(entry.stream),
            auth_flight: entry.auth_flight,
            return_target: Some(return_target),
        })
    }

    pub fn attach_checked_out(
        &self,
        auth_flight: Bytes,
        stream: TcpStream,
    ) -> PooledServerConnection {
        PooledServerConnection {
            stream: Some(stream),
            auth_flight,
            return_target: Some(Arc::new(PoolReturnTarget {
                key: self.key.clone(),
                idle: Arc::clone(&self.idle),
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
            return_target: Some(Arc::new(PoolReturnTarget {
                key: self.key.clone(),
                idle: Arc::clone(&self.idle),
            })),
        })
    }
}

struct PoolReturnTarget {
    key: PoolKey,
    idle: Arc<Mutex<Vec<IdleEntry>>>,
}

impl PoolReturnTarget {
    async fn release(&self, stream: TcpStream, auth_flight: Bytes) {
        let mut server = ServerConnection::from_tcp(stream);
        match server.prepare_for_reuse().await {
            Ok(()) => {
                debug!(
                    user = %self.key.user,
                    database = %self.key.database,
                    "pool: returned connection to idle queue"
                );
                self.idle.lock().await.push(IdleEntry {
                    stream: server.into_tcp(),
                    auth_flight,
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
}

/// Checkout handle; call [`PooledServerConnection::release`] after the client session ends.
pub struct PooledServerConnection {
    stream: Option<TcpStream>,
    auth_flight: Bytes,
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
            target.release(stream, self.auth_flight).await;
        }
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
        let pool = Arc::new(UserDatabasePool::new(key.clone(), upstream));
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
