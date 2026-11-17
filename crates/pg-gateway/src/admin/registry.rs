use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

#[derive(Clone, Debug)]
pub struct ClientRecord {
    pub client_id: u64,
    pub peer: SocketAddr,
    pub user: String,
    pub database: String,
}

pub struct ClientRegistry {
    next_id: AtomicU64,
    sessions: Mutex<HashMap<u64, ClientRecord>>,
}

impl ClientRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            next_id: AtomicU64::new(1),
            sessions: Mutex::new(HashMap::new()),
        })
    }

    pub async fn register(
        self: &Arc<Self>,
        peer: SocketAddr,
        user: String,
        database: String,
    ) -> ClientGuard {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let record = ClientRecord {
            client_id: id,
            peer,
            user,
            database,
        };
        self.sessions.lock().await.insert(id, record);
        ClientGuard {
            id,
            registry: Arc::clone(self),
        }
    }

    pub async fn snapshot(&self) -> Vec<ClientRecord> {
        self.sessions.lock().await.values().cloned().collect()
    }
}

pub struct ClientGuard {
    id: u64,
    registry: Arc<ClientRegistry>,
}

impl ClientGuard {
    pub async fn unregister(self) {
        self.registry.sessions.lock().await.remove(&self.id);
    }
}