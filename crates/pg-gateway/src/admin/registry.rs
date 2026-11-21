use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// What a client session is doing right now (shown in the admin `clients` table).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientState {
    /// Startup / authentication in progress.
    Login,
    /// Connected, not in a transaction, waiting for the client's next query.
    Idle,
    /// Waiting for a free upstream connection.
    Waiting,
    /// A query is running on an upstream connection.
    Active,
    /// Inside an open transaction, waiting for the client's next query.
    IdleInTransaction,
}

impl ClientState {
    pub fn as_str(self) -> &'static str {
        match self {
            ClientState::Login => "login",
            ClientState::Idle => "idle",
            ClientState::Waiting => "waiting",
            ClientState::Active => "active",
            ClientState::IdleInTransaction => "idle_in_transaction",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientRecord {
    pub client_id: u64,
    pub peer: SocketAddr,
    pub user: String,
    pub database: String,
    /// Unix seconds.
    pub connected_at: u64,
    pub state: ClientState,
    pub state_since: Instant,
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
        self.try_register(peer, user, database, None)
            .expect("no cap to exceed")
    }

    /// Register a client unless `max_clients` sessions for the same database already exist.
    /// The count and the insert happen under one lock, so concurrent logins can't overshoot.
    pub fn try_register(
        self: &Arc<Self>,
        peer: SocketAddr,
        user: String,
        database: String,
        max_clients: Option<u32>,
    ) -> Option<ClientGuard> {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(max) = max_clients {
            let current = sessions.values().filter(|c| c.database == database).count();
            if current >= max as usize {
                return None;
            }
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        sessions.insert(
            id,
            ClientRecord {
                client_id: id,
                peer,
                user,
                database,
                connected_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0),
                state: ClientState::Login,
                state_since: Instant::now(),
            },
        );
        Some(ClientGuard {
            id,
            registry: Arc::clone(self),
        })
    }

    pub async fn snapshot(&self) -> Vec<ClientRecord> {
        self.sessions.lock().unwrap().values().cloned().collect()
    }

    pub fn count_for_database(&self, database: &str) -> usize {
        self.sessions
            .lock()
            .unwrap()
            .values()
            .filter(|c| c.database == database)
            .count()
    }
}

/// Keeps a client listed while its session runs; removes it when dropped.
pub struct ClientGuard {
    id: u64,
    registry: Arc<ClientRegistry>,
}

impl ClientGuard {
    pub fn set_state(&self, state: ClientState) {
        if let Some(record) = self.registry.sessions.lock().unwrap().get_mut(&self.id) {
            if record.state != state {
                record.state = state;
                record.state_since = Instant::now();
            }
        }
    }

    pub async fn unregister(self) {
        // Drop does the work.
    }
}

impl Drop for ClientGuard {
    fn drop(&mut self) {
        self.registry.sessions.lock().unwrap().remove(&self.id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer() -> SocketAddr {
        "127.0.0.1:1".parse().unwrap()
    }

    #[test]
    fn try_register_enforces_per_database_cap() {
        let registry = ClientRegistry::new();
        let first = registry.try_register(peer(), "u".into(), "a".into(), Some(1));
        assert!(first.is_some());
        assert!(registry
            .try_register(peer(), "u".into(), "a".into(), Some(1))
            .is_none());
        // Other databases are unaffected.
        assert!(registry
            .try_register(peer(), "u".into(), "b".into(), Some(1))
            .is_some());
        drop(first);
        assert!(registry
            .try_register(peer(), "u".into(), "a".into(), Some(1))
            .is_some());
    }

    #[test]
    fn guard_drop_unregisters_and_state_updates() {
        let registry = ClientRegistry::new();
        let guard = registry
            .try_register(peer(), "u".into(), "a".into(), None)
            .unwrap();
        guard.set_state(ClientState::Active);
        let state = registry.sessions.lock().unwrap().values().next().unwrap().state;
        assert_eq!(state, ClientState::Active);
        drop(guard);
        assert_eq!(registry.count_for_database("a"), 0);
    }
}
