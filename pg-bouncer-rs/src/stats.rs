use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Default)]
pub struct Stats {
    pub client_connections: AtomicU64,
    pub server_connections: AtomicU64,
    pub queries: AtomicU64,
    pub transactions: AtomicU64,
    pub pooler_errors: AtomicU64,
}

impl Stats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            client_connections: self.client_connections.load(Ordering::Relaxed),
            server_connections: self.server_connections.load(Ordering::Relaxed),
            queries: self.queries.load(Ordering::Relaxed),
            transactions: self.transactions.load(Ordering::Relaxed),
            pooler_errors: self.pooler_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StatsSnapshot {
    pub client_connections: u64,
    pub server_connections: u64,
    pub queries: u64,
    pub transactions: u64,
    pub pooler_errors: u64,
}

#[derive(Default)]
pub struct PoolStats {
    pub active: AtomicU64,
    pub idle: AtomicU64,
    pub waiting: AtomicU64,
}

#[derive(Default)]
pub struct ServerState {
    paused: Mutex<bool>,
}

impl ServerState {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn pause(&self) {
        *self.paused.lock() = true;
    }

    pub fn resume(&self) {
        *self.paused.lock() = false;
    }

    pub fn is_paused(&self) -> bool {
        *self.paused.lock()
    }
}

#[derive(Clone)]
pub struct StatsHandle {
    pub global: Arc<Stats>,
    pub pools: Arc<dashmap::DashMap<String, Arc<PoolStats>>>,
    pub server_state: Arc<ServerState>,
}

impl StatsHandle {
    pub fn new(server_state: Arc<ServerState>) -> Self {
        Self {
            global: Arc::new(Stats::default()),
            pools: Arc::new(dashmap::DashMap::new()),
            server_state,
        }
    }

    pub fn pool(&self, key: &str) -> Arc<PoolStats> {
        self.pools
            .entry(key.to_string())
            .or_insert_with(|| Arc::new(PoolStats::default()))
            .clone()
    }
}
