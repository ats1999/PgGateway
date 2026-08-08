use std::hash::{Hash, Hasher};

/// Pool identity: one mutex-protected idle list per `(user, database)`.
#[derive(Debug, Clone, Eq)]
pub struct PoolKey {
    pub user: String,
    pub database: String,
}

impl PartialEq for PoolKey {
    fn eq(&self, other: &Self) -> bool {
        self.user == other.user && self.database == other.database
    }
}

impl Hash for PoolKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.user.hash(state);
        self.database.hash(state);
    }
}

impl PoolKey {
    pub fn new(user: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            user: user.into(),
            database: database.into(),
        }
    }
}
