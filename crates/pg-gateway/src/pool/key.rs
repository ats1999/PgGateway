use crate::config::UpstreamTarget;

/// Pool identity: one idle list per `(user, database, upstream instance)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PoolKey {
    pub user: String,
    pub database: String,
    pub target: UpstreamTarget,
}

impl PoolKey {
    /// Pool on the primary (the only instance clients are routed to today).
    pub fn new(user: impl Into<String>, database: impl Into<String>) -> Self {
        Self::for_target(user, database, UpstreamTarget::Primary)
    }

    pub fn for_target(
        user: impl Into<String>,
        database: impl Into<String>,
        target: UpstreamTarget,
    ) -> Self {
        Self {
            user: user.into(),
            database: database.into(),
            target,
        }
    }
}
