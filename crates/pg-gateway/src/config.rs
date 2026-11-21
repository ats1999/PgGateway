use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

pub use crate::pool_config::{
    merge_with_sources, validate_pool_settings, ConfigLayer, PoolSettings, ResolvedPoolSettings,
    SettingSource,
};

const DEFAULT_LISTEN: &str = "127.0.0.1:6432";
const DEFAULT_PG_PORT: u16 = 5432;

/// Pool mode determines how connections are pinned and released.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PoolMode {
    #[default]
    Session,
    Statement,
    Transaction,
}

impl PoolMode {
    pub fn as_str(self) -> &'static str {
        match self {
            PoolMode::Session => "session",
            PoolMode::Transaction => "transaction",
            PoolMode::Statement => "statement",
        }
    }
}

/// Which upstream instance of a database cluster a pool talks to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum UpstreamTarget {
    #[default]
    Primary,
    Replica(usize),
}

impl fmt::Display for UpstreamTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpstreamTarget::Primary => f.write_str("primary"),
            UpstreamTarget::Replica(i) => write!(f, "replica[{i}]"),
        }
    }
}

/// Top-level gateway configuration (HashiCorp HCL-serializable).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_admin_database")]
    pub admin_database: String,
    /// Global pool defaults; lowest priority in the merge chain.
    #[serde(default)]
    pub pool_defaults: PoolSettings,
    #[serde(default = "default_databases")]
    pub databases: BTreeMap<String, DatabaseCluster>,
}

/// Logical database cluster (primary + replicas).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseCluster {
    /// Actual database name on every instance (PgBouncer `dbname`). Clients connect with the
    /// logical name (the `databases` key); unset = the logical name.
    #[serde(default)]
    pub dbname: Option<String>,
    pub primary: UpstreamEndpoint,
    #[serde(default)]
    pub replicas: Vec<UpstreamEndpoint>,
    /// Server-level pool settings for this logical database (all users on primary path).
    #[serde(default)]
    pub pool_config: PoolSettings,
    #[serde(default)]
    pub userlist: Vec<UserEntry>,
}

/// Upstream host with optional per-instance pool overrides (primary or replica).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UpstreamEndpoint {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub pool_config: PoolSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserEntry {
    pub name: String,
    /// Required (checked by `validate`): the gateway checks every client login against it and
    /// uses it for its own upstream logins. Optional in the type only for a clear config error.
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub pool_config: PoolSettings,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            admin_database: default_admin_database(),
            pool_defaults: PoolSettings::default(),
            databases: default_databases(),
        }
    }
}

impl DatabaseCluster {
    /// Primary first, then every replica.
    pub fn targets(&self) -> Vec<UpstreamTarget> {
        let mut targets = vec![UpstreamTarget::Primary];
        targets.extend((0..self.replicas.len()).map(UpstreamTarget::Replica));
        targets
    }

    /// Users from the userlist; `None` stands for "any user" when the userlist is empty.
    pub fn configured_users(&self) -> Vec<Option<&str>> {
        if self.userlist.is_empty() {
            vec![None]
        } else {
            self.userlist.iter().map(|u| Some(u.name.as_str())).collect()
        }
    }
}

impl UpstreamEndpoint {
    pub fn upstream_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl GatewayConfig {
    pub fn from_hcl_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("read config {}", path.as_ref().display()))?;
        Self::from_hcl_str(&text)
    }

    pub fn from_hcl_str(hcl_config: &str) -> anyhow::Result<Self> {
        let config: Self = hcl::from_str(hcl_config).context("parse gateway config hcl")?;
        config.validate()?;
        Ok(config)
    }

    pub fn load() -> anyhow::Result<Self> {
        match std::env::var("PG_GATEWAY_CONFIG") {
            Ok(path) => Self::from_hcl_file(path),
            Err(_) => {
                let config = Self::default();
                config.validate()?;
                Ok(config)
            }
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self.databases.is_empty() {
            bail!("config must define at least one database under `databases`");
        }
        validate_pool_settings("pool_defaults", ConfigLayer::Global, &self.pool_defaults)?;
        for (name, cluster) in &self.databases {
            validate_pool_settings(name, ConfigLayer::Database, &cluster.pool_config)?;
            validate_pool_settings(
                &format!("{name}/primary"),
                ConfigLayer::Primary,
                &cluster.primary.pool_config,
            )?;
            for (i, replica) in cluster.replicas.iter().enumerate() {
                validate_pool_settings(
                    &format!("{name}/replica[{i}]"),
                    ConfigLayer::Replica(i),
                    &replica.pool_config,
                )?;
            }
            for user in &cluster.userlist {
                if user.password.as_deref().map_or(true, str::is_empty) {
                    bail!(
                        "`{name}/user:{}`: password is required (pg-gateway only supports password \
                         authentication)",
                        user.name
                    );
                }
                validate_pool_settings(
                    &format!("{name}/user:{}", user.name),
                    ConfigLayer::User,
                    &user.pool_config,
                )?;
            }
            // Validate the fully merged result too: min/max can come from different layers.
            for target in cluster.targets() {
                for user in cluster.configured_users() {
                    let merged = self.pool_settings_for(user.unwrap_or(""), name, target)?;
                    let label = format!("{name}/{target}/user:{}", user.unwrap_or("*"));
                    if let (Some(max), Some(min)) = (merged.pool_size, merged.min_pool_size) {
                        if min > max {
                            bail!(
                                "`{label}`: merged min_pool_size ({min}) exceeds pool_size ({max})"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn cluster(&self, database: &str) -> anyhow::Result<&DatabaseCluster> {
        self.databases
            .get(database)
            .with_context(|| format!("unknown database `{database}`"))
    }

    pub fn primary_upstream(&self, database: &str) -> anyhow::Result<String> {
        Ok(self.cluster(database)?.primary.upstream_addr())
    }

    pub fn replicas(&self, database: &str) -> anyhow::Result<&[UpstreamEndpoint]> {
        Ok(self.cluster(database)?.replicas.as_slice())
    }

    pub fn is_admin_database(&self, database: &str) -> bool {
        database == self.admin_database
    }

    pub fn allows_admin_client(&self, user: &str) -> bool {
        self.allows_client(user, "postgres")
    }

    /// Upstream `host:port` for one instance of a database cluster.
    pub fn upstream_addr(&self, database: &str, target: UpstreamTarget) -> anyhow::Result<String> {
        let cluster = self.cluster(database)?;
        match target {
            UpstreamTarget::Primary => Ok(cluster.primary.upstream_addr()),
            UpstreamTarget::Replica(i) => cluster
                .replicas
                .get(i)
                .map(UpstreamEndpoint::upstream_addr)
                .with_context(|| format!("database `{database}` has no replica {i}")),
        }
    }

    /// Actual database name upstream for logical database `database` (its `dbname`, else the
    /// logical name); the same on the primary and every replica.
    pub fn upstream_dbname(&self, database: &str) -> anyhow::Result<String> {
        let cluster = self.cluster(database)?;
        Ok(cluster.dbname.clone().unwrap_or_else(|| database.to_string()))
    }

    /// Userlist password for `user` on logical database `database`, used for gateway-initiated
    /// upstream logins (pool warm-up, transaction/statement re-checkout).
    pub fn user_password(&self, database: &str, user: &str) -> Option<&str> {
        self.databases
            .get(database)?
            .userlist
            .iter()
            .find(|entry| entry.name == user)?
            .password
            .as_deref()
    }

    /// The config layers that apply to one pool, lowest priority first:
    /// `pool_defaults` → database → instance (primary or `replicas[i]`) → userlist entry.
    pub fn pool_layers<'a>(
        &'a self,
        user: &str,
        database: &str,
        target: UpstreamTarget,
    ) -> anyhow::Result<Vec<(ConfigLayer, &'a PoolSettings)>> {
        let cluster = self.cluster(database)?;
        let mut layers = vec![
            (ConfigLayer::Global, &self.pool_defaults),
            (ConfigLayer::Database, &cluster.pool_config),
        ];
        match target {
            UpstreamTarget::Primary => {
                layers.push((ConfigLayer::Primary, &cluster.primary.pool_config));
            }
            UpstreamTarget::Replica(i) => {
                let replica = cluster
                    .replicas
                    .get(i)
                    .with_context(|| format!("database `{database}` has no replica {i}"))?;
                layers.push((ConfigLayer::Replica(i), &replica.pool_config));
            }
        }
        if let Some(entry) = cluster.userlist.iter().find(|e| e.name == user) {
            layers.push((ConfigLayer::User, &entry.pool_config));
        }
        Ok(layers)
    }

    /// Merged (still optional) settings for one pool.
    pub fn pool_settings_for(
        &self,
        user: &str,
        database: &str,
        target: UpstreamTarget,
    ) -> anyhow::Result<PoolSettings> {
        Ok(merge_with_sources(&self.pool_layers(user, database, target)?).0)
    }

    /// Effective value and source layer of every setting for one pool.
    pub fn pool_settings_with_sources(
        &self,
        user: &str,
        database: &str,
        target: UpstreamTarget,
    ) -> anyhow::Result<BTreeMap<&'static str, SettingSource>> {
        Ok(merge_with_sources(&self.pool_layers(user, database, target)?).1)
    }

    /// Runtime settings for one pool.
    pub fn resolved_pool_for(
        &self,
        user: &str,
        database: &str,
        target: UpstreamTarget,
    ) -> anyhow::Result<ResolvedPoolSettings> {
        Ok(self.pool_settings_for(user, database, target)?.resolve())
    }

    /// Database-wide settings (`pool_defaults` → database); used for the database-scoped caps.
    pub fn database_server_settings(&self, database: &str) -> anyhow::Result<PoolSettings> {
        let cluster = self.cluster(database)?;
        Ok(PoolSettings::merge_layers(
            self.pool_defaults.clone(),
            &cluster.pool_config,
        ))
    }

    /// Settings known before the client has said which user/database it wants.
    pub fn startup_settings(&self) -> ResolvedPoolSettings {
        self.pool_defaults.resolve()
    }

    /// Only users listed (with their password) in the database's userlist may connect; an empty
    /// userlist admits nobody.
    pub fn allows_client(&self, user: &str, database: &str) -> bool {
        self.user_password(database, user).is_some()
    }
}

fn default_listen() -> String {
    DEFAULT_LISTEN.to_string()
}

fn default_admin_database() -> String {
    "pg_gateway".to_string()
}

fn default_port() -> u16 {
    DEFAULT_PG_PORT
}

fn default_databases() -> BTreeMap<String, DatabaseCluster> {
    let mut map = BTreeMap::new();
    map.insert(
        "postgres".to_string(),
        DatabaseCluster {
            dbname: None,
            primary: UpstreamEndpoint {
                host: "127.0.0.1".to_string(),
                port: DEFAULT_PG_PORT,
                pool_config: PoolSettings::default(),
            },
            replicas: Vec::new(),
            pool_config: PoolSettings::default(),
            userlist: Vec::new(),
        },
    );
    map
}