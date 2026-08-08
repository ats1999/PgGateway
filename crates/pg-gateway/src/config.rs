use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

const DEFAULT_LISTEN: &str = "127.0.0.1:6432";
const DEFAULT_PG_PORT: u16 = 5432;

/// Top-level gateway configuration (YAML-serializable).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatewayConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_databases")]
    pub databases: BTreeMap<String, DatabaseCluster>,
    #[serde(default)]
    pub users: Vec<UserEntry>,
}

/// Logical database: one primary and zero or more read replicas (replicas unused until routing exists).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DatabaseCluster {
    pub primary: HostPort,
    #[serde(default)]
    pub replicas: Vec<HostPort>,
    #[serde(default)]
    pub pool: PoolSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostPort {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

/// Per-database pool limits (optional; not all enforced yet).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolSettings {
    #[serde(default)]
    pub max_connections: Option<u32>,
    #[serde(default)]
    pub max_idle: Option<u32>,
}

/// Allowed client identity; password reserved for future pooler-side auth.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserEntry {
    pub name: String,
    pub database: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub pool: PoolSettings,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            databases: default_databases(),
            users: Vec::new(),
        }
    }
}

impl HostPort {
    pub fn upstream_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl GatewayConfig {
    pub fn from_yaml_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path.as_ref())
            .with_context(|| format!("read config {}", path.as_ref().display()))?;
        Self::from_yaml_str(&text)
    }

    pub fn from_yaml_str(yaml: &str) -> anyhow::Result<Self> {
        let config: Self = serde_yaml::from_str(yaml).context("parse gateway config yaml")?;
        config.validate()?;
        Ok(config)
    }

    pub fn load() -> anyhow::Result<Self> {
        match std::env::var("PG_GATEWAY_CONFIG") {
            Ok(path) => Self::from_yaml_file(path),
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

    pub fn replicas(&self, database: &str) -> anyhow::Result<&[HostPort]> {
        Ok(self.cluster(database)?.replicas.as_slice())
    }

    /// When `users` is empty, all client users are allowed (dev convenience).
    pub fn allows_client(&self, user: &str, database: &str) -> bool {
        if self.users.is_empty() {
            return true;
        }
        self.users
            .iter()
            .any(|entry| entry.name == user && entry.database == database)
    }
}

fn default_listen() -> String {
    DEFAULT_LISTEN.to_string()
}

fn default_port() -> u16 {
    DEFAULT_PG_PORT
}

fn default_databases() -> BTreeMap<String, DatabaseCluster> {
    let mut map = BTreeMap::new();
    map.insert(
        "postgres".to_string(),
        DatabaseCluster {
            primary: HostPort {
                host: "127.0.0.1".to_string(),
                port: DEFAULT_PG_PORT,
            },
            replicas: Vec::new(),
            pool: PoolSettings::default(),
        },
    );
    map
}
