//! Pool settings, the layered merge, and where each effective value came from.
//!
//! Resolution order (later wins), applied identically everywhere a setting is read:
//!
//! ```text
//! built-in default → pool_defaults → databases.<db>.pool_config
//!                  → primary/replicas[i].pool_config → userlist[user].pool_config
//! ```
//!
//! A few settings are database-wide by definition (`max_db_connections`,
//! `max_db_client_connections`); they may only be set in `pool_defaults` or a database's
//! `pool_config`, and config validation rejects them at the instance or user layer instead of
//! silently ignoring them.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::PoolMode;

/// Settings that only make sense for a whole logical database.
pub const DATABASE_SCOPED_SETTINGS: &[&str] = &["max_db_connections", "max_db_client_connections"];

/// PgBouncer's default `server_check_delay`.
const DEFAULT_SERVER_CHECK_DELAY: Duration = Duration::from_secs(30);

/// Pool / upstream settings. Unset fields inherit from the previous config layer.
///
/// Every `*_seconds` timeout treats `0` as "disabled" (PgBouncer semantics); leaving it unset
/// inherits from the layer below, and unset at every layer means disabled.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PoolSettings {
    /// Per-pool max upstream connections (`user` × `database` × upstream). Alias: `max_connections`.
    #[serde(default, alias = "max_connections", skip_serializing_if = "Option::is_none")]
    pub pool_size: Option<u32>,
    /// Keep at least this many upstream connections open per pool (opened in the background).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_pool_size: Option<u32>,
    /// Max idle upstream connections kept per pool; extra ones are closed on release.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_idle: Option<u32>,
    /// Database-scoped: cap on upstream connections across every pool of this database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_db_connections: Option<u32>,
    /// Database-scoped: cap on client connections to this database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_db_client_connections: Option<u32>,
    /// Run once on every new upstream connection after login; errors are logged and ignored.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_query: Option<String>,
    /// Max time a client may wait for an upstream connection before it is disconnected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_wait_timeout_seconds: Option<u64>,
    /// Max time a client may sit idle inside an open transaction.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_transaction_timeout_seconds: Option<u64>,
    /// Max total time a transaction may stay open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transaction_timeout_seconds: Option<u64>,
    /// Max time a client may stay connected without sending anything (outside a transaction).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_idle_timeout_seconds: Option<u64>,
    /// Run when a connection goes back to the pool (session mode, or always — see below).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_reset_query: Option<String>,
    /// Run `server_reset_query` in every pool mode, not just session mode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_reset_query_always: Option<bool>,
    /// Health check run on an idle connection before reuse; empty string disables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_check_query: Option<String>,
    /// Only check connections that have been idle at least this long (default 30).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_check_delay_seconds: Option<u64>,
    /// Close upstream connections idle longer than this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_idle_timeout_seconds: Option<u64>,
    /// Max time for TCP connect + login to an upstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_connect_timeout_seconds: Option<u64>,
    /// After a failed upstream connect/login, wait this long before trying that pool again.
    #[serde(default, alias = "server_login_retry", skip_serializing_if = "Option::is_none")]
    pub server_login_retry_seconds: Option<u64>,
    /// Max time for a client to finish startup + authentication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_login_timeout_seconds: Option<u64>,
    /// Legacy name; used if `server_idle_timeout_seconds` is unset at every layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_lifetime_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pool_mode: Option<PoolMode>,
}

/// Render a setting value the same way in logs, errors, and the admin database.
trait SettingValue {
    fn render(&self) -> String;
}

impl SettingValue for u32 {
    fn render(&self) -> String {
        self.to_string()
    }
}

impl SettingValue for u64 {
    fn render(&self) -> String {
        self.to_string()
    }
}

impl SettingValue for bool {
    fn render(&self) -> String {
        self.to_string()
    }
}

impl SettingValue for String {
    fn render(&self) -> String {
        self.clone()
    }
}

impl SettingValue for PoolMode {
    fn render(&self) -> String {
        self.as_str().to_string()
    }
}

/// Lists every setting once so merge, provenance, and rendering can't drift apart.
macro_rules! pool_setting_fields {
    ($($field:ident),* $(,)?) => {
        impl PoolSettings {
            /// Every setting name, in declaration order.
            pub const FIELD_NAMES: &'static [&'static str] = &[$(stringify!($field)),*];

            /// Overlay set fields from `upper` onto `base` (later wins).
            pub fn merge_layers(mut base: PoolSettings, upper: &PoolSettings) -> PoolSettings {
                $(
                    if upper.$field.is_some() {
                        base.$field = upper.$field.clone();
                    }
                )*
                base
            }

            /// `(name, rendered value)` for every field explicitly set in this layer.
            pub fn set_fields(&self) -> Vec<(&'static str, String)> {
                let mut out = Vec::new();
                $(
                    if let Some(value) = &self.$field {
                        out.push((stringify!($field), value.render()));
                    }
                )*
                out
            }
        }
    };
}

pool_setting_fields!(
    pool_size,
    min_pool_size,
    max_idle,
    max_db_connections,
    max_db_client_connections,
    connect_query,
    query_wait_timeout_seconds,
    idle_transaction_timeout_seconds,
    transaction_timeout_seconds,
    client_idle_timeout_seconds,
    server_reset_query,
    server_reset_query_always,
    server_check_query,
    server_check_delay_seconds,
    server_idle_timeout_seconds,
    server_connect_timeout_seconds,
    server_login_retry_seconds,
    client_login_timeout_seconds,
    server_lifetime_seconds,
    pool_mode,
);

/// Where a config layer sits in the merge chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ConfigLayer {
    /// Built into the gateway; used when no layer sets the value.
    Builtin,
    /// `pool_defaults`
    Global,
    /// `databases.<db>.pool_config`
    Database,
    /// `databases.<db>.primary.pool_config`
    Primary,
    /// `databases.<db>.replicas[i].pool_config`
    Replica(usize),
    /// `databases.<db>.userlist[name].pool_config`
    User,
}

impl fmt::Display for ConfigLayer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigLayer::Builtin => f.write_str("default"),
            ConfigLayer::Global => f.write_str("pool_defaults"),
            ConfigLayer::Database => f.write_str("database"),
            ConfigLayer::Primary => f.write_str("primary"),
            ConfigLayer::Replica(i) => write!(f, "replica[{i}]"),
            ConfigLayer::User => f.write_str("user"),
        }
    }
}

/// One effective setting and the layer that supplied it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettingSource {
    pub value: String,
    pub source: ConfigLayer,
}

/// Merge `layers` in order and record which layer supplied each effective value.
/// Unset settings report their built-in default with source [`ConfigLayer::Builtin`].
pub fn merge_with_sources(
    layers: &[(ConfigLayer, &PoolSettings)],
) -> (PoolSettings, BTreeMap<&'static str, SettingSource>) {
    let mut merged = PoolSettings::default();
    let mut sources = BTreeMap::new();
    for (layer, settings) in layers {
        merged = PoolSettings::merge_layers(merged, settings);
        for (name, value) in settings.set_fields() {
            sources.insert(
                name,
                SettingSource {
                    value,
                    source: *layer,
                },
            );
        }
    }
    for name in PoolSettings::FIELD_NAMES {
        sources.entry(name).or_insert_with(|| SettingSource {
            value: builtin_default(name).to_string(),
            source: ConfigLayer::Builtin,
        });
    }
    (merged, sources)
}

/// Human-readable built-in default for the admin database.
fn builtin_default(name: &str) -> &'static str {
    match name {
        "pool_size" | "max_idle" | "max_db_connections" | "max_db_client_connections" => {
            "unlimited"
        }
        "min_pool_size" => "0",
        "server_reset_query" => "DISCARD ALL",
        "server_reset_query_always" => "false",
        "server_check_delay_seconds" => "30",
        "pool_mode" => "session",
        "connect_query" | "server_check_query" => "",
        _ => "disabled",
    }
}

/// Runtime values for one pool after merging every layer.
#[derive(Debug, Clone)]
pub struct ResolvedPoolSettings {
    pub pool_size: u32,
    pub min_pool_size: u32,
    pub max_idle: u32,
    pub max_db_connections: Option<u32>,
    pub max_db_client_connections: Option<u32>,
    pub pool_mode: PoolMode,
    pub connect_query: Option<String>,
    pub server_reset_query: String,
    pub server_reset_query_always: bool,
    pub server_check_query: Option<String>,
    pub server_check_delay: Duration,
    pub server_idle_timeout: Option<Duration>,
    pub server_connect_timeout: Option<Duration>,
    pub server_login_retry: Option<Duration>,
    pub query_wait_timeout: Option<Duration>,
    pub idle_transaction_timeout: Option<Duration>,
    pub transaction_timeout: Option<Duration>,
    pub client_idle_timeout: Option<Duration>,
    pub client_login_timeout: Option<Duration>,
}

/// `None` and `Some(0)` both mean disabled.
fn timeout(seconds: Option<u64>) -> Option<Duration> {
    seconds.filter(|s| *s > 0).map(Duration::from_secs)
}

/// Empty strings disable optional queries (PgBouncer semantics).
fn non_empty(query: &Option<String>) -> Option<String> {
    query.as_ref().filter(|q| !q.trim().is_empty()).cloned()
}

impl PoolSettings {
    pub fn resolve(&self) -> ResolvedPoolSettings {
        let reset = self
            .server_reset_query
            .clone()
            .unwrap_or_else(|| "DISCARD ALL".to_string());

        ResolvedPoolSettings {
            pool_size: self.pool_size.unwrap_or(u32::MAX),
            min_pool_size: self.min_pool_size.unwrap_or(0),
            max_idle: self.max_idle.unwrap_or(u32::MAX),
            max_db_connections: self.max_db_connections,
            max_db_client_connections: self.max_db_client_connections,
            pool_mode: self.pool_mode.unwrap_or_default(),
            connect_query: non_empty(&self.connect_query),
            server_reset_query: reset,
            server_reset_query_always: self.server_reset_query_always.unwrap_or(false),
            server_check_query: non_empty(&self.server_check_query),
            server_check_delay: self
                .server_check_delay_seconds
                .map(Duration::from_secs)
                .unwrap_or(DEFAULT_SERVER_CHECK_DELAY),
            server_idle_timeout: timeout(
                self.server_idle_timeout_seconds
                    .or(self.server_lifetime_seconds),
            ),
            server_connect_timeout: timeout(self.server_connect_timeout_seconds),
            server_login_retry: timeout(self.server_login_retry_seconds),
            query_wait_timeout: timeout(self.query_wait_timeout_seconds),
            idle_transaction_timeout: timeout(self.idle_transaction_timeout_seconds),
            transaction_timeout: timeout(self.transaction_timeout_seconds),
            client_idle_timeout: timeout(self.client_idle_timeout_seconds),
            client_login_timeout: timeout(self.client_login_timeout_seconds),
        }
    }
}

impl ResolvedPoolSettings {
    /// Whether `server_reset_query` runs when a connection is released to the pool.
    pub fn needs_server_reset(&self) -> bool {
        if self.server_reset_query.trim().is_empty() {
            return false;
        }
        self.server_reset_query_always || matches!(self.pool_mode, PoolMode::Session)
    }
}

/// Per-layer sanity checks. `layer` decides which settings are allowed at all.
pub fn validate_pool_settings(
    label: &str,
    layer: ConfigLayer,
    pool: &PoolSettings,
) -> anyhow::Result<()> {
    if let (Some(max), Some(min)) = (pool.pool_size, pool.min_pool_size) {
        if min > max {
            anyhow::bail!("`{label}`: min_pool_size ({min}) cannot exceed pool_size ({max})");
        }
    }
    if let (Some(max_idle), Some(max)) = (pool.max_idle, pool.pool_size) {
        if max_idle > max {
            anyhow::bail!("`{label}`: max_idle ({max_idle}) cannot exceed pool_size ({max})");
        }
    }
    if matches!(
        layer,
        ConfigLayer::Primary | ConfigLayer::Replica(_) | ConfigLayer::User
    ) {
        for (name, _) in pool.set_fields() {
            if DATABASE_SCOPED_SETTINGS.contains(&name) {
                anyhow::bail!(
                    "`{label}`: `{name}` is database-wide; set it in `pool_defaults` or the \
                     database's `pool_config`"
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(hcl: &str) -> PoolSettings {
        hcl::from_str(hcl).expect("parse pool settings")
    }

    #[test]
    fn later_layer_wins_and_records_source() {
        let global = settings("pool_size = 100\nserver_idle_timeout_seconds = 600");
        let database = settings("pool_size = 50");
        let replica = settings("pool_size = 20");
        let user = settings("query_wait_timeout_seconds = 5");
        let (merged, sources) = merge_with_sources(&[
            (ConfigLayer::Global, &global),
            (ConfigLayer::Database, &database),
            (ConfigLayer::Replica(1), &replica),
            (ConfigLayer::User, &user),
        ]);
        assert_eq!(merged.pool_size, Some(20));
        assert_eq!(merged.server_idle_timeout_seconds, Some(600));
        assert_eq!(merged.query_wait_timeout_seconds, Some(5));
        assert_eq!(sources["pool_size"].source, ConfigLayer::Replica(1));
        assert_eq!(sources["server_idle_timeout_seconds"].source, ConfigLayer::Global);
        assert_eq!(sources["query_wait_timeout_seconds"].source, ConfigLayer::User);
        assert_eq!(sources["min_pool_size"].source, ConfigLayer::Builtin);
        assert_eq!(sources["min_pool_size"].value, "0");
    }

    #[test]
    fn every_field_has_a_source_row() {
        let (_, sources) = merge_with_sources(&[]);
        assert_eq!(sources.len(), PoolSettings::FIELD_NAMES.len());
        assert!(sources.values().all(|s| s.source == ConfigLayer::Builtin));
    }

    #[test]
    fn zero_disables_timeouts_and_unset_means_disabled() {
        let resolved = settings(
            "query_wait_timeout_seconds = 0\nidle_transaction_timeout_seconds = 3",
        )
        .resolve();
        assert_eq!(resolved.query_wait_timeout, None);
        assert_eq!(resolved.idle_transaction_timeout, Some(Duration::from_secs(3)));
        assert_eq!(resolved.transaction_timeout, None);
        assert_eq!(resolved.server_check_delay, Duration::from_secs(30));
    }

    #[test]
    fn empty_queries_are_disabled() {
        let resolved = settings("connect_query = \"\"\nserver_check_query = \"  \"").resolve();
        assert_eq!(resolved.connect_query, None);
        assert_eq!(resolved.server_check_query, None);
    }

    #[test]
    fn reset_runs_in_session_mode_or_when_always() {
        let session = settings("pool_mode = \"session\"").resolve();
        assert!(session.needs_server_reset());
        let txn = settings("pool_mode = \"transaction\"").resolve();
        assert!(!txn.needs_server_reset());
        let txn_always =
            settings("pool_mode = \"transaction\"\nserver_reset_query_always = true").resolve();
        assert!(txn_always.needs_server_reset());
        let disabled = settings("server_reset_query = \"\"").resolve();
        assert!(!disabled.needs_server_reset());
    }

    #[test]
    fn legacy_aliases_still_parse() {
        let parsed = settings("max_connections = 7\nserver_login_retry = 4");
        assert_eq!(parsed.pool_size, Some(7));
        assert_eq!(parsed.server_login_retry_seconds, Some(4));
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let err = hcl::from_str::<PoolSettings>("reserve_pool_size = 5").unwrap_err();
        assert!(err.to_string().contains("reserve_pool_size"), "{err}");
    }

    #[test]
    fn database_scoped_settings_rejected_below_database_layer() {
        let pool = settings("max_db_connections = 10");
        assert!(validate_pool_settings("db", ConfigLayer::Database, &pool).is_ok());
        assert!(validate_pool_settings("g", ConfigLayer::Global, &pool).is_ok());
        for layer in [ConfigLayer::Primary, ConfigLayer::Replica(0), ConfigLayer::User] {
            let err = validate_pool_settings("x", layer, &pool).unwrap_err();
            assert!(err.to_string().contains("database-wide"), "{err}");
        }
    }

    #[test]
    fn min_pool_size_cannot_exceed_pool_size() {
        let pool = settings("pool_size = 2\nmin_pool_size = 3");
        assert!(validate_pool_settings("db", ConfigLayer::Database, &pool).is_err());
    }
}
