//! PostgreSQL proxy gateway with session pooling.
//!
//! # Library usage
//!
//! ```no_run
//! use pg_gateway::{Gateway, GatewayConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = GatewayConfig::from_hcl_str(
//!         r#"
//!         listen = "127.0.0.1:6432"
//!         databases = {
//!           postgres = {
//!             primary = { host = "127.0.0.1", port = 5432 }
//!             replicas = []
//!             userlist = [{ name = "postgres", password = "postgres" }]
//!           }
//!         }
//!         "#,
//!     )?;
//!     let gateway = Gateway::new(config)?;
//!     gateway.run().await
//! }
//! ```

mod admin;
mod config;
mod connection;
mod framed;
mod gateway;
mod pool;
mod pool_config;
mod statement_detector;
mod startup_parse;

pub use config::{
    DatabaseCluster, GatewayConfig, PoolMode, UpstreamEndpoint, UpstreamTarget, UserEntry,
};
pub use pool_config::{
    merge_with_sources, ConfigLayer, PoolSettings, ResolvedPoolSettings, SettingSource,
    DATABASE_SCOPED_SETTINGS,
};
pub use connection::{ClientConnection, ServerConnection, StartupIdentity};
pub use gateway::{serve_connection, Gateway, MAINTENANCE_INTERVAL};
pub use pool::{
    Checkout, CheckoutError, ConnectSlot, ConnectionState, PoolKey, PoolManager, PoolSnapshot,
    PooledServerConnection, UserDatabasePool,
};
pub use startup_parse::{build_startup_packet, parse_startup_params};
pub use statement_detector::{
    detect_statement_type, detect_transaction_status, StatementType, TransactionOp,
    TransactionStatus,
};
