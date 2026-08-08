//! PostgreSQL proxy gateway with session pooling.
//!
//! # Library usage
//!
//! ```no_run
//! use pg_gateway::{Gateway, GatewayConfig};
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = GatewayConfig::from_yaml_str(
//!         r#"
//!         listen: "127.0.0.1:6432"
//!         databases:
//!           postgres:
//!             primary:
//!               host: 127.0.0.1
//!               port: 5432
//!             replicas: []
//!         users:
//!           - name: postgres
//!             database: postgres
//!         "#,
//!     )?;
//!     let gateway = Gateway::new(config)?;
//!     gateway.run().await
//! }
//! ```

mod config;
mod connection;
mod gateway;
mod pool;
mod startup_parse;

pub use config::{
    DatabaseCluster, GatewayConfig, HostPort, PoolSettings, UserEntry,
};
pub use connection::{ClientConnection, ServerConnection, StartupIdentity};
pub use gateway::{serve_connection, Gateway};
pub use pool::{PoolKey, PoolManager, PooledServerConnection, UserDatabasePool};
pub use startup_parse::{build_startup_packet, parse_startup_params};
