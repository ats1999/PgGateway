mod registry;
mod serve;
mod store;
mod wire;

pub use registry::ClientRegistry;
pub use serve::serve_admin_connection;
pub use store::AdminStore;