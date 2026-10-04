//! PostgreSQL frontend/backend wire protocol: startup handling and framed messages.

mod error;
mod frame;
mod relay;
mod startup;
mod stream;

pub use error::ProtocolError;
pub use frame::{
    read_backend, read_frontend, write_backend, write_frontend, BackendMessage, FrontendMessage,
};
pub use relay::{relay_session, RelayOutcome};
pub use startup::{
    read_startup_request, write_gssenc_response, write_ssl_response, StartupRequest,
    SSL_REQUEST_CODE, GSSENC_REQUEST_CODE,
};
pub use stream::{BackendStream, FrontendStream, ServerSide, ClientSide};
