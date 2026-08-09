use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("unexpected EOF while reading from connection")]
    UnexpectedEof,

    #[error("invalid message length: {0}")]
    InvalidLength(i32),

    #[error("startup message too short")]
    StartupTooShort,

    #[error("I/O error")]
    Io(#[from] io::Error),
}
