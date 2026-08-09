use std::io;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::ProtocolError;

/// Special startup packet codes (protocol version field).
pub const SSL_REQUEST_CODE: i32 = 80877103;
pub const GSSENC_REQUEST_CODE: i32 = 80877104;
pub const CANCEL_REQUEST_CODE: i32 = 80877102;

/// First message(s) from a client before the normal frontend message stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupRequest {
    SslRequest,
    GssEncRequest,
    /// Full on-wire startup packet (length prefix + protocol version + parameters).
    Startup(Bytes),
    CancelRequest(Bytes),
}

pub fn startup_code(raw: &[u8]) -> Result<i32, ProtocolError> {
    if raw.len() < 8 {
        return Err(ProtocolError::StartupTooShort);
    }
    Ok(i32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]))
}

/// Reads one startup-phase packet (length-prefixed, no message-type byte).
pub async fn read_startup_request(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<StartupRequest, ProtocolError> {
    let len = read_u32(reader).await?;
    if len < 8 {
        return Err(ProtocolError::InvalidLength(len));
    }

    let mut raw = BytesMut::with_capacity(len as usize);
    raw.put_u32(len as u32);
    raw.resize(len as usize, 0);
    reader
        .read_exact(&mut raw[4..])
        .await
        .map_err(map_eof)?;
    let raw = raw.freeze();

    match startup_code(&raw)? {
        SSL_REQUEST_CODE => Ok(StartupRequest::SslRequest),
        GSSENC_REQUEST_CODE => Ok(StartupRequest::GssEncRequest),
        CANCEL_REQUEST_CODE => Ok(StartupRequest::CancelRequest(raw)),
        _ => Ok(StartupRequest::Startup(raw)),
    }
}

pub async fn write_ssl_response(
    writer: &mut (impl AsyncWrite + Unpin),
    allow: bool,
) -> Result<(), ProtocolError> {
    let byte = if allow { b'S' } else { b'N' };
    writer.write_all(&[byte]).await.map_err(ProtocolError::Io)
}

pub async fn write_gssenc_response(
    writer: &mut (impl AsyncWrite + Unpin),
    allow: bool,
) -> Result<(), ProtocolError> {
    write_ssl_response(writer, allow).await
}

async fn read_u32(reader: &mut (impl AsyncRead + Unpin)) -> Result<i32, ProtocolError> {
    let mut buf = [0u8; 4];
    reader.read_exact(&mut buf).await.map_err(map_eof)?;
    Ok(i32::from_be_bytes(buf))
}

fn map_eof(err: io::Error) -> ProtocolError {
    if err.kind() == io::ErrorKind::UnexpectedEof {
        ProtocolError::UnexpectedEof
    } else {
        ProtocolError::Io(err)
    }
}
