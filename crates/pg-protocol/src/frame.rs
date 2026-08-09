use std::io;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::ProtocolError;

/// A complete frontend (client → server) message, including the type tag and length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontendMessage {
    pub raw: Bytes,
}

impl FrontendMessage {
    pub fn tag(&self) -> u8 {
        self.raw[0]
    }
}

/// A complete backend (server → client) message, including the type tag and length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendMessage {
    pub raw: Bytes,
}

impl BackendMessage {
    pub fn tag(&self) -> u8 {
        self.raw[0]
    }
}

pub async fn read_frontend(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<FrontendMessage, ProtocolError> {
    let raw = read_tagged_message(reader).await?;
    Ok(FrontendMessage { raw })
}

pub async fn read_backend(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<BackendMessage, ProtocolError> {
    let raw = read_tagged_message(reader).await?;
    Ok(BackendMessage { raw })
}

pub async fn write_frontend(
    writer: &mut (impl AsyncWrite + Unpin),
    message: &FrontendMessage,
) -> Result<(), ProtocolError> {
    writer
        .write_all(&message.raw)
        .await
        .map_err(ProtocolError::Io)
}

pub async fn write_backend(
    writer: &mut (impl AsyncWrite + Unpin),
    message: &BackendMessage,
) -> Result<(), ProtocolError> {
    writer
        .write_all(&message.raw)
        .await
        .map_err(ProtocolError::Io)
}

async fn read_tagged_message(
    reader: &mut (impl AsyncRead + Unpin),
) -> Result<Bytes, ProtocolError> {
    let mut header = [0u8; 5];
    reader.read_exact(&mut header).await.map_err(map_eof)?;

    let len = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    if len < 4 {
        return Err(ProtocolError::InvalidLength(len));
    }

    let body_len = len as usize - 4;
    let mut raw = BytesMut::with_capacity(1 + len as usize);
    raw.extend_from_slice(&header);
    raw.resize(1 + len as usize, 0);
    if body_len > 0 {
        reader
            .read_exact(&mut raw[5..])
            .await
            .map_err(map_eof)?;
    }
    Ok(raw.freeze())
}

fn map_eof(err: io::Error) -> ProtocolError {
    if err.kind() == io::ErrorKind::UnexpectedEof {
        ProtocolError::UnexpectedEof
    } else {
        ProtocolError::Io(err)
    }
}
