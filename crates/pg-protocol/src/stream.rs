use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::ProtocolError;
use crate::frame::{
    read_backend, read_frontend, write_backend, write_frontend, BackendMessage, FrontendMessage,
};
use crate::startup::{
    read_startup_request, write_gssenc_response, write_ssl_response, StartupRequest,
};

/// Client-side connection: reads startup + frontend messages, writes backend messages.
pub struct ClientSide<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> ClientSide<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    pub fn split(self) -> (ClientReader<R>, ClientWriter<W>) {
        (ClientReader { reader: self.reader }, ClientWriter { writer: self.writer })
    }

    pub async fn read_startup(&mut self) -> Result<StartupRequest, ProtocolError> {
        read_startup_request(&mut self.reader).await
    }

    pub async fn reject_ssl(&mut self) -> Result<(), ProtocolError> {
        write_ssl_response(&mut self.writer, false).await
    }

    pub async fn reject_gssenc(&mut self) -> Result<(), ProtocolError> {
        write_gssenc_response(&mut self.writer, false).await
    }

    pub async fn read_frontend(&mut self) -> Result<FrontendMessage, ProtocolError> {
        read_frontend(&mut self.reader).await
    }

    pub async fn write_backend(&mut self, message: &BackendMessage) -> Result<(), ProtocolError> {
        write_backend(&mut self.writer, message).await
    }
}

pub struct ClientReader<R> {
    reader: R,
}

impl<R: AsyncRead + Unpin> ClientReader<R> {
    pub async fn read_frontend(&mut self) -> Result<FrontendMessage, ProtocolError> {
        read_frontend(&mut self.reader).await
    }
}

pub struct ClientWriter<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> ClientWriter<W> {
    pub async fn write_backend(&mut self, message: &BackendMessage) -> Result<(), ProtocolError> {
        write_backend(&mut self.writer, message).await
    }
}

/// Server-side (upstream Postgres): writes startup + frontend, reads backend.
pub struct ServerSide<R, W> {
    reader: R,
    writer: W,
}

impl<R, W> ServerSide<R, W>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    pub fn new(reader: R, writer: W) -> Self {
        Self { reader, writer }
    }

    pub fn split(self) -> (ServerReader<R>, ServerWriter<W>) {
        (ServerReader { reader: self.reader }, ServerWriter { writer: self.writer })
    }

    pub async fn write_startup(&mut self, raw: &[u8]) -> Result<(), ProtocolError> {
        use tokio::io::AsyncWriteExt;
        self.writer
            .write_all(raw)
            .await
            .map_err(ProtocolError::Io)
    }

    pub async fn write_frontend(&mut self, message: &FrontendMessage) -> Result<(), ProtocolError> {
        write_frontend(&mut self.writer, message).await
    }

    pub async fn read_backend(&mut self) -> Result<BackendMessage, ProtocolError> {
        read_backend(&mut self.reader).await
    }
}

pub struct ServerReader<R> {
    reader: R,
}

impl<R: AsyncRead + Unpin> ServerReader<R> {
    pub async fn read_backend(&mut self) -> Result<BackendMessage, ProtocolError> {
        read_backend(&mut self.reader).await
    }
}

pub struct ServerWriter<W> {
    writer: W,
}

impl<W: AsyncWrite + Unpin> ServerWriter<W> {
    pub async fn write_frontend(&mut self, message: &FrontendMessage) -> Result<(), ProtocolError> {
        write_frontend(&mut self.writer, message).await
    }
}

/// Alias for client leg after startup completes.
pub type FrontendStream<R, W> = ClientSide<R, W>;

/// Alias for upstream leg after startup completes.
pub type BackendStream<R, W> = ServerSide<R, W>;
