use anyhow::Context;
use bytes::Bytes;
use pg_protocol::{read_backend, write_frontend, FrontendMessage, ProtocolError};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::startup_parse::build_startup_packet;

/// Upstream Postgres connection owned by the gateway.
pub struct ServerConnection {
    stream: TcpStream,
}

impl ServerConnection {
    /// Pass-through: connect and forward the client's startup packet unchanged.
    pub async fn connect_pass_through(
        upstream_addr: &str,
        startup_bytes: &[u8],
    ) -> anyhow::Result<Self> {
        let mut stream = TcpStream::connect(upstream_addr)
            .await
            .with_context(|| format!("connect upstream {upstream_addr}"))?;
        stream
            .write_all(startup_bytes)
            .await
            .context("forward startup to upstream")?;
        Ok(Self { stream })
    }

    /// Pool warm path: connect with fixed user/database and consume auth through ReadyForQuery.
    pub async fn connect_and_authenticate(
        upstream_addr: &str,
        user: &str,
        database: &str,
    ) -> anyhow::Result<(Self, Bytes)> {
        let startup = build_startup_packet(user, database);
        let mut stream = TcpStream::connect(upstream_addr)
            .await
            .with_context(|| format!("connect upstream {upstream_addr}"))?;
        stream
            .write_all(&startup)
            .await
            .context("write pooled startup")?;

        let mut auth_flight = Vec::new();
        loop {
            let msg = read_backend(&mut stream).await?;
            auth_flight.extend_from_slice(&msg.raw);
            match msg.tag() {
                b'Z' => break,
                b'E' => anyhow::bail!("upstream rejected startup for {user}/{database}"),
                _ => {}
            }
        }

        Ok((Self { stream }, Bytes::from(auth_flight)))
    }

    pub fn into_tcp(self) -> TcpStream {
        self.stream
    }

    pub fn from_tcp(stream: TcpStream) -> Self {
        Self { stream }
    }

    /// Reset session state before returning to the idle pool.
    pub async fn prepare_for_reuse(&mut self) -> Result<(), ProtocolError> {
        discard_all(&mut self.stream).await
    }
}

async fn discard_all(stream: &mut TcpStream) -> Result<(), ProtocolError> {
    let query = FrontendMessage {
        raw: Bytes::from(frontend_query(b"DISCARD ALL")),
    };
    write_frontend(stream, &query).await?;

    loop {
        let msg = read_backend(stream).await?;
        match msg.tag() {
            b'Z' => return Ok(()),
            b'E' => return Err(ProtocolError::UnexpectedEof),
            _ => {}
        }
    }
}

fn frontend_query(sql: &[u8]) -> Vec<u8> {
    let mut body = Vec::from(sql);
    body.push(0);
    let len = (4 + body.len()) as i32;
    let mut out = Vec::with_capacity(1 + 4 + body.len());
    out.push(b'Q');
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&body);
    out
}
