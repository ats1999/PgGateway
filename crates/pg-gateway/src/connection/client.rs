use std::net::SocketAddr;

use anyhow::Context;
use bytes::Bytes;
use pg_protocol::{
    read_backend, read_frontend, read_startup_request, relay_session, write_backend,
    write_frontend, write_gssenc_response, write_ssl_response, ProtocolError, RelayOutcome,
    StartupRequest,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::startup_parse::parse_startup_params;

/// Client-facing side of a gateway session (Postgres client → gateway).
pub struct ClientConnection {
    stream: TcpStream,
    peer: SocketAddr,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StartupIdentity {
    pub user: String,
    pub database: String,
    pub raw: Bytes,
}

impl ClientConnection {
    pub fn new(stream: TcpStream, peer: SocketAddr) -> Self {
        Self { stream, peer }
    }

    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Handles SSL/GSS negotiation and returns startup identity.
    pub async fn complete_startup(&mut self) -> anyhow::Result<StartupIdentity> {
        let raw = read_startup_loop(&mut self.stream).await?;
        let params = parse_startup_params(&Bytes::from(raw.clone()));
        let user = params
            .get("user")
            .cloned()
            .context("startup missing user")?;
        let database = params
            .get("database")
            .cloned()
            .unwrap_or_else(|| user.clone());
        Ok(StartupIdentity {
            user,
            database,
            raw: Bytes::from(raw),
        })
    }

    /// Replays cached backend auth bytes (pooled upstream path).
    pub async fn replay_auth_flight(&mut self, auth_flight: &Bytes) -> anyhow::Result<()> {
        self.stream
            .write_all(auth_flight)
            .await
            .context("replay auth to client")
    }

    /// Forwards authentication with the real client (password/SCRAM) and records backend bytes for replay.
    pub async fn authenticate_upstream(
        &mut self,
        upstream: &mut TcpStream,
    ) -> anyhow::Result<Bytes> {
        let mut auth_flight = Vec::new();
        loop {
            let msg = read_backend(upstream).await?;
            auth_flight.extend_from_slice(&msg.raw);
            write_backend(&mut self.stream, &msg).await?;
            match msg.tag() {
                b'Z' => break,
                b'E' => anyhow::bail!("upstream rejected authentication"),
                b'R' if auth_request_needs_password(&msg.raw) => {
                    let client_msg = read_frontend(&mut self.stream).await?;
                    write_frontend(upstream, &client_msg).await?;
                }
                _ => {}
            }
        }
        Ok(Bytes::from(auth_flight))
    }

    /// Bidirectional relay after startup/auth; returns the upstream socket for pool release.
    pub async fn relay_with_upstream(
        self,
        upstream: TcpStream,
    ) -> anyhow::Result<TcpStream> {
        let (client_read, client_write) = self.stream.into_split();
        let (server_read, server_write) = upstream.into_split();

        let mut client_read = client_read;
        let mut client_write = client_write;
        let mut server_read = server_read;
        let mut server_write = server_write;

        match relay_session(
            &mut client_read,
            &mut client_write,
            &mut server_read,
            &mut server_write,
        )
        .await
        {
            Ok(RelayOutcome::ClientClosed) => {
                let _ = server_write.shutdown().await;
            }
            Ok(RelayOutcome::ServerClosed) => {
                let _ = client_write.shutdown().await;
            }
            Err(ProtocolError::UnexpectedEof) => {}
            Err(error) => return Err(error.into()),
        }

        server_read
            .reunite(server_write)
            .map_err(|_| anyhow::anyhow!("reunite upstream after relay"))
    }
}

fn auth_request_needs_password(raw: &[u8]) -> bool {
    if raw.first() != Some(&b'R') || raw.len() < 9 {
        return false;
    }
    let auth_type = i32::from_be_bytes([raw[5], raw[6], raw[7], raw[8]]);
    auth_type != 0
}

async fn read_startup_loop(client: &mut TcpStream) -> anyhow::Result<Vec<u8>> {
    loop {
        match read_startup_request(client).await? {
            StartupRequest::SslRequest => {
                write_ssl_response(client, false).await?;
            }
            StartupRequest::GssEncRequest => {
                write_gssenc_response(client, false).await?;
            }
            StartupRequest::Startup(raw) => {
                return Ok(raw.to_vec());
            }
            StartupRequest::CancelRequest(_) => {
                anyhow::bail!("cancel request on main listener is not supported yet");
            }
        }
    }
}
