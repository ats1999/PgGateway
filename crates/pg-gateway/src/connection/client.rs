use std::net::SocketAddr;

use anyhow::Context;
use bytes::Bytes;
use pg_protocol::{
    read_frontend, read_startup_request, relay_session, write_gssenc_response, write_ssl_response,
    ProtocolError, RelayOutcome, StartupRequest,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

use crate::startup_parse::parse_startup_params;

const AUTH_MD5_PASSWORD: i32 = 5;

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

    pub(crate) fn stream_mut(&mut self) -> &mut TcpStream {
        &mut self.stream
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

    /// Gateway-side password check against the userlist `password` (PgBouncer-style): md5
    /// challenge with a random salt. `Ok(false)` = wrong or missing password; the caller sends
    /// the FATAL. Nothing is sent on success: the client's AuthenticationOk comes from the
    /// upstream login flight.
    pub async fn verify_password(&mut self, user: &str, password: &str) -> anyhow::Result<bool> {
        let salt: [u8; 4] = rand::random();
        let mut body = AUTH_MD5_PASSWORD.to_be_bytes().to_vec();
        body.extend_from_slice(&salt);
        let mut request = vec![b'R'];
        request.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
        request.extend_from_slice(&body);
        self.stream
            .write_all(&request)
            .await
            .context("send password request")?;
        let reply = read_frontend(&mut self.stream)
            .await
            .context("read client password")?;
        Ok(reply.tag() == b'p' && md5_reply_matches(user, password, salt, &reply.raw))
    }

    /// Replays cached backend auth bytes (pooled upstream path).
    pub async fn replay_auth_flight(&mut self, auth_flight: &Bytes) -> anyhow::Result<()> {
        replay_auth_flight_to_writer(&mut self.stream, auth_flight).await
    }

    /// Consume this client connection and split into read/write halves
    pub fn into_split(
        self,
    ) -> (
        tokio::net::tcp::OwnedReadHalf,
        tokio::net::tcp::OwnedWriteHalf,
    ) {
        self.stream.into_split()
    }

    /// Replay stored auth bytes on the split client path (pooled upstream, client not authenticated yet).
    pub async fn replay_auth_flight_to_client_write(
        client_write: &mut tokio::net::tcp::OwnedWriteHalf,
        auth_flight: &Bytes,
    ) -> anyhow::Result<()> {
        replay_auth_flight_to_writer(client_write, auth_flight).await
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

/// Cached bytes from a previous upstream login (R/S/Z…); psql thinks it talked to Postgres.
async fn replay_auth_flight_to_writer(
    writer: &mut (impl AsyncWriteExt + Unpin),
    auth_flight: &Bytes,
) -> anyhow::Result<()> {
    writer
        .write_all(auth_flight)
        .await
        .context("replay auth to client")
}

/// Does a PasswordMessage (`raw`, full frame) carry the md5 hash of `password` for `salt`?
/// Compared in constant time.
fn md5_reply_matches(user: &str, password: &str, salt: [u8; 4], raw: &[u8]) -> bool {
    let Some(sent) = raw.get(5..).and_then(|body| body.strip_suffix(&[0])) else {
        return false;
    };
    let expected = postgres_protocol::authentication::md5_hash(user.as_bytes(), password.as_bytes(), salt);
    sent.len() == expected.len()
        && sent
            .iter()
            .zip(expected.as_bytes())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
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

#[cfg(test)]
mod tests {
    use super::*;

    fn password_frame(sent: &[u8]) -> Vec<u8> {
        let mut raw = vec![b'p'];
        raw.extend_from_slice(&((4 + sent.len() + 1) as i32).to_be_bytes());
        raw.extend_from_slice(sent);
        raw.push(0);
        raw
    }

    #[test]
    fn md5_reply_must_match_user_password_and_salt() {
        let salt = [9, 8, 7, 6];
        let good = postgres_protocol::authentication::md5_hash(b"svc", b"s3cret", salt);
        assert!(md5_reply_matches("svc", "s3cret", salt, &password_frame(good.as_bytes())));
        assert!(!md5_reply_matches("svc", "wrong", salt, &password_frame(good.as_bytes())));
        assert!(!md5_reply_matches("other", "s3cret", salt, &password_frame(good.as_bytes())));
        assert!(!md5_reply_matches("svc", "s3cret", [0, 0, 0, 0], &password_frame(good.as_bytes())));
        // A cleartext password is not accepted in place of the hash.
        assert!(!md5_reply_matches("svc", "s3cret", salt, &password_frame(b"s3cret")));
        assert!(!md5_reply_matches("svc", "s3cret", salt, &password_frame(b"")));
        assert!(!md5_reply_matches("svc", "s3cret", salt, b"p\0\0"));
    }
}
