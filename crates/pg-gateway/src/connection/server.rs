use anyhow::Context;
use bytes::{Bytes, BytesMut};
use pg_protocol::{read_backend, write_frontend, FrontendMessage, ProtocolError};
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256, SCRAM_SHA_256};
use postgres_protocol::message::frontend;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use tracing::warn;

use crate::startup_parse::build_startup_packet;

const AUTH_OK: i32 = 0;
const AUTH_CLEARTEXT_PASSWORD: i32 = 3;
const AUTH_MD5_PASSWORD: i32 = 5;
const AUTH_SASL: i32 = 10;
const AUTH_SASL_CONTINUE: i32 = 11;
const AUTH_SASL_FINAL: i32 = 12;

/// Upstream Postgres connection owned by the gateway.
pub struct ServerConnection {
    stream: TcpStream,
}

impl ServerConnection {
    /// Gateway-owned login (pool warm-up and transaction/statement re-checkout): connect with
    /// fixed user/database and consume auth through ReadyForQuery, answering a password request
    /// (cleartext, md5, SCRAM-SHA-256) with the userlist `password`; trust auth needs none.
    /// `database` is the actual upstream name. `connect_timeout` bounds TCP connect *and* login
    /// (PgBouncer `server_connect_timeout`).
    pub async fn connect_and_authenticate(
        upstream_addr: &str,
        user: &str,
        database: &str,
        password: Option<&str>,
        connect_timeout: Option<Duration>,
        connect_query: Option<&str>,
    ) -> anyhow::Result<(Self, Bytes)> {
        let (mut stream, auth_flight) =
            with_timeout(connect_timeout, "upstream connect and login", async {
                login(upstream_addr, user, database, password).await
            })
            .await?;

        if let Some(sql) = connect_query {
            run_connect_query_logged(&mut stream, sql).await;
        }

        Ok((Self { stream }, auth_flight))
    }

    pub fn into_tcp(self) -> TcpStream {
        self.stream
    }

    pub fn from_tcp(stream: TcpStream) -> Self {
        Self { stream }
    }

    /// Reset session state before returning to the idle pool.
    pub async fn prepare_for_reuse(&mut self, reset_sql: &str) -> Result<(), ProtocolError> {
        run_simple_query(&mut self.stream, reset_sql.as_bytes()).await
    }

    /// `server_check_query`: is this idle connection still alive?
    pub async fn check(&mut self, check_sql: &str, limit: Duration) -> anyhow::Result<()> {
        timeout(limit, run_simple_query(&mut self.stream, check_sql.as_bytes()))
            .await
            .context("server_check_query timed out")?
            .map_err(|e| anyhow::anyhow!("server_check_query failed: {e:?}"))
    }
}

async fn login(
    upstream_addr: &str,
    user: &str,
    database: &str,
    password: Option<&str>,
) -> anyhow::Result<(TcpStream, Bytes)> {
    let mut stream = TcpStream::connect(upstream_addr)
        .await
        .with_context(|| format!("connect upstream {upstream_addr}"))?;
    stream
        .write_all(&build_startup_packet(user, database))
        .await
        .context("write pooled startup")?;

    let mut scram: Option<ScramSha256> = None;
    let mut auth_flight = Vec::new();
    loop {
        let msg = read_backend(&mut stream).await?;
        match msg.tag() {
            b'Z' => {
                auth_flight.extend_from_slice(&msg.raw);
                break;
            }
            b'E' => anyhow::bail!("upstream rejected startup for {user}/{database}"),
            b'R' => {
                let code = auth_code(&msg.raw);
                if code == Some(AUTH_OK) {
                    // Clients get the flight replayed on checkout: only AuthenticationOk onward.
                    auth_flight.extend_from_slice(&msg.raw);
                    continue;
                }
                let reply = auth_reply(code, &msg.raw[9..], user, database, password, &mut scram)?;
                if let Some(reply) = reply {
                    stream.write_all(&reply).await.context("send upstream auth reply")?;
                }
            }
            _ => auth_flight.extend_from_slice(&msg.raw),
        }
    }
    Ok((stream, Bytes::from(auth_flight)))
}

/// Answer one upstream authentication request; `None` when no reply is due (SASL final).
fn auth_reply(
    code: Option<i32>,
    payload: &[u8],
    user: &str,
    database: &str,
    password: Option<&str>,
    scram: &mut Option<ScramSha256>,
) -> anyhow::Result<Option<BytesMut>> {
    let need_password = || {
        password.with_context(|| {
            format!(
                "upstream requested password authentication for {user}/{database}; \
                 set `password` for this user in the database's userlist"
            )
        })
    };
    let mut out = BytesMut::new();
    match code {
        Some(AUTH_CLEARTEXT_PASSWORD) => {
            frontend::password_message(need_password()?.as_bytes(), &mut out)?;
        }
        Some(AUTH_MD5_PASSWORD) => {
            let salt: [u8; 4] = payload
                .get(..4)
                .and_then(|s| s.try_into().ok())
                .context("malformed md5 auth request")?;
            let hash = md5_hash(user.as_bytes(), need_password()?.as_bytes(), salt);
            frontend::password_message(hash.as_bytes(), &mut out)?;
        }
        Some(AUTH_SASL) => {
            let offers_scram = payload
                .split(|&b| b == 0)
                .any(|mechanism| mechanism == SCRAM_SHA_256.as_bytes());
            anyhow::ensure!(
                offers_scram,
                "upstream offered no supported SASL mechanism for {user}/{database}"
            );
            let session = ScramSha256::new(need_password()?.as_bytes(), ChannelBinding::unsupported());
            frontend::sasl_initial_response(SCRAM_SHA_256, session.message(), &mut out)?;
            *scram = Some(session);
        }
        Some(AUTH_SASL_CONTINUE) => {
            let session = scram.as_mut().context("SASL continue before SASL start")?;
            session.update(payload).context("SCRAM server-first message")?;
            frontend::sasl_response(session.message(), &mut out)?;
        }
        Some(AUTH_SASL_FINAL) => {
            let mut session = scram.take().context("SASL final before SASL start")?;
            session.finish(payload).context("SCRAM server signature")?;
            return Ok(None);
        }
        other => anyhow::bail!(
            "unsupported upstream authentication request {other:?} for {user}/{database}"
        ),
    }
    Ok(Some(out))
}

fn auth_code(raw: &[u8]) -> Option<i32> {
    (raw.len() >= 9).then(|| i32::from_be_bytes([raw[5], raw[6], raw[7], raw[8]]))
}

async fn with_timeout<T>(
    limit: Option<Duration>,
    what: &str,
    fut: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    match limit {
        Some(limit) => timeout(limit, fut)
            .await
            .map_err(|_| anyhow::anyhow!("{what} timed out after {}s", limit.as_secs()))?,
        None => fut.await,
    }
}

async fn run_connect_query_logged(stream: &mut TcpStream, sql: &str) {
    if let Err(error) = run_simple_query(stream, sql.as_bytes()).await {
        warn!("connect_query failed (ignored): {error:?}");
    }
}

/// Send one simple query and read through `ReadyForQuery`. On `ErrorResponse` the rest of the
/// response is still drained so the connection is left at a clean message boundary.
async fn run_simple_query(stream: &mut TcpStream, sql: &[u8]) -> Result<(), ProtocolError> {
    let query = FrontendMessage {
        raw: Bytes::from(frontend_query(sql)),
    };
    write_frontend(stream, &query).await?;

    let mut failed = false;
    loop {
        let msg = read_backend(stream).await?;
        match msg.tag() {
            b'Z' if failed => return Err(ProtocolError::UnexpectedEof),
            b'Z' => return Ok(()),
            b'E' => failed = true,
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

#[cfg(test)]
#[path = "server_tests.rs"]
mod tests;
