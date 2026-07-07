use crate::config::{Config, PoolMode};
use crate::protocol::{self, PgMessage, StartupMessage};
use crate::stats::StatsHandle;
use anyhow::{Context, Result};
use bytes::{BufMut, BytesMut};
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};

pub struct PoolKey {
    pub user: String,
    pub database: String,
}

impl PoolKey {
    pub fn from_startup(startup: &StartupMessage) -> Self {
        Self {
            user: startup.user().to_string(),
            database: startup.database().to_string(),
        }
    }

    pub fn id(&self) -> String {
        format!("{}/{}", self.user, self.database)
    }
}

pub struct BackendConn {
    pub stream: TcpStream,
    pub in_transaction: bool,
}

pub struct ConnectionPool {
    cfg: Arc<Config>,
    stats: StatsHandle,
    idle: Mutex<VecDeque<BackendConn>>,
    active: Mutex<usize>,
    notify: Arc<Notify>,
}

impl ConnectionPool {
    pub fn new(cfg: Arc<Config>, stats: StatsHandle) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            stats,
            idle: Mutex::new(VecDeque::new()),
            active: Mutex::new(0),
            notify: Arc::new(Notify::new()),
        })
    }

    pub async fn acquire(&self, key: &PoolKey, startup: &StartupMessage) -> Result<BackendConn> {
        let pool_stats = self.stats.pool(&key.id());
        loop {
            if let Some(mut conn) = self.idle.lock().await.pop_front() {
                pool_stats.idle.fetch_sub(1, Ordering::Relaxed);
                conn.in_transaction = false;
                return Ok(conn);
            }

            {
                let mut active = self.active.lock().await;
                if *active < self.cfg.default_pool_size {
                    *active += 1;
                    pool_stats.active.fetch_add(1, Ordering::Relaxed);
                    drop(active);
                    let conn = connect_backend(&self.cfg, startup).await?;
                    self.stats
                        .global
                        .server_connections
                        .fetch_add(1, Ordering::Relaxed);
                    return Ok(conn);
                }
            }

            pool_stats.waiting.fetch_add(1, Ordering::Relaxed);
            self.notify.notified().await;
            pool_stats.waiting.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub async fn release(&self, key: &PoolKey, mut conn: BackendConn, force_close: bool) {
        let pool_stats = self.stats.pool(&key.id());

        let keep = !force_close
            && match self.cfg.pool_mode {
                PoolMode::Session => false,
                PoolMode::Transaction => !conn.in_transaction,
                PoolMode::Statement => true,
            };

        if keep {
            if let Err(e) = reset_connection(&mut conn.stream).await {
                tracing::warn!("reset failed, closing server conn: {e}");
                let mut active = self.active.lock().await;
                *active = active.saturating_sub(1);
                pool_stats.active.fetch_sub(1, Ordering::Relaxed);
            } else {
                self.idle.lock().await.push_back(conn);
                pool_stats.idle.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            let mut active = self.active.lock().await;
            *active = active.saturating_sub(1);
            pool_stats.active.fetch_sub(1, Ordering::Relaxed);
        }

        self.notify.notify_one();
    }

    pub fn pool_mode(&self) -> PoolMode {
        self.cfg.pool_mode
    }
}

async fn connect_backend(cfg: &Config, startup: &StartupMessage) -> Result<BackendConn> {
    let addr = format!("{}:{}", cfg.backend_host, cfg.backend_port);
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("connect backend {addr}"))?;

    let mut params = startup.params.clone();
    params.insert("user".into(), cfg.backend_user.clone());
    params.insert("database".into(), startup.database().to_string());
    let backend_startup = StartupMessage { params };
    stream.write_all(&backend_startup.encode()).await?;

    loop {
        let Some(msg) = protocol::read_message(&mut stream).await? else {
            anyhow::bail!("backend closed during auth");
        };
        match msg.tag {
            b'R' => {
                let auth_type = i32::from_be_bytes(msg.body[0..4].try_into()?);
                match auth_type {
                    0 => {}
                    3 => send_password(&mut stream, &cfg.backend_password).await?,
                    5 => {
                        let salt: [u8; 4] = msg.body[4..8].try_into()?;
                        let md5_pass = md5_password(&cfg.backend_password, &cfg.backend_user, &salt);
                        send_password(&mut stream, &md5_pass).await?;
                    }
                    10 => scram_auth(&mut stream, &msg.body[4..], &cfg.backend_password).await?,
                    n => anyhow::bail!("unsupported backend auth type {n}"),
                }
            }
            b'Z' => {
                return Ok(BackendConn {
                    stream,
                    in_transaction: false,
                });
            }
            b'E' => anyhow::bail!("backend error during auth"),
            _ => {}
        }
    }
}

fn md5_password(password: &str, user: &str, salt: &[u8; 4]) -> String {
    let inner = format!("{:x}", md5::compute(format!("{password}{user}")));
    let mut outer_input = inner.into_bytes();
    outer_input.extend_from_slice(salt);
    format!("md5{:x}", md5::compute(outer_input))
}

async fn send_password(stream: &mut TcpStream, password: &str) -> Result<()> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(password.as_bytes());
    payload.put_u8(0);
    protocol::write_message(
        stream,
        &PgMessage {
            tag: b'p',
            body: payload.freeze(),
        },
    )
    .await
}

async fn scram_auth(stream: &mut TcpStream, mechanisms: &[u8], password: &str) -> Result<()> {
    let mech = std::str::from_utf8(mechanisms)
        .unwrap_or("")
        .trim_end_matches('\0');
    if !mech.contains("SCRAM-SHA-256") {
        anyhow::bail!("no SCRAM-SHA-256 mechanism");
    }
    let client_first = crate::scram::client_first();
    send_sasl_initial(stream, client_first.as_bytes()).await?;
    let continue_msg = read_sasl_continue(stream).await?;
    let server_first = String::from_utf8(continue_msg)?;
    let client_final = crate::scram::client_final(password, &client_first, &server_first)?;
    send_sasl_response(stream, client_final.as_bytes()).await?;
    let _final_msg = read_sasl_final(stream).await?;
    Ok(())
}

async fn read_sasl_continue(stream: &mut TcpStream) -> Result<Vec<u8>> {
    loop {
        let Some(msg) = protocol::read_message(stream).await? else {
            anyhow::bail!("backend closed during scram");
        };
        if msg.tag == b'R' {
            let t = i32::from_be_bytes(msg.body[0..4].try_into()?);
            if t == 11 {
                return Ok(msg.body[4..].to_vec());
            }
        }
        if msg.tag == b'E' {
            anyhow::bail!("backend scram error");
        }
    }
}

async fn read_sasl_final(stream: &mut TcpStream) -> Result<Vec<u8>> {
    loop {
        let Some(msg) = protocol::read_message(stream).await? else {
            anyhow::bail!("backend closed during scram");
        };
        if msg.tag == b'R' {
            let t = i32::from_be_bytes(msg.body[0..4].try_into()?);
            if t == 12 {
                return Ok(msg.body[4..].to_vec());
            }
            if t == 0 {
                return Ok(Vec::new());
            }
        }
        if msg.tag == b'E' {
            anyhow::bail!("backend scram final error");
        }
    }
}

async fn send_sasl_initial(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
    let mut body = BytesMut::new();
    body.extend_from_slice(b"SCRAM-SHA-256");
    body.put_u8(0);
    body.put_i32(payload.len() as i32);
    body.extend_from_slice(payload);
    protocol::write_message(
        stream,
        &PgMessage {
            tag: b'p',
            body: body.freeze(),
        },
    )
    .await
}

async fn send_sasl_response(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
    protocol::write_message(
        stream,
        &PgMessage {
            tag: b'p',
            body: bytes::Bytes::copy_from_slice(payload),
        },
    )
    .await
}

async fn reset_connection(stream: &mut TcpStream) -> Result<()> {
    protocol::write_message_raw(stream, b'Q', b"DISCARD ALL;").await?;
    loop {
        let Some(msg) = protocol::read_message(stream).await? else {
            break;
        };
        if msg.tag == b'Z' {
            break;
        }
    }
    Ok(())
}
