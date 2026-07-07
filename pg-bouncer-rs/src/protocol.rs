use anyhow::{bail, Context, Result};
use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const PROTOCOL_VERSION_3: i32 = 196608;

#[derive(Debug, Clone)]
pub struct StartupMessage {
    pub params: HashMap<String, String>,
}

impl StartupMessage {
    pub fn user(&self) -> &str {
        self.params.get("user").map(String::as_str).unwrap_or("postgres")
    }

    pub fn database(&self) -> &str {
        self.params
            .get("database")
            .map(String::as_str)
            .unwrap_or(self.user())
    }

    pub fn encode(&self) -> BytesMut {
        let mut payload = BytesMut::new();
        payload.put_i32(PROTOCOL_VERSION_3);
        for (k, v) in &self.params {
            payload.put_slice(k.as_bytes());
            payload.put_u8(0);
            payload.put_slice(v.as_bytes());
            payload.put_u8(0);
        }
        payload.put_u8(0);
        let mut msg = BytesMut::with_capacity(4 + payload.len());
        msg.put_u32((payload.len() + 4) as u32);
        msg.put_slice(&payload);
        msg
    }
}

pub async fn read_startup(stream: &mut tokio::net::TcpStream) -> Result<StartupMessage> {
    loop {
        let len = stream.read_i32().await.context("startup length")? as usize;
        if len == 8 {
            let code = stream.read_i32().await.context("ssl code")?;
            if code == 80877103 {
                write_ssl_decline(stream).await?;
                continue;
            }
            bail!("unknown special packet code {code}");
        }
        if len < 8 {
            bail!("invalid startup length {len}");
        }
        let mut body = vec![0u8; len - 4];
        stream.read_exact(&mut body).await.context("startup body")?;
        let mut params = HashMap::new();
        let mut i = 4;
        while i < body.len() && body[i] != 0 {
            let key = read_cstr(&body, &mut i)?;
            let val = read_cstr(&body, &mut i)?;
            params.insert(key, val);
        }
        return Ok(StartupMessage { params });
    }
}

fn read_cstr(body: &[u8], i: &mut usize) -> Result<String> {
    let start = *i;
    while *i < body.len() && body[*i] != 0 {
        *i += 1;
    }
    if *i >= body.len() {
        bail!("unterminated cstring");
    }
    let s = std::str::from_utf8(&body[start..*i])?.to_string();
    *i += 1;
    Ok(s)
}

#[derive(Debug, Clone)]
pub struct PgMessage {
    pub tag: u8,
    pub body: Bytes,
}

impl PgMessage {
    pub fn encode(&self) -> BytesMut {
        let mut buf = BytesMut::with_capacity(1 + 4 + self.body.len());
        buf.put_u8(self.tag);
        buf.put_u32((self.body.len() + 4) as u32);
        buf.put_slice(&self.body);
        buf
    }
}

pub async fn read_message(stream: &mut tokio::net::TcpStream) -> Result<Option<PgMessage>> {
    let mut tag_buf = [0u8; 1];
    match stream.read_exact(&mut tag_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = stream.read_u32().await.context("message length")? as usize;
    if len < 4 {
        bail!("invalid message length {len}");
    }
    let mut body = vec![0u8; len - 4];
    stream.read_exact(&mut body).await.context("message body")?;
    Ok(Some(PgMessage {
        tag: tag_buf[0],
        body: Bytes::from(body),
    }))
}

pub async fn write_message(stream: &mut tokio::net::TcpStream, msg: &PgMessage) -> Result<()> {
    stream.write_all(&msg.encode()).await?;
    Ok(())
}

pub async fn write_message_raw(stream: &mut tokio::net::TcpStream, tag: u8, body: &[u8]) -> Result<()> {
    write_message(
        stream,
        &PgMessage {
            tag,
            body: Bytes::copy_from_slice(body),
        },
    )
    .await
}

pub fn auth_ok() -> PgMessage {
    PgMessage {
        tag: b'R',
        body: Bytes::from_static(&[0, 0, 0, 0]),
    }
}

pub fn ready_for_query_idle() -> PgMessage {
    PgMessage {
        tag: b'Z',
        body: Bytes::from_static(b"I"),
    }
}

pub fn parameter_status(name: &str, value: &str) -> PgMessage {
    let mut body = BytesMut::new();
    body.put_slice(name.as_bytes());
    body.put_u8(0);
    body.put_slice(value.as_bytes());
    body.put_u8(0);
    PgMessage {
        tag: b'S',
        body: body.freeze(),
    }
}

pub fn backend_key_data(pid: i32, key: i32) -> PgMessage {
    let mut body = BytesMut::with_capacity(8);
    body.put_i32(pid);
    body.put_i32(key);
    PgMessage {
        tag: b'K',
        body: body.freeze(),
    }
}

pub fn error_response(message: &str) -> PgMessage {
    let mut body = BytesMut::new();
    body.put_u8(b'M');
    body.put_slice(message.as_bytes());
    body.put_u8(0);
    body.put_u8(0);
    PgMessage {
        tag: b'E',
        body: body.freeze(),
    }
}

pub fn row_description_and_data(rows: &[(&str, &str)]) -> Vec<PgMessage> {
    let mut rd = BytesMut::new();
    rd.put_i16(1);
    rd.put_slice(b"result");
    rd.put_u8(0);
    rd.put_i32(0);
    rd.put_i16(0);
    rd.put_i32(25);
    rd.put_i16(-1);
    rd.put_i32(-1);
    rd.put_i16(0);
    let mut msgs = vec![PgMessage {
        tag: b'T',
        body: rd.freeze(),
    }];
    for (_, val) in rows {
        let mut data = BytesMut::new();
        data.put_i16(1);
        data.put_i32(val.len() as i32);
        data.put_slice(val.as_bytes());
        msgs.push(PgMessage {
            tag: b'D',
            body: data.freeze(),
        });
    }
    msgs.push(PgMessage {
        tag: b'C',
        body: Bytes::from_static(b"SELECT 0\0"),
    });
    msgs
}

pub fn ready_for_query_status(body: &Bytes) -> Option<u8> {
    body.first().copied()
}

pub fn query_sql(body: &[u8]) -> &str {
    let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
    std::str::from_utf8(&body[..end]).unwrap_or("").trim()
}

pub fn is_query(msg: &PgMessage) -> bool {
    msg.tag == b'Q'
}

pub fn is_terminate(msg: &PgMessage) -> bool {
    msg.tag == b'X'
}

pub fn is_begin_query(msg: &PgMessage) -> bool {
    if msg.tag != b'Q' {
        return false;
    }
    let sql = query_sql(msg.body.as_ref()).to_ascii_lowercase();
    sql.starts_with("begin") || sql.starts_with("start transaction")
}

pub fn is_commit_or_rollback(msg: &PgMessage) -> bool {
    if msg.tag != b'Q' {
        return false;
    }
    let sql = query_sql(msg.body.as_ref())
        .trim_end_matches(';')
        .to_ascii_lowercase();
    sql == "commit" || sql == "rollback"
}

pub async fn write_ssl_decline(stream: &mut tokio::net::TcpStream) -> Result<()> {
    stream.write_all(b"N").await?;
    Ok(())
}
