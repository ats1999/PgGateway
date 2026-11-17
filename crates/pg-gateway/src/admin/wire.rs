use anyhow::Context;
use tokio::io::{AsyncWrite, AsyncWriteExt};

/// PostgreSQL type OIDs we emit.
const OID_TEXT: i32 = 25;
const OID_INT4: i32 = 23;

#[derive(Clone, Copy)]
pub enum FieldFormat {
    Text,
    Int4,
}

pub struct ColumnMeta {
    pub name: String,
    pub format: FieldFormat,
}

fn append_message(tag: u8, body: &[u8], out: &mut Vec<u8>) {
    let len = (4 + body.len()) as i32;
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
}

fn append_cstring(s: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(s.as_bytes());
    out.push(0);
}

fn type_oid(fmt: FieldFormat) -> i32 {
    match fmt {
        FieldFormat::Text => OID_TEXT,
        FieldFormat::Int4 => OID_INT4,
    }
}

/// Build `RowDescription` + `DataRow`* + `CommandComplete` + `ReadyForQuery` for a SELECT.
pub async fn write_select_result(
    writer: &mut (impl AsyncWrite + Unpin),
    columns: &[ColumnMeta],
    rows: &[Vec<Option<String>>],
) -> anyhow::Result<()> {
    let mut buf = Vec::new();

    // RowDescription ('T')
    let mut rd = Vec::new();
    rd.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        append_cstring(&col.name, &mut rd);
        rd.extend_from_slice(&0u32.to_be_bytes()); // table_oid
        rd.extend_from_slice(&0i16.to_be_bytes()); // attnum
        let oid = type_oid(col.format);
        rd.extend_from_slice(&oid.to_be_bytes());
        rd.extend_from_slice(&(-1i16).to_be_bytes()); // typlen
        rd.extend_from_slice(&(-1i32).to_be_bytes()); // typmod
        rd.extend_from_slice(&0i16.to_be_bytes()); // format (text)
    }
    append_message(b'T', &rd, &mut buf);

    for row in rows {
        let mut dr = Vec::new();
        dr.extend_from_slice(&(row.len() as i16).to_be_bytes());
        for cell in row {
            match cell {
                Some(s) => {
                    let bytes = s.as_bytes();
                    dr.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                    dr.extend_from_slice(bytes);
                }
                None => dr.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        append_message(b'D', &dr, &mut buf);
    }

    let tag = format!("SELECT {}", rows.len());
    let mut cc = Vec::new();
    append_cstring(&tag, &mut cc);
    append_message(b'C', &cc, &mut buf);

    append_message(b'Z', b"I", &mut buf);

    writer
        .write_all(&buf)
        .await
        .context("write admin query response")?;
    Ok(())
}

pub async fn write_error_response(
    writer: &mut (impl AsyncWrite + Unpin),
    message: &str,
) -> anyhow::Result<()> {
    let mut body = Vec::new();
    body.push(b'M');
    append_cstring(message, &mut body);
    body.push(0);

    let mut buf = Vec::new();
    append_message(b'E', &body, &mut buf);
    append_message(b'Z', b"I", &mut buf);

    writer.write_all(&buf).await.context("write admin error")?;
    Ok(())
}

pub async fn write_auth_ok_and_ready(
    writer: &mut (impl AsyncWrite + Unpin),
) -> anyhow::Result<()> {
    let mut buf = Vec::new();
    // AuthenticationOk
    let auth_body = 0i32.to_be_bytes().to_vec();
    append_message(b'R', &auth_body, &mut buf);
    // ParameterStatus server_version (minimal)
    let mut ps = Vec::new();
    append_cstring("server_version", &mut ps);
    append_cstring("14.0", &mut ps);
    append_message(b'S', &ps, &mut buf);
    append_message(b'Z', b"I", &mut buf);
    writer
        .write_all(&buf)
        .await
        .context("write admin auth ok")?;
    Ok(())
}