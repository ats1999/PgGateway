use std::net::SocketAddr;

use anyhow::Context;
use pg_protocol::{read_frontend, FrontendMessage, ProtocolError};
use tokio::net::TcpStream;

use crate::admin::registry::ClientGuard;
use crate::admin::wire::{write_auth_ok_and_ready, write_error_response, write_select_result};
use crate::connection::ClientConnection;
use crate::connection::StartupIdentity;
use crate::Gateway;

/// Virtual admin database: `Query` messages run against SQLite; rows go back as Postgres protocol.
pub async fn serve_admin_connection(
    mut client: ClientConnection,
    _peer: SocketAddr,
    gateway: &Gateway,
    _identity: StartupIdentity,
    client_guard: ClientGuard,
) -> anyhow::Result<()> {
    let stream = client.stream_mut();
    write_auth_ok_and_ready(stream).await?;

    loop {
        match read_frontend(stream).await {
            Ok(msg) => {
                if msg.tag() == b'X' {
                    break;
                }
                handle_admin_message(&msg, gateway, stream).await?;
            }
            Err(ProtocolError::UnexpectedEof) => break,
            Err(e) => return Err(e.into()),
        }
    }
    drop(client_guard);
    Ok(())
}

async fn handle_admin_message(
    msg: &FrontendMessage,
    gateway: &Gateway,
    client: &mut TcpStream,
) -> anyhow::Result<()> {
    match msg.tag() {
        b'Q' => {
            let sql = parse_simple_query(msg.raw.as_ref())?;
            let store = gateway.admin_store();
            let clients = gateway.clients().snapshot().await;
            store
                .refresh(gateway.config(), gateway.pools(), &clients)
                .await?;
            match store.query(sql) {
                Ok((columns, rows)) => {
                    write_select_result(client, &columns, &rows).await?;
                }
                Err(error) => {
                    write_error_response(client, &format!("{error:#}")).await?;
                }
            }
        }
        _ => {
            write_error_response(client, "only simple query protocol is supported on admin db")
                .await?;
        }
    }
    Ok(())
}

fn parse_simple_query(raw: &[u8]) -> anyhow::Result<&str> {
    if raw.first() != Some(&b'Q') || raw.len() < 6 {
        anyhow::bail!("invalid query message");
    }
    let body = &raw[5..];
    let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
    std::str::from_utf8(&body[..end]).context("query not utf-8")
}