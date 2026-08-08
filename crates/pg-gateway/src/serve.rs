use std::net::SocketAddr;

use anyhow::Context;
use pg_protocol::{
    read_startup_request, relay_session, write_gssenc_response, write_ssl_response,
    ProtocolError, RelayOutcome, StartupRequest,
};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

pub async fn run_listener(listen: &str, upstream: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("bind {listen}"))?;
    let upstream = upstream.to_string();

    loop {
        let (client, peer) = listener.accept().await.context("accept")?;
        let upstream = upstream.clone();
        tokio::spawn(async move {
            let _ = serve_connection(client, peer, &upstream).await;
        });
    }
}

pub async fn serve_connection(
    mut client: TcpStream,
    _peer: SocketAddr,
    upstream_addr: &str,
) -> anyhow::Result<()> {
    let mut upstream = TcpStream::connect(upstream_addr)
        .await
        .with_context(|| format!("connect upstream {upstream_addr}"))?;

    complete_client_startup(&mut client, &mut upstream)
        .await
        .context("startup")?;

    let (client_read, client_write) = client.into_split();
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

    Ok(())
}

async fn complete_client_startup(
    client: &mut TcpStream,
    upstream: &mut TcpStream,
) -> anyhow::Result<Vec<u8>> {
    loop {
        match read_startup_request(client).await? {
            StartupRequest::SslRequest => {
                write_ssl_response(client, false).await?;
            }
            StartupRequest::GssEncRequest => {
                write_gssenc_response(client, false).await?;
            }
            StartupRequest::Startup(raw) => {
                let bytes = raw.to_vec();
                upstream
                    .write_all(&bytes)
                    .await
                    .context("forward startup to upstream")?;
                return Ok(bytes);
            }
            StartupRequest::CancelRequest(_) => {
                anyhow::bail!("cancel request on main listener is not supported yet");
            }
        }
    }
}
