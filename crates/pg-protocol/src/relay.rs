use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::ProtocolError;
use crate::frame::{read_backend, read_frontend, write_backend, write_frontend};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayOutcome {
    ClientClosed,
    ServerClosed,
}

pub async fn relay_session<CIn, COut, SIn, SOut>(
    client_read: &mut CIn,
    client_write: &mut COut,
    server_read: &mut SIn,
    server_write: &mut SOut,
) -> Result<RelayOutcome, ProtocolError>
where
    CIn: AsyncRead + Unpin,
    COut: AsyncWrite + Unpin,
    SIn: AsyncRead + Unpin,
    SOut: AsyncWrite + Unpin,
{
    let client_to_server = relay_frontend_to_backend(client_read, server_write);
    let server_to_client = relay_backend_to_frontend(server_read, client_write);

    tokio::pin!(client_to_server);
    tokio::pin!(server_to_client);

    tokio::select! {
        result = &mut client_to_server => {
            result?;
            Ok(RelayOutcome::ClientClosed)
        }
        result = &mut server_to_client => {
            result?;
            Ok(RelayOutcome::ServerClosed)
        }
    }
}

async fn relay_frontend_to_backend<R, W>(reader: &mut R, writer: &mut W) -> Result<(), ProtocolError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let message = read_frontend(reader).await?;
        if message.tag() == b'X' {
            write_frontend(writer, &message).await?;
            break;
        }
        write_frontend(writer, &message).await?;
    }
    Ok(())
}

async fn relay_backend_to_frontend<R, W>(reader: &mut R, writer: &mut W) -> Result<(), ProtocolError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        let message = match read_backend(reader).await {
            Ok(message) => message,
            Err(ProtocolError::UnexpectedEof) => break,
            Err(error) => return Err(error),
        };
        write_backend(writer, &message).await?;
    }
    Ok(())
}
