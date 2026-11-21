//! Cancellation-safe message reader.
//!
//! `pg_protocol::read_frontend`/`read_backend` use `read_exact`, which loses already-read bytes
//! if its future is dropped — exactly what `tokio::select!` does to the losing branch. The relay
//! loop races client reads, server reads, and timeout timers, so it reads through a
//! [`FrameReader`] instead: partial messages stay in the reader's buffer across cancellations.

use bytes::{Bytes, BytesMut};
use pg_protocol::ProtocolError;
use tokio::io::{AsyncRead, AsyncReadExt};

const READ_CHUNK: usize = 8 * 1024;

#[derive(Default)]
pub struct FrameReader {
    buf: BytesMut,
}

impl FrameReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Next complete tagged message (`tag + length + body`), or `None` on clean EOF.
    pub async fn next(
        &mut self,
        reader: &mut (impl AsyncRead + Unpin),
    ) -> Result<Option<Bytes>, ProtocolError> {
        loop {
            if let Some(frame) = self.take_frame()? {
                return Ok(Some(frame));
            }
            if self.buf.capacity() - self.buf.len() < READ_CHUNK / 2 {
                self.buf.reserve(READ_CHUNK);
            }
            // `read_buf` is cancellation-safe: bytes it read are already in `self.buf`.
            let read = reader.read_buf(&mut self.buf).await?;
            if read == 0 {
                return if self.buf.is_empty() {
                    Ok(None)
                } else {
                    Err(ProtocolError::UnexpectedEof)
                };
            }
        }
    }

    fn take_frame(&mut self) -> Result<Option<Bytes>, ProtocolError> {
        if self.buf.len() < 5 {
            return Ok(None);
        }
        let len = i32::from_be_bytes([self.buf[1], self.buf[2], self.buf[3], self.buf[4]]);
        if len < 4 {
            return Err(ProtocolError::InvalidLength(len));
        }
        let total = 1 + len as usize;
        if self.buf.len() < total {
            return Ok(None);
        }
        Ok(Some(self.buf.split_to(total).freeze()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn message(tag: u8, body: &[u8]) -> Vec<u8> {
        let mut out = vec![tag];
        out.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
        out.extend_from_slice(body);
        out
    }

    #[tokio::test]
    async fn reassembles_split_messages_and_reports_eof() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        let mut bytes = message(b'Q', b"SELECT 1\0");
        bytes.extend(message(b'X', b""));
        tokio::spawn(async move {
            for chunk in bytes.chunks(3) {
                tx.write_all(chunk).await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        let mut reader = FrameReader::new();
        assert_eq!(reader.next(&mut rx).await.unwrap().unwrap()[0], b'Q');
        assert_eq!(reader.next(&mut rx).await.unwrap().unwrap()[0], b'X');
        assert!(reader.next(&mut rx).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn survives_cancellation_mid_message() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        let full = message(b'Q', b"SELECT 42\0");
        tx.write_all(&full[..4]).await.unwrap();

        let mut reader = FrameReader::new();
        // Cancel the read after it has consumed the first bytes.
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            reader.next(&mut rx),
        )
        .await;
        assert!(cancelled.is_err());

        tx.write_all(&full[4..]).await.unwrap();
        let frame = reader.next(&mut rx).await.unwrap().unwrap();
        assert_eq!(&frame[..], &full[..]);
    }

    #[tokio::test]
    async fn eof_mid_message_is_an_error() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_all(&message(b'Q', b"SELECT 1\0")[..6]).await.unwrap();
        drop(tx);
        let mut reader = FrameReader::new();
        assert!(matches!(
            reader.next(&mut rx).await,
            Err(ProtocolError::UnexpectedEof)
        ));
    }
}
