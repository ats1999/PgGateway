//! End-to-end: client → pg-gateway → mock upstream Postgres.

#[path = "common/mod.rs"]
mod common;

use std::time::Duration;

use pg_gateway::Gateway;
use pg_protocol::{read_backend, read_frontend, read_startup_request, StartupRequest};
use common::config_with_primary;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

#[tokio::test]
async fn proxy_forwards_startup_and_simple_query() {
    let mock = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock.local_addr().unwrap();

    let mock_task = tokio::spawn(async move {
        let (mut upstream, _) = mock.accept().await.unwrap();
        let startup = read_startup_request(&mut upstream).await.unwrap();
        assert!(matches!(startup, StartupRequest::Startup(_)));

        upstream.write_all(&backend_message(b'R', &[0, 0, 0, 0])).await.unwrap();
        upstream
            .write_all(&backend_message(
                b'K',
                &[0, 0, 0, 1, 0, 0, 0, 0, 2],
            ))
            .await
            .unwrap();
        upstream
            .write_all(&backend_message(b'S', b"server_version\0129.0\012"))
            .await
            .unwrap();
        upstream
            .write_all(&backend_message(b'Z', b"I"))
            .await
            .unwrap();

        let query = read_frontend(&mut upstream).await.unwrap();
        assert_eq!(query.tag(), b'Q');

        upstream
            .write_all(&backend_message(b'C', b"SELECT 1\012"))
            .await
            .unwrap();
        upstream
            .write_all(&backend_message(b'Z', b"I"))
            .await
            .unwrap();
    });

    let gateway_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_addr = gateway_listener.local_addr().unwrap();

    let config = config_with_primary(&gateway_addr.to_string(), "postgres", &mock_addr.to_string());
    let gateway = Gateway::new(config).unwrap();

    let gateway_task = tokio::spawn(async move {
        let (client, peer) = gateway_listener.accept().await.unwrap();
        gateway.serve(client, peer).await.unwrap();
    });

    let mut client = TcpStream::connect(gateway_addr).await.unwrap();

    let mut params = Vec::new();
    params.extend_from_slice(b"user\0test\0database\0postgres\0\0");
    let startup = startup_packet(3 << 16, &params);
    client.write_all(&startup).await.unwrap();

    let auth = read_backend(&mut client).await.unwrap();
    assert_eq!(auth.tag(), b'R');
    let _ = read_backend(&mut client).await.unwrap();
    let _ = read_backend(&mut client).await.unwrap();
    let rfq = read_backend(&mut client).await.unwrap();
    assert_eq!(rfq.tag(), b'Z');

    client
        .write_all(&frontend_message(b'Q', b"SELECT 1\0"))
        .await
        .unwrap();

    let complete = read_backend(&mut client).await.unwrap();
    assert_eq!(complete.tag(), b'C');
    let rfq = read_backend(&mut client).await.unwrap();
    assert_eq!(rfq.tag(), b'Z');

    client.shutdown().await.unwrap();

    timeout(Duration::from_secs(5), async {
        mock_task.await.unwrap();
        gateway_task.await.unwrap();
    })
    .await
    .expect("test timed out");
}

fn backend_message(tag: u8, body: &[u8]) -> Vec<u8> {
    let len = (4 + body.len()) as i32;
    let mut out = Vec::with_capacity(1 + 4 + body.len());
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn frontend_message(tag: u8, body: &[u8]) -> Vec<u8> {
    backend_message(tag, body)
}

fn startup_packet(protocol: i32, params: &[u8]) -> Vec<u8> {
    let body_len = 4 + params.len();
    let len = (4 + body_len) as i32;
    let mut out = Vec::with_capacity(len as usize);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&protocol.to_be_bytes());
    out.extend_from_slice(params);
    out
}
