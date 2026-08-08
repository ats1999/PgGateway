//! End-to-end: client → pg-gateway → mock upstream Postgres.

use std::time::Duration;

use pg_protocol::{read_backend, read_frontend, read_startup_request, StartupRequest};
use pg_gateway::serve_connection;
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

        // AuthenticationOk
        upstream.write_all(&backend_message(b'R', &[0, 0, 0, 0])).await.unwrap();
        // BackendKeyData pid=1 secret=2
        upstream
            .write_all(&backend_message(
                b'K',
                &[0, 0, 0, 1, 0, 0, 0, 0, 2],
            ))
            .await
            .unwrap();
        // ParameterStatus
        upstream
            .write_all(&backend_message(b'S', b"server_version\0129.0\012"))
            .await
            .unwrap();
        // ReadyForQuery
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

    let gateway = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_addr = gateway.local_addr().unwrap();
    let upstream = mock_addr.to_string();

    let gateway_task = tokio::spawn(async move {
        let (client, peer) = gateway.accept().await.unwrap();
        serve_connection(client, peer, &upstream).await.unwrap();
    });

    let mut client = TcpStream::connect(gateway_addr).await.unwrap();

    let mut params = Vec::new();
    params.extend_from_slice(b"user\0test\0\0");
    let startup = startup_packet(3 << 16, &params);
    client.write_all(&startup).await.unwrap();

    let auth = read_backend(&mut client).await.unwrap();
    assert_eq!(auth.tag(), b'R');
    let _ = read_backend(&mut client).await.unwrap(); // K
    let _ = read_backend(&mut client).await.unwrap(); // S
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
