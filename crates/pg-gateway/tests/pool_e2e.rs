//! Session pool reuses one upstream TCP connection per user/database key.

#[path = "common/mod.rs"]
mod common;

use std::sync::Arc;
use std::time::Duration;

use pg_gateway::Gateway;
use pg_protocol::{read_backend, read_frontend, read_startup_request, StartupRequest};
use common::config_with_primary;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

#[tokio::test]
async fn pool_reuses_upstream_after_discard() {
    let mock = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mock_addr = mock.local_addr().unwrap();

    let mock_task = tokio::spawn(async move {
        let (mut upstream, _) = mock.accept().await.unwrap();

        let startup = read_startup_request(&mut upstream).await.unwrap();
        assert!(matches!(startup, StartupRequest::Startup(_)));

        send_auth_ok(&mut upstream).await;

        loop {
            let msg = read_frontend(&mut upstream).await.unwrap();
            if msg.tag() != b'Q' {
                continue;
            }
            upstream
                .write_all(&backend_message(b'C', b"DISCARD ALL\012"))
                .await
                .unwrap();
            upstream
                .write_all(&backend_message(b'Z', b"I"))
                .await
                .unwrap();
        }
    });

    let config = config_with_primary("127.0.0.1:0", "pooldb", &mock_addr.to_string());
    let gateway = Arc::new(Gateway::new(config).unwrap());

    let gateway_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_addr = gateway_listener.local_addr().unwrap();

    let gateway_task = tokio::spawn({
        let gateway = std::sync::Arc::clone(&gateway);
        async move {
            let (client, peer) = gateway_listener.accept().await.unwrap();
            gateway.serve(client, peer).await.unwrap();
        }
    });

    let mut client = TcpStream::connect(gateway_addr).await.unwrap();
    client_startup(&mut client, "pooluser", "pooldb").await;
    read_auth_flight(&mut client).await;
    client.shutdown().await.unwrap();

    timeout(Duration::from_secs(5), gateway_task)
        .await
        .expect("gateway timed out")
        .unwrap();

    let gateway_listener2 = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_addr2 = gateway_listener2.local_addr().unwrap();

    let gateway_task2 = tokio::spawn({
        let gateway = std::sync::Arc::clone(&gateway);
        async move {
            let (client, peer) = gateway_listener2.accept().await.unwrap();
            gateway.serve(client, peer).await.unwrap();
        }
    });

    let mut client2 = TcpStream::connect(gateway_addr2).await.unwrap();
    client_startup(&mut client2, "pooluser", "pooldb").await;
    read_auth_flight(&mut client2).await;
    client2.shutdown().await.unwrap();

    timeout(Duration::from_secs(5), gateway_task2)
        .await
        .expect("gateway2 timed out")
        .unwrap();

    mock_task.abort();
}

async fn client_startup(client: &mut TcpStream, user: &str, database: &str) {
    let mut params = Vec::new();
    params.extend_from_slice(format!("user\0{user}\0database\0{database}\0\0").as_bytes());
    let startup = startup_packet(3 << 16, &params);
    client.write_all(&startup).await.unwrap();
}

async fn read_auth_flight(client: &mut TcpStream) {
    loop {
        let msg = read_backend(client).await.unwrap();
        if msg.tag() == b'Z' {
            break;
        }
    }
}

async fn send_auth_ok(upstream: &mut TcpStream) {
    upstream
        .write_all(&backend_message(b'R', &[0, 0, 0, 0]))
        .await
        .unwrap();
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
}

fn backend_message(tag: u8, body: &[u8]) -> Vec<u8> {
    let len = (4 + body.len()) as i32;
    let mut out = Vec::with_capacity(1 + 4 + body.len());
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(body);
    out
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
