use std::collections::BTreeMap;

use pg_gateway::{DatabaseCluster, GatewayConfig, UpstreamEndpoint, UserEntry};
use pg_protocol::read_backend;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Password of the one userlist user in [`config_with_primary`].
pub const TEST_PASSWORD: &str = "test-secret";

/// One database on `upstream`, admitting `user` with [`TEST_PASSWORD`].
pub fn config_with_primary(listen: &str, database: &str, upstream: &str, user: &str) -> GatewayConfig {
    let mut databases = BTreeMap::new();
    let (host, port) = parse_host_port(upstream);
    databases.insert(
        database.to_string(),
        DatabaseCluster {
            dbname: None,
            primary: UpstreamEndpoint {
                host,
                port,
                pool_config: Default::default(),
            },
            replicas: Vec::new(),
            pool_config: Default::default(),
            userlist: vec![UserEntry {
                name: user.to_string(),
                password: Some(TEST_PASSWORD.to_string()),
                pool_config: Default::default(),
            }],
        },
    );
    GatewayConfig {
        listen: listen.to_string(),
        databases,
        ..Default::default()
    }
}

fn parse_host_port(addr: &str) -> (String, u16) {
    match addr.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().expect("port")),
        None => (addr.to_string(), 5432),
    }
}

/// Answer the gateway's md5 password request (sent right after startup) as `user`.
#[allow(dead_code)]
pub async fn answer_password(client: &mut TcpStream, user: &str, password: &str) {
    let request = read_backend(client).await.unwrap();
    assert_eq!(request.tag(), b'R');
    let code = i32::from_be_bytes([request.raw[5], request.raw[6], request.raw[7], request.raw[8]]);
    assert_eq!(code, 5, "gateway must ask for an md5 password");
    let salt = [request.raw[9], request.raw[10], request.raw[11], request.raw[12]];
    let hash = postgres_protocol::authentication::md5_hash(user.as_bytes(), password.as_bytes(), salt);
    let mut reply = vec![b'p'];
    reply.extend_from_slice(&((4 + hash.len() + 1) as i32).to_be_bytes());
    reply.extend_from_slice(hash.as_bytes());
    reply.push(0);
    client.write_all(&reply).await.unwrap();
}
