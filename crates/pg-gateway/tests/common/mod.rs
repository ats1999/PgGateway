use std::collections::BTreeMap;

use pg_gateway::{DatabaseCluster, GatewayConfig, HostPort};

pub fn config_with_primary(listen: &str, database: &str, upstream: &str) -> GatewayConfig {
    let mut databases = BTreeMap::new();
    let (host, port) = parse_host_port(upstream);
    databases.insert(
        database.to_string(),
        DatabaseCluster {
            primary: HostPort { host, port },
            replicas: Vec::new(),
            pool: Default::default(),
        },
    );
    GatewayConfig {
        listen: listen.to_string(),
        databases,
        users: Vec::new(),
    }
}

fn parse_host_port(addr: &str) -> (String, u16) {
    match addr.rsplit_once(':') {
        Some((host, port)) => (host.to_string(), port.parse().expect("port")),
        None => (addr.to_string(), 5432),
    }
}
