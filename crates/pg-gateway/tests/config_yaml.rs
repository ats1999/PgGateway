use pg_gateway::GatewayConfig;

#[test]
fn yaml_round_trip() {
    let yaml = r#"
listen: "0.0.0.0:6432"
databases:
  postgres:
    primary:
      host: db-primary
      port: 5432
    replicas:
      - host: db-replica
        port: 5432
    pool:
      max_connections: 40
users:
  - name: app
    database: postgres
    password: secret
"#;
    let config = GatewayConfig::from_yaml_str(yaml).unwrap();
    assert_eq!(config.listen, "0.0.0.0:6432");
    assert_eq!(config.databases["postgres"].primary.host, "db-primary");
    assert_eq!(config.databases["postgres"].replicas.len(), 1);
    assert_eq!(
        config.databases["postgres"].pool.max_connections,
        Some(40)
    );
    assert_eq!(config.users.len(), 1);
    assert_eq!(config.primary_upstream("postgres").unwrap(), "db-primary:5432");
}

#[test]
fn defaults_when_fields_omitted() {
    let config = GatewayConfig::from_yaml_str("{}").unwrap();
    assert_eq!(config.listen, "127.0.0.1:6432");
    assert!(config.databases.contains_key("postgres"));
    assert_eq!(
        config.primary_upstream("postgres").unwrap(),
        "127.0.0.1:5432"
    );
}

#[test]
fn rejects_unknown_database_lookup() {
    let config = GatewayConfig::default();
    assert!(config.primary_upstream("missing").is_err());
}

#[test]
fn user_allowlist() {
    let yaml = r#"
databases:
  postgres:
    primary:
      host: h
      port: 5432
users:
  - name: alice
    database: postgres
"#;
    let config = GatewayConfig::from_yaml_str(yaml).unwrap();
    assert!(config.allows_client("alice", "postgres"));
    assert!(!config.allows_client("bob", "postgres"));
}
