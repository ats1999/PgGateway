use pg_gateway::GatewayConfig;

#[test]
fn hcl_round_trip() {
    let hcl_config = r#"
listen = "0.0.0.0:6432"
databases = {
  postgres = {
    primary = { host = "db-primary", port = 5432 }
    replicas = [{ host = "db-replica", port = 5432 }]
    pool_config = { max_connections = 40 }
    userlist = [{ name = "app", password = "secret" }]
  }
}
"#;
    let config = GatewayConfig::from_hcl_str(hcl_config).unwrap();
    assert_eq!(config.listen, "0.0.0.0:6432");
    assert_eq!(config.databases["postgres"].primary.host, "db-primary");
    assert_eq!(config.databases["postgres"].replicas.len(), 1);
    assert_eq!(config.databases["postgres"].pool_config.pool_size, Some(40));
    assert_eq!(config.databases["postgres"].userlist.len(), 1);
    assert_eq!(config.primary_upstream("postgres").unwrap(), "db-primary:5432");
}

#[test]
fn defaults_when_fields_omitted() {
    let config = GatewayConfig::from_hcl_str("").unwrap();
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
fn rejects_invalid_min_pool_size() {
    let hcl_config = r#"
databases = {
  postgres = {
    primary = { host = "h", port = 5432 }
    pool_config = { max_connections = 5, min_pool_size = 10 }
  }
}
"#;
    assert!(GatewayConfig::from_hcl_str(hcl_config).is_err());
}

#[test]
fn user_allowlist() {
    let hcl_config = r#"
databases = {
  postgres = {
    primary = { host = "h", port = 5432 }
    userlist = [{ name = "alice", password = "alice" }]
  }
}
"#;
    let config = GatewayConfig::from_hcl_str(hcl_config).unwrap();
    assert!(config.allows_client("alice", "postgres"));
    assert!(!config.allows_client("bob", "postgres"));
}

const LAYERED: &str = r#"
pool_defaults = {
  pool_size = 100
  server_idle_timeout_seconds = 600
}
databases = {
  app = {
    primary = { host = "p", port = 5432, pool_config = { server_connect_timeout_seconds = 5 } }
    replicas = [
      { host = "r0", port = 5432, pool_config = { pool_size = 30 } },
      { host = "r1", port = 5432 },
    ]
    pool_config = { pool_size = 50, max_db_connections = 200, pool_mode = "transaction" }
    userlist = [
      { name = "reporting", password = "r", pool_config = { pool_size = 5, query_wait_timeout_seconds = 3 } },
      { name = "web", password = "w" },
    ]
  }
}
"#;

#[test]
fn layered_resolution_global_database_instance_user() {
    use pg_gateway::UpstreamTarget::{Primary, Replica};
    let config = GatewayConfig::from_hcl_str(LAYERED).unwrap();

    let web_primary = config.resolved_pool_for("web", "app", Primary).unwrap();
    assert_eq!(web_primary.pool_size, 50, "database beats pool_defaults");
    assert_eq!(web_primary.server_connect_timeout, Some(std::time::Duration::from_secs(5)));
    assert_eq!(web_primary.pool_mode, pg_gateway::PoolMode::Transaction);

    let web_r0 = config.resolved_pool_for("web", "app", Replica(0)).unwrap();
    assert_eq!(web_r0.pool_size, 30, "replica beats database");
    assert_eq!(web_r0.server_connect_timeout, None, "primary override doesn't leak to replicas");

    let web_r1 = config.resolved_pool_for("web", "app", Replica(1)).unwrap();
    assert_eq!(web_r1.pool_size, 50, "replica without overrides inherits the database");

    let reporting_r0 = config.resolved_pool_for("reporting", "app", Replica(0)).unwrap();
    assert_eq!(reporting_r0.pool_size, 5, "user beats replica");
    assert_eq!(reporting_r0.query_wait_timeout, Some(std::time::Duration::from_secs(3)));
    assert_eq!(reporting_r0.server_idle_timeout, Some(std::time::Duration::from_secs(600)));

    assert_eq!(config.upstream_addr("app", Replica(1)).unwrap(), "r1:5432");
    assert!(config.upstream_addr("app", Replica(2)).is_err());
}

#[test]
fn provenance_names_the_winning_layer() {
    use pg_gateway::{ConfigLayer, UpstreamTarget};
    let config = GatewayConfig::from_hcl_str(LAYERED).unwrap();
    let sources = config
        .pool_settings_with_sources("reporting", "app", UpstreamTarget::Replica(0))
        .unwrap();
    assert_eq!(sources["pool_size"].source, ConfigLayer::User);
    assert_eq!(sources["server_idle_timeout_seconds"].source, ConfigLayer::Global);
    assert_eq!(sources["max_db_connections"].source, ConfigLayer::Database);
    assert_eq!(sources["transaction_timeout_seconds"].source, ConfigLayer::Builtin);
    assert_eq!(sources["transaction_timeout_seconds"].value, "disabled");
}

#[test]
fn database_wide_settings_rejected_on_user_and_instance() {
    for snippet in [
        r#"userlist = [{ name = "u", password = "p", pool_config = { max_db_connections = 5 } }]"#,
        r#"primary = { host = "h", pool_config = { max_db_client_connections = 5 } }"#,
    ] {
        let primary = if snippet.starts_with("primary") { "" } else { r#"primary = { host = "h" }"# };
        let hcl = format!("databases = {{\n app = {{\n {primary}\n {snippet}\n }}\n}}");
        let err = GatewayConfig::from_hcl_str(&hcl).unwrap_err();
        assert!(format!("{err:#}").contains("database-wide"), "{err:#}");
    }
}

#[test]
fn merged_min_pool_size_cannot_exceed_merged_pool_size() {
    let hcl = r#"
databases = {
  app = {
    primary = { host = "h" }
    pool_config = { min_pool_size = 10 }
    userlist = [{ name = "u", password = "p", pool_config = { pool_size = 3 } }]
  }
}
"#;
    let err = GatewayConfig::from_hcl_str(hcl).unwrap_err();
    assert!(format!("{err:#}").contains("min_pool_size"), "{err:#}");
}

#[test]
fn reserve_pool_size_is_rejected() {
    let hcl = r#"
databases = {
  app = {
    primary = { host = "h" }
    pool_config = { reserve_pool_size = 5 }
  }
}
"#;
    let err = GatewayConfig::from_hcl_str(hcl).unwrap_err();
    assert!(format!("{err:#}").contains("reserve_pool_size"), "{err:#}");
}

#[test]
fn dbname_is_set_per_database_for_primary_and_replicas() {
    let config = GatewayConfig::from_hcl_str(
        r#"
databases = {
  app = {
    dbname   = "rule_executor"
    primary  = { host = "db-primary" }
    replicas = [{ host = "db-replica" }]
  }
}
"#,
    )
    .unwrap();
    assert_eq!(config.databases["app"].dbname.as_deref(), Some("rule_executor"));
    assert_eq!(config.upstream_dbname("app").unwrap(), "rule_executor");
}

#[test]
fn dbname_defaults_to_logical_name() {
    let config = GatewayConfig::from_hcl_str(
        r#"
databases = {
  app = { primary = { host = "db" } }
}
"#,
    )
    .unwrap();
    assert_eq!(config.databases["app"].dbname, None);
    assert_eq!(config.upstream_dbname("app").unwrap(), "app");
    assert!(config.upstream_dbname("missing").is_err());
}

#[test]
fn user_password_comes_from_the_database_userlist() {
    let config = GatewayConfig::from_hcl_str(
        r#"
databases = {
  app = {
    primary  = { host = "db" }
    userlist = [{ name = "svc", password = "s3cret" }]
  }
  other = {
    primary  = { host = "db" }
    userlist = [{ name = "svc", password = "different" }]
  }
}
"#,
    )
    .unwrap();
    assert_eq!(config.user_password("app", "svc"), Some("s3cret"));
    assert_eq!(config.user_password("other", "svc"), Some("different"));
    assert_eq!(config.user_password("app", "unknown"), None);
    assert_eq!(config.user_password("missing", "svc"), None);
}

#[test]
fn every_userlist_entry_needs_a_password() {
    for entry in [r#"{ name = "svc" }"#, r#"{ name = "svc", password = "" }"#] {
        let hcl = format!(
            "databases = {{\n  app = {{\n    primary  = {{ host = \"db\" }}\n    userlist = [{entry}]\n  }}\n}}\n"
        );
        let error = GatewayConfig::from_hcl_str(&hcl).expect_err("password is mandatory");
        assert!(error.to_string().contains("password is required"), "{error:#}");
    }
}

#[test]
fn only_listed_users_with_passwords_are_allowed() {
    let config = GatewayConfig::from_hcl_str(
        r#"
databases = {
  app = {
    primary  = { host = "db" }
    userlist = [{ name = "svc", password = "s3cret" }]
  }
  empty = {
    primary = { host = "db" }
  }
}
"#,
    )
    .unwrap();
    assert!(config.allows_client("svc", "app"));
    assert!(!config.allows_client("other", "app"));
    assert!(!config.allows_client("svc", "empty"), "an empty userlist admits nobody");
    assert!(!config.allows_client("svc", "missing"));
}
