//! DB e2e: pool settings enforced by a live pg-gateway process in front of real Postgres.
//! Run via `scripts/run-integration-tests.sh`; every test restarts the gateway with its own
//! settings (see `docker/integration-gateway.hcl.tpl`).

#[path = "common/integration.rs"]
mod integration;

use std::time::{Duration, Instant};

use integration::{
    admin_query_rows, admin_scalar_i64, attach_gateway_with, connect_capturing, direct_dsn,
    error_text, simple_query_one_i32, simple_query_one_string, skip_unless_db_e2e,
    IntegrationTarget, PoolOverrides,
};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

type ConnTask = JoinHandle<Result<(), tokio_postgres::Error>>;

fn start(mode: &str, overrides: PoolOverrides) -> IntegrationTarget {
    attach_gateway_with(mode, &overrides).expect("restart gateway")
}

fn db(lines: &str) -> PoolOverrides {
    PoolOverrides::database(lines)
}

async fn connect(target: &IntegrationTarget) -> (tokio_postgres::Client, ConnTask) {
    connect_capturing(&target.proxy_dsn())
        .await
        .expect("connect through gateway")
}

/// The FATAL the gateway sent on this connection (it closes right after).
async fn fatal_of(task: ConnTask) -> String {
    let result = timeout(Duration::from_secs(10), task)
        .await
        .expect("connection did not end")
        .expect("connection task panicked");
    match result {
        Err(error) => error_text(&error),
        Ok(()) => panic!("connection closed without an error"),
    }
}

async fn wait_for_admin(target: &IntegrationTarget, sql: &str, want: impl Fn(i64) -> bool) -> i64 {
    let deadline = Instant::now() + Duration::from_secs(6);
    loop {
        let value = admin_scalar_i64(target, sql).await.unwrap_or(-1);
        if want(value) || Instant::now() > deadline {
            return value;
        }
        sleep(Duration::from_millis(200)).await;
    }
}

const PRIMARY_POOL: &str = "FROM pools WHERE database = 'pg-gateway' AND target = 'primary'";

#[tokio::test]
async fn query_wait_timeout_when_pool_size_is_exhausted() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("pool_size = 1\nquery_wait_timeout_seconds = 1"));
    let (a, _a_task) = connect(&target).await;
    let (b, b_task) = connect(&target).await;
    simple_query_one_i32(&b, "SELECT 1").await.expect("b first query");

    // `a` holds the only upstream inside a transaction; `b` has to wait for it.
    a.simple_query("BEGIN").await.unwrap();
    let started = Instant::now();
    // The FATAL arrives while `b`'s query is in flight, so it is reported on the query.
    let err = b.simple_query("SELECT 1").await.expect_err("b must time out");
    assert!(started.elapsed() >= Duration::from_millis(900));
    let fatal = error_text(&err);
    assert!(fatal.contains("query_wait_timeout"), "{fatal}");
    let _ = b_task.await;

    let timeouts = admin_scalar_i64(&target, &format!("SELECT wait_timeouts {PRIMARY_POOL}"))
        .await
        .unwrap();
    assert!(timeouts >= 1);
    a.simple_query("COMMIT").await.unwrap();
}

#[tokio::test]
async fn waiting_client_proceeds_when_connection_is_released() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("pool_size = 1"));
    let (a, _a_task) = connect(&target).await;
    let (b, _b_task) = connect(&target).await;
    simple_query_one_i32(&b, "SELECT 1").await.unwrap();

    a.simple_query("BEGIN").await.unwrap();
    let waiter = tokio::spawn(async move { simple_query_one_i32(&b, "SELECT 2").await });
    sleep(Duration::from_millis(300)).await;
    assert!(!waiter.is_finished(), "b must wait while a holds the only connection");
    a.simple_query("COMMIT").await.unwrap();
    let got = timeout(Duration::from_secs(5), waiter).await.unwrap().unwrap();
    assert_eq!(got.unwrap(), 2);
}

#[tokio::test]
async fn max_db_connections_caps_server_connections() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start(
        "transaction",
        db("pool_size = 5\nmax_db_connections = 1\nquery_wait_timeout_seconds = 1"),
    );
    let (a, _a_task) = connect(&target).await;
    let (b, b_task) = connect(&target).await;
    simple_query_one_i32(&b, "SELECT 1").await.unwrap();
    a.simple_query("BEGIN").await.unwrap();
    let err = b.simple_query("SELECT 1").await.expect_err("b must time out");
    assert!(error_text(&err).contains("query_wait_timeout"), "{}", error_text(&err));
    let _ = b_task.await;
    let current = admin_scalar_i64(
        &target,
        "SELECT current_connections FROM databases WHERE name = 'pg-gateway'",
    )
    .await
    .unwrap();
    assert_eq!(current, 1);
}

#[tokio::test]
async fn max_db_client_connections_rejects_extra_clients() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("max_db_client_connections = 1"));
    let (_a, _a_task) = connect(&target).await;
    let err = connect_capturing(&target.proxy_dsn())
        .await
        .err()
        .expect("second client must be rejected");
    let text = error_text(&err);
    assert!(text.contains("max_db_client_connections"), "{text}");
    assert!(text.starts_with("53300"), "{text}");
}

#[tokio::test]
async fn idle_transaction_timeout_disconnects_idle_transaction() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("idle_transaction_timeout_seconds = 1"));
    let (client, task) = connect(&target).await;
    client.simple_query("BEGIN").await.unwrap();
    sleep(Duration::from_millis(1600)).await;
    let fatal = fatal_of(task).await;
    assert!(fatal.contains("idle_transaction_timeout"), "{fatal}");
    assert!(fatal.starts_with("25P03"), "{fatal}");
    // The upstream had an open transaction: it is closed, not pooled.
    let closed = wait_for_admin(
        &target,
        &format!("SELECT closed_in_transaction {PRIMARY_POOL}"),
        |v| v >= 1,
    )
    .await;
    assert!(closed >= 1);
}

#[tokio::test]
async fn transaction_timeout_fires_even_while_a_query_runs() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("transaction_timeout_seconds = 1"));
    let (client, task) = connect(&target).await;
    client.simple_query("BEGIN").await.unwrap();
    let started = Instant::now();
    // A FATAL that arrives while a query is in flight is reported on that query.
    let err = client
        .simple_query("SELECT pg_sleep(5)")
        .await
        .expect_err("transaction_timeout must abort the transaction");
    assert!(started.elapsed() < Duration::from_secs(3), "{:?}", started.elapsed());
    let fatal = error_text(&err);
    assert!(fatal.contains("transaction_timeout"), "{fatal}");
    assert!(fatal.starts_with("25P04"), "{fatal}");
    let _ = task.await;
}

#[tokio::test]
async fn transaction_timeout_does_not_touch_autocommit_queries() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("transaction_timeout_seconds = 1"));
    let (client, _task) = connect(&target).await;
    client.simple_query("SELECT pg_sleep(1.5)").await.expect("no transaction, no timeout");
}

#[tokio::test]
async fn client_idle_timeout_session_mode() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("session", db("client_idle_timeout_seconds = 1"));
    let (client, task) = connect(&target).await;
    simple_query_one_i32(&client, "SELECT 1").await.unwrap();
    sleep(Duration::from_millis(1600)).await;
    let fatal = fatal_of(task).await;
    assert!(fatal.contains("client_idle_timeout"), "{fatal}");
    assert!(fatal.starts_with("57P05"), "{fatal}");
    // Not in a transaction: the upstream goes back to the pool.
    let idle = wait_for_admin(&target, &format!("SELECT idle_count {PRIMARY_POOL}"), |v| v >= 1).await;
    assert_eq!(idle, 1);
}

#[tokio::test]
async fn client_idle_timeout_transaction_mode_between_transactions() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("client_idle_timeout_seconds = 1"));
    let (client, task) = connect(&target).await;
    simple_query_one_i32(&client, "SELECT 1").await.unwrap();
    sleep(Duration::from_millis(1600)).await;
    assert!(fatal_of(task).await.contains("client_idle_timeout"));
}

#[tokio::test]
async fn active_clients_are_not_idle() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("session", db("client_idle_timeout_seconds = 1"));
    let (client, _task) = connect(&target).await;
    // Runs longer than the idle timeout but keeps the client busy the whole time.
    client.simple_query("SELECT pg_sleep(1.5)").await.expect("busy client is not idle");
}

#[tokio::test]
async fn client_login_timeout_closes_silent_connections() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start(
        "transaction",
        PoolOverrides {
            defaults: "client_login_timeout_seconds = 1".to_string(),
            ..PoolOverrides::default()
        },
    );
    let mut raw = TcpStream::connect(("127.0.0.1", target.gateway_port))
        .await
        .unwrap();
    let started = Instant::now();
    let mut buf = Vec::new();
    // Never send a startup packet: the gateway must give up and close the socket.
    timeout(Duration::from_secs(5), raw.read_to_end(&mut buf))
        .await
        .expect("gateway kept a silent connection open")
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(800));
    assert!(String::from_utf8_lossy(&buf).contains("client_login_timeout"));
}

#[tokio::test]
async fn connect_query_runs_on_new_connections() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start(
        "transaction",
        db("connect_query = \"SET application_name = 'gw_connect_query'\""),
    );
    let (client, _task) = connect(&target).await;
    let name = simple_query_one_string(&client, "SHOW application_name").await.unwrap();
    assert_eq!(name, "gw_connect_query");
}

#[tokio::test]
async fn failing_connect_query_is_ignored() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("connect_query = \"SELECT no_such_column\""));
    let (client, _task) = connect(&target).await;
    assert_eq!(simple_query_one_i32(&client, "SELECT 7").await.unwrap(), 7);
}

#[tokio::test]
async fn server_reset_query_runs_in_session_mode() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start(
        "session",
        db("pool_size = 1\nserver_reset_query = \"SET application_name = 'after_reset'\""),
    );
    {
        let (client, _task) = connect(&target).await;
        client.simple_query("SET application_name = 'first_client'").await.unwrap();
    }
    let (client, _task) = connect(&target).await;
    let name = simple_query_one_string(&client, "SHOW application_name").await.unwrap();
    assert_eq!(name, "after_reset");
}

#[tokio::test]
async fn server_reset_query_always_applies_to_transaction_mode() {
    if !skip_unless_db_e2e() {
        return;
    }
    let reset = "server_reset_query = \"SET application_name = 'always_reset'\"";

    // Off (default): no reset in transaction mode, session state leaks to the next transaction.
    let target = start("transaction", db(&format!("pool_size = 1\n{reset}")));
    let (client, _task) = connect(&target).await;
    client.simple_query("SET application_name = 'mine'").await.unwrap();
    let name = simple_query_one_string(&client, "SHOW application_name").await.unwrap();
    assert_eq!(name, "mine");

    // On: reset runs every time the connection goes back to the pool.
    let target = start(
        "transaction",
        db(&format!("pool_size = 1\n{reset}\nserver_reset_query_always = true")),
    );
    let (client, _task) = connect(&target).await;
    client.simple_query("SET application_name = 'mine'").await.unwrap();
    let name = simple_query_one_string(&client, "SHOW application_name").await.unwrap();
    assert_eq!(name, "always_reset");
}

#[tokio::test]
async fn server_check_query_replaces_dead_idle_connection() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start(
        "transaction",
        db("server_check_query = \"SELECT 1\"\nserver_check_delay_seconds = 0"),
    );
    let (client, _task) = connect(&target).await;
    simple_query_one_i32(&client, "SELECT 1").await.unwrap();

    // Kill the pooled upstream behind the gateway's back.
    let (direct, _direct_task) = connect_capturing(&direct_dsn(&target)).await.unwrap();
    let killed = direct
        .simple_query(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
             WHERE datname = 'pg-gateway' AND pid <> pg_backend_pid()",
        )
        .await
        .unwrap();
    assert!(!killed.is_empty());
    sleep(Duration::from_millis(300)).await;

    assert_eq!(simple_query_one_i32(&client, "SELECT 2").await.unwrap(), 2);
    let failed = admin_scalar_i64(&target, &format!("SELECT closed_check_failed {PRIMARY_POOL}"))
        .await
        .unwrap();
    assert_eq!(failed, 1);
}

#[tokio::test]
async fn server_idle_timeout_closes_idle_connections() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("server_idle_timeout_seconds = 1"));
    {
        let (client, _task) = connect(&target).await;
        simple_query_one_i32(&client, "SELECT 1").await.unwrap();
    }
    let closed = wait_for_admin(
        &target,
        &format!("SELECT closed_idle_timeout {PRIMARY_POOL}"),
        |v| v >= 1,
    )
    .await;
    assert!(closed >= 1);
    assert_eq!(
        admin_scalar_i64(&target, &format!("SELECT idle_count {PRIMARY_POOL}"))
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn min_pool_size_opens_connections_before_any_client() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", db("pool_size = 5\nmin_pool_size = 2"));
    let idle = wait_for_admin(&target, &format!("SELECT idle_count {PRIMARY_POOL}"), |v| v >= 2).await;
    assert_eq!(idle, 2);

    let (direct, _task) = connect_capturing(&direct_dsn(&target)).await.unwrap();
    let backends = simple_query_one_i32(
        &direct,
        "SELECT count(*)::int FROM pg_stat_activity WHERE datname = 'pg-gateway'",
    )
    .await
    .unwrap();
    assert_eq!(backends, 2);
}

#[tokio::test]
async fn user_override_beats_database_setting() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start(
        "transaction",
        PoolOverrides {
            database: "pool_size = 5\nquery_wait_timeout_seconds = 1".to_string(),
            user: "pool_size = 1".to_string(),
            ..PoolOverrides::default()
        },
    );
    let (a, _a_task) = connect(&target).await;
    a.simple_query("BEGIN").await.unwrap();
    // The user-level pool_size of 1 applies, so a second client can't get a connection.
    let err = connect_capturing(&target.proxy_dsn())
        .await
        .err()
        .expect("user pool_size = 1 must block the second client");
    assert!(error_text(&err).contains("query_wait_timeout"), "{}", error_text(&err));
}

#[tokio::test]
async fn admin_pool_config_reports_effective_values_and_sources() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start(
        "transaction",
        PoolOverrides {
            defaults: "server_idle_timeout_seconds = 600".to_string(),
            database: "pool_size = 20".to_string(),
            user: "query_wait_timeout_seconds = 9".to_string(),
        },
    );
    let rows = admin_query_rows(
        &target.admin_dsn(),
        "SELECT target, setting, value, source FROM pool_config \
         WHERE database = 'pg-gateway' AND user = 'postgres' \
           AND setting IN ('pool_size', 'server_idle_timeout_seconds', 'min_pool_size', \
                           'pool_mode', 'query_wait_timeout_seconds') \
         ORDER BY target, setting",
    )
    .await
    .unwrap();
    let row = |target: &str, setting: &str| -> (String, String) {
        let found = rows
            .iter()
            .find(|r| r[0].as_deref() == Some(target) && r[1].as_deref() == Some(setting))
            .unwrap_or_else(|| panic!("no {target}/{setting} row in {rows:?}"));
        (found[2].clone().unwrap(), found[3].clone().unwrap())
    };
    assert_eq!(row("primary", "pool_size"), ("20".into(), "database".into()));
    // The replica's own override wins over the database value for replica pools.
    assert_eq!(row("replica[0]", "pool_size"), ("7".into(), "replica[0]".into()));
    assert_eq!(
        row("primary", "server_idle_timeout_seconds"),
        ("600".into(), "pool_defaults".into())
    );
    assert_eq!(
        row("replica[0]", "query_wait_timeout_seconds"),
        ("9".into(), "user".into())
    );
    assert_eq!(row("primary", "pool_mode"), ("transaction".into(), "database".into()));
    assert_eq!(row("primary", "min_pool_size"), ("0".into(), "default".into()));

    // `SHOW POOL_CONFIG` is the psql-friendly alias.
    let all = admin_query_rows(&target.admin_dsn(), "SHOW POOL_CONFIG").await.unwrap();
    assert!(all.len() >= rows.len());
}

#[tokio::test]
async fn admin_clients_show_session_state() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = start("transaction", PoolOverrides::default());
    let (client, _task) = connect(&target).await;
    client.simple_query("BEGIN").await.unwrap();
    sleep(Duration::from_millis(100)).await;
    let rows = admin_query_rows(
        &target.admin_dsn(),
        "SELECT state FROM clients WHERE database = 'pg-gateway'",
    )
    .await
    .unwrap();
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0][0].as_deref(), Some("idle_in_transaction"));
    client.simple_query("COMMIT").await.unwrap();
}
