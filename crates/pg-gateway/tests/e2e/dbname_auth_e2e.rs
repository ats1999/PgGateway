//! DB e2e: logical database names (`dbname`) and gateway-initiated logins with the userlist
//! password, against real Postgres. `gw-logical` (see `docker/integration-gateway.hcl.tpl`) maps
//! to database `gw_actual`; role `gw_secure` must authenticate with SCRAM-SHA-256 upstream (see
//! `docker/integration-entrypoint.sh`). Run via `scripts/run-integration-tests.sh`.

#[path = "common/integration.rs"]
mod integration;

use std::time::{Duration, Instant};

use integration::{
    admin_scalar_i64, attach_gateway, connect_capturing, direct_dsn, error_text,
    simple_query_one_i32, simple_query_one_string, skip_unless_db_e2e, IntegrationTarget,
};
use tokio::time::sleep;

const LOGICAL_POOL: &str = "FROM pools WHERE database = 'gw-logical' AND target = 'primary'";

fn logical_dsn(target: &IntegrationTarget, password: &str) -> String {
    format!(
        "host=127.0.0.1 port={} user=gw_secure password={password} dbname=gw-logical",
        target.gateway_port
    )
}

fn upstream_dsn(target: &IntegrationTarget, password: &str) -> String {
    format!(
        "host=127.0.0.1 port={} user=gw_secure password={password} dbname=gw_actual",
        target.pg_port
    )
}

async fn query_string(dsn: &str, sql: &str) -> String {
    let (client, _task) = connect_capturing(dsn).await.expect("connect");
    simple_query_one_string(&client, sql).await.expect(sql)
}

/// Upstream sessions the gateway holds as gw_secure on gw_actual (counted on Postgres itself).
async fn upstream_sessions(target: &IntegrationTarget) -> i32 {
    let (client, _task) = connect_capturing(&direct_dsn(target)).await.expect("direct connect");
    simple_query_one_i32(
        &client,
        "SELECT count(*)::int FROM pg_stat_activity \
         WHERE usename = 'gw_secure' AND datname = 'gw_actual' AND pid <> pg_backend_pid()",
    )
    .await
    .expect("count sessions")
}

async fn wait_until<F, Fut>(limit: Duration, mut probe: F) -> i64
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = i64>,
{
    let deadline = Instant::now() + limit;
    loop {
        let value = probe().await;
        if value >= 2 || Instant::now() > deadline {
            return value;
        }
        sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test]
async fn upstream_really_requires_the_password() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("restart gateway");
    let wrong = connect_capturing(&upstream_dsn(&target, "nope")).await;
    assert!(wrong.is_err(), "gw_secure must not log in to Postgres with a wrong password");
    let right = query_string(&upstream_dsn(&target, "gw_secret"), "SELECT current_user").await;
    assert_eq!(right, "gw_secure");
}

#[tokio::test]
async fn logical_database_name_reaches_dbname() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("restart gateway");
    let dsn = logical_dsn(&target, "gw_secret");
    assert_eq!(query_string(&dsn, "SELECT current_database()").await, "gw_actual");
    assert_eq!(query_string(&dsn, "SELECT current_user").await, "gw_secure");
}

#[tokio::test]
async fn min_pool_size_prewarms_with_userlist_scram_password() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("restart gateway");

    // No client has connected to gw-logical: these logins come from background warm-up.
    let held = wait_until(Duration::from_secs(20), || async {
        i64::from(upstream_sessions(&target).await)
    })
    .await;
    assert!(held >= 2, "gateway should hold min_pool_size SCRAM logins upstream, got {held}");

    let idle = wait_until(Duration::from_secs(5), || async {
        admin_scalar_i64(&target, &format!("SELECT idle_count {LOGICAL_POOL}"))
            .await
            .unwrap_or(-1)
    })
    .await;
    assert!(idle >= 2, "admin pools should show the warmed connections idle, got {idle}");
}

#[tokio::test]
async fn transaction_mode_relogins_use_dbname_and_password() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("transaction").expect("restart gateway");
    let dsn = logical_dsn(&target, "gw_secret");

    // More concurrent clients than warmed connections, each running several transactions, so
    // the gateway has to open extra upstreams with its own login mid-session.
    let mut clients = Vec::new();
    for _ in 0..5 {
        let dsn = dsn.clone();
        clients.push(tokio::spawn(async move {
            let (client, _task) = connect_capturing(&dsn).await.expect("connect");
            let mut databases = Vec::new();
            for _ in 0..5 {
                let db = simple_query_one_string(
                    &client,
                    "SELECT current_database() FROM pg_sleep(0.05)",
                )
                .await
                .expect("query");
                databases.push(db);
            }
            databases
        }));
    }
    for client in clients {
        let databases = client.await.expect("client task");
        assert!(databases.iter().all(|db| db == "gw_actual"), "{databases:?}");
    }
    assert!(upstream_sessions(&target).await >= 2);
}

/// The gateway's error for a login that must fail.
async fn login_error(dsn: &str) -> String {
    match connect_capturing(dsn).await {
        Ok((client, _task)) => {
            let db = simple_query_one_string(&client, "SELECT current_database()").await;
            panic!("login must be rejected, but the client got in: {db:?}");
        }
        Err(error) => error_text(&error),
    }
}

#[tokio::test]
async fn wrong_password_is_rejected_even_with_a_warm_pool() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("restart gateway");
    // Pooled, already-authenticated upstreams are waiting: the bypass this guards against.
    let held = wait_until(Duration::from_secs(20), || async {
        i64::from(upstream_sessions(&target).await)
    })
    .await;
    assert!(held >= 2, "pool should be warm first, got {held}");

    let wrong = login_error(&logical_dsn(&target, "nope")).await;
    assert!(wrong.contains("28P01"), "{wrong}");
    assert!(wrong.contains("password authentication failed"), "{wrong}");

    let empty = login_error(&format!(
        "host=127.0.0.1 port={} user=gw_secure dbname=gw-logical",
        target.gateway_port
    ))
    .await;
    assert!(!empty.is_empty(), "a missing password must be rejected");

    // The right password still gets a pooled connection.
    let dsn = logical_dsn(&target, "gw_secret");
    assert_eq!(query_string(&dsn, "SELECT current_database()").await, "gw_actual");
}

#[tokio::test]
async fn transaction_mode_rejects_wrong_password() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("transaction").expect("restart gateway");
    let wrong = login_error(&logical_dsn(&target, "nope")).await;
    assert!(wrong.contains("28P01"), "{wrong}");
}

fn dsn_for(target: &IntegrationTarget, user: &str, password: &str, dbname: &str) -> String {
    format!(
        "host=127.0.0.1 port={} user={user} password={password} dbname={dbname}",
        target.gateway_port
    )
}

#[tokio::test]
async fn gateway_checks_password_even_when_upstream_trusts_the_user() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("restart gateway");
    // Postgres trusts `postgres` on 127.0.0.1, so only the gateway can refuse this login.
    let wrong = login_error(&dsn_for(&target, "postgres", "nope", "pg-gateway")).await;
    assert!(wrong.contains("28P01"), "{wrong}");
    let right = query_string(&dsn_for(&target, "postgres", "postgres", "pg-gateway"), "SELECT 'ok'").await;
    assert_eq!(right, "ok");
}

#[tokio::test]
async fn admin_database_checks_password() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("restart gateway");
    let wrong = login_error(&dsn_for(&target, "postgres", "nope", "pg_gateway")).await;
    assert!(wrong.contains("28P01"), "{wrong}");
    assert!(admin_scalar_i64(&target, "SELECT 1").await.is_ok(), "right password still works");
}

#[tokio::test]
async fn user_not_in_userlist_is_refused() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("restart gateway");
    let refused = login_error(&dsn_for(&target, "gw_secure", "gw_secret", "pg-gateway")).await;
    assert!(refused.contains("28000"), "{refused}");
}
