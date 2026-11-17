//! DB e2e: pool modes against real Postgres (`docker-compose.test.yml`).

#[path = "common/integration.rs"]
mod integration;

use integration::{
    attach_gateway, connect_proxy, simple_query_one_i32, simple_query_one_string,
    skip_unless_db_e2e,
};

#[tokio::test]
async fn session_mode_select_via_proxy() {
    if !skip_unless_db_e2e() {
        return;
    }
    run_pool_smoke("session").await;
}

#[tokio::test]
async fn transaction_mode_select_and_txn_via_proxy() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("transaction").expect("attach gateway");
    let dsn = target.proxy_dsn();
    let (client, conn) = connect_proxy(&dsn).await.expect("connect");
    let one = simple_query_one_i32(&client, "SELECT 1::int")
        .await
        .expect("select 1");
    assert_eq!(one, 1);

    simple_query_rows_multi(&client, "BEGIN; SELECT 2::int; COMMIT;")
        .await
        .expect("txn block");
    let three = simple_query_one_i32(&client, "SELECT 3::int")
        .await
        .expect("after commit");
    assert_eq!(three, 3);

    drop(client);
    conn.abort();
}

#[tokio::test]
async fn statement_mode_multiple_simple_queries() {
    if !skip_unless_db_e2e() {
        return;
    }
    run_pool_smoke("statement").await;
}

async fn run_pool_smoke(pool_mode: &str) {
    let target = attach_gateway(pool_mode)
        .unwrap_or_else(|e| panic!("attach gateway ({pool_mode}): {e:#}"));
    let dsn = target.proxy_dsn();
    let (client, conn) = connect_proxy(&dsn).await.expect("connect");

    let v = simple_query_one_i32(&client, "SELECT 10::int")
        .await
        .expect("query");
    assert_eq!(v, 10);

    let name = simple_query_one_string(&client, "SELECT current_database()")
        .await
        .expect("current_database");
    assert_eq!(name, "pg-gateway");

    drop(client);
    conn.abort();
}

async fn simple_query_rows_multi(
    client: &tokio_postgres::Client,
    sql: &str,
) -> anyhow::Result<()> {
    client.simple_query(sql).await?;
    Ok(())
}