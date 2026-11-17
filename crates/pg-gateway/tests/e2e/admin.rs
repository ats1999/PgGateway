//! DB e2e: admin virtual DB (`pgbouncer`) against a live gateway process.

#[path = "common/integration.rs"]
mod integration;

use std::time::Duration;

use integration::{
    admin_query_rows, attach_gateway, connect_proxy, skip_unless_db_e2e, IntegrationTarget,
};
use tokio::time::sleep;

#[tokio::test]
async fn admin_show_version() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("transaction").expect("gateway");
    let rows = admin_query_rows(&target.admin_dsn(), "SHOW VERSION")
        .await
        .expect("SHOW VERSION");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0].as_deref(), Some("pg-gateway"));
}

#[tokio::test]
async fn admin_show_databases_lists_pg_gateway() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("transaction").expect("gateway");
    let rows = admin_query_rows(&target.admin_dsn(), "SHOW DATABASES")
        .await
        .expect("SHOW DATABASES");
    let pg_gw = rows
        .iter()
        .find(|row| row.first().map(|n| n.as_deref()) == Some(Some("pg-gateway")))
        .expect("pg-gateway row");
    assert_eq!(pg_gw[1].as_deref(), Some("127.0.0.1"));
    assert_eq!(pg_gw[3].as_deref(), Some("transaction"));
}

#[tokio::test]
async fn admin_show_pools_after_proxy_traffic() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("transaction").expect("gateway");
    let (proxy, proxy_conn) = connect_proxy(&target.proxy_dsn())
        .await
        .expect("proxy");
    integration::simple_query_one_i32(&proxy, "SELECT 1")
        .await
        .expect("warm pool");
    drop(proxy);
    proxy_conn.abort();
    sleep(Duration::from_millis(200)).await;

    let rows = admin_query_rows(&target.admin_dsn(), "SHOW POOLS")
        .await
        .expect("SHOW POOLS");
    assert_eq!(rows.len(), 1, "expected one pool row: {rows:?}");
    assert_eq!(rows[0][0].as_deref(), Some("pg-gateway"));
    assert_eq!(rows[0][1].as_deref(), Some("postgres"));
    assert_eq!(rows[0][2].as_deref(), Some("transaction"));
    let idle_count: i64 = rows[0][4]
        .as_deref()
        .unwrap_or("0")
        .parse()
        .expect("idle_count numeric");
    assert!(idle_count >= 0);
}

#[tokio::test]
async fn admin_show_clients_lists_active_proxy_session() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("session").expect("gateway");
    let hold = hold_proxy_connection(&target).await;
    sleep(Duration::from_millis(300)).await;

    let rows = admin_query_rows(&target.admin_dsn(), "SHOW CLIENTS")
        .await
        .expect("SHOW CLIENTS");
    let proxy_clients: Vec<_> = rows
        .iter()
        .filter(|row| row.get(3).and_then(|d| d.as_deref()) == Some("pg-gateway"))
        .collect();
    assert_eq!(proxy_clients.len(), 1);
    assert_eq!(proxy_clients[0][2].as_deref(), Some("postgres"));

    hold.abort();
}

#[tokio::test]
async fn admin_select_star_tables() {
    if !skip_unless_db_e2e() {
        return;
    }
    let target = attach_gateway("statement").expect("gateway");
    let dsn = target.admin_dsn();

    for sql in [
        "SELECT name, primary_host, primary_port, pool_mode FROM databases ORDER BY name",
        "SELECT database, user, pool_mode, upstream, idle_count FROM pools ORDER BY database, user",
        "SELECT client_id, peer, user, database FROM clients ORDER BY client_id",
    ] {
        let _ = admin_query_rows(&dsn, sql).await.expect(sql);
    }
}

async fn hold_proxy_connection(target: &IntegrationTarget) -> tokio::task::JoinHandle<()> {
    let dsn = target.proxy_dsn();
    tokio::spawn(async move {
        let (client, connection) = connect_proxy(&dsn).await.expect("hold connect");
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("hold: {e}");
            }
        });
        let _ = client.simple_query("SELECT pg_sleep(30)").await;
    })
}