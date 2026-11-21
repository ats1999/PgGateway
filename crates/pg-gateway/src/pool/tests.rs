//! Pool behaviour against an in-process mock Postgres.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use pg_protocol::{read_frontend, read_startup_request, StartupRequest};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use super::*;
use crate::config::GatewayConfig;

/// Mock upstream: trust auth, answers every simple query with `CommandComplete` +
/// `ReadyForQuery`, or with an `ErrorResponse` when the SQL contains `fail`.
struct MockPg {
    addr: String,
    accepted: Arc<AtomicUsize>,
}

impl MockPg {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else { return };
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve_mock(socket));
            }
        });
        Self { addr, accepted }
    }

    fn port(&self) -> u16 {
        self.addr.rsplit_once(':').unwrap().1.parse().unwrap()
    }

    fn accepted(&self) -> usize {
        self.accepted.load(Ordering::SeqCst)
    }
}

fn backend(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    out.extend_from_slice(&((4 + body.len()) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

async fn serve_mock(mut socket: TcpStream) {
    let Ok(StartupRequest::Startup(_)) = read_startup_request(&mut socket).await else { return };
    let mut hello = backend(b'R', &0i32.to_be_bytes());
    hello.extend(backend(b'Z', b"I"));
    if socket.write_all(&hello).await.is_err() {
        return;
    }
    while let Ok(msg) = read_frontend(&mut socket).await {
        if msg.tag() == b'X' {
            return;
        }
        if msg.tag() != b'Q' {
            continue;
        }
        let sql = String::from_utf8_lossy(&msg.raw[5..]).to_lowercase();
        if sql.contains("slow") {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let mut reply = if sql.contains("fail") {
            backend(b'E', b"SERROR\0Mboom\0\0")
        } else {
            backend(b'C', b"OK\0")
        };
        reply.extend(backend(b'Z', b"I"));
        if socket.write_all(&reply).await.is_err() {
            return;
        }
    }
}

/// Config for database `db` on the mock, with `pool` as the database `pool_config` body and
/// optional extra users.
fn config(mock: &MockPg, pool: &str, users: &str) -> Arc<GatewayConfig> {
    let hcl = format!(
        r#"
databases = {{
  db = {{
    primary = {{ host = "127.0.0.1", port = {port} }}
    pool_config = {{
      {pool}
    }}
    userlist = [{users}]
  }}
}}
"#,
        port = mock.port(),
    );
    Arc::new(GatewayConfig::from_hcl_str(&hcl).expect("config"))
}

/// Connect a reserved slot to the mock the way the gateway does.
async fn connect(slot: ConnectSlot) -> PooledServerConnection {
    let pool = Arc::clone(slot.pool());
    let (server, flight) = ServerConnection::connect_and_authenticate(
        pool.upstream(),
        &pool.key().user,
        pool.upstream_database(),
        pool.password(),
        None,
        None,
    )
    .await
    .expect("mock login");
    slot.attach(server.into_tcp(), flight)
}

async fn checkout_connected(manager: &PoolManager, key: &PoolKey) -> PooledServerConnection {
    match manager.checkout(key).await.expect("checkout") {
        Checkout::Connect(slot) => connect(slot).await,
        Checkout::Idle(conn) => conn,
    }
}

async fn release(mut conn: PooledServerConnection) {
    let stream = conn.take_stream();
    conn.release(stream).await;
}

async fn wait_for(condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < deadline, "condition not reached in time");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn key(user: &str) -> PoolKey {
    PoolKey::new(user, "db")
}

fn db_connections(manager: &PoolManager) -> u32 {
    manager.database_state("db").unwrap().server_connections()
}

#[tokio::test]
async fn full_pool_times_out_after_query_wait_timeout() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "pool_size = 1\nquery_wait_timeout_seconds = 1", ""));
    let _held = checkout_connected(&manager, &key("u")).await;

    let started = Instant::now();
    let err = match manager.checkout(&key("u")).await {
        Err(err) => err,
        Ok(_) => panic!("expected query_wait_timeout"),
    };
    assert!(err.is::<CheckoutError>(), "{err:#}");
    assert!(started.elapsed() >= Duration::from_millis(900));
    let pool = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(pool.stats().wait_timeouts.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn waiting_client_gets_the_released_connection() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "pool_size = 1", ""));
    let held = checkout_connected(&manager, &key("u")).await;

    let waiter = {
        let manager = Arc::clone(&manager);
        tokio::spawn(async move { manager.checkout(&key("u")).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!waiter.is_finished(), "pool is full, waiter must block");
    release(held).await;

    let got = tokio::time::timeout(Duration::from_secs(2), waiter)
        .await
        .expect("waiter woke")
        .unwrap()
        .expect("checkout");
    assert!(matches!(got, Checkout::Idle(_)), "reuses the released connection");
    assert_eq!(mock.accepted(), 1);
}

#[tokio::test]
async fn max_db_connections_evicts_idle_from_other_pools() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "max_db_connections = 1", ""));
    release(checkout_connected(&manager, &key("a")).await).await;
    assert_eq!(db_connections(&manager), 1);

    // `b` needs a connection; the database is at its cap, `a`'s idle one makes room.
    let b = checkout_connected(&manager, &key("b")).await;
    assert_eq!(db_connections(&manager), 1);
    let a_pool = manager.pool_for(key("a")).await.unwrap();
    assert_eq!(a_pool.idle_count(), 0);
    assert_eq!(a_pool.stats().closed_evicted.load(Ordering::Relaxed), 1);
    drop(b);
}

#[tokio::test]
async fn max_db_connections_waits_when_every_connection_is_busy() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(
        &mock,
        "max_db_connections = 1\nquery_wait_timeout_seconds = 1",
        "",
    ));
    let _a = checkout_connected(&manager, &key("a")).await;
    assert!(manager.checkout(&key("b")).await.is_err());
}

#[tokio::test]
async fn idle_connections_past_server_idle_timeout_are_closed() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "server_idle_timeout_seconds = 1", ""));
    release(checkout_connected(&manager, &key("u")).await).await;
    let pool = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(pool.prune_idle(), 0, "not idle long enough yet");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(pool.prune_idle(), 1);
    assert_eq!(pool.idle_count(), 0);
    assert_eq!(db_connections(&manager), 0);
    assert_eq!(pool.stats().closed_idle_timeout.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn maintain_opens_min_pool_size_for_configured_users() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(
        &mock,
        "pool_size = 5\nmin_pool_size = 2",
        r#"{ name = "app", password = "app" }"#,
    ));
    // No client has connected yet: maintenance creates the pool and fills it.
    manager.maintain().await;
    let pool = manager.pool_for(key("app")).await.unwrap();
    // `pool_for` also starts a warm-up task; both count reserved slots, so the total is
    // exact even while one of them is still connecting.
    assert_eq!(pool.total_count(), 2);
    wait_for(|| pool.idle_count() == 2).await;
    assert_eq!(mock.accepted(), 2);

    // Taking one out doesn't trigger more: min_pool_size counts every connection.
    let held = checkout_connected(&manager, &key("app")).await;
    manager.maintain().await;
    assert_eq!(pool.total_count(), 2);
    drop(held);
    manager.maintain().await;
    assert_eq!(pool.total_count(), 2, "refilled after a connection was closed");
}

#[tokio::test]
async fn failing_server_check_query_closes_and_reconnects() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(
        &mock,
        "server_check_query = \"select 'fail'\"\nserver_check_delay_seconds = 0",
        "",
    ));
    release(checkout_connected(&manager, &key("u")).await).await;

    let next = manager.checkout(&key("u")).await.unwrap();
    assert!(matches!(next, Checkout::Connect(_)), "bad idle connection must not be reused");
    let pool = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(pool.stats().closed_check_failed.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn passing_server_check_query_reuses_connection() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(
        &mock,
        "server_check_query = \"select 1\"\nserver_check_delay_seconds = 0",
        "",
    ));
    release(checkout_connected(&manager, &key("u")).await).await;
    assert!(matches!(
        manager.checkout(&key("u")).await.unwrap(),
        Checkout::Idle(_)
    ));
}

#[tokio::test]
async fn login_failure_backs_off_for_server_login_retry() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "server_login_retry_seconds = 1", ""));
    match manager.checkout(&key("u")).await.unwrap() {
        Checkout::Connect(slot) => slot.login_failed(),
        Checkout::Idle(_) => panic!("empty pool"),
    }

    let started = Instant::now();
    let retry = manager.checkout(&key("u")).await.unwrap();
    assert!(matches!(retry, Checkout::Connect(_)));
    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "retried after {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn login_backoff_respects_query_wait_timeout() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(
        &mock,
        "server_login_retry_seconds = 30\nquery_wait_timeout_seconds = 1",
        "",
    ));
    if let Checkout::Connect(slot) = manager.checkout(&key("u")).await.unwrap() {
        slot.login_failed();
    }
    assert!(manager.checkout(&key("u")).await.is_err());
}

#[tokio::test]
async fn release_inside_transaction_closes_connection() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "", ""));
    let mut conn = checkout_connected(&manager, &key("u")).await;
    conn.update_state(ConnectionState::InTransaction);
    release(conn).await;
    let pool = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(pool.idle_count(), 0);
    assert_eq!(db_connections(&manager), 0);
    assert_eq!(pool.stats().closed_in_transaction.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn dropped_checkout_frees_its_slot() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "pool_size = 1", ""));
    drop(checkout_connected(&manager, &key("u")).await);
    let pool = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(pool.total_count(), 0);
    assert_eq!(db_connections(&manager), 0);
    // And an unused reserved slot is freed too.
    drop(manager.checkout(&key("u")).await.unwrap());
    assert_eq!(pool.total_count(), 0);
}

#[tokio::test]
async fn failed_reset_query_closes_connection() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "server_reset_query = \"fail reset\"", ""));
    release(checkout_connected(&manager, &key("u")).await).await;
    let pool = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(pool.idle_count(), 0);
    assert_eq!(pool.stats().closed_reset_failed.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn connection_being_reset_still_counts_toward_pool_size() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(
        &mock,
        "pool_size = 1\nserver_reset_query = \"select 'slow'\"",
        "",
    ));
    let conn = checkout_connected(&manager, &key("u")).await;
    let releasing = tokio::spawn(release(conn));
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The reset is still running: the only slot is taken, so this must wait and then reuse
    // the reset connection instead of opening a second one.
    let next = manager.checkout(&key("u")).await.unwrap();
    assert!(matches!(next, Checkout::Idle(_)));
    assert_eq!(mock.accepted(), 1);
    releasing.await.unwrap();
}

#[tokio::test]
async fn max_idle_closes_surplus_on_release() {
    let mock = MockPg::start().await;
    let manager = PoolManager::new(config(&mock, "pool_size = 2\nmax_idle = 1", ""));
    let a = checkout_connected(&manager, &key("u")).await;
    let b = checkout_connected(&manager, &key("u")).await;
    release(a).await;
    release(b).await;
    let pool = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(pool.idle_count(), 1);
    assert_eq!(pool.stats().closed_max_idle.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn replica_pool_uses_replica_address_and_overrides() {
    let primary = MockPg::start().await;
    let replica = MockPg::start().await;
    let hcl = format!(
        r#"
pool_defaults = {{ pool_size = 50 }}
databases = {{
  db = {{
    primary = {{ host = "127.0.0.1", port = {p} }}
    replicas = [{{ host = "127.0.0.1", port = {r}, pool_config = {{ pool_size = 3 }} }}]
    pool_config = {{ pool_size = 20 }}
  }}
}}
"#,
        p = primary.port(),
        r = replica.port(),
    );
    let manager = PoolManager::new(Arc::new(GatewayConfig::from_hcl_str(&hcl).unwrap()));
    let on_replica = manager
        .pool_for(PoolKey::for_target("u", "db", UpstreamTarget::Replica(0)))
        .await
        .unwrap();
    assert_eq!(on_replica.upstream(), replica.addr);
    assert_eq!(on_replica.limits().pool_size, 3);
    let on_primary = manager.pool_for(key("u")).await.unwrap();
    assert_eq!(on_primary.upstream(), primary.addr);
    assert_eq!(on_primary.limits().pool_size, 20);
}

#[tokio::test]
async fn connect_timeout_covers_login() {
    // Accepts TCP but never answers the startup packet.
    let silent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = silent.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = silent.accept().await {
            held.push(socket);
        }
    });
    let started = Instant::now();
    let result = ServerConnection::connect_and_authenticate(
        &addr,
        "u",
        "db",
        None,
        Some(Duration::from_millis(300)),
        None,
    )
    .await;
    let err = result.err().expect("login must time out");
    assert!(err.to_string().contains("timed out"), "{err:#}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

/// Mock upstream that requires a cleartext password and records each login's database and
/// whether the password matched.
async fn password_mock(password: &'static str) -> (u16, Arc<std::sync::Mutex<Vec<(String, bool)>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let logins = Arc::new(std::sync::Mutex::new(Vec::new()));
    let seen = Arc::clone(&logins);
    tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else { return };
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let Ok(StartupRequest::Startup(raw)) = read_startup_request(&mut socket).await else {
                    return;
                };
                let database = crate::startup_parse::parse_startup_params(&raw)
                    .remove("database")
                    .unwrap_or_default();
                socket.write_all(&backend(b'R', &3i32.to_be_bytes())).await.unwrap();
                let Ok(reply) = read_frontend(&mut socket).await else { return };
                let ok = &reply.raw[5..reply.raw.len() - 1] == password.as_bytes();
                seen.lock().unwrap().push((database, ok));
                let mut answer = if ok {
                    backend(b'R', &0i32.to_be_bytes())
                } else {
                    backend(b'E', b"SFATAL\0C28P01\0Mbad password\0\0")
                };
                answer.extend(backend(b'Z', b"I"));
                if socket.write_all(&answer).await.is_err() || !ok {
                    return;
                }
                // Hold the connection open like an idle server would.
                while let Ok(msg) = read_frontend(&mut socket).await {
                    if msg.tag() == b'X' {
                        return;
                    }
                }
            });
        }
    });
    (port, logins)
}

fn logical_db_config(port: u16, password: &str) -> Arc<GatewayConfig> {
    let hcl = format!(
        r#"
databases = {{
  logical = {{
    dbname  = "actual_db"
    primary = {{ host = "127.0.0.1", port = {port} }}
    pool_config = {{
      pool_size     = 5
      min_pool_size = 2
    }}
    userlist = [{{ name = "svc", password = "{password}" }}]
  }}
}}
"#
    );
    Arc::new(GatewayConfig::from_hcl_str(&hcl).expect("config"))
}

async fn wait_for_idle(pool: &Arc<UserDatabasePool>, want: usize) -> usize {
    let deadline = Instant::now() + Duration::from_secs(3);
    while pool.idle_count() < want && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    pool.idle_count()
}

#[tokio::test]
async fn min_pool_size_warms_up_with_userlist_password_and_dbname() {
    let (port, logins) = password_mock("s3cret").await;
    let manager = PoolManager::new(logical_db_config(port, "s3cret"));

    let pool = manager.pool_for(PoolKey::new("svc", "logical")).await.unwrap();
    assert_eq!(pool.upstream_database(), "actual_db");
    assert_eq!(pool.password(), Some("s3cret"));
    pool.maintain_min_pool_size().await;

    assert_eq!(wait_for_idle(&pool, 2).await, 2);
    let logins = logins.lock().unwrap().clone();
    assert!(logins.len() >= 2, "{logins:?}");
    assert!(
        logins.iter().all(|(db, ok)| db == "actual_db" && *ok),
        "warm-up must log in to the actual database with the userlist password: {logins:?}"
    );
}

#[tokio::test]
async fn warm_up_with_wrong_password_opens_nothing_and_backs_off() {
    let (port, logins) = password_mock("s3cret").await;
    let manager = PoolManager::new(logical_db_config(port, "wrong"));

    let pool = manager.pool_for(PoolKey::new("svc", "logical")).await.unwrap();
    pool.maintain_min_pool_size().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(pool.idle_count(), 0);
    assert_eq!(pool.total_count(), 0, "failed logins must release their slots");
    let logins = logins.lock().unwrap().clone();
    assert!(!logins.is_empty() && logins.iter().all(|(_, ok)| !ok), "{logins:?}");
}
