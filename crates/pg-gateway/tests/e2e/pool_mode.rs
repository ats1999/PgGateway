//! Pool modes against a real Postgres, checked three ways:
//! - which server connection ran a statement: `pg_backend_pid()` from the client,
//! - the gateway's own view: admin console `pools` / `clients` tables,
//! - what Postgres actually executed and on which backend: its statement log
//!   (`log_statement = all`, see `setup::postgres`).
//!
//! No test relies on timing or on a pid being reused by chance: waits poll a condition with a
//! deadline, and "random" client order comes from a fixed seed.

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, ensure, Context, Result};
use hcl::Value;
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

use super::setup::{
    admin::{self, PoolState},
    config::SINGLE_PRIMARY,
    pg_log::LoggedStatement,
    runner,
    test_context::TestContext,
};

const WAIT: Duration = Duration::from_secs(10);
const RESET_QUERY: &str = "DISCARD ALL";

// ── harness ────────────────────────────────────────────────────────────────────────────

/// `setup::runner::run` with the single-primary fixture plus this test's `pool_config`.
async fn run_with<F, Fut>(test_name: &str, mode: &str, pool_size: i64, test: F) -> Result<()>
where
    F: FnOnce(Arc<TestContext>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let overrides = [
        ("pool_mode", Value::from(mode)),
        ("pool_size", Value::from(pool_size)),
        // A client waiting for a server connection gives up after 2s with a FATAL.
        ("query_wait_timeout_seconds", Value::from(2)),
    ];
    let config = with_pool_config(SINGLE_PRIMARY, &overrides)?;
    runner::run(test_name, &config, test).await
}

/// Set keys in `databases.pg_gateway_test.pool_config` of the fixture.
fn with_pool_config(config: &str, overrides: &[(&str, Value)]) -> Result<String> {
    let mut config: Value = hcl::from_str(config).context("parse gateway fixture")?;
    let pool_config = config
        .as_object_mut()
        .and_then(|root| root.get_mut("databases"))
        .and_then(Value::as_object_mut)
        .and_then(|databases| databases.get_mut("pg_gateway_test"))
        .and_then(Value::as_object_mut)
        .and_then(|database| database.get_mut("pool_config"))
        .and_then(Value::as_object_mut)
        .context("fixture has no databases.pg_gateway_test.pool_config")?;
    for (key, value) in overrides {
        pool_config.insert((*key).into(), value.clone());
    }
    hcl::to_string(&config).context("serialize gateway config")
}

/// A client connected through the gateway.
struct GatewayClient {
    client: Client,
    task: JoinHandle<()>,
}

impl GatewayClient {
    async fn connect(context: &TestContext) -> Result<Self> {
        let (client, connection) =
            tokio_postgres::connect(&context.gateway.connection_string(), NoTls).await?;
        let task = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Self { client, task })
    }

    /// Run `sql` (simple protocol, so Postgres logs it verbatim as `statement: <sql>`).
    async fn run(&self, sql: &str) -> Result<Vec<Vec<Option<String>>>> {
        let mut rows = Vec::new();
        for message in self.client.simple_query(sql).await? {
            if let SimpleQueryMessage::Row(row) = message {
                rows.push((0..row.len()).map(|i| row.get(i).map(str::to_string)).collect());
            }
        }
        Ok(rows)
    }

    /// Backend pid of the server connection that ran this statement. `tag` makes the statement
    /// unique in the Postgres log: `SELECT pg_backend_pid(), '<tag>'`.
    async fn pid(&self, tag: &str) -> Result<i32> {
        let rows = self.run(&tagged(tag)).await?;
        rows.first()
            .and_then(|row| row.first().cloned().flatten())
            .context("no pid returned")?
            .parse()
            .context("pid")
    }

    async fn show(&self, setting: &str) -> Result<String> {
        self.run(&format!("SHOW {setting}"))
            .await?
            .first()
            .and_then(|row| row.first().cloned().flatten())
            .context("SHOW returned nothing")
    }

    /// Disconnect and wait for the client's connection task to end.
    async fn close(self) {
        drop(self.client);
        let _ = tokio::time::timeout(WAIT, self.task).await;
    }
}

fn tagged(tag: &str) -> String {
    format!("SELECT pg_backend_pid(), '{tag}'")
}

/// Poll the admin console until the pool reaches the wanted state.
async fn wait_for_pool(
    context: &TestContext,
    what: &str,
    want: impl Fn(&PoolState) -> bool,
) -> Result<PoolState> {
    let deadline = Instant::now() + WAIT;
    loop {
        let state = admin::pool_state(&context.gateway).await?;
        if want(&state) {
            return Ok(state);
        }
        if Instant::now() > deadline {
            bail!("admin pools never reached `{what}`; last state {state:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll Postgres' statement log until `sql` has been logged, then return the whole log.
async fn wait_for_logged(context: &TestContext, sql: &str) -> Result<Vec<LoggedStatement>> {
    let deadline = Instant::now() + WAIT;
    loop {
        let statements = context.postgres.statements().await?;
        if statements.iter().any(|s| s.sql == sql) {
            return Ok(statements);
        }
        if Instant::now() > deadline {
            bail!("Postgres never logged `{sql}`");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Backend pids that ran exactly `sql`, in log order.
fn pids_that_ran(log: &[LoggedStatement], sql: &str) -> Vec<i32> {
    log.iter().filter(|s| s.sql == sql).map(|s| s.pid).collect()
}

/// Position of the first log entry matching `pid` and `sql` at or after `from`.
fn position(log: &[LoggedStatement], from: usize, pid: i32, sql: &str) -> Option<usize> {
    log.iter()
        .enumerate()
        .skip(from)
        .find(|(_, s)| s.pid == pid && s.sql == sql)
        .map(|(i, _)| i)
}

/// Fixed-seed xorshift: "random" client order that is the same on every run.
struct SeededOrder(u64);

impl SeededOrder {
    fn next_below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

// ── session ────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn session_mode_gives_each_client_its_own_connection_for_the_whole_session() -> Result<()> {
    run_with("session_mode_gives_each_client_its_own_connection_for_the_whole_session", "session", 3, |context| async move {
        let mut clients = Vec::new();
        let mut pids = Vec::new();
        for i in 0..3 {
            let client = GatewayClient::connect(&context).await?;
            pids.push(client.pid(&format!("open-c{i}")).await?);
            clients.push(client);
        }
        ensure!(pids.iter().collect::<HashSet<_>>().len() == 3, "each client needs its own backend: {pids:?}");

        // All three server connections stay assigned while the clients sit idle.
        let pinned = wait_for_pool(&context, "3 active, 0 idle", |p| p.active == 3 && p.idle == 0).await?;
        ensure!(pinned.connections_opened == 3, "{pinned:?}");
        ensure!(admin::client_states(&context.gateway).await? == ["idle", "idle", "idle"]);

        // Random order across clients, many rounds: every statement stays on its client's backend.
        let mut order = SeededOrder(0x5eed_cafe);
        let mut expected = Vec::new();
        for round in 0..30 {
            let i = order.next_below(3);
            let tag = format!("c{i}-round{round}");
            let pid = clients[i].pid(&tag).await?;
            ensure!(pid == pids[i], "round {round}: client {i} moved from {} to {pid}", pids[i]);
            expected.push((tagged(&tag), pids[i]));
        }

        // The gateway opened no extra connections and still has all three pinned.
        let after = admin::pool_state(&context.gateway).await?;
        ensure!(after.active == 3 && after.idle == 0 && after.connections_opened == 3, "{after:?}");

        // Postgres ran every statement on the backend of the client that sent it.
        let log = wait_for_logged(&context, &expected.last().unwrap().0).await?;
        for i in 0..3 {
            ensure!(pids_that_ran(&log, &tagged(&format!("open-c{i}"))) == [pids[i]]);
        }
        for (sql, pid) in &expected {
            ensure!(pids_that_ran(&log, sql) == [*pid], "`{sql}` should run once on {pid}");
        }
        ensure!(pids_that_ran(&log, RESET_QUERY).is_empty(), "no reset while clients are connected");

        for client in clients {
            client.close().await;
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn session_mode_full_pool_makes_the_next_client_wait() -> Result<()> {
    run_with("session_mode_full_pool_makes_the_next_client_wait", "session", 1, |context| async move {
        let holder = GatewayClient::connect(&context).await?;
        let pid = holder.pid("holder").await?;
        wait_for_pool(&context, "1 active", |p| p.active == 1 && p.idle == 0).await?;

        // The only server connection is pinned to `holder`: this login waits, then times out.
        ensure!(GatewayClient::connect(&context).await.is_err(), "second client must not get a connection");
        let state = admin::pool_state(&context.gateway).await?;
        ensure!(state.wait_timeouts == 1 && state.active == 1, "{state:?}");

        holder.close().await;
        wait_for_pool(&context, "released to idle", |p| p.active == 0 && p.idle == 1).await?;

        let next = GatewayClient::connect(&context).await?;
        ensure!(next.pid("next").await? == pid, "the released connection serves the next client");
        ensure!(admin::pool_state(&context.gateway).await?.connections_opened == 1);
        next.close().await;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn session_mode_resets_the_connection_before_the_next_client() -> Result<()> {
    run_with("session_mode_resets_the_connection_before_the_next_client", "session", 1, |context| async move {
        let first = GatewayClient::connect(&context).await?;
        let pid = first.pid("first").await?;
        first.run("SET application_name = 'leaky'").await?;
        first.run("CREATE TEMP TABLE leaky (id int)").await?;
        first.close().await;
        wait_for_pool(&context, "released to idle", |p| p.active == 0 && p.idle == 1).await?;

        // Postgres saw the reset query on that backend, after the first client's statements.
        let log = wait_for_logged(&context, RESET_QUERY).await?;
        let last_of_first = position(&log, 0, pid, "CREATE TEMP TABLE leaky (id int)").context("client statement not logged")?;
        ensure!(position(&log, last_of_first, pid, RESET_QUERY).is_some(), "DISCARD ALL must follow on {pid}");

        let second = GatewayClient::connect(&context).await?;
        ensure!(second.pid("second").await? == pid, "pool_size = 1: same backend");
        ensure!(second.show("application_name").await? != "leaky", "settings must be reset");
        let temp = second.run("SELECT to_regclass('pg_temp.leaky')").await?;
        ensure!(temp[0][0].is_none(), "temp tables must be dropped");
        second.close().await;
        Ok(())
    })
    .await
}

// ── transaction ────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn transaction_mode_idle_clients_hold_no_server_connection() -> Result<()> {
    run_with("transaction_mode_idle_clients_hold_no_server_connection", "transaction", 3, |context| async move {
        let mut clients = Vec::new();
        for i in 0..3 {
            let client = GatewayClient::connect(&context).await?;
            client.pid(&format!("c{i}")).await?;
            clients.push(client);
        }

        // Three connected clients, none in a transaction: every server connection is back in the pool.
        let state = wait_for_pool(&context, "0 active", |p| p.active == 0).await?;
        ensure!(state.idle >= 1, "{state:?}");
        ensure!(admin::client_states(&context.gateway).await? == ["idle", "idle", "idle"]);

        // Released between transactions without a reset query (PgBouncer semantics).
        let log = wait_for_logged(&context, &tagged("c2")).await?;
        ensure!(pids_that_ran(&log, RESET_QUERY).is_empty(), "transaction mode must not run DISCARD ALL");

        for client in clients {
            client.close().await;
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn transaction_mode_open_transaction_keeps_its_connection() -> Result<()> {
    run_with("transaction_mode_open_transaction_keeps_its_connection", "transaction", 1, |context| async move {
        let a = GatewayClient::connect(&context).await?;
        let b = GatewayClient::connect(&context).await?;

        a.run("BEGIN").await?;
        let pid = a.pid("a-in-txn").await?;
        wait_for_pool(&context, "1 active", |p| p.active == 1).await?;
        ensure!(admin::client_states(&context.gateway).await?.contains(&"idle_in_transaction".to_string()));

        // `a` holds the only connection inside its transaction: `b` waits, then times out.
        ensure!(b.pid("b-blocked").await.is_err(), "b must not run while a's transaction is open");
        ensure!(admin::pool_state(&context.gateway).await?.wait_timeouts == 1);

        ensure!(a.pid("a-still-in-txn").await? == pid);
        a.run("COMMIT").await?;
        wait_for_pool(&context, "released after COMMIT", |p| p.active == 0 && p.idle == 1).await?;

        let c = GatewayClient::connect(&context).await?;
        ensure!(c.pid("c-after-commit").await? == pid);

        // Postgres ran the whole transaction on one backend and never saw b's statement.
        let log = wait_for_logged(&context, &tagged("c-after-commit")).await?;
        let begin = position(&log, 0, pid, "BEGIN").context("BEGIN not logged")?;
        let inside = position(&log, begin, pid, &tagged("a-still-in-txn")).context("txn statement")?;
        ensure!(position(&log, inside, pid, "COMMIT").is_some());
        ensure!(pids_that_ran(&log, &tagged("b-blocked")).is_empty(), "b's query must never reach Postgres");

        a.close().await;
        b.close().await;
        c.close().await;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn transaction_mode_failed_transaction_holds_the_connection_until_rollback() -> Result<()> {
    run_with("transaction_mode_failed_transaction_holds_the_connection_until_rollback", "transaction", 1, |context| async move {
        let a = GatewayClient::connect(&context).await?;
        let b = GatewayClient::connect(&context).await?;

        a.run("BEGIN").await?;
        ensure!(a.run("SELECT 1/0").await.is_err());
        let held = admin::pool_state(&context.gateway).await?;
        ensure!(held.active == 1, "a failed transaction still holds its connection: {held:?}");
        ensure!(admin::client_states(&context.gateway).await?.contains(&"idle_in_transaction".to_string()));

        a.run("ROLLBACK").await?;
        wait_for_pool(&context, "released after ROLLBACK", |p| p.active == 0).await?;
        ensure!(b.pid("b-after-rollback").await.is_ok());

        a.close().await;
        b.close().await;
        Ok(())
    })
    .await
}

#[tokio::test]
async fn transaction_mode_does_not_reset_session_state_between_clients() -> Result<()> {
    run_with("transaction_mode_does_not_reset_session_state_between_clients", "transaction", 1, |context| async move {
        // server_reset_query only runs in session mode (PgBouncer semantics): with one server
        // connection, a SET outside a transaction is visible to the next client.
        let a = GatewayClient::connect(&context).await?;
        let b = GatewayClient::connect(&context).await?;

        let pid = a.pid("a").await?;
        a.run("SET application_name = 'from_a'").await?;
        ensure!(b.pid("b").await? == pid, "pool_size = 1: same backend");
        ensure!(b.show("application_name").await? == "from_a");

        let log = wait_for_logged(&context, &tagged("b")).await?;
        ensure!(pids_that_ran(&log, RESET_QUERY).is_empty());

        a.close().await;
        b.close().await;
        Ok(())
    })
    .await
}

// ── statement ──────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn statement_mode_releases_after_every_statement() -> Result<()> {
    run_with("statement_mode_releases_after_every_statement", "statement", 3, |context| async move {
        let mut clients = Vec::new();
        for _ in 0..3 {
            clients.push(GatewayClient::connect(&context).await?);
        }

        let mut order = SeededOrder(0x0ddb_a11);
        let mut last_tag = String::new();
        for round in 0..15 {
            let i = order.next_below(3);
            last_tag = format!("c{i}-round{round}");
            clients[i].pid(&last_tag).await?;
            // Back in the pool as soon as the statement is done.
            wait_for_pool(&context, "0 active", |p| p.active == 0).await?;
        }
        ensure!(admin::client_states(&context.gateway).await? == ["idle", "idle", "idle"]);

        let log = wait_for_logged(&context, &tagged(&last_tag)).await?;
        ensure!(pids_that_ran(&log, RESET_QUERY).is_empty());

        for client in clients {
            client.close().await;
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn statement_mode_explicit_transaction_keeps_its_connection() -> Result<()> {
    run_with("statement_mode_explicit_transaction_keeps_its_connection", "statement", 1, |context| async move {
        // Today statement mode releases on an idle ReadyForQuery, like transaction mode, so an
        // explicit transaction still works and holds its connection until COMMIT.
        let a = GatewayClient::connect(&context).await?;
        let b = GatewayClient::connect(&context).await?;

        a.run("BEGIN").await?;
        let pid = a.pid("a-in-txn").await?;
        wait_for_pool(&context, "1 active", |p| p.active == 1).await?;
        ensure!(b.pid("b-blocked").await.is_err(), "b waits while a is in a transaction");
        a.run("COMMIT").await?;
        wait_for_pool(&context, "released after COMMIT", |p| p.active == 0).await?;

        let c = GatewayClient::connect(&context).await?;
        ensure!(c.pid("c-after-commit").await? == pid);

        let log = wait_for_logged(&context, &tagged("c-after-commit")).await?;
        ensure!(pids_that_ran(&log, &tagged("b-blocked")).is_empty());
        let begin = position(&log, 0, pid, "BEGIN").context("BEGIN not logged")?;
        ensure!(position(&log, begin, pid, "COMMIT").is_some());

        a.close().await;
        b.close().await;
        c.close().await;
        Ok(())
    })
    .await
}
