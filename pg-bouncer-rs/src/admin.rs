use crate::config::Config;
use crate::protocol::{
    ready_for_query_idle, row_description_and_data, write_message,
};
use crate::stats::StatsHandle;
use anyhow::Result;
use tokio::net::TcpStream;

pub async fn serve_admin(
    client: &mut TcpStream,
    cfg: &Config,
    stats: StatsHandle,
    client_id: u64,
) -> Result<()> {
    crate::protocol::write_message(client, &crate::protocol::auth_ok()).await?;
    crate::protocol::write_message(
        client,
        &crate::protocol::parameter_status("server_version", "pgbouncer-rs"),
    )
    .await?;
    crate::protocol::write_message(
        client,
        &crate::protocol::backend_key_data(client_id as i32, 0),
    )
    .await?;
    crate::protocol::write_message(client, &ready_for_query_idle()).await?;

    loop {
        let Some(msg) = crate::protocol::read_message(client).await? else {
            break;
        };
        if crate::protocol::is_terminate(&msg) {
            break;
        }
        if !crate::protocol::is_query(&msg) {
            continue;
        }
        let sql = crate::protocol::query_sql(&msg.body)
            .trim_end_matches(';')
            .to_ascii_lowercase();

        let response = match sql.as_str() {
            "show pools" => render_pools(&stats, cfg),
            "show stats" => render_stats(&stats),
            "show config" => render_config(cfg),
            "show help" => render_help(),
            "reload" => "RELOAD\n".to_string(),
            "pause" => {
                stats.server_state.pause();
                "PAUSE\n".to_string()
            }
            "resume" => {
                stats.server_state.resume();
                "RESUME\n".to_string()
            }
            _ => format!("ERROR: unknown admin command: {sql}\n"),
        };

        for m in row_description_and_data(&[("result", response.trim_end())]) {
            write_message(client, &m).await?;
        }
        write_message(client, &ready_for_query_idle()).await?;
    }
    Ok(())
}

fn render_pools(stats: &StatsHandle, cfg: &Config) -> String {
    let mut out = String::from("database | user | sv_active | sv_idle | sv_waiting | pool_mode\n");
    for entry in stats.pools.iter() {
        let key = entry.key();
        let ps = entry.value();
        out.push_str(&format!(
            "{key} | | {} | {} | {} | {:?}\n",
            ps.active.load(std::sync::atomic::Ordering::Relaxed),
            ps.idle.load(std::sync::atomic::Ordering::Relaxed),
            ps.waiting.load(std::sync::atomic::Ordering::Relaxed),
            cfg.pool_mode,
        ));
    }
    out
}

fn render_stats(stats: &StatsHandle) -> String {
    let s = stats.global.snapshot();
    format!(
        "total_client_conn={}\ntotal_server_conn={}\ntotal_queries={}\ntotal_transactions={}\ntotal_errors={}\npaused={}\n",
        s.client_connections,
        s.server_connections,
        s.queries,
        s.transactions,
        s.pooler_errors,
        stats.server_state.is_paused(),
    )
}

fn render_config(cfg: &Config) -> String {
    format!(
        "listen_addr={}\nbackend={}: {}\npool_mode={:?}\ndefault_pool_size={}\nmax_client_conn={}\nadmin_database={}\nworker_threads={}\n",
        cfg.listen_addr,
        cfg.backend_host,
        cfg.backend_port,
        cfg.pool_mode,
        cfg.default_pool_size,
        cfg.max_client_conn,
        cfg.admin_database,
        cfg.worker_threads,
    )
}

fn render_help() -> String {
    "SHOW POOLS; SHOW STATS; SHOW CONFIG; SHOW HELP; RELOAD; PAUSE; RESUME;\n".to_string()
}
