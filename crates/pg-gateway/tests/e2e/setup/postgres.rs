use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};

use super::pg_log::{self, LoggedStatement};

/// Docker DNS name for Postgres on the shared test network (reachable from the gateway container).
pub const POSTGRES_UPSTREAM_HOST: &str = "postgres";

pub struct TestPostgres {
    _container: ContainerAsync<Postgres>,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub network: String,
    pub container_name: String,
    /// Hostname the gateway container should use for the upstream Postgres (same Docker network).
    pub upstream_host: String,
}

impl TestPostgres {
    pub async fn start(test_id: String) -> anyhow::Result<Self> {
        let network = format!("pg-gateway-network-{}", test_id);
        let container_name = format!("pg-gateway-postgres-{}", test_id);
        let username = "ats1999".to_string();
        let password = "badboy".to_string();
        let database = "pg_gateway_test".to_string();

        let container = Postgres::default()
            .with_db_name(&database)
            .with_user(&username)
            .with_password(&password)
            .with_network(network.clone())
            .with_container_name(container_name.clone())
            .with_hostname(POSTGRES_UPSTREAM_HOST)
            // Every statement is logged with its backend pid (`log_line_prefix` = '%m [%p] '),
            // so tests can check which server connection ran what.
            .with_cmd(["-c", "fsync=off", "-c", "log_statement=all"])
            .start()
            .await?;

        let port = container.get_host_port_ipv4(5432).await?;

        Ok(Self {
            _container: container,
            host: "127.0.0.1".to_string(),
            port,
            username,
            password,
            database,
            network,
            container_name,
            upstream_host: POSTGRES_UPSTREAM_HOST.to_string(),
        })
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.username, self.password, self.database,
        )
    }

    /// Everything Postgres has logged so far (it logs to stderr).
    pub async fn logs(&self) -> anyhow::Result<String> {
        let stderr = self._container.stderr_to_vec().await?;
        let stdout = self._container.stdout_to_vec().await?;
        Ok(format!(
            "{}{}",
            String::from_utf8_lossy(&stderr),
            String::from_utf8_lossy(&stdout)
        ))
    }

    /// Statements Postgres has logged so far (`log_statement = all`), in order.
    pub async fn statements(&self) -> anyhow::Result<Vec<LoggedStatement>> {
        Ok(pg_log::parse(&self.logs().await?))
    }

    pub async fn dump_logs(&self, directory: &std::path::Path) -> anyhow::Result<()> {
        let stdout = self._container.stdout_to_vec().await?;
        let stderr = self._container.stderr_to_vec().await?;

        tokio::fs::write(directory.join("postgres.stdout.log"), stdout).await?;
        tokio::fs::write(directory.join("postgres.stderr.log"), stderr).await?;

        Ok(())
    }
}
