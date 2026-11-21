use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt},
};

pub struct TestPostgres {
    _container: ContainerAsync<Postgres>,
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub network: String,
}

impl TestPostgres {
    pub async fn start() -> anyhow::Result<Self> {
        let network = format!("pg-gateway-{}", uuid::Uuid::new_v4());
        let username = "ats1999".to_string();
        let password = "badboy".to_string();
        let database = "pg_gateway_test".to_string();

        let container = Postgres::default()
            .with_db_name(&database)
            .with_user(&username)
            .with_password(&password)
            .with_network(network.clone())
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
        })
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.username, self.password, self.database,
        )
    }

    pub async fn dump_logs(&self, directory: &std::path::Path) -> anyhow::Result<()> {
        let stdout = self._container.stdout_to_vec().await?;
        let stderr = self._container.stderr_to_vec().await?;

        tokio::fs::write(directory.join("postgres.stdout.log"), stdout).await?;
        tokio::fs::write(directory.join("postgres.stderr.log"), stderr).await?;

        Ok(())
    }
}
