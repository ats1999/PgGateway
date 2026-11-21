use std::println;

use anyhow::Result;
use testcontainers_modules::testcontainers::{
    core::IntoContainerPort, runners::AsyncRunner, ContainerAsync, GenericImage, ImageExt,
};

use super::postgres::TestPostgres;

const GATEWAY_IMAGE: &str = "pg-gateway";
const GATEWAY_TAG: &str = "latest";
const GATEWAY_PORT: u16 = 6432;

pub struct TestGateway {
    _container: ContainerAsync<GenericImage>,
    pub host: String,
    pub port: u16,
}

impl TestGateway {
    pub async fn start_single_primary(postgres: &TestPostgres) -> Result<Self> {
        // PostgreSQL is exposed on a dynamically mapped host port.
        // The Gateway container reaches it through Docker Desktop.
        let config = include_str!("../fixtures/gateway.single-primary.hcl");
        let config = super::config::update_config(config, "host.docker.internal", postgres.port)?;

        println!("Config: {}", config);
        Self::start(config).await
    }

    pub async fn start(config: String) -> Result<Self> {
        let container = GenericImage::new(GATEWAY_IMAGE, GATEWAY_TAG)
            .with_exposed_port(GATEWAY_PORT.tcp())
            .with_env_var("PG_GATEWAY_CONFIG", "/etc/pg-gateway/config.hcl")
            .with_copy_to("/etc/pg-gateway/config.hcl", config.into_bytes())
            .with_env_var("NO_COLOR", "1")
            .with_env_var("CLICOLOR", "0")
            .start()
            .await?;

        let host = container.get_host().await?.to_string();
        let port = container.get_host_port_ipv4(GATEWAY_PORT.tcp()).await?;

        Ok(Self {
            _container: container,
            host,
            port,
        })
    }

    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user=ats1999 password=badboy dbname=pg_gateway_test",
            self.host, self.port
        )
    }

    pub async fn dump_logs(&self, directory: &std::path::Path) -> anyhow::Result<()> {
        let stdout = self._container.stdout_to_vec().await?;
        let stderr = self._container.stderr_to_vec().await?;

        tokio::fs::write(directory.join("gateway.stdout.log"), stdout).await?;
        tokio::fs::write(directory.join("gateway.stderr.log"), stderr).await?;

        Ok(())
    }
}
