use anyhow::Result;
use std::path::PathBuf;

use super::{gateway::TestGateway, postgres::TestPostgres};

pub struct TestContext {
    pub postgres: TestPostgres,
    pub gateway: TestGateway,
    test_name: String,
}

impl TestContext {
    pub async fn start(
        test_name: &str,
        postgres: TestPostgres,
        gateway: TestGateway,
    ) -> Result<Self> {
        Ok(Self {
            postgres,
            gateway,
            test_name: test_name.to_string(),
        })
    }

    pub async fn dump_logs(&self) -> Result<()> {
        let directory = PathBuf::from("test-logs").join(&self.test_name);

        tokio::fs::create_dir_all(&directory).await?;

        self.postgres.dump_logs(&directory).await?;

        self.gateway.dump_logs(&directory).await?;

        Ok(())
    }
}
