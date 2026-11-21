use anyhow::Result;
use std::{future::Future, sync::Arc};

use super::test_context::TestContext;
use crate::setup::{gateway::TestGateway, postgres::TestPostgres};

pub async fn run<F, Fut>(
    test_name: &str,
    postgres: TestPostgres,
    gateway: TestGateway,
    test: F,
) -> Result<()>
where
    F: FnOnce(Arc<TestContext>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let context = Arc::new(TestContext::start(test_name, postgres, gateway).await?);

    let result = test(Arc::clone(&context)).await;

    if let Err(error) = &result {
        eprintln!("E2E test failed: {error:#}");

        if let Err(log_error) = context.dump_logs().await {
            eprintln!("Failed to dump container logs: {log_error:#}");
        }
    }

    result
}
