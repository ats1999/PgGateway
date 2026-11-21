use anyhow::Result;
use std::{future::Future, sync::Arc};

use super::test_context::TestContext;

pub async fn run<F, Fut>(test_name: &str, test: F) -> Result<()>
where
    F: FnOnce(Arc<TestContext>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let test_id = uuid::Uuid::new_v4().to_string();
    let postgres = super::postgres::TestPostgres::start(test_id.clone()).await?;
    let gateway = super::gateway::TestGateway::start_single_primary(&postgres, test_id).await?;

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
