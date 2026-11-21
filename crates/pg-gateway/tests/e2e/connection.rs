use std::println;

use anyhow::Result;

#[tokio::test]
async fn gateway_connects_to_postgres() -> Result<()> {
    let postgres = super::setup::postgres::TestPostgres::start().await?;

    let gateway = super::setup::gateway::TestGateway::start_single_primary(&postgres).await?;

    super::setup::runner::run(
        "gateway_connects_to_postgres",
        postgres,
        gateway,
        |context| async move {
            println!("connection string {}", context.gateway.connection_string());
            
            let (client, connection) = tokio_postgres::connect(
                &context.gateway.connection_string(),
                tokio_postgres::NoTls,
            )
            .await?;

            let connection_task = tokio::spawn(connection);

            let row = client.query_one("SELECT 1", &[]).await?;
            let value: i32 = row.get(0);

            assert_eq!(value, 1);

            connection_task.abort();

            Ok(())
        },
    )
    .await
}
