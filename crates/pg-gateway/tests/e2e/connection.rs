use std::println;

use anyhow::Result;

#[tokio::test]
async fn gateway_connects_to_postgres() -> Result<()> {
    super::setup::runner::run("gateway_connects_to_postgres", |context| async move {
        println!("connection string {}", context.gateway.connection_string());

        let (client, connection) =
            tokio_postgres::connect(&context.gateway.connection_string(), tokio_postgres::NoTls)
                .await?;

        let connection_task = tokio::spawn(connection);

        let row = client.query_one("SELECT 1", &[]).await?;
        let value: i32 = row.get(0);

        assert_eq!(value, 1);

        connection_task.abort();

        Ok(())
    })
    .await
}
