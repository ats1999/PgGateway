use super::setup;

use anyhow::Result;
use tokio_postgres::NoTls;

#[tokio::test]
async fn postgres_is_available() -> Result<()> {
    let postgres = setup::postgres::TestPostgres::start().await?;

    let (client, connection) = tokio_postgres::connect(
        &postgres.connection_string(),
        NoTls,
    ).await?;

    let connection_task = tokio::spawn(connection);

    let row = client.query_one("SELECT 1", &[]).await?;

    let value: i32 = row.get(0);

    assert_eq!(value, 1);

    drop(client);
    connection_task.await??;

    Ok(())
}