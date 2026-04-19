use anyhow::Result;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::ConnectOptions;
use std::str::FromStr;
use std::time::Duration;

pub async fn create_pool(database_url: &str) -> Result<PgPool> {
    let options = PgConnectOptions::from_str(database_url)?.disable_statement_logging();

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .min_connections(2)
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(600))
        // synchronous_commit=off trades ~200ms of WAL durability on crash for
        // significantly higher insert throughput. Acceptable for a crawler
        // because upstream data can be re-fetched.
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                sqlx::query("SET synchronous_commit = off")
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect_with(options)
        .await?;

    Ok(pool)
}
