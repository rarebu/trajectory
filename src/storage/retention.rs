use anyhow::Result;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::config::Config;

pub async fn apply_retention_policies(pool: &PgPool, config: &Config) -> Result<()> {
    apply_policy(pool, "ships", config.retention_ships_days).await;
    apply_policy(pool, "flights", config.retention_flights_days).await;
    apply_policy(pool, "service_alerts", config.retention_service_alerts_days).await;
    apply_policy(
        pool,
        "vehicle_positions",
        config.retention_vehicle_positions_days,
    )
    .await;
    apply_policy(pool, "db_departures", config.retention_departures_days).await;
    Ok(())
}

async fn apply_policy(pool: &PgPool, table: &str, days: Option<i64>) {
    let Some(d) = days else { return };

    let interval = format!("{d} days");
    let table_qualified = format!("public.{table}");

    // add_retention_policy is idempotent by design; swallow the "policy already
    // exists" case (SQLSTATE 42710) but surface everything else.
    let result = sqlx::query("SELECT add_retention_policy($1::regclass, $2::interval)")
        .bind(&table_qualified)
        .bind(&interval)
        .execute(pool)
        .await;

    match result {
        Ok(_) => info!(table, days = d, "retention policy applied"),
        Err(sqlx::Error::Database(db)) if db.code().as_deref() == Some("42710") => {
            info!(table, days = d, "retention policy already present");
        }
        Err(e) => warn!(table, error = %e, "retention policy setup failed"),
    }
}
