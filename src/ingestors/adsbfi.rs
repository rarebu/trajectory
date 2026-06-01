use anyhow::Result;
use chrono::Utc;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::http::create_client;
use crate::ingestors::adsb::AdsbResponse;
use crate::storage::queries::insert_flights;

const SNAPSHOT_URL: &str = "https://opendata.adsb.fi/api/v2/snapshot";

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let timestamp = Utc::now();

    let response = client.get(SNAPSHOT_URL).send().await?;
    if !response.status().is_success() {
        warn!(status = %response.status(), "adsb.fi non-ok");
        return Ok(());
    }

    let data: AdsbResponse = response.json().await?;
    let Some(aircraft) = data.aircraft else {
        warn!("adsb.fi returned no aircraft");
        return Ok(());
    };

    let flights: Vec<_> = aircraft
        .iter()
        .filter(|ac| ac.has_position())
        .map(|ac| ac.to_flight("adsbfi", timestamp))
        .collect();

    if flights.is_empty() {
        warn!("adsb.fi: no valid aircraft with positions");
        return Ok(());
    }

    insert_flights(pool, &flights).await?;
    info!(count = flights.len(), "adsb.fi ingested");
    Ok(())
}
