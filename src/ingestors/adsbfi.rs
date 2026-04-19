use anyhow::Result;
use chrono::Utc;
use serde::Deserialize;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::storage::models::Flight;
use crate::storage::queries::insert_flights;
use crate::utils::http::create_client;

const SNAPSHOT_URL: &str = "https://opendata.adsb.fi/api/v2/snapshot";

#[derive(Debug, Deserialize)]
struct AdsbFiResponse {
    aircraft: Option<Vec<AdsbFiAircraft>>,
}

#[derive(Debug, Deserialize)]
struct AdsbFiAircraft {
    hex: Option<String>,
    flight: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    alt_baro: Option<serde_json::Value>,
    alt_geom: Option<f64>,
    gs: Option<f64>,
    track: Option<f64>,
    baro_rate: Option<f64>,
}

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let timestamp = Utc::now();

    let response = client.get(SNAPSHOT_URL).send().await?;
    if !response.status().is_success() {
        warn!(status = %response.status(), "adsb.fi non-ok");
        return Ok(());
    }

    let data: AdsbFiResponse = response.json().await?;
    let Some(aircraft) = data.aircraft else {
        warn!("adsb.fi returned no aircraft");
        return Ok(());
    };

    let flights: Vec<Flight> = aircraft
        .iter()
        .filter(|ac| ac.lat.is_some() && ac.lon.is_some())
        .map(|ac| parse(ac, timestamp))
        .collect();

    if flights.is_empty() {
        warn!("adsb.fi: no valid aircraft with positions");
        return Ok(());
    }

    insert_flights(pool, &flights).await?;
    info!(count = flights.len(), "adsb.fi ingested");
    Ok(())
}

fn parse(ac: &AdsbFiAircraft, timestamp: chrono::DateTime<Utc>) -> Flight {
    let altitude = ac
        .alt_baro
        .as_ref()
        .and_then(|v| v.as_f64())
        .or(ac.alt_geom);
    let on_ground = ac
        .alt_baro
        .as_ref()
        .and_then(|v| v.as_str())
        .map(|s| s == "ground");

    Flight {
        timestamp,
        icao24: ac.hex.clone(),
        callsign: ac.flight.as_deref().map(|s| s.trim().to_string()),
        origin_country: None,
        longitude: ac.lon,
        latitude: ac.lat,
        baro_altitude: altitude,
        on_ground,
        velocity: ac.gs,
        true_track: ac.track,
        vertical_rate: ac.baro_rate,
        source: "adsbfi".to_string(),
    }
}
