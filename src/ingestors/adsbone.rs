//! ADSB One: queries by aircraft type (ADS-B Exchange v2 schema) to approximate
//! global coverage without a bbox.

use anyhow::Result;
use chrono::Utc;
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::HashSet;
use std::time::Duration;
use tracing::{info, warn};

use crate::storage::models::Flight;
use crate::storage::queries::insert_flights;
use crate::utils::http::create_client;

const TYPE_ENDPOINTS: &[&str] = &[
    "https://api.adsb.one/v2/type/A319",
    "https://api.adsb.one/v2/type/A320",
    "https://api.adsb.one/v2/type/A321",
    "https://api.adsb.one/v2/type/A20N",
    "https://api.adsb.one/v2/type/A21N",
    "https://api.adsb.one/v2/type/A330",
    "https://api.adsb.one/v2/type/A332",
    "https://api.adsb.one/v2/type/A333",
    "https://api.adsb.one/v2/type/A339",
    "https://api.adsb.one/v2/type/A350",
    "https://api.adsb.one/v2/type/A359",
    "https://api.adsb.one/v2/type/A35K",
    "https://api.adsb.one/v2/type/A388",
    "https://api.adsb.one/v2/type/B737",
    "https://api.adsb.one/v2/type/B738",
    "https://api.adsb.one/v2/type/B739",
    "https://api.adsb.one/v2/type/B38M",
    "https://api.adsb.one/v2/type/B39M",
    "https://api.adsb.one/v2/type/B752",
    "https://api.adsb.one/v2/type/B763",
    "https://api.adsb.one/v2/type/B772",
    "https://api.adsb.one/v2/type/B77W",
    "https://api.adsb.one/v2/type/B788",
    "https://api.adsb.one/v2/type/B789",
    "https://api.adsb.one/v2/type/B78X",
    "https://api.adsb.one/v2/type/E170",
    "https://api.adsb.one/v2/type/E190",
    "https://api.adsb.one/v2/type/E195",
    "https://api.adsb.one/v2/type/CRJ9",
    "https://api.adsb.one/v2/type/CRJ7",
    "https://api.adsb.one/v2/type/B744",
    "https://api.adsb.one/v2/type/B748",
    "https://api.adsb.one/v2/type/C17",
    "https://api.adsb.one/v2/type/C130",
    "https://api.adsb.one/v2/type/A400",
];

#[derive(Debug, Deserialize)]
struct AdsbOneResponse {
    ac: Option<Vec<AdsbOneAircraft>>,
}

#[derive(Debug, Deserialize)]
struct AdsbOneAircraft {
    hex: Option<String>,
    flight: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    alt_baro: Option<serde_json::Value>,
    alt_geom: Option<i64>,
    gs: Option<f64>,
    track: Option<f64>,
    baro_rate: Option<f64>,
}

pub async fn fetch(pool: &PgPool, delay_ms: u64) -> Result<()> {
    let client = create_client()?;
    let timestamp = Utc::now();
    let mut all_flights = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let delay = Duration::from_millis(delay_ms);

    for url in TYPE_ENDPOINTS {
        match client.get(*url).send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(data) = response.json::<AdsbOneResponse>().await {
                    if let Some(aircraft) = data.ac {
                        for ac in &aircraft {
                            let Some(hex) = &ac.hex else { continue };
                            if ac.lat.is_some() && ac.lon.is_some() && seen.insert(hex.clone()) {
                                all_flights.push(parse(ac, timestamp));
                            }
                        }
                    }
                }
            }
            Ok(response) => warn!(url = %url, status = %response.status(), "adsb.one non-ok"),
            Err(e) => warn!(url = %url, error = %e, "adsb.one request failed"),
        }

        tokio::time::sleep(delay).await;
    }

    if all_flights.is_empty() {
        warn!("adsb.one returned no aircraft");
        return Ok(());
    }

    insert_flights(pool, &all_flights).await?;
    info!(count = all_flights.len(), "adsb.one ingested");
    Ok(())
}

fn parse(ac: &AdsbOneAircraft, timestamp: chrono::DateTime<Utc>) -> Flight {
    let altitude = ac
        .alt_baro
        .as_ref()
        .and_then(|v| v.as_f64())
        .or_else(|| ac.alt_geom.map(|x| x as f64));
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
        source: "adsbone".to_string(),
    }
}
