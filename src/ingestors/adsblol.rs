use anyhow::Result;
use chrono::Utc;
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::HashSet;
use tracing::{info, warn};

use crate::storage::models::Flight;
use crate::storage::queries::insert_flights;
use crate::utils::http::create_client;

const ENDPOINTS: &[(&str, &str)] = &[
    ("ladd", "https://api.adsb.lol/v2/ladd"),
    ("mil", "https://api.adsb.lol/v2/mil"),
    ("pia", "https://api.adsb.lol/v2/pia"),
];

#[derive(Debug, Deserialize)]
struct AdsbLolResponse {
    ac: Option<Vec<AdsbLolAircraft>>,
}

#[derive(Debug, Deserialize)]
struct AdsbLolAircraft {
    hex: Option<String>,
    flight: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    alt_baro: Option<serde_json::Value>,
    gs: Option<f64>,
    track: Option<f64>,
    baro_rate: Option<f64>,
}

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let timestamp = Utc::now();
    let mut all_flights = Vec::new();
    let mut seen_icaos: HashSet<String> = HashSet::new();

    for (name, url) in ENDPOINTS {
        match client.get(*url).send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(data) = response.json::<AdsbLolResponse>().await {
                    if let Some(aircraft) = data.ac {
                        for ac in &aircraft {
                            if let Some(hex) = &ac.hex {
                                if seen_icaos.insert(hex.clone()) {
                                    all_flights.push(parse(ac, timestamp));
                                }
                            }
                        }
                    }
                }
            }
            Ok(response) => warn!(endpoint = name, status = %response.status(), "adsb.lol non-ok"),
            Err(e) => warn!(endpoint = name, error = %e, "adsb.lol request failed"),
        }
    }

    if all_flights.is_empty() {
        warn!("adsb.lol returned no aircraft");
        return Ok(());
    }

    insert_flights(pool, &all_flights).await?;
    info!(count = all_flights.len(), "adsb.lol ingested");
    Ok(())
}

fn parse(ac: &AdsbLolAircraft, timestamp: chrono::DateTime<Utc>) -> Flight {
    let altitude = ac.alt_baro.as_ref().and_then(|v| v.as_f64());
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
        source: "adsblol".to_string(),
    }
}
