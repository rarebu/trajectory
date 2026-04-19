use anyhow::Result;
use chrono::Utc;
use serde::Deserialize;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::config::Config;
use crate::storage::models::Flight;
use crate::storage::queries::insert_flights;
use crate::utils::http::create_client;

const OPENSKY_URL: &str = "https://opensky-network.org/api/states/all";

#[derive(Debug, Deserialize)]
struct OpenSkyResponse {
    #[allow(dead_code)]
    time: i64,
    states: Option<Vec<Vec<serde_json::Value>>>,
}

pub async fn fetch(pool: &PgPool, config: &Config) -> Result<()> {
    let client = create_client()?;
    let mut request = client.get(OPENSKY_URL);

    if let (Some(user), Some(pass)) = (&config.opensky_username, &config.opensky_password) {
        request = request.basic_auth(user, Some(pass));
    }

    let response = request.send().await?;
    if !response.status().is_success() {
        anyhow::bail!("OpenSky API returned status {}", response.status());
    }

    let data: OpenSkyResponse = response.json().await?;
    let timestamp = Utc::now();

    let Some(states) = data.states else {
        warn!("OpenSky returned no states");
        return Ok(());
    };

    let flights: Vec<Flight> = states
        .iter()
        .filter_map(|state| parse_state(state, timestamp))
        .collect();

    insert_flights(pool, &flights).await?;
    info!(count = flights.len(), "opensky ingested");
    Ok(())
}

fn parse_state(state: &[serde_json::Value], timestamp: chrono::DateTime<Utc>) -> Option<Flight> {
    if state.len() < 17 {
        return None;
    }

    Some(Flight {
        timestamp,
        icao24: state[0].as_str().map(str::to_string),
        callsign: state[1].as_str().map(|s| s.trim().to_string()),
        origin_country: state[2].as_str().map(str::to_string),
        longitude: state[5].as_f64(),
        latitude: state[6].as_f64(),
        baro_altitude: state[7].as_f64(),
        on_ground: state[8].as_bool(),
        velocity: state[9].as_f64(),
        true_track: state[10].as_f64(),
        vertical_rate: state[11].as_f64(),
        source: "opensky".to_string(),
    })
}
