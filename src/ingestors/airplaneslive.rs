//! airplanes.live: 250 nm radial queries from a fixed grid of reference points.
//! The grid is intentionally wide so coverage overlaps; we dedupe by ICAO24.

use anyhow::Result;
use chrono::Utc;
use serde::Deserialize;
use sqlx::PgPool;
use std::collections::HashSet;
use tracing::{info, warn};

use crate::storage::models::Flight;
use crate::storage::queries::insert_flights;
use crate::utils::http::create_client;

const REGIONS: &[(&str, &str)] = &[
    (
        "eu-central",
        "https://api.airplanes.live/v2/point/50/10/250",
    ),
    ("eu-west", "https://api.airplanes.live/v2/point/48/-2/250"),
    ("eu-east", "https://api.airplanes.live/v2/point/52/20/250"),
    ("eu-south", "https://api.airplanes.live/v2/point/42/12/250"),
    ("eu-north", "https://api.airplanes.live/v2/point/60/15/250"),
    ("uk", "https://api.airplanes.live/v2/point/52/-1/250"),
    ("spain", "https://api.airplanes.live/v2/point/40/-4/250"),
    ("turkey", "https://api.airplanes.live/v2/point/39/32/250"),
    ("greece", "https://api.airplanes.live/v2/point/38/24/250"),
    (
        "scandinavia",
        "https://api.airplanes.live/v2/point/63/10/250",
    ),
    (
        "russia-west",
        "https://api.airplanes.live/v2/point/56/38/250",
    ),
    (
        "us-northeast",
        "https://api.airplanes.live/v2/point/42/-72/250",
    ),
    ("us-east", "https://api.airplanes.live/v2/point/35/-80/250"),
    (
        "us-southeast",
        "https://api.airplanes.live/v2/point/28/-82/250",
    ),
    (
        "us-central",
        "https://api.airplanes.live/v2/point/40/-95/250",
    ),
    (
        "us-midwest",
        "https://api.airplanes.live/v2/point/42/-88/250",
    ),
    (
        "us-southwest",
        "https://api.airplanes.live/v2/point/33/-112/250",
    ),
    ("us-west", "https://api.airplanes.live/v2/point/37/-120/250"),
    (
        "us-northwest",
        "https://api.airplanes.live/v2/point/47/-122/250",
    ),
    ("us-south", "https://api.airplanes.live/v2/point/30/-95/250"),
    (
        "us-florida",
        "https://api.airplanes.live/v2/point/26/-80/250",
    ),
    (
        "canada-east",
        "https://api.airplanes.live/v2/point/45/-75/250",
    ),
    (
        "canada-west",
        "https://api.airplanes.live/v2/point/49/-123/250",
    ),
    (
        "canada-central",
        "https://api.airplanes.live/v2/point/51/-114/250",
    ),
    ("mexico", "https://api.airplanes.live/v2/point/19/-99/250"),
    (
        "caribbean",
        "https://api.airplanes.live/v2/point/18/-66/250",
    ),
    ("japan", "https://api.airplanes.live/v2/point/35/139/250"),
    ("korea", "https://api.airplanes.live/v2/point/37/127/250"),
    (
        "china-east",
        "https://api.airplanes.live/v2/point/31/121/250",
    ),
    (
        "china-north",
        "https://api.airplanes.live/v2/point/40/116/250",
    ),
    (
        "china-south",
        "https://api.airplanes.live/v2/point/23/113/250",
    ),
    ("hongkong", "https://api.airplanes.live/v2/point/22/114/250"),
    ("taiwan", "https://api.airplanes.live/v2/point/25/121/250"),
    (
        "philippines",
        "https://api.airplanes.live/v2/point/14/121/250",
    ),
    ("thailand", "https://api.airplanes.live/v2/point/14/100/250"),
    ("singapore", "https://api.airplanes.live/v2/point/1/104/250"),
    (
        "indonesia",
        "https://api.airplanes.live/v2/point/-6/107/250",
    ),
    ("malaysia", "https://api.airplanes.live/v2/point/3/101/250"),
    ("vietnam", "https://api.airplanes.live/v2/point/21/106/250"),
    (
        "india-north",
        "https://api.airplanes.live/v2/point/28/77/250",
    ),
    (
        "india-south",
        "https://api.airplanes.live/v2/point/13/77/250",
    ),
    (
        "india-west",
        "https://api.airplanes.live/v2/point/19/73/250",
    ),
    ("uae", "https://api.airplanes.live/v2/point/25/55/250"),
    ("qatar", "https://api.airplanes.live/v2/point/25/51/250"),
    ("saudi", "https://api.airplanes.live/v2/point/24/46/250"),
    ("israel", "https://api.airplanes.live/v2/point/32/35/250"),
    ("iran", "https://api.airplanes.live/v2/point/35/51/250"),
    (
        "australia-east",
        "https://api.airplanes.live/v2/point/-33/151/250",
    ),
    (
        "australia-north",
        "https://api.airplanes.live/v2/point/-12/131/250",
    ),
    (
        "australia-west",
        "https://api.airplanes.live/v2/point/-32/116/250",
    ),
    (
        "newzealand",
        "https://api.airplanes.live/v2/point/-41/175/250",
    ),
    (
        "brazil-south",
        "https://api.airplanes.live/v2/point/-23/-46/250",
    ),
    (
        "brazil-north",
        "https://api.airplanes.live/v2/point/-3/-60/250",
    ),
    (
        "argentina",
        "https://api.airplanes.live/v2/point/-34/-58/250",
    ),
    ("chile", "https://api.airplanes.live/v2/point/-33/-71/250"),
    ("colombia", "https://api.airplanes.live/v2/point/4/-74/250"),
    ("peru", "https://api.airplanes.live/v2/point/-12/-77/250"),
    (
        "south-africa",
        "https://api.airplanes.live/v2/point/-26/28/250",
    ),
    ("egypt", "https://api.airplanes.live/v2/point/30/31/250"),
    ("morocco", "https://api.airplanes.live/v2/point/34/-6/250"),
    ("nigeria", "https://api.airplanes.live/v2/point/6/3/250"),
    ("kenya", "https://api.airplanes.live/v2/point/-1/37/250"),
    ("ethiopia", "https://api.airplanes.live/v2/point/9/39/250"),
];

#[derive(Debug, Deserialize)]
struct AirplanesLiveResponse {
    ac: Option<Vec<AirplanesLiveAircraft>>,
}

#[derive(Debug, Deserialize)]
struct AirplanesLiveAircraft {
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
    let mut seen: HashSet<String> = HashSet::new();
    let mut all_flights = Vec::new();

    for (region, url) in REGIONS {
        match fetch_region(&client, url, timestamp).await {
            Ok(flights) => {
                for f in flights {
                    // Dedupe on ICAO24; drop records without one since
                    // HashSet<&str> can't represent "missing" safely.
                    let Some(ref hex) = f.icao24 else { continue };
                    if seen.insert(hex.clone()) {
                        all_flights.push(f);
                    }
                }
            }
            Err(e) => warn!(region, error = %e, "airplanes.live region failed"),
        }
    }

    insert_flights(pool, &all_flights).await?;
    info!(count = all_flights.len(), "airplanes.live ingested");
    Ok(())
}

async fn fetch_region(
    client: &reqwest::Client,
    url: &str,
    timestamp: chrono::DateTime<Utc>,
) -> Result<Vec<Flight>> {
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        anyhow::bail!("airplanes.live returned status {}", response.status());
    }

    let data: AirplanesLiveResponse = response.json().await?;
    let Some(aircraft) = data.ac else {
        return Ok(Vec::new());
    };

    Ok(aircraft
        .iter()
        .filter_map(|ac| parse(ac, timestamp))
        .collect())
}

fn parse(ac: &AirplanesLiveAircraft, timestamp: chrono::DateTime<Utc>) -> Option<Flight> {
    if ac.lat.is_none() || ac.lon.is_none() {
        return None;
    }

    let altitude = ac.alt_baro.as_ref().and_then(|v| v.as_f64());
    let on_ground = ac
        .alt_baro
        .as_ref()
        .and_then(|v| v.as_str())
        .map(|s| s == "ground");

    Some(Flight {
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
        source: "airplaneslive".to_string(),
    })
}
