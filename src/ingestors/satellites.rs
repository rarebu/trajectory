//! CelesTrak two-line-element (TLE) orbital elements, keyed by category.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

use crate::storage::models::Satellite;
use crate::storage::queries::insert_satellites;
use crate::utils::http::create_client_with_timeout;

const CELESTRAK_BASE: &str = "https://celestrak.org/NORAD/elements/gp.php";

const CATEGORIES: &[(&str, &str)] = &[
    ("stations", "Space Stations"),
    ("starlink", "Starlink"),
    ("oneweb", "OneWeb"),
    ("planet", "Planet Labs"),
    ("spire", "Spire Global"),
    ("iridium-NEXT", "Iridium NEXT"),
    ("globalstar", "Globalstar"),
    ("orbcomm", "Orbcomm"),
    ("intelsat", "Intelsat"),
    ("ses", "SES"),
    ("geo", "Geostationary"),
    ("gps-ops", "GPS"),
    ("galileo", "Galileo"),
    ("glonass", "GLONASS"),
    ("beidou", "BeiDou"),
    ("weather", "Weather"),
    ("resource", "Earth Resources"),
    ("active", "Active Satellites"),
    ("visual", "Visual"),
    ("amateur", "Amateur Radio"),
    ("cubesat", "CubeSats"),
    ("science", "Science"),
    ("engineering", "Engineering"),
    ("education", "Education"),
];

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
struct CelesTrakSatellite {
    norad_cat_id: i32,
    object_name: String,
    epoch: Option<String>,
    mean_motion: Option<f64>,
    eccentricity: Option<f64>,
    inclination: Option<f64>,
    tle_line1: Option<String>,
    tle_line2: Option<String>,
}

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client_with_timeout(60)?;
    let mut all_satellites = Vec::new();
    let timestamp = Utc::now();

    for (category, description) in CATEGORIES {
        match fetch_category(&client, category, timestamp).await {
            Ok(satellites) => {
                info!(
                    category,
                    description,
                    count = satellites.len(),
                    "celestrak fetched"
                );
                all_satellites.extend(satellites);
            }
            Err(e) => warn!(category, error = %e, "celestrak fetch failed"),
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    insert_satellites(pool, &all_satellites).await?;
    info!(count = all_satellites.len(), "celestrak ingested");
    Ok(())
}

async fn fetch_category(
    client: &reqwest::Client,
    category: &str,
    timestamp: DateTime<Utc>,
) -> Result<Vec<Satellite>> {
    let url = format!("{CELESTRAK_BASE}?GROUP={category}&FORMAT=json");
    let response = client.get(&url).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("celestrak returned status {}", response.status());
    }

    let data: Vec<CelesTrakSatellite> = response.json().await?;
    Ok(data.iter().map(|sat| parse(sat, timestamp)).collect())
}

fn parse(sat: &CelesTrakSatellite, timestamp: DateTime<Utc>) -> Satellite {
    let epoch = sat
        .epoch
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    Satellite {
        timestamp,
        norad_id: sat.norad_cat_id,
        name: sat.object_name.clone(),
        epoch,
        mean_motion: sat.mean_motion,
        inclination: sat.inclination,
        eccentricity: sat.eccentricity,
        tle_line1: sat.tle_line1.clone(),
        tle_line2: sat.tle_line2.clone(),
    }
}
