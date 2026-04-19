//! Private Bratwurst ADS-B network.
//!
//! Upstream URLs and credentials are injected at runtime (BRATWURST_LOGIN_URL,
//! BRATWURST_API_URL, BRATWURST_USERNAME, BRATWURST_PASSWORD). If any of them
//! is missing the scheduler skips this source.
//!
//! Auth is form-POST with a short-lived cookie session that we refresh on
//! every fetch.

use anyhow::{Context, Result};
use chrono::Utc;
use reqwest::cookie::Jar;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

use crate::storage::models::Flight;
use crate::storage::queries::insert_flights;

#[derive(Debug, Deserialize)]
struct BratwurstResponse {
    aircraft: Option<Vec<BratwurstAircraft>>,
}

#[derive(Debug, Deserialize)]
struct BratwurstAircraft {
    hex: Option<String>,
    flight: Option<String>,
    lat: Option<f64>,
    lon: Option<f64>,
    alt_baro: Option<serde_json::Value>,
    alt_geom: Option<i32>,
    gs: Option<f64>,
    track: Option<f64>,
    baro_rate: Option<f64>,
}

fn build_client() -> Result<reqwest::Client> {
    let jar = Arc::new(Jar::default());
    reqwest::Client::builder()
        .cookie_store(true)
        .cookie_provider(jar)
        .timeout(Duration::from_secs(30))
        .build()
        .context("building bratwurst http client")
}

async fn login(
    login_url: &str,
    api_url: &str,
    username: &str,
    password: &str,
) -> Result<reqwest::Client> {
    let client = build_client()?;

    let response = client
        .post(login_url)
        .form(&[
            ("name", username),
            ("password", password),
            ("submit", "Login"),
        ])
        .send()
        .await?;

    let status = response.status();
    if !(status.is_success() || status.is_redirection()) {
        anyhow::bail!("Login failed (status {status})");
    }

    let probe = client.get(api_url).send().await?;
    if !probe.status().is_success() {
        anyhow::bail!("Login failed (probe status {})", probe.status());
    }

    let body = probe.text().await?;
    if !body.contains("aircraft") {
        anyhow::bail!("Login failed (session not accepted)");
    }

    info!("bratwurst login ok");
    Ok(client)
}

pub async fn fetch(
    pool: &PgPool,
    login_url: &str,
    api_url: &str,
    username: &str,
    password: &str,
) -> Result<()> {
    let client = login(login_url, api_url, username, password).await?;

    let response = client.get(api_url).send().await?;
    if !response.status().is_success() {
        anyhow::bail!("bratwurst API status {}", response.status());
    }

    let data: BratwurstResponse = response.json().await?;
    let aircraft = data.aircraft.unwrap_or_default();
    let timestamp = Utc::now();

    let flights: Vec<Flight> = aircraft
        .iter()
        .filter_map(|ac| {
            let (hex, lat, lon) = match (&ac.hex, ac.lat, ac.lon) {
                (Some(h), Some(la), Some(lo)) => (h, la, lo),
                _ => return None,
            };

            let altitude = match &ac.alt_baro {
                Some(serde_json::Value::Number(n)) => n.as_f64(),
                Some(serde_json::Value::String(s)) if s == "ground" => Some(0.0),
                _ => ac.alt_geom.map(f64::from),
            };
            let on_ground = altitude == Some(0.0);

            Some(Flight {
                timestamp,
                icao24: Some(hex.to_ascii_lowercase()),
                callsign: ac.flight.as_deref().map(|s| s.trim().to_string()),
                origin_country: None,
                longitude: Some(lon),
                latitude: Some(lat),
                baro_altitude: altitude,
                on_ground: Some(on_ground),
                velocity: ac.gs,
                true_track: ac.track,
                vertical_rate: ac.baro_rate,
                source: "bratwurst".to_string(),
            })
        })
        .collect();

    if flights.is_empty() {
        warn!("bratwurst returned no aircraft");
        return Ok(());
    }

    insert_flights(pool, &flights).await?;
    info!(count = flights.len(), "bratwurst ingested");
    Ok(())
}
