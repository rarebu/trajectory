//! Belgian Railways via the iRail public API. No key required; the service
//! limits traffic to roughly 3 requests/second per IP.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

use crate::storage::models::Departure;
use crate::storage::queries::insert_departures;
use crate::utils::http::create_client;

const API_BASE: &str = "https://api.irail.be";

const STATIONS: &[(&str, &str)] = &[
    ("BE.NMBS.008814001", "Brussel-Zuid"),
    ("BE.NMBS.008813003", "Brussel-Noord"),
    ("BE.NMBS.008812005", "Brussel-Centraal"),
    ("BE.NMBS.008821006", "Antwerpen-Centraal"),
    ("BE.NMBS.008892007", "Gent-Sint-Pieters"),
    ("BE.NMBS.008841004", "Liège-Guillemins"),
    ("BE.NMBS.008891009", "Brugge"),
    ("BE.NMBS.008872009", "Charleroi-Central"),
    ("BE.NMBS.008844008", "Namur"),
    ("BE.NMBS.008831005", "Leuven"),
    ("BE.NMBS.008821121", "Antwerpen-Berchem"),
    ("BE.NMBS.008891702", "Oostende"),
    ("BE.NMBS.008893120", "Kortrijk"),
    ("BE.NMBS.008833001", "Mechelen"),
    ("BE.NMBS.008861002", "Hasselt"),
    ("BE.NMBS.008843000", "Mons"),
    ("BE.NMBS.008819406", "Brussels Airport"),
    ("BE.NMBS.008892601", "Gent-Dampoort"),
    ("BE.NMBS.008811916", "Schaarbeek"),
    ("BE.NMBS.008815040", "Midi"),
];

#[derive(Debug, Deserialize)]
struct LiveboardResponse {
    departures: Option<DepartureList>,
}

#[derive(Debug, Deserialize)]
struct DepartureList {
    departure: Option<Vec<IrailDeparture>>,
}

#[derive(Debug, Deserialize)]
struct IrailDeparture {
    station: Option<String>,
    time: Option<String>,
    delay: Option<String>,
    canceled: Option<String>,
    vehicle: Option<String>,
    platform: Option<String>,
    platforminfo: Option<PlatformInfo>,
}

#[derive(Debug, Deserialize)]
struct PlatformInfo {
    normal: Option<String>,
}

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let fetch_time = Utc::now();
    let mut all_departures = Vec::new();

    for (station_id, station_name) in STATIONS {
        let url = format!("{API_BASE}/liveboard/?id={station_id}&format=json");

        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(data) = response.json::<LiveboardResponse>().await {
                    if let Some(deps) = data.departures.and_then(|d| d.departure) {
                        for dep in deps {
                            if let Some(parsed) = parse(&dep, station_id, station_name, fetch_time)
                            {
                                all_departures.push(parsed);
                            }
                        }
                    }
                }
            }
            Ok(r) => warn!(station = station_name, status = %r.status(), "irail non-ok"),
            Err(e) => warn!(station = station_name, error = %e, "irail request failed"),
        }

        tokio::time::sleep(Duration::from_millis(350)).await;
    }

    if !all_departures.is_empty() {
        insert_departures(pool, &all_departures).await?;
    }

    info!(
        count = all_departures.len(),
        stations = STATIONS.len(),
        "irail ingested"
    );
    Ok(())
}

fn parse(
    dep: &IrailDeparture,
    station_id: &str,
    station_name: &str,
    fetch_time: DateTime<Utc>,
) -> Option<Departure> {
    let planned_time = dep
        .time
        .as_deref()
        .and_then(|t| t.parse::<i64>().ok())
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    let delay_seconds = dep.delay.as_deref().and_then(|d| d.parse::<i32>().ok());
    let cancelled = dep.canceled.as_deref() == Some("1");
    let platform_changed = dep
        .platforminfo
        .as_ref()
        .and_then(|p| p.normal.as_deref())
        .map(|n| n != "1")
        .unwrap_or(false);

    Some(Departure {
        timestamp: fetch_time,
        station_id: format!("be:{station_id}"),
        station_name: station_name.to_string(),
        trip_id: dep.vehicle.clone(),
        line_name: dep.vehicle.clone(),
        direction: dep.station.clone(),
        planned_time,
        actual_time: None,
        delay_seconds,
        cancelled,
        platform_planned: dep.platform.clone(),
        platform_actual: if platform_changed {
            dep.platform.clone()
        } else {
            None
        },
        platform_changed,
    })
}
