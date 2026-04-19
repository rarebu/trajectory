//! Austrian Railways via the oebb.macistry.com API (transport-rest compatible).

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

use crate::storage::models::Departure;
use crate::storage::queries::insert_departures;
use crate::utils::http::create_client;

const API_BASE: &str = "https://oebb.macistry.com/api";

const STATIONS: &[(&str, &str)] = &[
    ("8100002", "Wien Hbf"),
    ("8100003", "Wien Meidling"),
    ("8100008", "Wien Praterstern"),
    ("8100353", "Wien Floridsdorf"),
    ("8100173", "Salzburg Hbf"),
    ("8100013", "Linz Hbf"),
    ("8100101", "Graz Hbf"),
    ("8100108", "Innsbruck Hbf"),
    ("8100020", "Klagenfurt Hbf"),
    ("8100085", "St. Pölten Hbf"),
    ("8100050", "Villach Hbf"),
    ("8100041", "Bregenz"),
    ("8100017", "Wels Hbf"),
    ("8100079", "Leoben Hbf"),
    ("8100061", "Wiener Neustadt Hbf"),
    ("8100037", "Krems an der Donau"),
    ("8100113", "Feldkirch"),
    ("8100024", "Attnang-Puchheim"),
    ("8100063", "Bruck an der Mur"),
    ("8100128", "Dornbirn"),
];

#[derive(Debug, Deserialize)]
struct OebbResponse {
    departures: Option<Vec<OebbDeparture>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct OebbDeparture {
    trip_id: Option<String>,
    direction: Option<String>,
    line: Option<OebbLine>,
    when: Option<String>,
    planned_when: Option<String>,
    delay: Option<i32>,
    platform: Option<String>,
    planned_platform: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OebbLine {
    name: Option<String>,
}

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let fetch_time = Utc::now();
    let mut all_departures = Vec::new();

    for (station_id, station_name) in STATIONS {
        let url = format!("{API_BASE}/stops/{station_id}/departures?duration=60");

        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(data) = response.json::<OebbResponse>().await {
                    if let Some(deps) = data.departures {
                        for dep in deps {
                            all_departures.push(parse(&dep, station_id, station_name, fetch_time));
                        }
                    }
                }
            }
            Ok(r) => warn!(station = station_name, status = %r.status(), "oebb non-ok"),
            Err(e) => warn!(station = station_name, error = %e, "oebb request failed"),
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    if !all_departures.is_empty() {
        insert_departures(pool, &all_departures).await?;
    }

    info!(
        count = all_departures.len(),
        stations = STATIONS.len(),
        "oebb ingested"
    );
    Ok(())
}

fn parse(
    dep: &OebbDeparture,
    station_id: &str,
    station_name: &str,
    fetch_time: DateTime<Utc>,
) -> Departure {
    let planned_time = dep
        .planned_when
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let actual_time = dep
        .when
        .as_deref()
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let platform_changed = dep.platform != dep.planned_platform
        && dep.platform.is_some()
        && dep.planned_platform.is_some();

    Departure {
        timestamp: fetch_time,
        station_id: format!("at:{station_id}"),
        station_name: station_name.to_string(),
        trip_id: dep.trip_id.clone(),
        line_name: dep.line.as_ref().and_then(|l| l.name.clone()),
        direction: dep.direction.clone(),
        planned_time,
        actual_time,
        delay_seconds: dep.delay,
        cancelled: false,
        platform_planned: dep.planned_platform.clone(),
        platform_actual: dep.platform.clone(),
        platform_changed,
    }
}
