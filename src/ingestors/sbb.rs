use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

use crate::storage::models::Departure;
use crate::storage::queries::insert_departures;
use crate::utils::http::create_client;

const API_BASE: &str = "https://transport.opendata.ch/v1/stationboard";

const STATIONS: &[(&str, &str)] = &[
    ("8503000", "Zürich HB"),
    ("8507000", "Bern"),
    ("8500010", "Basel SBB"),
    ("8501120", "Genève"),
    ("8501008", "Lausanne"),
    ("8505000", "Luzern"),
    ("8506302", "Winterthur"),
    ("8506000", "St. Gallen"),
    ("8503006", "Zürich Flughafen"),
    ("8503504", "Olten"),
    ("8502113", "Aarau"),
    ("8504300", "Biel/Bienne"),
    ("8504100", "Fribourg/Freiburg"),
    ("8502204", "Zug"),
    ("8505400", "Locarno"),
    ("8505300", "Lugano"),
    ("8508100", "Bellinzona"),
    ("8509000", "Chur"),
    ("8507483", "Interlaken Ost"),
    ("8501300", "Montreux"),
    ("8507493", "Interlaken West"),
    ("8501400", "Aigle"),
    ("8500090", "Basel Bad Bf"),
    ("8506112", "Rapperswil SG"),
    ("8505004", "Arth-Goldau"),
    ("8502202", "Zug Oberwil"),
    ("8509404", "Zermatt"),
    ("8503503", "Baden"),
    ("8506121", "Wil SG"),
    ("8502007", "Zürich Oerlikon"),
    ("8503003", "Zürich Stadelhofen"),
    ("8503016", "Zürich Hardbrücke"),
    ("8503010", "Zürich Enge"),
    ("8507002", "Bern Wankdorf"),
    ("8504421", "Biel Mett"),
    ("8506349", "Schaffhausen"),
    ("8509073", "Davos Platz"),
    ("8509072", "Davos Dorf"),
    ("8500309", "Brugg AG"),
    ("8500218", "Liestal"),
    ("8501026", "Genève-Aéroport"),
    ("8501007", "Lausanne-Flon"),
    ("8508419", "Sion"),
    ("8500501", "Neuchâtel"),
    ("8506300", "Kreuzlingen"),
    ("8507785", "Thun"),
    ("8506009", "Gossau SG"),
];

#[derive(Debug, Deserialize)]
struct StationboardResponse {
    stationboard: Option<Vec<StationboardEntry>>,
}

#[derive(Debug, Deserialize)]
struct StationboardEntry {
    stop: Option<StopInfo>,
    name: Option<String>,
    category: Option<String>,
    number: Option<String>,
    to: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StopInfo {
    #[serde(rename = "departureTimestamp")]
    departure_timestamp: Option<i64>,
    delay: Option<i32>,
    platform: Option<String>,
    prognosis: Option<Prognosis>,
}

#[derive(Debug, Deserialize)]
struct Prognosis {
    platform: Option<String>,
    departure: Option<String>,
}

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let fetch_time = Utc::now();
    let mut all_departures = Vec::new();

    for (station_id, station_name) in STATIONS {
        let url = format!("{API_BASE}?id={station_id}&limit=30");

        match client.get(&url).send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(data) = response.json::<StationboardResponse>().await {
                    if let Some(entries) = data.stationboard {
                        for entry in &entries {
                            if let Some(dep) = parse(entry, station_id, station_name, fetch_time) {
                                all_departures.push(dep);
                            }
                        }
                    }
                }
            }
            Ok(response) => {
                warn!(station = station_name, status = %response.status(), "sbb non-ok")
            }
            Err(e) => warn!(station = station_name, error = %e, "sbb request failed"),
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if all_departures.is_empty() {
        warn!("sbb returned no departures");
        return Ok(());
    }

    insert_departures(pool, &all_departures).await?;
    info!(
        count = all_departures.len(),
        stations = STATIONS.len(),
        "sbb ingested"
    );
    Ok(())
}

fn parse(
    entry: &StationboardEntry,
    station_id: &str,
    station_name: &str,
    fetch_time: DateTime<Utc>,
) -> Option<Departure> {
    let stop = entry.stop.as_ref()?;

    let planned_time = stop
        .departure_timestamp
        .and_then(|ts| DateTime::from_timestamp(ts, 0));

    let actual_time = stop
        .prognosis
        .as_ref()
        .and_then(|p| p.departure.as_deref())
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc));

    let delay_seconds = stop.delay.map(|d| d * 60);

    let line_name = match (&entry.category, &entry.number) {
        (Some(cat), Some(num)) => Some(format!("{cat} {num}")),
        (Some(cat), None) => Some(cat.clone()),
        _ => entry.name.clone(),
    };

    let platform_planned = stop.platform.clone();
    let platform_actual = stop.prognosis.as_ref().and_then(|p| p.platform.clone());
    let platform_changed = platform_actual.is_some() && platform_planned != platform_actual;

    Some(Departure {
        timestamp: fetch_time,
        station_id: format!("sbb:{station_id}"),
        station_name: station_name.to_string(),
        trip_id: None,
        line_name,
        direction: entry.to.clone(),
        planned_time,
        actual_time,
        delay_seconds,
        cancelled: false,
        platform_planned,
        platform_actual,
        platform_changed,
    })
}
