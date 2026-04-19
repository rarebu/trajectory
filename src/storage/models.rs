use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flight {
    pub timestamp: DateTime<Utc>,
    pub icao24: Option<String>,
    pub callsign: Option<String>,
    pub origin_country: Option<String>,
    pub longitude: Option<f64>,
    pub latitude: Option<f64>,
    pub baro_altitude: Option<f64>,
    pub on_ground: Option<bool>,
    pub velocity: Option<f64>,
    pub true_track: Option<f64>,
    pub vertical_rate: Option<f64>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ship {
    pub timestamp: DateTime<Utc>,
    pub mmsi: Option<String>,
    pub ship_name: Option<String>,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub message_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Departure {
    pub timestamp: DateTime<Utc>,
    pub station_id: String,
    pub station_name: String,
    pub trip_id: Option<String>,
    pub line_name: Option<String>,
    pub direction: Option<String>,
    pub planned_time: Option<DateTime<Utc>>,
    pub actual_time: Option<DateTime<Utc>>,
    pub delay_seconds: Option<i32>,
    pub cancelled: bool,
    pub platform_planned: Option<String>,
    pub platform_actual: Option<String>,
    pub platform_changed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Satellite {
    pub timestamp: DateTime<Utc>,
    pub norad_id: i32,
    pub name: String,
    pub epoch: Option<DateTime<Utc>>,
    pub mean_motion: Option<f64>,
    pub inclination: Option<f64>,
    pub eccentricity: Option<f64>,
    pub tle_line1: Option<String>,
    pub tle_line2: Option<String>,
}

/// GTFS-Realtime VehiclePosition row. Latitude/longitude are required by
/// spec (`Position` fields); bearing/speed are optional.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VehiclePositionRecord {
    pub timestamp: DateTime<Utc>,
    pub feed_timestamp: i64,
    pub vehicle_id: Option<String>,
    pub trip_id: Option<String>,
    pub route_id: Option<String>,
    pub latitude: f64,
    pub longitude: f64,
    pub bearing: Option<f32>,
    pub speed: Option<f32>,
}
