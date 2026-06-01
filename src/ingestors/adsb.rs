//! Shared types for the readsb / ADS-B Exchange v2 JSON schema served by
//! adsb.fi, adsb.lol, adsb.one, and airplanes.live. The sources differ only in
//! their endpoints and dedup strategy; the aircraft record and its mapping to a
//! [`Flight`] are identical, so they live here once.

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::storage::models::Flight;

/// Feed envelope. readsb names the array `ac`; the adsb.fi snapshot names it
/// `aircraft` — accept either.
#[derive(Debug, Deserialize)]
pub struct AdsbResponse {
    #[serde(rename = "ac", alias = "aircraft")]
    pub aircraft: Option<Vec<AdsbAircraft>>,
}

#[derive(Debug, Deserialize)]
pub struct AdsbAircraft {
    pub hex: Option<String>,
    pub flight: Option<String>,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alt_baro: Option<serde_json::Value>,
    pub alt_geom: Option<f64>,
    pub gs: Option<f64>,
    pub track: Option<f64>,
    pub baro_rate: Option<f64>,
}

impl AdsbAircraft {
    pub fn has_position(&self) -> bool {
        self.lat.is_some() && self.lon.is_some()
    }

    pub fn to_flight(&self, source: &str, timestamp: DateTime<Utc>) -> Flight {
        // `alt_baro` is numeric in flight; the string "ground" marks an aircraft
        // on the surface. Fall back to geometric altitude when present.
        let baro_altitude = self
            .alt_baro
            .as_ref()
            .and_then(|v| v.as_f64())
            .or(self.alt_geom);
        let on_ground = self
            .alt_baro
            .as_ref()
            .and_then(|v| v.as_str())
            .map(|s| s == "ground");

        Flight {
            timestamp,
            icao24: self.hex.clone(),
            callsign: self.flight.as_deref().map(|s| s.trim().to_string()),
            origin_country: None,
            longitude: self.lon,
            latitude: self.lat,
            baro_altitude,
            on_ground,
            velocity: self.gs,
            true_track: self.track,
            vertical_rate: self.baro_rate,
            source: source.to_string(),
        }
    }
}
