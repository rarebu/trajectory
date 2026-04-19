//! Bulk inserts via PostgreSQL UNNEST.
//!
//! Per-column array binding is 50-100x faster than per-row INSERTs and lets
//! sqlx cache prepared statements. Arrays borrow from the caller where
//! possible to avoid per-row clones.

use std::time::Instant;

use anyhow::Result;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use tracing::debug;

use super::models::{Departure, Flight, Satellite, Ship, VehiclePositionRecord};

pub async fn insert_flights(pool: &PgPool, flights: &[Flight]) -> Result<u64> {
    if flights.is_empty() {
        return Ok(0);
    }

    let start = Instant::now();
    let batch_len = flights.len();

    let timestamps: Vec<DateTime<Utc>> = flights.iter().map(|f| f.timestamp).collect();
    let icao24s: Vec<Option<&str>> = flights.iter().map(|f| f.icao24.as_deref()).collect();
    let callsigns: Vec<Option<&str>> = flights.iter().map(|f| f.callsign.as_deref()).collect();
    let countries: Vec<Option<&str>> = flights
        .iter()
        .map(|f| f.origin_country.as_deref())
        .collect();
    let longitudes: Vec<Option<f64>> = flights.iter().map(|f| f.longitude).collect();
    let latitudes: Vec<Option<f64>> = flights.iter().map(|f| f.latitude).collect();
    let altitudes: Vec<Option<f64>> = flights.iter().map(|f| f.baro_altitude).collect();
    let on_grounds: Vec<Option<bool>> = flights.iter().map(|f| f.on_ground).collect();
    let velocities: Vec<Option<f64>> = flights.iter().map(|f| f.velocity).collect();
    let tracks: Vec<Option<f64>> = flights.iter().map(|f| f.true_track).collect();
    let vert_rates: Vec<Option<f64>> = flights.iter().map(|f| f.vertical_rate).collect();
    let sources: Vec<&str> = flights.iter().map(|f| f.source.as_str()).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO flights (timestamp, icao24, callsign, origin_country, longitude, latitude,
                            baro_altitude, on_ground, velocity, true_track, vertical_rate,
                            geom, source)
        SELECT
            t.timestamp, t.icao24, t.callsign, t.origin_country, t.longitude, t.latitude,
            t.baro_altitude, t.on_ground, t.velocity, t.true_track, t.vertical_rate,
            CASE WHEN t.longitude IS NOT NULL AND t.latitude IS NOT NULL
                 THEN ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326)
                 ELSE NULL END,
            t.source
        FROM UNNEST(
            $1::timestamptz[], $2::text[], $3::text[], $4::text[],
            $5::float8[], $6::float8[], $7::float8[], $8::bool[],
            $9::float8[], $10::float8[], $11::float8[], $12::text[]
        ) AS t(timestamp, icao24, callsign, origin_country, longitude, latitude,
               baro_altitude, on_ground, velocity, true_track, vertical_rate, source)
        "#,
    )
    .bind(&timestamps)
    .bind(&icao24s)
    .bind(&callsigns)
    .bind(&countries)
    .bind(&longitudes)
    .bind(&latitudes)
    .bind(&altitudes)
    .bind(&on_grounds)
    .bind(&velocities)
    .bind(&tracks)
    .bind(&vert_rates)
    .bind(&sources)
    .execute(pool)
    .await?;

    let count = result.rows_affected();
    debug!(
        table = "flights",
        count,
        batch = batch_len,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "bulk insert"
    );
    Ok(count)
}

pub async fn insert_ships(pool: &PgPool, ships: &[Ship]) -> Result<u64> {
    if ships.is_empty() {
        return Ok(0);
    }

    let start = Instant::now();
    let batch_len = ships.len();

    let timestamps: Vec<DateTime<Utc>> = ships.iter().map(|s| s.timestamp).collect();
    let mmsis: Vec<Option<&str>> = ships.iter().map(|s| s.mmsi.as_deref()).collect();
    let names: Vec<Option<&str>> = ships.iter().map(|s| s.ship_name.as_deref()).collect();
    let latitudes: Vec<Option<f64>> = ships.iter().map(|s| s.latitude).collect();
    let longitudes: Vec<Option<f64>> = ships.iter().map(|s| s.longitude).collect();
    let msg_types: Vec<Option<&str>> = ships.iter().map(|s| s.message_type.as_deref()).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO ships (timestamp, mmsi, ship_name, latitude, longitude, message_type, geom)
        SELECT
            t.timestamp, t.mmsi, t.ship_name, t.latitude, t.longitude, t.message_type,
            CASE WHEN t.longitude IS NOT NULL AND t.latitude IS NOT NULL
                 THEN ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326)
                 ELSE NULL END
        FROM UNNEST(
            $1::timestamptz[], $2::text[], $3::text[],
            $4::float8[], $5::float8[], $6::text[]
        ) AS t(timestamp, mmsi, ship_name, latitude, longitude, message_type)
        "#,
    )
    .bind(&timestamps)
    .bind(&mmsis)
    .bind(&names)
    .bind(&latitudes)
    .bind(&longitudes)
    .bind(&msg_types)
    .execute(pool)
    .await?;

    let count = result.rows_affected();
    debug!(
        table = "ships",
        count,
        batch = batch_len,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "bulk insert"
    );
    Ok(count)
}

pub async fn insert_departures(pool: &PgPool, departures: &[Departure]) -> Result<u64> {
    if departures.is_empty() {
        return Ok(0);
    }

    let start = Instant::now();
    let batch_len = departures.len();

    let timestamps: Vec<DateTime<Utc>> = departures.iter().map(|d| d.timestamp).collect();
    let station_ids: Vec<&str> = departures.iter().map(|d| d.station_id.as_str()).collect();
    let station_names: Vec<&str> = departures.iter().map(|d| d.station_name.as_str()).collect();
    let trip_ids: Vec<Option<&str>> = departures.iter().map(|d| d.trip_id.as_deref()).collect();
    let line_names: Vec<Option<&str>> = departures.iter().map(|d| d.line_name.as_deref()).collect();
    let directions: Vec<Option<&str>> = departures.iter().map(|d| d.direction.as_deref()).collect();
    let planned_times: Vec<Option<DateTime<Utc>>> =
        departures.iter().map(|d| d.planned_time).collect();
    let actual_times: Vec<Option<DateTime<Utc>>> =
        departures.iter().map(|d| d.actual_time).collect();
    let delays: Vec<Option<i32>> = departures.iter().map(|d| d.delay_seconds).collect();
    let cancelleds: Vec<bool> = departures.iter().map(|d| d.cancelled).collect();
    let platforms_planned: Vec<Option<&str>> = departures
        .iter()
        .map(|d| d.platform_planned.as_deref())
        .collect();
    let platforms_actual: Vec<Option<&str>> = departures
        .iter()
        .map(|d| d.platform_actual.as_deref())
        .collect();
    let platform_changeds: Vec<bool> = departures.iter().map(|d| d.platform_changed).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO db_departures (timestamp, station_id, station_name, trip_id, line_name,
                                   direction, planned_time, actual_time, delay_seconds,
                                   cancelled, platform_planned, platform_actual, platform_changed)
        SELECT
            t.timestamp, t.station_id, t.station_name, t.trip_id, t.line_name,
            t.direction, t.planned_time, t.actual_time, t.delay_seconds,
            t.cancelled, t.platform_planned, t.platform_actual, t.platform_changed
        FROM UNNEST(
            $1::timestamptz[], $2::text[], $3::text[], $4::text[], $5::text[],
            $6::text[], $7::timestamptz[], $8::timestamptz[], $9::int4[],
            $10::bool[], $11::text[], $12::text[], $13::bool[]
        ) AS t(timestamp, station_id, station_name, trip_id, line_name,
               direction, planned_time, actual_time, delay_seconds,
               cancelled, platform_planned, platform_actual, platform_changed)
        "#,
    )
    .bind(&timestamps)
    .bind(&station_ids)
    .bind(&station_names)
    .bind(&trip_ids)
    .bind(&line_names)
    .bind(&directions)
    .bind(&planned_times)
    .bind(&actual_times)
    .bind(&delays)
    .bind(&cancelleds)
    .bind(&platforms_planned)
    .bind(&platforms_actual)
    .bind(&platform_changeds)
    .execute(pool)
    .await?;

    let count = result.rows_affected();
    debug!(
        table = "db_departures",
        count,
        batch = batch_len,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "bulk insert"
    );
    Ok(count)
}

pub async fn insert_satellites(pool: &PgPool, satellites: &[Satellite]) -> Result<u64> {
    if satellites.is_empty() {
        return Ok(0);
    }

    let start = Instant::now();
    let batch_len = satellites.len();

    let timestamps: Vec<DateTime<Utc>> = satellites.iter().map(|s| s.timestamp).collect();
    let norad_ids: Vec<i32> = satellites.iter().map(|s| s.norad_id).collect();
    let names: Vec<&str> = satellites.iter().map(|s| s.name.as_str()).collect();
    let epochs: Vec<Option<DateTime<Utc>>> = satellites.iter().map(|s| s.epoch).collect();
    let mean_motions: Vec<Option<f64>> = satellites.iter().map(|s| s.mean_motion).collect();
    let inclinations: Vec<Option<f64>> = satellites.iter().map(|s| s.inclination).collect();
    let eccentricities: Vec<Option<f64>> = satellites.iter().map(|s| s.eccentricity).collect();
    let tle1s: Vec<Option<&str>> = satellites.iter().map(|s| s.tle_line1.as_deref()).collect();
    let tle2s: Vec<Option<&str>> = satellites.iter().map(|s| s.tle_line2.as_deref()).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO satellites (timestamp, norad_id, name, epoch, mean_motion,
                               inclination, eccentricity, tle_line1, tle_line2)
        SELECT
            t.timestamp, t.norad_id, t.name, t.epoch, t.mean_motion,
            t.inclination, t.eccentricity, t.tle_line1, t.tle_line2
        FROM UNNEST(
            $1::timestamptz[], $2::int4[], $3::text[], $4::timestamptz[], $5::float8[],
            $6::float8[], $7::float8[], $8::text[], $9::text[]
        ) AS t(timestamp, norad_id, name, epoch, mean_motion,
               inclination, eccentricity, tle_line1, tle_line2)
        "#,
    )
    .bind(&timestamps)
    .bind(&norad_ids)
    .bind(&names)
    .bind(&epochs)
    .bind(&mean_motions)
    .bind(&inclinations)
    .bind(&eccentricities)
    .bind(&tle1s)
    .bind(&tle2s)
    .execute(pool)
    .await?;

    let count = result.rows_affected();
    debug!(
        table = "satellites",
        count,
        batch = batch_len,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "bulk insert"
    );
    Ok(count)
}

pub async fn insert_vehicle_positions(
    pool: &PgPool,
    records: &[VehiclePositionRecord],
) -> Result<u64> {
    if records.is_empty() {
        return Ok(0);
    }

    let start = Instant::now();
    let batch_len = records.len();

    let timestamps: Vec<DateTime<Utc>> = records.iter().map(|r| r.timestamp).collect();
    let feed_timestamps: Vec<i64> = records.iter().map(|r| r.feed_timestamp).collect();
    let vehicle_ids: Vec<Option<&str>> = records.iter().map(|r| r.vehicle_id.as_deref()).collect();
    let trip_ids: Vec<Option<&str>> = records.iter().map(|r| r.trip_id.as_deref()).collect();
    let route_ids: Vec<Option<&str>> = records.iter().map(|r| r.route_id.as_deref()).collect();
    let latitudes: Vec<f64> = records.iter().map(|r| r.latitude).collect();
    let longitudes: Vec<f64> = records.iter().map(|r| r.longitude).collect();
    let bearings: Vec<Option<f32>> = records.iter().map(|r| r.bearing).collect();
    let speeds: Vec<Option<f32>> = records.iter().map(|r| r.speed).collect();

    let result = sqlx::query(
        r#"
        INSERT INTO vehicle_positions (timestamp, feed_timestamp, vehicle_id, trip_id, route_id,
                                       latitude, longitude, bearing, speed, geom)
        SELECT
            t.timestamp, t.feed_timestamp, t.vehicle_id, t.trip_id, t.route_id,
            t.latitude, t.longitude, t.bearing, t.speed,
            ST_SetSRID(ST_MakePoint(t.longitude, t.latitude), 4326)
        FROM UNNEST(
            $1::timestamptz[], $2::int8[], $3::text[], $4::text[], $5::text[],
            $6::float8[], $7::float8[], $8::float4[], $9::float4[]
        ) AS t(timestamp, feed_timestamp, vehicle_id, trip_id, route_id,
               latitude, longitude, bearing, speed)
        "#,
    )
    .bind(&timestamps)
    .bind(&feed_timestamps)
    .bind(&vehicle_ids)
    .bind(&trip_ids)
    .bind(&route_ids)
    .bind(&latitudes)
    .bind(&longitudes)
    .bind(&bearings)
    .bind(&speeds)
    .execute(pool)
    .await?;

    let count = result.rows_affected();
    debug!(
        table = "vehicle_positions",
        count,
        batch = batch_len,
        elapsed_ms = start.elapsed().as_millis() as u64,
        "bulk insert"
    );
    Ok(count)
}
