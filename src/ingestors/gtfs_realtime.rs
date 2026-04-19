//! GTFS-Realtime feed for German rail, via gtfs.de.
//!
//! The feed is a single 28-34 MB protobuf payload carrying three entity
//! types; we consume `vehicle_position` only (per-vehicle live location).
//! `trip_update` (schedule deltas) and `alert` are ignored for now.
//! We stream through the payload in fixed-size batches so peak heap stays
//! bounded across the decode + insert path.

use anyhow::Result;
use chrono::{DateTime, Utc};
use prost::Message;
use sqlx::PgPool;
use tracing::info;

use crate::storage::models::VehiclePositionRecord;
use crate::storage::queries::insert_vehicle_positions;
use crate::utils::http::create_client;

pub mod transit_realtime {
    include!(concat!(env!("OUT_DIR"), "/transit_realtime.rs"));
}

use transit_realtime::FeedMessage;

const GTFS_RT_URL: &str = "https://realtime.gtfs.de/realtime-free.pb";
const BATCH_SIZE: usize = 2000;

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let response = client.get(GTFS_RT_URL).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("GTFS-RT returned status {}", response.status());
    }

    let bytes = response.bytes().await?;
    let bytes_len = bytes.len();

    let feed = FeedMessage::decode(&*bytes)?;
    drop(bytes);

    let feed_timestamp = feed.header.timestamp.unwrap_or(0) as i64;
    let total_entities = feed.entity.len();
    let mut vp_count: u64 = 0;
    let mut inserted_count: u64 = 0;

    let mut batch: Vec<VehiclePositionRecord> = Vec::with_capacity(BATCH_SIZE);

    for entity in &feed.entity {
        let Some(vp) = &entity.vehicle else { continue };
        let Some(pos) = &vp.position else { continue };

        vp_count += 1;

        let timestamp = resolve_timestamp(vp.timestamp, feed_timestamp);

        batch.push(VehiclePositionRecord {
            timestamp,
            feed_timestamp,
            vehicle_id: vp.vehicle.as_ref().and_then(|v| v.id.clone()),
            trip_id: vp.trip.as_ref().and_then(|t| t.trip_id.clone()),
            route_id: vp.trip.as_ref().and_then(|t| t.route_id.clone()),
            latitude: f64::from(pos.latitude),
            longitude: f64::from(pos.longitude),
            bearing: pos.bearing,
            speed: pos.speed,
        });

        if batch.len() >= BATCH_SIZE {
            inserted_count += insert_vehicle_positions(pool, &batch).await?;
            batch.clear();
        }
    }

    if !batch.is_empty() {
        inserted_count += insert_vehicle_positions(pool, &batch).await?;
    }

    drop(feed);

    info!(
        bytes = bytes_len,
        vehicle_positions = vp_count,
        total_entities,
        inserted = inserted_count,
        "gtfs-rt ingested",
    );

    Ok(())
}

/// Prefer the per-entity timestamp, fall back to the feed header, then to
/// wallclock. All three are UNIX seconds.
fn resolve_timestamp(entity_ts: Option<u64>, feed_ts: i64) -> DateTime<Utc> {
    if let Some(t) = entity_ts {
        if let Some(dt) = DateTime::from_timestamp(t as i64, 0) {
            return dt;
        }
    }
    if feed_ts > 0 {
        if let Some(dt) = DateTime::from_timestamp(feed_ts, 0) {
            return dt;
        }
    }
    Utc::now()
}
