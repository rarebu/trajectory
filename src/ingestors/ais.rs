//! AIS Stream consumer.
//!
//! aisstream.io requires WebSocket permessage-deflate (RFC 7692), which
//! tokio-tungstenite does not implement; we use yawc. Sessions are recycled
//! every 10 minutes to cap heap growth from internal buffers.

use anyhow::Result;
use bytes::Bytes;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;
use tracing::{error, info, warn};
use yawc::frame::OpCode;
use yawc::WebSocket;

use crate::storage::{models::Ship, queries::insert_ships};

const AIS_STREAM_URL: &str = "wss://stream.aisstream.io/v0/stream";
const BATCH_SIZE: usize = 500;
const FLUSH_INTERVAL: Duration = Duration::from_secs(3);
const SESSION_MAX: Duration = Duration::from_secs(600);
const RECONNECT_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Serialize)]
struct AisSubscribe {
    #[serde(rename = "APIKey")]
    api_key: String,
    #[serde(rename = "BoundingBoxes")]
    bounding_boxes: Vec<[[f64; 2]; 2]>,
}

#[derive(Debug, Deserialize)]
struct AisMessageWrapper {
    #[serde(rename = "MessageType")]
    message_type: Option<String>,
    #[serde(rename = "MetaData")]
    meta_data: Option<AisMetaData>,
}

#[derive(Debug, Deserialize)]
struct AisMetaData {
    #[serde(rename = "MMSI")]
    mmsi: Option<i64>,
    #[serde(rename = "ShipName")]
    ship_name: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

fn tls_connector() -> TlsConnector {
    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    TlsConnector::from(Arc::new(tls_config))
}

pub async fn run_continuous(pool: &PgPool, api_key: &str, dedup_enabled: bool) -> Result<()> {
    info!("ais continuous collector starting");
    loop {
        if let Err(e) = run_session(pool, api_key, dedup_enabled).await {
            error!(error = %e, "ais session failed, reconnecting");
        }
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}

async fn run_session(pool: &PgPool, api_key: &str, dedup_enabled: bool) -> Result<()> {
    let connector = tls_connector();
    let mut ws = WebSocket::connect(AIS_STREAM_URL.parse()?, Some(connector)).await?;

    let subscribe = AisSubscribe {
        api_key: api_key.to_string(),
        bounding_boxes: vec![[[-90.0, -180.0], [90.0, 180.0]]],
    };
    let subscribe_json = serde_json::to_string(&subscribe)?;
    ws.send(yawc::frame::FrameView::text(Bytes::from(subscribe_json)))
        .await?;
    info!("ais stream subscribed");

    let mut batch: Vec<Ship> = Vec::with_capacity(BATCH_SIZE);
    let mut last_flush = Instant::now();
    let session_start = Instant::now();
    let mut msg_count: u64 = 0;
    let mut inserted_total: u64 = 0;

    while let Some(frame) = ws.next().await {
        if session_start.elapsed() > SESSION_MAX {
            info!("ais session limit reached, recycling");
            break;
        }

        match frame.opcode {
            OpCode::Text | OpCode::Binary => {
                msg_count += 1;

                if let Ok(w) = serde_json::from_slice::<AisMessageWrapper>(&frame.payload) {
                    if let Some(m) = w.meta_data {
                        batch.push(Ship {
                            timestamp: Utc::now(),
                            mmsi: m.mmsi.map(|x| x.to_string()),
                            ship_name: m.ship_name,
                            latitude: m.latitude,
                            longitude: m.longitude,
                            message_type: w.message_type,
                        });
                    }
                }

                if batch.len() >= BATCH_SIZE || last_flush.elapsed() > FLUSH_INTERVAL {
                    if !batch.is_empty() {
                        if dedup_enabled {
                            dedup_ships(&mut batch);
                        }
                        match insert_ships(pool, &batch).await {
                            Ok(n) => inserted_total += n,
                            Err(e) => warn!(error = %e, "ais insert failed"),
                        }
                        batch.clear();
                    }
                    last_flush = Instant::now();
                }
            }
            OpCode::Close => {
                info!("ais server closed connection");
                break;
            }
            _ => {}
        }
    }

    if !batch.is_empty() {
        if dedup_enabled {
            dedup_ships(&mut batch);
        }
        if let Ok(n) = insert_ships(pool, &batch).await {
            inserted_total += n;
        }
    }

    info!(
        messages = msg_count,
        inserted = inserted_total,
        "ais session ended"
    );
    Ok(())
}

pub async fn fetch_once(pool: &PgPool, api_key: &str, duration_secs: u64) -> Result<()> {
    let connector = tls_connector();
    let mut ws = WebSocket::connect(AIS_STREAM_URL.parse()?, Some(connector)).await?;

    let sub = AisSubscribe {
        api_key: api_key.to_string(),
        bounding_boxes: vec![[[-90.0, -180.0], [90.0, 180.0]]],
    };
    let subscribe_json = serde_json::to_string(&sub)?;
    ws.send(yawc::frame::FrameView::text(Bytes::from(subscribe_json)))
        .await?;

    let mut ships = Vec::new();
    let start = Instant::now();
    let budget = Duration::from_secs(duration_secs);

    while start.elapsed() < budget {
        match tokio::time::timeout(Duration::from_secs(10), ws.next()).await {
            Ok(Some(frame)) => match frame.opcode {
                OpCode::Text | OpCode::Binary => {
                    if let Ok(w) = serde_json::from_slice::<AisMessageWrapper>(&frame.payload) {
                        if let Some(m) = w.meta_data {
                            ships.push(Ship {
                                timestamp: Utc::now(),
                                mmsi: m.mmsi.map(|x| x.to_string()),
                                ship_name: m.ship_name,
                                latitude: m.latitude,
                                longitude: m.longitude,
                                message_type: w.message_type,
                            });
                        }
                    }
                }
                OpCode::Close => break,
                _ => {}
            },
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    if !ships.is_empty() {
        dedup_ships(&mut ships);
        insert_ships(pool, &ships).await?;
    }
    info!(count = ships.len(), duration_secs, "ais one-shot complete");
    Ok(())
}

/// Coordinates are rounded to 1e-5 degrees (~1 m) and timestamps to the second
/// before keying; this collapses near-duplicate pings without rejecting real
/// position updates.
fn dedup_ships(batch: &mut Vec<Ship>) {
    let mut seen = HashSet::with_capacity(batch.len());
    batch.retain(|s| {
        let key = (
            s.mmsi.clone(),
            s.latitude.map(|v| (v * 1e5).round() as i64),
            s.longitude.map(|v| (v * 1e5).round() as i64),
            s.timestamp.timestamp(),
        );
        seen.insert(key)
    });
}

#[cfg(test)]
mod tests {
    use super::dedup_ships;
    use crate::storage::models::Ship;
    use chrono::DateTime;

    fn ship(mmsi: &str, lat: f64, lon: f64, ts_sec: i64) -> Ship {
        Ship {
            timestamp: DateTime::from_timestamp(ts_sec, 0).unwrap(),
            mmsi: Some(mmsi.to_string()),
            ship_name: None,
            latitude: Some(lat),
            longitude: Some(lon),
            message_type: None,
        }
    }

    #[test]
    fn collapses_identical_pings() {
        let mut batch = vec![
            ship("111", 50.0, 10.0, 1000),
            ship("111", 50.0, 10.0, 1000),
            ship("111", 50.0, 10.0, 1000),
        ];
        dedup_ships(&mut batch);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn keeps_motion_beyond_rounding_radius() {
        // 1e-5 deg ~= 1.1 m at equator; 2e-5 deg ~= 2.2 m -> distinct buckets.
        let mut batch = vec![
            ship("111", 50.0, 10.0, 1000),
            ship("111", 50.0 + 2e-5, 10.0, 1000),
        ];
        dedup_ships(&mut batch);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn collapses_within_rounding_radius() {
        // 4e-7 deg is well under 1e-5 deg -> rounds to the same bucket.
        let mut batch = vec![
            ship("111", 50.0, 10.0, 1000),
            ship("111", 50.000_000_4, 10.0, 1000),
        ];
        dedup_ships(&mut batch);
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn keeps_distinct_mmsi() {
        let mut batch = vec![ship("111", 50.0, 10.0, 1000), ship("222", 50.0, 10.0, 1000)];
        dedup_ships(&mut batch);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn keeps_distinct_second() {
        let mut batch = vec![ship("111", 50.0, 10.0, 1000), ship("111", 50.0, 10.0, 1001)];
        dedup_ships(&mut batch);
        assert_eq!(batch.len(), 2);
    }

    #[test]
    fn preserves_insertion_order_of_survivors() {
        let mut batch = vec![
            ship("111", 50.0, 10.0, 1000),
            ship("222", 51.0, 11.0, 1000),
            ship("111", 50.0, 10.0, 1000),
            ship("333", 52.0, 12.0, 1000),
        ];
        dedup_ships(&mut batch);
        let mmsis: Vec<&str> = batch.iter().filter_map(|s| s.mmsi.as_deref()).collect();
        assert_eq!(mmsis, vec!["111", "222", "333"]);
    }
}
