//! GTFS-Static feeds. We archive the raw ZIPs to disk; parsing is deferred
//! to offline tools because the 0.1 ingest path does not surface stops/routes
//! in the query layer.

use anyhow::Result;
use chrono::Utc;
use tracing::{error, info, warn};

use crate::utils::http::create_client_with_timeout;

const GTFS_DOWNLOAD_TIMEOUT_SECS: u64 = 600;

const GTFS_SOURCES: &[(&str, &str, &str)] = &[
    ("germany", "https://download.gtfs.de/germany/free/latest.zip", "German public transport"),
    ("switzerland", "https://gtfs.geops.ch/dl/gtfs_complete.zip", "Swiss public transport"),
    ("austria", "https://data.oebb.at/oebb?dataset=uddi:cd90d039-7d3f-11e5-b2b6-0050568a0029", "Austrian railways"),
    ("netherlands", "http://gtfs.ovapi.nl/nl/gtfs-nl.zip", "Dutch public transport"),
    ("belgium", "https://gtfs.irail.be/nmbs/gtfs/latest.zip", "Belgian railways"),
    ("sweden", "https://opendata.samtrafiken.se/gtfs/sweden.zip", "Swedish public transport"),
    ("norway", "https://storage.googleapis.com/marduk-production/outbound/gtfs/rb_norway-aggregated-gtfs.zip", "Norwegian public transport"),
    ("finland", "https://api.digitransit.fi/routing/v1/routers/finland/gtfs", "Finnish public transport"),
    ("france-tgv", "https://eu.ftp.opendatasoft.com/sncf/gtfs/export-ter-gtfs-last.zip", "French TER"),
    ("spain-renfe", "https://data.renfe.com/dataset/horarios-de-alta-velocidad-702d4d59-cab2-4eb4-8e5b-de987e5a5f85/resource/ae527a26-df01-4ef0-b48e-ae04b302c84d", "Spanish railways"),
    ("poland-pkp", "https://mkuran.pl/gtfs/warsaw.zip", "Warsaw public transport"),
    ("czechia-prague", "https://data.pid.cz/PID_GTFS.zip", "Prague public transport"),
];

pub async fn fetch_one(feed_id: &str, url: &str, data_dir: &str) -> Result<usize> {
    let client = create_client_with_timeout(GTFS_DOWNLOAD_TIMEOUT_SECS)?;
    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("GTFS download failed with status {}", response.status());
    }

    let bytes = response.bytes().await?;
    let len = bytes.len();

    let timestamp = Utc::now().format("%Y%m%d");
    let dir = format!("{data_dir}/gtfs");
    tokio::fs::create_dir_all(&dir).await?;
    let filename = format!("{dir}/{feed_id}_{timestamp}.zip");
    tokio::fs::write(&filename, &bytes).await?;

    info!(feed = feed_id, bytes = len, path = %filename, "gtfs archive saved");
    Ok(len)
}

pub async fn fetch_germany(data_dir: &str) -> Result<()> {
    let (_, url, _) = GTFS_SOURCES[0];
    if let Err(e) = fetch_one("germany", url, data_dir).await {
        warn!(error = %e, "germany gtfs failed");
    }
    Ok(())
}

pub async fn fetch_europe(data_dir: &str) -> Result<()> {
    for (feed_id, url, description) in GTFS_SOURCES.iter().skip(1) {
        info!(feed = feed_id, description, "gtfs downloading");
        if let Err(e) = fetch_one(feed_id, url, data_dir).await {
            error!(feed = feed_id, error = %e, "gtfs failed");
        }
    }
    Ok(())
}
