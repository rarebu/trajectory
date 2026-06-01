use anyhow::Result;
use chrono::Utc;
use sqlx::PgPool;
use std::collections::HashSet;
use tracing::{info, warn};

use crate::http::create_client;
use crate::ingestors::adsb::AdsbResponse;
use crate::storage::queries::insert_flights;

const ENDPOINTS: &[(&str, &str)] = &[
    ("ladd", "https://api.adsb.lol/v2/ladd"),
    ("mil", "https://api.adsb.lol/v2/mil"),
    ("pia", "https://api.adsb.lol/v2/pia"),
];

pub async fn fetch(pool: &PgPool) -> Result<()> {
    let client = create_client()?;
    let timestamp = Utc::now();
    let mut all_flights = Vec::new();
    let mut seen_icaos: HashSet<String> = HashSet::new();

    for (name, url) in ENDPOINTS {
        match client.get(*url).send().await {
            Ok(response) if response.status().is_success() => {
                if let Ok(data) = response.json::<AdsbResponse>().await {
                    if let Some(aircraft) = data.aircraft {
                        for ac in &aircraft {
                            if let Some(hex) = &ac.hex {
                                if seen_icaos.insert(hex.clone()) {
                                    all_flights.push(ac.to_flight("adsblol", timestamp));
                                }
                            }
                        }
                    }
                }
            }
            Ok(response) => warn!(endpoint = name, status = %response.status(), "adsb.lol non-ok"),
            Err(e) => warn!(endpoint = name, error = %e, "adsb.lol request failed"),
        }
    }

    if all_flights.is_empty() {
        warn!("adsb.lol returned no aircraft");
        return Ok(());
    }

    insert_flights(pool, &all_flights).await?;
    info!(count = all_flights.len(), "adsb.lol ingested");
    Ok(())
}
