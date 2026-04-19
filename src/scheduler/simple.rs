//! Interval-based scheduler built on `tokio::interval`.
//!
//! We deliberately avoid `tokio-cron-scheduler`: its job storage allocated
//! without bound in long-running processes in our deployments. With plain
//! intervals we get `MissedTickBehavior::Skip` semantics and a flat memory
//! profile.
//!
//! Sources inside a group run sequentially rather than concurrently to keep
//! peak heap small: at 117 rec/s steady state this is not the bottleneck.

use anyhow::Result;
use sqlx::PgPool;
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::ingestors;

#[derive(Clone)]
struct AdsbConfig {
    opensky: bool,
    lol: bool,
    fi: bool,
    one: bool,
    airplaneslive: bool,
    adsb_one_delay_ms: u64,
}

pub async fn run(pool: PgPool, config: Config) -> Result<()> {
    let flights_interval = Duration::from_secs(config.flights_interval_secs);
    let trains_interval = Duration::from_secs(config.trains_interval_secs);
    let gtfs_rt_interval = Duration::from_secs(config.gtfs_rt_interval_secs);
    let gtfs_static_interval = Duration::from_secs(config.gtfs_static_interval_secs);
    let satellites_interval = Duration::from_secs(config.satellites_interval_secs);
    let bratwurst_interval = Duration::from_secs(config.bratwurst_interval_secs);

    let adsb_config = AdsbConfig {
        opensky: config.adsb_opensky_enabled,
        lol: config.adsb_lol_enabled,
        fi: config.adsb_fi_enabled,
        one: config.adsb_one_enabled,
        airplaneslive: config.adsb_airplaneslive_enabled,
        adsb_one_delay_ms: config.adsb_one_delay_ms,
    };

    let mut handles = Vec::new();

    if config.gtfs_rt_enabled {
        handles.push(tokio::spawn(run_gtfs_rt_loop(
            pool.clone(),
            gtfs_rt_interval,
        )));
    }

    handles.push(tokio::spawn(run_flights_loop(
        pool.clone(),
        flights_interval,
        config.clone(),
        adsb_config.clone(),
    )));

    if config.trains_enabled {
        handles.push(tokio::spawn(run_trains_loop(pool.clone(), trains_interval)));
    }

    if config.satellites_enabled {
        handles.push(tokio::spawn(run_satellites_loop(
            pool.clone(),
            satellites_interval,
        )));
    }

    if config.adsb_bratwurst_enabled {
        match (
            config.bratwurst_login_url.clone(),
            config.bratwurst_api_url.clone(),
            config.bratwurst_username.clone(),
            config.bratwurst_password.clone(),
        ) {
            (Some(login_url), Some(api_url), Some(user), Some(pass)) => {
                handles.push(tokio::spawn(run_bratwurst_loop(
                    pool.clone(),
                    bratwurst_interval,
                    login_url,
                    api_url,
                    user,
                    pass,
                )));
            }
            _ => info!("bratwurst enabled but credentials or URLs missing; skipping"),
        }
    }

    if config.gtfs_static_enabled {
        handles.push(tokio::spawn(run_gtfs_static_loop(
            config.data_dir.clone(),
            gtfs_static_interval,
        )));
    }

    info!(tasks = handles.len(), "scheduler loops started");

    for handle in handles {
        if let Err(e) = handle.await {
            error!(error = %e, "scheduler task panicked");
        }
    }

    Ok(())
}

async fn run_gtfs_rt_loop(pool: PgPool, interval_duration: Duration) {
    tokio::time::sleep(Duration::from_secs(10)).await;

    let mut tick = interval(interval_duration);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;
        if let Err(e) = ingestors::gtfs_realtime::fetch(&pool).await {
            error!(error = %e, "gtfs-rt cycle failed");
        }
    }
}

async fn run_flights_loop(
    pool: PgPool,
    interval_duration: Duration,
    config: Config,
    adsb: AdsbConfig,
) {
    tokio::time::sleep(Duration::from_secs(60)).await;

    let mut tick = interval(interval_duration);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;

        if adsb.opensky {
            if let Err(e) = ingestors::opensky::fetch(&pool, &config).await {
                warn!(source = "opensky", error = %e, "flights fetch failed");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if adsb.lol {
            if let Err(e) = ingestors::adsblol::fetch(&pool).await {
                warn!(source = "adsblol", error = %e, "flights fetch failed");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if adsb.airplaneslive {
            if let Err(e) = ingestors::airplaneslive::fetch(&pool).await {
                warn!(source = "airplaneslive", error = %e, "flights fetch failed");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if adsb.one {
            if let Err(e) = ingestors::adsbone::fetch(&pool, adsb.adsb_one_delay_ms).await {
                warn!(source = "adsbone", error = %e, "flights fetch failed");
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if adsb.fi {
            if let Err(e) = ingestors::adsbfi::fetch(&pool).await {
                warn!(source = "adsbfi", error = %e, "flights fetch failed");
            }
        }
    }
}

async fn run_trains_loop(pool: PgPool, interval_duration: Duration) {
    tokio::time::sleep(Duration::from_secs(60)).await;

    let mut tick = interval(interval_duration);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;

        if let Err(e) = ingestors::sbb::fetch(&pool).await {
            warn!(source = "sbb", error = %e, "trains fetch failed");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        if let Err(e) = ingestors::irail::fetch(&pool).await {
            warn!(source = "irail", error = %e, "trains fetch failed");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;

        if let Err(e) = ingestors::oebb::fetch(&pool).await {
            warn!(source = "oebb", error = %e, "trains fetch failed");
        }
    }
}

async fn run_satellites_loop(pool: PgPool, interval_duration: Duration) {
    tokio::time::sleep(Duration::from_secs(120)).await;

    let mut tick = interval(interval_duration);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        if let Err(e) = ingestors::satellites::fetch(&pool).await {
            warn!(error = %e, "satellites fetch failed");
        }
        tick.tick().await;
    }
}

async fn run_bratwurst_loop(
    pool: PgPool,
    interval_duration: Duration,
    login_url: String,
    api_url: String,
    username: String,
    password: String,
) {
    tokio::time::sleep(Duration::from_secs(15)).await;

    let mut tick = interval(interval_duration);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        tick.tick().await;

        let result =
            ingestors::bratwurst::fetch(&pool, &login_url, &api_url, &username, &password).await;
        if let Err(e) = result {
            if e.to_string().contains("Login failed") {
                warn!(error = %e, "bratwurst login failed, backing off 5m");
                tokio::time::sleep(Duration::from_secs(300)).await;
            } else {
                warn!(error = %e, "bratwurst fetch failed");
            }
        }
    }
}

async fn run_gtfs_static_loop(data_dir: String, interval_duration: Duration) {
    tokio::time::sleep(Duration::from_secs(300)).await;

    let mut tick = interval(interval_duration);
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        if let Err(e) = ingestors::gtfs_static::fetch_germany(&data_dir).await {
            warn!(error = %e, "gtfs-static germany failed");
        }
        tokio::time::sleep(Duration::from_secs(60)).await;
        if let Err(e) = ingestors::gtfs_static::fetch_europe(&data_dir).await {
            warn!(error = %e, "gtfs-static europe failed");
        }
        tick.tick().await;
    }
}
