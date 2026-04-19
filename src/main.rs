use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use sqlx::PgPool;
use tokio::signal;
use tracing::{error, info, warn};

use trajectory::{config::Config, ingestors, scheduler, storage, telemetry};

#[derive(Parser)]
#[command(name = "trajectory", about = "Real-time trajectory ingestion daemon")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Run the long-lived daemon with all configured ingestors.
    Daemon,
    /// Fetch a single source once and exit (useful for ad-hoc backfills).
    Fetch {
        #[command(subcommand)]
        source: FetchSource,
    },
    /// Verify the database connection and report row counts.
    DbTest,
}

#[derive(Subcommand)]
enum FetchSource {
    Opensky,
    Adsblol,
    Adsbfi,
    Adsbone,
    Airplaneslive,
    Sbb,
    Irail,
    Oebb,
    GtfsRt,
    GtfsStaticDe,
    GtfsStaticEu,
    Satellites,
    Ais,
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    telemetry::init();

    let cli = Cli::parse();
    let config = Config::from_env().context("loading configuration")?;
    info!(
        ais = config.ais_enabled,
        satellites = config.satellites_enabled,
        trains = config.trains_enabled,
        gtfs_rt = config.gtfs_rt_enabled,
        gtfs_static = config.gtfs_static_enabled,
        "configuration loaded",
    );

    let pool = storage::create_pool(&config.database_url)
        .await
        .context("creating database pool")?;
    info!("database connected");

    if let Err(e) = storage::apply_retention_policies(&pool, &config).await {
        warn!(error = %e, "retention policy setup skipped");
    }

    match cli.command {
        Some(Command::Daemon) | None => run_daemon(pool, config).await,
        Some(Command::Fetch { source }) => run_fetch(source, &pool, &config).await,
        Some(Command::DbTest) => run_db_test(&pool).await,
    }
}

async fn run_daemon(pool: PgPool, config: Config) -> Result<()> {
    let ais_handle = if config.ais_enabled {
        match &config.ais_api_key {
            Some(key) => {
                let pool = pool.clone();
                let key = key.clone();
                let dedup = config.ais_dedup_enabled;
                let handle = tokio::spawn(async move {
                    loop {
                        if let Err(e) = ingestors::ais::run_continuous(&pool, &key, dedup).await {
                            error!(error = %e, "ais collector exited, restarting in 10s");
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                    }
                });
                Some(handle)
            }
            None => {
                warn!("ais enabled but api key missing; skipping collector");
                None
            }
        }
    } else {
        None
    };

    let sched_handle = tokio::spawn(async move {
        if let Err(e) = scheduler::run(pool, config).await {
            error!(error = %e, "scheduler exited");
        }
    });

    info!("daemon running; Ctrl+C to stop");

    match ais_handle {
        Some(ais) => tokio::select! {
            _ = signal::ctrl_c() => info!("shutdown requested"),
            _ = sched_handle => error!("scheduler exited unexpectedly"),
            _ = ais => error!("ais collector exited unexpectedly"),
        },
        None => tokio::select! {
            _ = signal::ctrl_c() => info!("shutdown requested"),
            _ = sched_handle => error!("scheduler exited unexpectedly"),
        },
    }

    info!("shutdown complete");
    Ok(())
}

async fn run_fetch(source: FetchSource, pool: &PgPool, config: &Config) -> Result<()> {
    match source {
        FetchSource::Opensky => ingestors::opensky::fetch(pool, config).await,
        FetchSource::Adsblol => ingestors::adsblol::fetch(pool).await,
        FetchSource::Adsbfi => ingestors::adsbfi::fetch(pool).await,
        FetchSource::Adsbone => ingestors::adsbone::fetch(pool, config.adsb_one_delay_ms).await,
        FetchSource::Airplaneslive => ingestors::airplaneslive::fetch(pool).await,
        FetchSource::Sbb => ingestors::sbb::fetch(pool).await,
        FetchSource::Irail => ingestors::irail::fetch(pool).await,
        FetchSource::Oebb => ingestors::oebb::fetch(pool).await,
        FetchSource::GtfsRt => ingestors::gtfs_realtime::fetch(pool).await,
        FetchSource::GtfsStaticDe => ingestors::gtfs_static::fetch_germany(&config.data_dir).await,
        FetchSource::GtfsStaticEu => ingestors::gtfs_static::fetch_europe(&config.data_dir).await,
        FetchSource::Satellites => ingestors::satellites::fetch(pool).await,
        FetchSource::Ais => match &config.ais_api_key {
            Some(key) => ingestors::ais::fetch_once(pool, key, 30).await,
            None => {
                anyhow::bail!("ais api key not configured");
            }
        },
    }
}

async fn run_db_test(pool: &PgPool) -> Result<()> {
    let (flights,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM flights")
        .fetch_one(pool)
        .await?;
    info!(flights, "database reachable");
    Ok(())
}
