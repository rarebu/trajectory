//! Integration tests against a real PostgreSQL+PostGIS instance spun up via
//! testcontainers. Docker daemon must be reachable; in CI we rely on the
//! standard `ubuntu-latest` runner.

use std::time::Duration;

use chrono::{TimeZone, Utc};
use sqlx::PgPool;
use testcontainers::{
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    ContainerAsync, GenericImage, ImageExt,
};
use trajectory::storage::{
    create_pool,
    models::{Flight, Ship, VehiclePositionRecord},
    queries::{insert_flights, insert_ships, insert_vehicle_positions},
};

struct TestDb {
    pool: PgPool,
    _container: ContainerAsync<GenericImage>,
}

async fn start_test_db() -> TestDb {
    let container = GenericImage::new("postgis/postgis", "15-3.4")
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_exposed_port(5432.tcp())
        .with_env_var("POSTGRES_DB", "test")
        .with_env_var("POSTGRES_USER", "test")
        .with_env_var("POSTGRES_PASSWORD", "test")
        .start()
        .await
        .expect("start postgis container");

    let host_port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("get host port");
    let dsn = format!("postgres://test:test@127.0.0.1:{host_port}/test");

    // Retry briefly: the "ready" message fires before the first-boot init
    // script sequence finishes configuring the cluster.
    let pool = connect_with_retry(&dsn, Duration::from_secs(30)).await;

    apply_schema(&pool).await;

    TestDb {
        pool,
        _container: container,
    }
}

async fn connect_with_retry(dsn: &str, budget: Duration) -> PgPool {
    let deadline = std::time::Instant::now() + budget;
    loop {
        match create_pool(dsn).await {
            Ok(pool) => return pool,
            Err(e) if std::time::Instant::now() < deadline => {
                eprintln!("pool not ready yet ({e}), retrying");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("pool never came up: {e}"),
        }
    }
}

async fn apply_schema(pool: &PgPool) {
    let schema = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/sql/schema.sql"))
        .expect("read schema.sql");

    for statement in split_sql(&schema) {
        if statement.trim().is_empty() {
            continue;
        }
        sqlx::query(statement)
            .execute(pool)
            .await
            .unwrap_or_else(|e| panic!("apply schema stmt failed ({e}): {statement}"));
    }
}

/// Trivial SQL splitter: we author schema.sql without PL/pgSQL blocks, so
/// semicolons on statement boundaries are safe to split on.
fn split_sql(source: &str) -> Vec<&str> {
    source
        .split(';')
        .map(str::trim)
        .filter(|s| {
            !s.is_empty()
                && !s
                    .lines()
                    .all(|l| l.trim().is_empty() || l.trim_start().starts_with("--"))
        })
        .collect()
}

fn sample_flight(icao: &str, source: &str) -> Flight {
    Flight {
        timestamp: Utc.with_ymd_and_hms(2026, 4, 19, 12, 0, 0).unwrap(),
        icao24: Some(icao.to_string()),
        callsign: Some("TEST01".to_string()),
        origin_country: Some("Switzerland".to_string()),
        longitude: Some(8.55),
        latitude: Some(47.37),
        baro_altitude: Some(10_000.0),
        on_ground: Some(false),
        velocity: Some(230.0),
        true_track: Some(95.0),
        vertical_rate: Some(0.0),
        source: source.to_string(),
    }
}

fn sample_ship(mmsi: &str) -> Ship {
    Ship {
        timestamp: Utc.with_ymd_and_hms(2026, 4, 19, 12, 0, 0).unwrap(),
        mmsi: Some(mmsi.to_string()),
        ship_name: Some("MV TEST".to_string()),
        latitude: Some(51.5),
        longitude: Some(0.1),
        message_type: Some("PositionReport".to_string()),
    }
}

#[tokio::test]
async fn insert_flights_round_trip() {
    let db = start_test_db().await;

    let batch = vec![
        sample_flight("abc123", "opensky"),
        sample_flight("def456", "adsblol"),
        sample_flight("ghi789", "opensky"),
    ];
    let inserted = insert_flights(&db.pool, &batch).await.expect("insert");
    assert_eq!(inserted, 3);

    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM flights")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(row.0, 3);

    // PostGIS geometry should be populated from longitude/latitude.
    let geom_nonnull: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM flights WHERE geom IS NOT NULL")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(geom_nonnull.0, 3);

    // ingested_at default should fire for every row.
    let all_ingested: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM flights WHERE ingested_at IS NOT NULL")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(all_ingested.0, 3);
}

#[tokio::test]
async fn insert_flights_empty_short_circuits() {
    let db = start_test_db().await;
    let inserted = insert_flights(&db.pool, &[]).await.expect("insert");
    assert_eq!(inserted, 0);

    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM flights")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(row.0, 0);
}

#[tokio::test]
async fn insert_flights_preserves_null_coords() {
    let db = start_test_db().await;

    let mut flight = sample_flight("nolatlon", "opensky");
    flight.latitude = None;
    flight.longitude = None;
    insert_flights(&db.pool, &[flight]).await.expect("insert");

    let geom_null: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM flights WHERE geom IS NULL")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(geom_null.0, 1);
}

fn sample_vehicle(vehicle_id: &str, trip_id: &str, lat: f64, lon: f64) -> VehiclePositionRecord {
    VehiclePositionRecord {
        timestamp: Utc.with_ymd_and_hms(2026, 4, 19, 12, 0, 0).unwrap(),
        feed_timestamp: 1_745_064_000,
        vehicle_id: Some(vehicle_id.to_string()),
        trip_id: Some(trip_id.to_string()),
        route_id: Some("ICE-100".to_string()),
        latitude: lat,
        longitude: lon,
        bearing: Some(42.0),
        speed: Some(55.5),
    }
}

#[tokio::test]
async fn insert_vehicle_positions_round_trip() {
    let db = start_test_db().await;

    let batch = vec![
        sample_vehicle("v1", "t-1001", 52.52, 13.4),
        sample_vehicle("v2", "t-1002", 48.14, 11.58),
    ];
    let inserted = insert_vehicle_positions(&db.pool, &batch)
        .await
        .expect("insert");
    assert_eq!(inserted, 2);

    let (count, geom_nonnull): (i64, i64) = sqlx::query_as(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE geom IS NOT NULL) FROM vehicle_positions",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(count, 2);
    assert_eq!(geom_nonnull, 2);

    // Spot-check the geometry round-trips.
    let (lat, lon): (f64, f64) = sqlx::query_as(
        r#"SELECT ST_Y(geom), ST_X(geom) FROM vehicle_positions WHERE vehicle_id = 'v1'"#,
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert!((lat - 52.52).abs() < 1e-6);
    assert!((lon - 13.4).abs() < 1e-6);
}

#[tokio::test]
async fn insert_ships_round_trip() {
    let db = start_test_db().await;

    let batch = vec![sample_ship("111222333"), sample_ship("444555666")];
    let inserted = insert_ships(&db.pool, &batch).await.expect("insert");
    assert_eq!(inserted, 2);

    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ships")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(row.0, 2);
}

#[tokio::test]
async fn ingest_latency_is_queryable() {
    let db = start_test_db().await;

    // Insert a record with a backdated timestamp so (ingested_at - timestamp)
    // is reliably non-zero.
    let mut flight = sample_flight("latcheck", "opensky");
    flight.timestamp = Utc::now() - chrono::Duration::seconds(2);
    insert_flights(&db.pool, &[flight]).await.expect("insert");

    let (p99_ms,): (f64,) = sqlx::query_as(
        r#"SELECT COALESCE(percentile_cont(0.99) WITHIN GROUP (ORDER BY
               EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000), 0.0)
           FROM flights"#,
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();

    // The delta should be between 2 s (our offset) and 30 s (CI slack).
    assert!(p99_ms >= 2_000.0, "expected >= 2000 ms, got {p99_ms}");
    assert!(p99_ms <= 30_000.0, "expected <= 30000 ms, got {p99_ms}");
}
