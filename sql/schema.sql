-- trajectory baseline schema: plain PostgreSQL 15+ with PostGIS.
--
-- All statements are idempotent so the file can be re-run by hand.
-- For the optional TimescaleDB upgrade path (hypertables, compression,
-- retention), see sql/schema_timescaledb.sql.
--
-- Every table carries:
--   * timestamp    TIMESTAMPTZ  -- upstream-observed or client-decode time
--   * ingested_at  TIMESTAMPTZ DEFAULT NOW()  -- server-assigned write time
-- Their difference is the per-row ingest latency, queryable in SQL.

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS flights (
    id             BIGSERIAL,
    timestamp      TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    icao24         TEXT,
    callsign       TEXT,
    origin_country TEXT,
    longitude      DOUBLE PRECISION,
    latitude       DOUBLE PRECISION,
    baro_altitude  DOUBLE PRECISION,
    on_ground      BOOLEAN,
    velocity       DOUBLE PRECISION,
    true_track     DOUBLE PRECISION,
    vertical_rate  DOUBLE PRECISION,
    geom           GEOMETRY(Point, 4326),
    source         TEXT
);
CREATE INDEX IF NOT EXISTS idx_flights_time   ON flights (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_flights_icao   ON flights (icao24, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_flights_source ON flights (source, timestamp DESC);

CREATE TABLE IF NOT EXISTS ships (
    id           BIGSERIAL,
    timestamp    TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    mmsi         TEXT,
    ship_name    TEXT,
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    message_type TEXT,
    geom         GEOMETRY(Point, 4326)
);
CREATE INDEX IF NOT EXISTS idx_ships_time ON ships (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_ships_mmsi ON ships (mmsi, timestamp DESC);

CREATE TABLE IF NOT EXISTS satellites (
    id           BIGSERIAL,
    timestamp    TIMESTAMPTZ NOT NULL,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    norad_id     INTEGER,
    name         TEXT,
    epoch        TIMESTAMPTZ,
    mean_motion  DOUBLE PRECISION,
    inclination  DOUBLE PRECISION,
    eccentricity DOUBLE PRECISION,
    tle_line1    TEXT,
    tle_line2    TEXT
);
CREATE INDEX IF NOT EXISTS idx_satellites_time  ON satellites (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_satellites_norad ON satellites (norad_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS db_departures (
    id               BIGSERIAL,
    timestamp        TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    station_id       TEXT,
    station_name     TEXT,
    trip_id          TEXT,
    line_name        TEXT,
    direction        TEXT,
    planned_time     TIMESTAMPTZ,
    actual_time      TIMESTAMPTZ,
    delay_seconds    INTEGER,
    cancelled        BOOLEAN DEFAULT FALSE,
    platform_planned TEXT,
    platform_actual  TEXT,
    platform_changed BOOLEAN DEFAULT FALSE
);
CREATE INDEX IF NOT EXISTS idx_departures_time    ON db_departures (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_departures_station ON db_departures (station_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS vehicle_positions (
    id             BIGSERIAL,
    timestamp      TIMESTAMPTZ NOT NULL,
    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    feed_timestamp BIGINT,
    vehicle_id     TEXT,
    trip_id        TEXT,
    route_id       TEXT,
    latitude       DOUBLE PRECISION,
    longitude      DOUBLE PRECISION,
    bearing        REAL,
    speed          REAL,
    geom           GEOMETRY(Point, 4326)
);
CREATE INDEX IF NOT EXISTS idx_vp_time  ON vehicle_positions (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_vp_geom  ON vehicle_positions USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_vp_route ON vehicle_positions (route_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_vp_trip  ON vehicle_positions (trip_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS service_alerts (
    id               BIGSERIAL,
    timestamp        TIMESTAMPTZ NOT NULL,
    ingested_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    alert_id         TEXT,
    header_text      TEXT,
    description_text TEXT,
    cause            TEXT,
    effect           TEXT
);
CREATE INDEX IF NOT EXISTS idx_service_alerts_time ON service_alerts (timestamp DESC);
