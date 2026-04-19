-- Optional TimescaleDB upgrade on top of the baseline schema.
--
-- Prerequisite: run sql/schema.sql first, then install the timescaledb
-- extension on the target cluster. This file converts the existing tables
-- into hypertables and registers compression + retention policies.
--
-- `migrate_data => TRUE` on create_hypertable() lets the conversion run
-- against tables that already hold data. Dropped: the data isn't moved,
-- it's reorganized into chunks in place.

CREATE EXTENSION IF NOT EXISTS timescaledb;

SELECT create_hypertable('flights',        'timestamp', chunk_time_interval => INTERVAL '1 day',  if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('ships',          'timestamp', chunk_time_interval => INTERVAL '1 day',  if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('satellites',     'timestamp', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('db_departures',  'timestamp', chunk_time_interval => INTERVAL '1 day',  if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('vehicle_positions',   'timestamp', chunk_time_interval => INTERVAL '1 day',  if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('service_alerts', 'timestamp', chunk_time_interval => INTERVAL '1 day',  if_not_exists => TRUE, migrate_data => TRUE);

-- Compression (segmentby chosen per table based on query pattern).
ALTER TABLE flights        SET (timescaledb.compress, timescaledb.compress_segmentby = 'source');
ALTER TABLE ships          SET (timescaledb.compress, timescaledb.compress_segmentby = 'mmsi');
ALTER TABLE satellites     SET (timescaledb.compress, timescaledb.compress_segmentby = 'norad_id');
ALTER TABLE db_departures  SET (timescaledb.compress, timescaledb.compress_segmentby = 'station_id');
ALTER TABLE vehicle_positions   SET (timescaledb.compress);
ALTER TABLE service_alerts SET (timescaledb.compress);

SELECT add_compression_policy('flights',        INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('ships',          INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('satellites',     INTERVAL '14 days', if_not_exists => TRUE);
SELECT add_compression_policy('db_departures',  INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('vehicle_positions',   INTERVAL '7 days',  if_not_exists => TRUE);
SELECT add_compression_policy('service_alerts', INTERVAL '7 days',  if_not_exists => TRUE);

-- Retention policies are driven by the daemon's RETENTION_*_DAYS env vars
-- via src/storage/retention.rs, not registered here.
