-- Ingest latency queries.
--
-- Every hot table has an `ingested_at TIMESTAMPTZ DEFAULT NOW()` column.
-- The difference `ingested_at - timestamp` is the per-row wall-clock
-- latency from "Rust has the record in hand" to "Postgres has it durable".
--
-- Use these as Grafana panels; the $__timeFilter() macro scopes to the
-- range picker.

-- ---------------------------------------------------------------------------
-- p50 / p95 / p99 ingest latency, last 5 minutes, per table.
-- ---------------------------------------------------------------------------
SELECT
    'flights' AS table,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY
        EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000) AS p50_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY
        EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000) AS p95_ms,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY
        EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000) AS p99_ms,
    count(*) AS samples
FROM flights
WHERE ingested_at > NOW() - INTERVAL '5 minutes'
UNION ALL
SELECT 'ships',
    percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000),
    percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000),
    percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000),
    count(*)
FROM ships
WHERE ingested_at > NOW() - INTERVAL '5 minutes';

-- ---------------------------------------------------------------------------
-- p99 time series (1-minute buckets, last 24 h).
-- Point this at a Grafana time-series panel; $__timeFilter replaces the
-- WHERE clause.
-- ---------------------------------------------------------------------------
SELECT
    date_trunc('minute', ingested_at) AS time,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY
        EXTRACT(EPOCH FROM (ingested_at - "timestamp")) * 1000) AS p99_ms_flights
FROM flights
WHERE $__timeFilter(ingested_at)
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- Alarm query: p99 latency over the last 60 s exceeds 2 s.
-- Wire into a Grafana alert rule.
-- ---------------------------------------------------------------------------
SELECT
    percentile_cont(0.99) WITHIN GROUP (ORDER BY
        EXTRACT(EPOCH FROM (ingested_at - "timestamp"))) AS p99_seconds
FROM flights
WHERE ingested_at > NOW() - INTERVAL '60 seconds';
