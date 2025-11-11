-- Enhanced TimescaleDB setup for ilmanhinta
-- Migration to all-in TimescaleDB architecture with compression, continuous aggregates, and analytics

-- =============================================================================
-- EXTENSIONS
-- =============================================================================

-- Enable TimescaleDB extension (time-series database)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Enable timescaledb-toolkit (advanced analytics functions)
-- Provides: stats_agg(), time_weight(), percentile_agg(), uddsketch(), etc.
-- Included in timescaledb-ha image
CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;
ALTER EXTENSION timescaledb_toolkit UPDATE;

-- Enable pg_stat_monitor (query performance monitoring)
-- Percona's enhanced version of pg_stat_statements with time-bucketed statistics
CREATE EXTENSION IF NOT EXISTS pg_stat_monitor;

-- =============================================================================
-- CORE TABLES (Time-series data with hypertables)
-- =============================================================================

-- Electricity consumption data (15-min intervals from Fingrid)
CREATE TABLE IF NOT EXISTS fingrid_power_actuals (
    time TIMESTAMPTZ NOT NULL,
    consumption_mw DOUBLE PRECISION,
    production_mw DOUBLE PRECISION,
    wind_mw DOUBLE PRECISION,
    nuclear_mw DOUBLE PRECISION,
    net_import_mw DOUBLE PRECISION,
    source TEXT DEFAULT 'fingrid',
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(time)
);

-- Electricity spot prices (hourly)
CREATE TABLE IF NOT EXISTS fingrid_price_actuals (
    time TIMESTAMPTZ NOT NULL,
    price_eur_mwh DOUBLE PRECISION NOT NULL,
    area TEXT DEFAULT 'FI',
    price_type TEXT DEFAULT 'imbalance',
    source TEXT DEFAULT 'nordpool',
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(time, area, price_type)
);

-- Weather observations (hourly from FMI)
CREATE TABLE IF NOT EXISTS fmi_weather_observations (
    time TIMESTAMPTZ NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT,
    temperature_c DOUBLE PRECISION,
    humidity_percent DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    cloud_cover INTEGER,
    data_type TEXT DEFAULT 'observation', -- 'observation' or 'forecast'
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(time, station_id, data_type)
);

-- Consumption model predictions (stores ALL historical runs for evaluation)
CREATE TABLE IF NOT EXISTS consumption_model_predictions (
    timestamp TIMESTAMPTZ NOT NULL,
    predicted_consumption_mw DOUBLE PRECISION NOT NULL,
    confidence_lower DOUBLE PRECISION NOT NULL,
    confidence_upper DOUBLE PRECISION NOT NULL,
    model_type TEXT NOT NULL, -- 'lightgbm', 'prophet', 'ensemble'
    model_version TEXT,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, model_type, generated_at) -- Allow multiple consumption_model_predictions per timestamp
);

-- FMI weather forecasts (feature store for ML)
CREATE TABLE IF NOT EXISTS fmi_weather_forecasts (
    forecast_time TIMESTAMPTZ NOT NULL,
    station_id TEXT NOT NULL,
    station_name TEXT,
    temperature_c DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    cloud_cover DOUBLE PRECISION,
    humidity_percent DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    global_radiation_wm2 DOUBLE PRECISION,
    forecast_model TEXT DEFAULT 'harmonie',
    generated_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (forecast_time, station_id, generated_at)
);

-- Fingrid official forecasts (baseline + ML features)
CREATE TABLE IF NOT EXISTS fingrid_power_forecasts (
    forecast_time TIMESTAMPTZ NOT NULL,
    forecast_type TEXT NOT NULL, -- consumption, production, wind, price
    value DOUBLE PRECISION NOT NULL,
    unit TEXT DEFAULT 'MW',
    update_frequency TEXT,
    generated_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (forecast_time, forecast_type, generated_at)
);

-- Price model predictions (vs Fingrid comparison)
CREATE TABLE IF NOT EXISTS price_model_predictions (
    prediction_time TIMESTAMPTZ NOT NULL,
    model_type TEXT NOT NULL,
    model_version TEXT,
    predicted_price_eur_mwh DOUBLE PRECISION NOT NULL,
    confidence_lower DOUBLE PRECISION,
    confidence_upper DOUBLE PRECISION,
    predicted_consumption_mw DOUBLE PRECISION,
    generated_at TIMESTAMPTZ NOT NULL,
    features_hash TEXT,
    training_end_date TIMESTAMPTZ,
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (prediction_time, model_type, generated_at)
);

-- =============================================================================
-- CONVERT TO HYPERTABLES (Automatic time-based partitioning)
-- =============================================================================

-- Partition by time with 7-day chunks (good balance for this data volume)
SELECT create_hypertable('fingrid_power_actuals', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT create_hypertable('fingrid_price_actuals', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT create_hypertable('fmi_weather_observations', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Predictions table: Use generated_at for partitioning (when prediction was made)
-- This allows efficient queries like "show me consumption predictions made in the last 7 days"
SELECT create_hypertable('consumption_model_predictions', 'generated_at',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

SELECT create_hypertable('fmi_weather_forecasts', 'forecast_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT create_hypertable('fingrid_power_forecasts', 'forecast_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

SELECT create_hypertable('price_model_predictions', 'prediction_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- =============================================================================
-- COMPRESSION POLICIES (Store 10x more data in same space)
-- =============================================================================

-- Enable compression on fingrid_power_actuals
ALTER TABLE fingrid_power_actuals SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source',
  timescaledb.compress_orderby = 'time DESC'
);

-- Compress data older than 7 days (still queryable, just compressed)
SELECT add_compression_policy('fingrid_power_actuals',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression on fmi_weather_observations
ALTER TABLE fmi_weather_observations SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'station_id, data_type',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('fmi_weather_observations',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression on consumption_model_predictions (segment by model type)
ALTER TABLE consumption_model_predictions SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'model_type',
  timescaledb.compress_orderby = 'generated_at DESC, timestamp DESC'
);

SELECT add_compression_policy('consumption_model_predictions',
    INTERVAL '30 days', -- Compress consumption_model_predictions older than 30 days
    if_not_exists => TRUE
);

-- Enable compression on prices
ALTER TABLE fingrid_price_actuals SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'area',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('fingrid_price_actuals',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

ALTER TABLE fmi_weather_forecasts SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'station_id',
  timescaledb.compress_orderby = 'forecast_time DESC'
);

SELECT add_compression_policy('fmi_weather_forecasts',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

ALTER TABLE fingrid_power_forecasts SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'forecast_type',
  timescaledb.compress_orderby = 'forecast_time DESC'
);

SELECT add_compression_policy('fingrid_power_forecasts',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

ALTER TABLE price_model_predictions SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'model_type',
  timescaledb.compress_orderby = 'prediction_time DESC'
);

SELECT add_compression_policy('price_model_predictions',
    INTERVAL '30 days',
    if_not_exists => TRUE
);

-- =============================================================================
-- INDEXES FOR COMMON QUERIES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_consumption_time ON fingrid_power_actuals(time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_station_time ON fmi_weather_observations(station_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_type ON fmi_weather_observations(data_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_forecast_lookup ON fmi_weather_observations(station_id, data_type, time DESC)
    WHERE data_type = 'forecast';
CREATE INDEX IF NOT EXISTS idx_fmi_weather_forecasts_station ON fmi_weather_forecasts (station_id, forecast_time DESC);
CREATE INDEX IF NOT EXISTS idx_fmi_weather_forecasts_generated ON fmi_weather_forecasts (generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_fingrid_power_forecasts_type_time ON fingrid_power_forecasts (forecast_type, forecast_time DESC);
CREATE INDEX IF NOT EXISTS idx_fingrid_power_forecasts_generated ON fingrid_power_forecasts (generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_prices_area ON fingrid_price_actuals(area, time DESC);
CREATE INDEX IF NOT EXISTS idx_prices_type ON fingrid_price_actuals(price_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_model ON consumption_model_predictions(model_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_generated ON consumption_model_predictions(generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_model_predictions_model ON price_model_predictions(model_type, prediction_time DESC);
CREATE INDEX IF NOT EXISTS idx_price_model_predictions_generated ON price_model_predictions(generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_price_model_predictions_version ON price_model_predictions(model_version, prediction_time DESC);

-- =============================================================================
-- CONTINUOUS AGGREGATES (Pre-computed analytics, auto-updating)
-- =============================================================================

-- 1. Hourly consumption statistics (for dashboards)
-- Using timescaledb-toolkit's stats_agg() for cleaner, more efficient statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS consumption_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    -- Use stats_agg for consumption (stores all statistics in one aggregate)
    stats_agg(consumption_mw) AS consumption_stats,
    -- Explicit max/min since stats_agg doesn't provide accessors for these
    MAX(consumption_mw) AS max_consumption_mw,
    MIN(consumption_mw) AS min_consumption_mw,
    -- Individual averages for other metrics
    AVG(production_mw) AS avg_production_mw,
    AVG(wind_mw) AS avg_wind_mw,
    AVG(nuclear_mw) AS avg_nuclear_mw,
    COUNT(*) AS sample_count
FROM fingrid_power_actuals
GROUP BY hour
WITH NO DATA;

-- Auto-refresh every hour, processing last 3 days
SELECT add_continuous_aggregate_policy('consumption_hourly',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- 2. Daily price statistics
-- Using timescaledb-toolkit's uddsketch for efficient percentile approximations
CREATE MATERIALIZED VIEW IF NOT EXISTS prices_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    area,
    -- Use stats_agg for basic statistics
    stats_agg(price_eur_mwh) AS price_stats,
    -- Explicit max/min since stats_agg doesn't provide accessors for these
    MAX(price_eur_mwh) AS max_price,
    MIN(price_eur_mwh) AS min_price,
    -- Use uddsketch for percentile approximations (faster than exact percentiles)
    uddsketch(100, 0.001, price_eur_mwh) AS price_distribution
FROM fingrid_price_actuals
GROUP BY day, area
WITH NO DATA;

SELECT add_continuous_aggregate_policy('prices_daily',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 3. CRITICAL: Prediction accuracy metrics (for model evaluation)
-- This joins consumption_model_predictions with actual consumption to track model performance
-- Using timescaledb-toolkit for cleaner error statistics
-- NOTE: Regular materialized view (not continuous aggregate) because it joins multiple hypertables
CREATE MATERIALIZED VIEW IF NOT EXISTS prediction_accuracy_hourly AS
SELECT
    time_bucket('1 hour', p.timestamp) AS hour,
    p.model_type,
    COUNT(*) AS prediction_count,

    -- Actual vs predicted using stats_agg
    stats_agg(c.consumption_mw) AS actual_stats,
    stats_agg(p.predicted_consumption_mw) AS predicted_stats,

    -- Error metrics using stats_agg on error
    stats_agg(p.predicted_consumption_mw - c.consumption_mw) AS error_stats,

    -- Absolute error for MAE
    AVG(ABS(p.predicted_consumption_mw - c.consumption_mw)) AS mae_mw,

    -- Relative error (percentage)
    AVG(ABS(p.predicted_consumption_mw - c.consumption_mw) / NULLIF(c.consumption_mw, 0) * 100) AS mape_percent,

    -- Confidence interval coverage (% of actuals within predicted range)
    AVG(CASE
        WHEN c.consumption_mw BETWEEN p.confidence_lower AND p.confidence_upper
        THEN 1.0
        ELSE 0.0
    END) * 100 AS coverage_percent,

    -- Confidence interval width using stats_agg
    stats_agg(p.confidence_upper - p.confidence_lower) AS interval_width_stats

FROM consumption_model_predictions p
-- Join with actual consumption (use hourly bucket to match prediction timestamps)
JOIN fingrid_power_actuals c
    ON time_bucket('1 hour', c.time) = time_bucket('1 hour', p.timestamp)
-- Only evaluate consumption_model_predictions that have passed (1 hour buffer)
WHERE p.timestamp < NOW() - INTERVAL '1 hour'
GROUP BY hour, p.model_type;

-- Create unique index for CONCURRENT refresh support
CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_accuracy_hourly_unique ON prediction_accuracy_hourly(hour, model_type);

-- 4. Daily prediction accuracy rollup (for trend analysis)
-- Rolls up hourly stats_agg into daily aggregates
-- NOTE: Regular materialized view (queries from another materialized view)
CREATE MATERIALIZED VIEW IF NOT EXISTS prediction_accuracy_daily AS
SELECT
    time_bucket('1 day', hour) AS day,
    model_type,
    SUM(prediction_count) AS total_predictions,
    -- Combine stats_agg across hours using rollup()
    rollup(actual_stats) AS daily_actual_stats,
    rollup(predicted_stats) AS daily_predicted_stats,
    rollup(error_stats) AS daily_error_stats,
    AVG(mae_mw) AS avg_mae_mw,
    AVG(mape_percent) AS avg_mape_percent,
    AVG(coverage_percent) AS avg_coverage_percent,
    rollup(interval_width_stats) AS daily_interval_width_stats
FROM prediction_accuracy_hourly
GROUP BY day, model_type;

-- Create unique index for CONCURRENT refresh support
CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_accuracy_daily_unique ON prediction_accuracy_daily(day, model_type);

-- 5. Model comparison view (latest 24h performance)
-- Using timescaledb-toolkit for efficient error statistics
-- NOTE: Regular materialized view (joins multiple hypertables)
CREATE MATERIALIZED VIEW IF NOT EXISTS model_comparison_24h AS
SELECT
    p.model_type,
    COUNT(*) AS prediction_count,
    -- Error statistics using stats_agg
    stats_agg(ABS(p.predicted_consumption_mw - c.consumption_mw)) AS absolute_error_stats,
    stats_agg(POWER(p.predicted_consumption_mw - c.consumption_mw, 2)) AS squared_error_stats,
    AVG(CASE
        WHEN c.consumption_mw BETWEEN p.confidence_lower AND p.confidence_upper
        THEN 1.0
        ELSE 0.0
    END) * 100 AS coverage_percent,
    MAX(p.model_version) AS latest_version,
    MAX(p.generated_at) AS latest_prediction_time
FROM consumption_model_predictions p
JOIN fingrid_power_actuals c
    ON time_bucket('1 hour', c.time) = time_bucket('1 hour', p.timestamp)
WHERE p.timestamp >= NOW() - INTERVAL '24 hours'
  AND p.timestamp < NOW() - INTERVAL '1 hour'
GROUP BY p.model_type;

-- Create unique index for CONCURRENT refresh support
CREATE UNIQUE INDEX IF NOT EXISTS idx_model_comparison_24h_unique ON model_comparison_24h(model_type);

-- =============================================================================
-- RETENTION POLICIES (Optional - auto-delete old data)
-- =============================================================================

-- Uncomment these if you want to auto-delete old data:

-- Keep consumption data for 2 years
-- SELECT add_retention_policy('fingrid_power_actuals', INTERVAL '2 years', if_not_exists => TRUE);

-- Keep weather for 2 years
-- SELECT add_retention_policy('fmi_weather_observations', INTERVAL '2 years', if_not_exists => TRUE);

-- Keep consumption_model_predictions forever (for model evaluation) - no retention policy
-- If disk space becomes an issue, you can add:
-- SELECT add_retention_policy('consumption_model_predictions', INTERVAL '5 years', if_not_exists => TRUE);

-- Keep prices for 3 years
-- SELECT add_retention_policy('fingrid_price_actuals', INTERVAL '3 years', if_not_exists => TRUE);

-- =============================================================================
-- HELPER VIEWS (For convenience)
-- =============================================================================

-- Latest consumption_model_predictions from each model
CREATE OR REPLACE VIEW latest_predictions_per_model AS
SELECT DISTINCT ON (model_type, timestamp)
    timestamp,
    model_type,
    predicted_consumption_mw,
    confidence_lower,
    confidence_upper,
    model_version,
    generated_at
FROM consumption_model_predictions
WHERE generated_at = (
    SELECT MAX(generated_at)
    FROM consumption_model_predictions p2
    WHERE p2.model_type = consumption_model_predictions.model_type
)
ORDER BY model_type, timestamp, generated_at DESC;

-- Current weather (latest observation per station)
CREATE OR REPLACE VIEW current_weather AS
SELECT DISTINCT ON (station_id)
    station_id,
    time,
    temperature_c,
    humidity_percent,
    wind_speed_ms,
    pressure_hpa,
    cloud_cover
FROM fmi_weather_observations
WHERE data_type = 'observation'
ORDER BY station_id, time DESC;

-- =============================================================================
-- GRANTS (Ensure API user has read access)
-- =============================================================================

-- Grant read access to continuous aggregates for API queries
GRANT SELECT ON consumption_hourly TO api;
GRANT SELECT ON prices_daily TO api;
GRANT SELECT ON prediction_accuracy_hourly TO api;
GRANT SELECT ON prediction_accuracy_daily TO api;
GRANT SELECT ON model_comparison_24h TO api;
GRANT SELECT ON latest_predictions_per_model TO api;
GRANT SELECT ON current_weather TO api;

-- Grant read/write access to base tables
GRANT SELECT, INSERT, UPDATE ON fingrid_power_actuals TO api;
GRANT SELECT, INSERT, UPDATE ON fingrid_price_actuals TO api;
GRANT SELECT, INSERT, UPDATE ON fmi_weather_observations TO api;
GRANT SELECT, INSERT, UPDATE ON consumption_model_predictions TO api;

-- =============================================================================
-- HELPER VIEWS FOR QUERYING STATS_AGG RESULTS
-- =============================================================================

-- View to extract consumption statistics from stats_agg
CREATE OR REPLACE VIEW consumption_hourly_stats AS
SELECT
    hour,
    average(consumption_stats) AS avg_consumption_mw,
    max_consumption_mw,
    min_consumption_mw,
    stddev(consumption_stats) AS std_consumption_mw,
    variance(consumption_stats) AS var_consumption_mw,
    avg_production_mw,
    avg_wind_mw,
    avg_nuclear_mw,
    sample_count
FROM consumption_hourly;

-- View to extract price statistics from stats_agg and uddsketch
CREATE OR REPLACE VIEW prices_daily_stats AS
SELECT
    day,
    area,
    average(price_stats) AS avg_price,
    max_price,
    min_price,
    stddev(price_stats) AS price_volatility,
    approx_percentile(0.5, price_distribution) AS median_price,
    approx_percentile(0.95, price_distribution) AS p95_price,
    approx_percentile(0.05, price_distribution) AS p05_price
FROM prices_daily;

-- View to extract prediction accuracy statistics
CREATE OR REPLACE VIEW prediction_accuracy_hourly_stats AS
SELECT
    hour,
    model_type,
    prediction_count,
    -- Actual consumption stats
    average(actual_stats) AS actual_avg_mw,
    stddev(actual_stats) AS actual_std_mw,
    -- Predicted consumption stats
    average(predicted_stats) AS predicted_avg_mw,
    stddev(predicted_stats) AS predicted_std_mw,
    -- Error stats (bias is average of error)
    average(error_stats) AS bias_mw,
    stddev(error_stats) AS error_std_mw,
    SQRT(variance(error_stats)) AS rmse_mw,
    mae_mw,
    mape_percent,
    coverage_percent,
    average(interval_width_stats) AS avg_interval_width_mw
FROM prediction_accuracy_hourly;

-- View to extract daily prediction accuracy statistics
CREATE OR REPLACE VIEW prediction_accuracy_daily_stats AS
SELECT
    day,
    model_type,
    total_predictions,
    average(daily_actual_stats) AS avg_actual_mw,
    average(daily_predicted_stats) AS avg_predicted_mw,
    average(daily_error_stats) AS avg_bias_mw,
    SQRT(variance(daily_error_stats)) AS avg_rmse_mw,
    avg_mae_mw,
    avg_mape_percent,
    avg_coverage_percent,
    average(daily_interval_width_stats) AS avg_interval_width_mw
FROM prediction_accuracy_daily;

-- View to extract model comparison metrics
CREATE OR REPLACE VIEW model_comparison_24h_stats AS
SELECT
    model_type,
    prediction_count,
    average(absolute_error_stats) AS mae_mw,
    SQRT(average(squared_error_stats)) AS rmse_mw,
    stddev(absolute_error_stats) AS mae_std_mw,
    coverage_percent,
    latest_version,
    latest_prediction_time
FROM model_comparison_24h;

-- =============================================================================
-- PG_STAT_MONITOR HELPER VIEWS
-- =============================================================================

-- View for top slow queries
CREATE OR REPLACE VIEW slow_queries AS
SELECT
    bucket,
    userid::regrole AS user,
    datname AS database,
    query,
    calls,
    mean_exec_time,
    max_exec_time,
    min_exec_time,
    stddev_exec_time,
    total_exec_time,
    rows,
    -- Query text (first 100 chars)
    substring(query, 1, 100) AS query_preview
FROM pg_stat_monitor
WHERE mean_exec_time > 10  -- Queries taking >10ms on average
ORDER BY mean_exec_time DESC
LIMIT 50;

-- View for query performance over time
CREATE OR REPLACE VIEW query_performance_buckets AS
SELECT
    bucket_start_time,
    COUNT(DISTINCT query) AS unique_queries,
    SUM(calls) AS total_calls,
    AVG(mean_exec_time) AS avg_query_time_ms,
    MAX(max_exec_time) AS slowest_query_ms,
    SUM(total_exec_time) AS total_db_time_ms
FROM pg_stat_monitor
GROUP BY bucket_start_time
ORDER BY bucket_start_time DESC
LIMIT 100;

-- View for most frequent queries
CREATE OR REPLACE VIEW frequent_queries AS
SELECT
    query,
    calls,
    mean_exec_time,
    total_exec_time,
    rows,
    substring(query, 1, 150) AS query_preview
FROM pg_stat_monitor
ORDER BY calls DESC
LIMIT 30;

-- =============================================================================
-- GRANTS FOR HELPER VIEWS
-- =============================================================================

GRANT SELECT ON consumption_hourly_stats TO api;
GRANT SELECT ON prices_daily_stats TO api;
GRANT SELECT ON prediction_accuracy_hourly_stats TO api;
GRANT SELECT ON prediction_accuracy_daily_stats TO api;
GRANT SELECT ON model_comparison_24h_stats TO api;
GRANT SELECT ON slow_queries TO api;
GRANT SELECT ON query_performance_buckets TO api;
GRANT SELECT ON frequent_queries TO api;

-- =============================================================================
-- PRICE FORECAST COMPARISON VIEWS
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS prediction_comparison AS
WITH latest_model_pred AS (
    SELECT DISTINCT ON (prediction_time)
        prediction_time,
        predicted_price_eur_mwh,
        confidence_lower,
        confidence_upper,
        model_type,
        model_version,
        generated_at
    FROM price_model_predictions
    WHERE model_type = 'production'
    ORDER BY prediction_time, generated_at DESC
),
latest_fingrid_fcst AS (
    SELECT DISTINCT ON (forecast_time)
        forecast_time,
        value AS fingrid_price_forecast
    FROM fingrid_power_forecasts
    WHERE forecast_type = 'price'
    ORDER BY forecast_time, generated_at DESC
)
SELECT
    p.prediction_time AS time,
    p.predicted_price_eur_mwh AS ml_prediction,
    p.confidence_lower AS ml_lower,
    p.confidence_upper AS ml_upper,
    p.model_type,
    p.model_version,
    ff.fingrid_price_forecast,
    ep.price_eur_mwh AS actual_price,
    CASE
        WHEN ep.price_eur_mwh IS NOT NULL
        THEN ABS(p.predicted_price_eur_mwh - ep.price_eur_mwh)
    END AS ml_absolute_error,
    CASE
        WHEN ep.price_eur_mwh IS NOT NULL AND ep.price_eur_mwh != 0
        THEN ABS(p.predicted_price_eur_mwh - ep.price_eur_mwh) / ABS(ep.price_eur_mwh) * 100
    END AS ml_percentage_error,
    CASE
        WHEN ep.price_eur_mwh IS NOT NULL AND ff.fingrid_price_forecast IS NOT NULL
        THEN ABS(ff.fingrid_price_forecast - ep.price_eur_mwh)
    END AS fingrid_absolute_error,
    CASE
        WHEN ep.price_eur_mwh IS NOT NULL AND ff.fingrid_price_forecast IS NOT NULL AND ep.price_eur_mwh != 0
        THEN ABS(ff.fingrid_price_forecast - ep.price_eur_mwh) / ABS(ep.price_eur_mwh) * 100
    END AS fingrid_percentage_error,
    CASE
        WHEN ep.price_eur_mwh IS NOT NULL AND ff.fingrid_price_forecast IS NOT NULL THEN
            CASE
                WHEN ABS(p.predicted_price_eur_mwh - ep.price_eur_mwh) < ABS(ff.fingrid_price_forecast - ep.price_eur_mwh) THEN 1
                WHEN ABS(p.predicted_price_eur_mwh - ep.price_eur_mwh) > ABS(ff.fingrid_price_forecast - ep.price_eur_mwh) THEN -1
                ELSE 0
            END
    END AS ml_vs_fingrid_winner,
    p.generated_at AS prediction_generated_at,
    NOW() AS view_updated_at
FROM latest_model_pred p
LEFT JOIN latest_fingrid_fcst ff ON ff.forecast_time = p.prediction_time
LEFT JOIN fingrid_price_actuals ep
    ON ep.time = p.prediction_time
    AND ep.area = 'FI'
    AND ep.price_type = 'imbalance'
WHERE p.prediction_time < NOW()
ORDER BY p.prediction_time DESC;

CREATE INDEX IF NOT EXISTS idx_comparison_time
    ON prediction_comparison (time DESC);
CREATE INDEX IF NOT EXISTS idx_comparison_winner
    ON prediction_comparison (ml_vs_fingrid_winner);

CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_prediction_performance AS
SELECT
    time_bucket('1 hour', time) AS hour,
    COUNT(*) AS num_predictions,
    AVG(ml_absolute_error) AS ml_avg_error,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ml_absolute_error) AS ml_median_error,
    AVG(ml_percentage_error) AS ml_avg_pct_error,
    AVG(fingrid_absolute_error) AS fingrid_avg_error,
    AVG(fingrid_percentage_error) AS fingrid_avg_pct_error,
    SUM(CASE WHEN ml_vs_fingrid_winner = 1 THEN 1 ELSE 0 END)::FLOAT /
        NULLIF(COUNT(*), 0) * 100 AS ml_win_rate_pct,
    AVG(actual_price) AS avg_actual_price,
    MIN(actual_price) AS min_actual_price,
    MAX(actual_price) AS max_actual_price
FROM prediction_comparison
WHERE actual_price IS NOT NULL
GROUP BY hour
WITH NO DATA;

CREATE OR REPLACE FUNCTION refresh_prediction_comparison()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY prediction_comparison;
    REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_prediction_performance;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION get_latest_performance_summary(
    hours_back INTEGER DEFAULT 24
)
RETURNS TABLE (
    metric VARCHAR,
    ml_value NUMERIC,
    fingrid_value NUMERIC,
    improvement_pct NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    WITH recent_data AS (
        SELECT *
        FROM prediction_comparison
        WHERE time >= NOW() - (hours_back || ' hours')::INTERVAL
        AND actual_price IS NOT NULL
        AND fingrid_price_forecast IS NOT NULL
    )
    SELECT
        'Mean Absolute Error (EUR/MWh)'::VARCHAR,
        ROUND(AVG(ml_absolute_error)::NUMERIC, 2),
        ROUND(AVG(fingrid_absolute_error)::NUMERIC, 2),
        ROUND(((AVG(fingrid_absolute_error) - AVG(ml_absolute_error)) /
               NULLIF(AVG(fingrid_absolute_error), 0) * 100)::NUMERIC, 1)
    FROM recent_data

    UNION ALL

    SELECT
        'Win Rate (%)'::VARCHAR,
        ROUND((SUM(CASE WHEN ml_vs_fingrid_winner = 1 THEN 1 ELSE 0 END)::FLOAT /
               NULLIF(COUNT(*), 0) * 100)::NUMERIC, 1),
        NULL,
        NULL
    FROM recent_data;
END;
$$ LANGUAGE plpgsql;

GRANT SELECT ON prediction_comparison TO api;
GRANT SELECT ON hourly_prediction_performance TO api;
GRANT EXECUTE ON FUNCTION refresh_prediction_comparison TO api;
GRANT EXECUTE ON FUNCTION get_latest_performance_summary TO api;

-- =============================================================================
-- RETENTION POLICIES
-- =============================================================================

SELECT add_retention_policy('fingrid_power_actuals', INTERVAL '730 days', if_not_exists => TRUE);
SELECT add_retention_policy('fingrid_price_actuals', INTERVAL '730 days', if_not_exists => TRUE);
SELECT add_retention_policy('fmi_weather_observations', INTERVAL '730 days', if_not_exists => TRUE);
SELECT add_retention_policy('fmi_weather_forecasts', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('fingrid_power_forecasts', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('consumption_model_predictions', INTERVAL '365 days', if_not_exists => TRUE);
SELECT add_retention_policy('price_model_predictions', INTERVAL '365 days', if_not_exists => TRUE);

-- =============================================================================
-- DONE! Now you have:
-- - Hypertables with automatic partitioning
-- - 10x compression on old data
-- - Auto-updating analytics (continuous aggregates)
-- - Prediction accuracy tracking with timescaledb-toolkit
-- - Model comparison metrics
-- - Fast queries via materialized views
-- - Query performance monitoring via pg_stat_monitor
-- - Cleaner statistics using stats_agg() and uddsketch()
-- =============================================================================

-- Check TimescaleDB setup
SELECT * FROM timescaledb_information.hypertables;
SELECT * FROM timescaledb_information.continuous_aggregates;
SELECT * FROM timescaledb_information.compression_settings;
