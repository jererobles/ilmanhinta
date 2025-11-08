-- Enhanced TimescaleDB setup for ilmanhinta
-- Migration to all-in TimescaleDB architecture with compression, continuous aggregates, and analytics

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- =============================================================================
-- CORE TABLES (Time-series data with hypertables)
-- =============================================================================

-- Electricity consumption data (15-min intervals from Fingrid)
CREATE TABLE IF NOT EXISTS electricity_consumption (
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
CREATE TABLE IF NOT EXISTS electricity_prices (
    time TIMESTAMPTZ NOT NULL,
    price_eur_mwh DOUBLE PRECISION NOT NULL,
    area TEXT DEFAULT 'FI',
    source TEXT DEFAULT 'nordpool',
    ingested_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(time, area)
);

-- Weather observations (hourly from FMI)
CREATE TABLE IF NOT EXISTS weather_observations (
    time TIMESTAMPTZ NOT NULL,
    station_id TEXT NOT NULL,
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

-- Predictions table (stores ALL historical predictions for model evaluation)
CREATE TABLE IF NOT EXISTS predictions (
    timestamp TIMESTAMPTZ NOT NULL,
    predicted_consumption_mw DOUBLE PRECISION NOT NULL,
    confidence_lower DOUBLE PRECISION NOT NULL,
    confidence_upper DOUBLE PRECISION NOT NULL,
    model_type TEXT NOT NULL, -- 'lightgbm', 'prophet', 'ensemble'
    model_version TEXT,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, model_type, generated_at) -- Allow multiple predictions per timestamp
);

-- =============================================================================
-- CONVERT TO HYPERTABLES (Automatic time-based partitioning)
-- =============================================================================

-- Partition by time with 7-day chunks (good balance for this data volume)
SELECT create_hypertable('electricity_consumption', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT create_hypertable('electricity_prices', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

SELECT create_hypertable('weather_observations', 'time',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Predictions table: Use generated_at for partitioning (when prediction was made)
-- This allows efficient queries like "show me predictions made in the last 7 days"
SELECT create_hypertable('predictions', 'generated_at',
    chunk_time_interval => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- =============================================================================
-- COMPRESSION POLICIES (Store 10x more data in same space)
-- =============================================================================

-- Enable compression on electricity_consumption
ALTER TABLE electricity_consumption SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'source',
  timescaledb.compress_orderby = 'time DESC'
);

-- Compress data older than 7 days (still queryable, just compressed)
SELECT add_compression_policy('electricity_consumption',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression on weather_observations
ALTER TABLE weather_observations SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'station_id, data_type',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('weather_observations',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression on predictions (segment by model type)
ALTER TABLE predictions SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'model_type',
  timescaledb.compress_orderby = 'generated_at DESC, timestamp DESC'
);

SELECT add_compression_policy('predictions',
    INTERVAL '30 days', -- Compress predictions older than 30 days
    if_not_exists => TRUE
);

-- Enable compression on prices
ALTER TABLE electricity_prices SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'area',
  timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('electricity_prices',
    INTERVAL '7 days',
    if_not_exists => TRUE
);

-- =============================================================================
-- INDEXES FOR COMMON QUERIES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_consumption_time ON electricity_consumption(time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_station_time ON weather_observations(station_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_type ON weather_observations(data_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_prices_area ON electricity_prices(area, time DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_model ON predictions(model_type, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_generated ON predictions(generated_at DESC);

-- =============================================================================
-- CONTINUOUS AGGREGATES (Pre-computed analytics, auto-updating)
-- =============================================================================

-- 1. Hourly consumption statistics (for dashboards)
CREATE MATERIALIZED VIEW IF NOT EXISTS consumption_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    AVG(consumption_mw) AS avg_consumption_mw,
    MAX(consumption_mw) AS max_consumption_mw,
    MIN(consumption_mw) AS min_consumption_mw,
    STDDEV(consumption_mw) AS std_consumption_mw,
    AVG(production_mw) AS avg_production_mw,
    AVG(wind_mw) AS avg_wind_mw,
    AVG(nuclear_mw) AS avg_nuclear_mw,
    COUNT(*) AS sample_count
FROM electricity_consumption
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
CREATE MATERIALIZED VIEW IF NOT EXISTS prices_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time) AS day,
    area,
    AVG(price_eur_mwh) AS avg_price,
    MAX(price_eur_mwh) AS max_price,
    MIN(price_eur_mwh) AS min_price,
    STDDEV(price_eur_mwh) AS price_volatility,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_eur_mwh) AS median_price,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price_eur_mwh) AS p95_price
FROM electricity_prices
GROUP BY day, area
WITH NO DATA;

SELECT add_continuous_aggregate_policy('prices_daily',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 3. CRITICAL: Prediction accuracy metrics (for model evaluation)
-- This joins predictions with actual consumption to track model performance
CREATE MATERIALIZED VIEW IF NOT EXISTS prediction_accuracy_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', p.timestamp) AS hour,
    p.model_type,
    COUNT(*) AS prediction_count,

    -- Actual vs predicted
    AVG(c.consumption_mw) AS actual_avg_mw,
    AVG(p.predicted_consumption_mw) AS predicted_avg_mw,

    -- Error metrics
    AVG(p.predicted_consumption_mw - c.consumption_mw) AS bias_mw,
    AVG(ABS(p.predicted_consumption_mw - c.consumption_mw)) AS mae_mw,
    SQRT(AVG(POWER(p.predicted_consumption_mw - c.consumption_mw, 2))) AS rmse_mw,

    -- Relative error (percentage)
    AVG(ABS(p.predicted_consumption_mw - c.consumption_mw) / NULLIF(c.consumption_mw, 0) * 100) AS mape_percent,

    -- Confidence interval coverage (% of actuals within predicted range)
    AVG(CASE
        WHEN c.consumption_mw BETWEEN p.confidence_lower AND p.confidence_upper
        THEN 1.0
        ELSE 0.0
    END) * 100 AS coverage_percent,

    -- Confidence interval width
    AVG(p.confidence_upper - p.confidence_lower) AS avg_interval_width_mw

FROM predictions p
-- Join with actual consumption (use hourly bucket to match prediction timestamps)
JOIN electricity_consumption c
    ON time_bucket('1 hour', c.time) = time_bucket('1 hour', p.timestamp)
-- Only evaluate predictions that have passed (1 hour buffer)
WHERE p.timestamp < NOW() - INTERVAL '1 hour'
GROUP BY hour, p.model_type
WITH NO DATA;

-- Refresh daily (predictions need time to materialize actual values)
SELECT add_continuous_aggregate_policy('prediction_accuracy_hourly',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '2 hours', -- 2-hour buffer to ensure actual data is available
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 4. Daily prediction accuracy rollup (for trend analysis)
CREATE MATERIALIZED VIEW IF NOT EXISTS prediction_accuracy_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', hour) AS day,
    model_type,
    SUM(prediction_count) AS total_predictions,
    AVG(actual_avg_mw) AS avg_actual_mw,
    AVG(predicted_avg_mw) AS avg_predicted_mw,
    AVG(bias_mw) AS avg_bias_mw,
    AVG(mae_mw) AS avg_mae_mw,
    AVG(rmse_mw) AS avg_rmse_mw,
    AVG(mape_percent) AS avg_mape_percent,
    AVG(coverage_percent) AS avg_coverage_percent,
    AVG(avg_interval_width_mw) AS avg_interval_width_mw
FROM prediction_accuracy_hourly
GROUP BY day, model_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('prediction_accuracy_daily',
    start_offset => INTERVAL '14 days',
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 5. Model comparison view (latest 24h performance)
CREATE MATERIALIZED VIEW IF NOT EXISTS model_comparison_24h
WITH (timescaledb.continuous) AS
SELECT
    p.model_type,
    COUNT(*) AS prediction_count,
    AVG(ABS(p.predicted_consumption_mw - c.consumption_mw)) AS mae_mw,
    SQRT(AVG(POWER(p.predicted_consumption_mw - c.consumption_mw, 2))) AS rmse_mw,
    AVG(CASE
        WHEN c.consumption_mw BETWEEN p.confidence_lower AND p.confidence_upper
        THEN 1.0
        ELSE 0.0
    END) * 100 AS coverage_percent,
    MAX(p.model_version) AS latest_version,
    MAX(p.generated_at) AS latest_prediction_time
FROM predictions p
JOIN electricity_consumption c
    ON time_bucket('1 hour', c.time) = time_bucket('1 hour', p.timestamp)
WHERE p.timestamp >= NOW() - INTERVAL '24 hours'
  AND p.timestamp < NOW() - INTERVAL '1 hour'
GROUP BY p.model_type
WITH NO DATA;

-- Refresh every hour
SELECT add_continuous_aggregate_policy('model_comparison_24h',
    start_offset => INTERVAL '2 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- =============================================================================
-- RETENTION POLICIES (Optional - auto-delete old data)
-- =============================================================================

-- Uncomment these if you want to auto-delete old data:

-- Keep consumption data for 2 years
-- SELECT add_retention_policy('electricity_consumption', INTERVAL '2 years', if_not_exists => TRUE);

-- Keep weather for 2 years
-- SELECT add_retention_policy('weather_observations', INTERVAL '2 years', if_not_exists => TRUE);

-- Keep predictions forever (for model evaluation) - no retention policy
-- If disk space becomes an issue, you can add:
-- SELECT add_retention_policy('predictions', INTERVAL '5 years', if_not_exists => TRUE);

-- Keep prices for 3 years
-- SELECT add_retention_policy('electricity_prices', INTERVAL '3 years', if_not_exists => TRUE);

-- =============================================================================
-- HELPER VIEWS (For convenience)
-- =============================================================================

-- Latest predictions from each model
CREATE OR REPLACE VIEW latest_predictions_per_model AS
SELECT DISTINCT ON (model_type, timestamp)
    timestamp,
    model_type,
    predicted_consumption_mw,
    confidence_lower,
    confidence_upper,
    model_version,
    generated_at
FROM predictions
WHERE generated_at = (
    SELECT MAX(generated_at)
    FROM predictions p2
    WHERE p2.model_type = predictions.model_type
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
FROM weather_observations
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
GRANT SELECT, INSERT, UPDATE ON electricity_consumption TO api;
GRANT SELECT, INSERT, UPDATE ON electricity_prices TO api;
GRANT SELECT, INSERT, UPDATE ON weather_observations TO api;
GRANT SELECT, INSERT, UPDATE ON predictions TO api;

-- =============================================================================
-- DONE! Now you have:
-- - Hypertables with automatic partitioning
-- - 10x compression on old data
-- - Auto-updating analytics (continuous aggregates)
-- - Prediction accuracy tracking
-- - Model comparison metrics
-- - Fast queries via materialized views
-- =============================================================================

-- Check TimescaleDB setup
SELECT * FROM timescaledb_information.hypertables;
SELECT * FROM timescaledb_information.continuous_aggregates;
SELECT * FROM timescaledb_information.compression_settings;
