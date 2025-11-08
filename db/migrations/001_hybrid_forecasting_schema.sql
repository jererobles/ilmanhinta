-- Migration 001: Hybrid Forecasting Schema
-- Creates tables for comparing ML predictions vs Fingrid forecasts vs actuals
-- Requires TimescaleDB extension

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =====================================================
-- ACTUAL DATA TABLES (Historical - for Training)
-- =====================================================

-- Electricity consumption and production (actual values)
CREATE TABLE IF NOT EXISTS electricity_consumption (
    time TIMESTAMPTZ NOT NULL,
    consumption_mw DOUBLE PRECISION,
    production_mw DOUBLE PRECISION,
    wind_mw DOUBLE PRECISION,
    nuclear_mw DOUBLE PRECISION,
    net_import_mw DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (time)
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable(
    'electricity_consumption',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_consumption_time_desc
    ON electricity_consumption (time DESC);

-- Electricity prices (actual - TARGET VARIABLE)
CREATE TABLE IF NOT EXISTS electricity_prices (
    time TIMESTAMPTZ NOT NULL,
    area VARCHAR(10) NOT NULL DEFAULT 'FI',
    price_eur_mwh DOUBLE PRECISION NOT NULL,
    price_type VARCHAR(50) DEFAULT 'imbalance', -- imbalance, day_ahead, intraday
    source VARCHAR(50) DEFAULT 'fingrid', -- fingrid, nord_pool, etc.
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (time, area, price_type)
);

SELECT create_hypertable(
    'electricity_prices',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_prices_time_area
    ON electricity_prices (time DESC, area);
CREATE INDEX IF NOT EXISTS idx_prices_type
    ON electricity_prices (price_type, time DESC);

-- Weather observations (actual - for training)
CREATE TABLE IF NOT EXISTS weather_observations (
    time TIMESTAMPTZ NOT NULL,
    station_id VARCHAR(50) NOT NULL,
    station_name VARCHAR(100),
    temperature_c DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    cloud_cover DOUBLE PRECISION,
    humidity_percent DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (time, station_id)
);

SELECT create_hypertable(
    'weather_observations',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_weather_obs_station
    ON weather_observations (station_id, time DESC);

-- =====================================================
-- FORECAST DATA TABLES (Predictions - for ML Features)
-- =====================================================

-- Weather forecasts from FMI HARMONIE (FEATURES)
CREATE TABLE IF NOT EXISTS weather_forecasts (
    forecast_time TIMESTAMPTZ NOT NULL, -- When this forecast is for
    station_id VARCHAR(50) NOT NULL,
    station_name VARCHAR(100),
    temperature_c DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    wind_direction DOUBLE PRECISION,
    cloud_cover DOUBLE PRECISION,
    humidity_percent DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    global_radiation_wm2 DOUBLE PRECISION, -- Solar radiation
    forecast_model VARCHAR(50) DEFAULT 'harmonie', -- harmonie, ecmwf
    generated_at TIMESTAMPTZ NOT NULL, -- When this forecast was made
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (forecast_time, station_id, generated_at)
);

SELECT create_hypertable(
    'weather_forecasts',
    'forecast_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_weather_fcst_station
    ON weather_forecasts (station_id, forecast_time DESC);
CREATE INDEX IF NOT EXISTS idx_weather_fcst_generated
    ON weather_forecasts (generated_at DESC);

-- Fingrid's official forecasts (FEATURES + COMPARISON BASELINE)
CREATE TABLE IF NOT EXISTS fingrid_forecasts (
    forecast_time TIMESTAMPTZ NOT NULL, -- When this forecast is for
    forecast_type VARCHAR(50) NOT NULL, -- consumption, production, wind, price
    value DOUBLE PRECISION NOT NULL,
    unit VARCHAR(20) DEFAULT 'MW', -- MW or EUR/MWh
    update_frequency VARCHAR(50), -- '15min', 'hourly', 'daily'
    generated_at TIMESTAMPTZ NOT NULL, -- When this forecast was made
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (forecast_time, forecast_type, generated_at)
);

SELECT create_hypertable(
    'fingrid_forecasts',
    'forecast_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_fingrid_fcst_type
    ON fingrid_forecasts (forecast_type, forecast_time DESC);
CREATE INDEX IF NOT EXISTS idx_fingrid_fcst_generated
    ON fingrid_forecasts (generated_at DESC);

-- =====================================================
-- ML PREDICTIONS TABLE (Your Model's Output)
-- =====================================================

CREATE TABLE IF NOT EXISTS model_predictions (
    prediction_time TIMESTAMPTZ NOT NULL, -- When this prediction is for
    model_type VARCHAR(50) NOT NULL, -- prophet, xgboost, ensemble, etc.
    model_version VARCHAR(50), -- v1.0, 2024-11-08, etc.

    -- Price prediction (primary target)
    predicted_price_eur_mwh DOUBLE PRECISION NOT NULL,
    confidence_lower DOUBLE PRECISION,
    confidence_upper DOUBLE PRECISION,

    -- Optional: consumption prediction if you predict that too
    predicted_consumption_mw DOUBLE PRECISION,

    -- Metadata
    generated_at TIMESTAMPTZ NOT NULL, -- When this prediction was made
    features_hash VARCHAR(64), -- Hash of features used (for reproducibility)
    training_end_date TIMESTAMPTZ, -- Last date in training data
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (prediction_time, model_type, generated_at)
);

SELECT create_hypertable(
    'model_predictions',
    'prediction_time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

CREATE INDEX IF NOT EXISTS idx_predictions_model
    ON model_predictions (model_type, prediction_time DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_generated
    ON model_predictions (generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_predictions_version
    ON model_predictions (model_version, prediction_time DESC);

-- =====================================================
-- COMPARISON VIEWS
-- =====================================================

-- Materialized view for prediction comparison
-- Compare: Your predictions vs Fingrid forecasts vs Actual outcomes
CREATE MATERIALIZED VIEW IF NOT EXISTS prediction_comparison AS
WITH latest_model_pred AS (
    -- Get the latest prediction for each time from the most recent model run
    SELECT DISTINCT ON (prediction_time)
        prediction_time,
        predicted_price_eur_mwh,
        confidence_lower,
        confidence_upper,
        model_type,
        model_version,
        generated_at
    FROM model_predictions
    WHERE model_type = 'production' -- Default to production model
    ORDER BY prediction_time, generated_at DESC
),
latest_fingrid_fcst AS (
    -- Get Fingrid's latest price forecast (if they provide it)
    SELECT DISTINCT ON (forecast_time)
        forecast_time,
        value AS fingrid_price_forecast
    FROM fingrid_forecasts
    WHERE forecast_type = 'price'
    ORDER BY forecast_time, generated_at DESC
)
SELECT
    p.prediction_time AS time,

    -- Your model's prediction
    p.predicted_price_eur_mwh AS ml_prediction,
    p.confidence_lower AS ml_lower,
    p.confidence_upper AS ml_upper,
    p.model_type,
    p.model_version,

    -- Fingrid's forecast (if available)
    ff.fingrid_price_forecast,

    -- Actual outcome
    ep.price_eur_mwh AS actual_price,

    -- Error metrics (only when actual is available)
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

    -- Win/loss indicator (1 = ML wins, -1 = Fingrid wins, 0 = tie/unknown)
    CASE
        WHEN ep.price_eur_mwh IS NOT NULL AND ff.fingrid_price_forecast IS NOT NULL THEN
            CASE
                WHEN ABS(p.predicted_price_eur_mwh - ep.price_eur_mwh) < ABS(ff.fingrid_price_forecast - ep.price_eur_mwh) THEN 1
                WHEN ABS(p.predicted_price_eur_mwh - ep.price_eur_mwh) > ABS(ff.fingrid_price_forecast - ep.price_eur_mwh) THEN -1
                ELSE 0
            END
    END AS ml_vs_fingrid_winner,

    -- Timestamps
    p.generated_at AS prediction_generated_at,
    NOW() AS view_updated_at

FROM latest_model_pred p
LEFT JOIN latest_fingrid_fcst ff ON ff.forecast_time = p.prediction_time
LEFT JOIN electricity_prices ep
    ON ep.time = p.prediction_time
    AND ep.area = 'FI'
    AND ep.price_type = 'imbalance'
WHERE p.prediction_time < NOW() -- Only include predictions that should have outcomes
ORDER BY p.prediction_time DESC;

-- Create index on materialized view
CREATE INDEX IF NOT EXISTS idx_comparison_time
    ON prediction_comparison (time DESC);
CREATE INDEX IF NOT EXISTS idx_comparison_winner
    ON prediction_comparison (ml_vs_fingrid_winner);

-- Continuous aggregate for hourly performance metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_prediction_performance
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS hour,
    COUNT(*) AS num_predictions,

    -- ML model metrics
    AVG(ml_absolute_error) AS ml_avg_error,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ml_absolute_error) AS ml_median_error,
    AVG(ml_percentage_error) AS ml_avg_pct_error,

    -- Fingrid metrics
    AVG(fingrid_absolute_error) AS fingrid_avg_error,
    AVG(fingrid_percentage_error) AS fingrid_avg_pct_error,

    -- Win rate
    SUM(CASE WHEN ml_vs_fingrid_winner = 1 THEN 1 ELSE 0 END)::FLOAT /
        NULLIF(COUNT(*), 0) * 100 AS ml_win_rate_pct,

    -- Actual price statistics
    AVG(actual_price) AS avg_actual_price,
    MIN(actual_price) AS min_actual_price,
    MAX(actual_price) AS max_actual_price

FROM prediction_comparison
WHERE actual_price IS NOT NULL
GROUP BY hour
WITH NO DATA;

-- Refresh policy for continuous aggregate (refresh every hour)
SELECT add_continuous_aggregate_policy('hourly_prediction_performance',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- =====================================================
-- HELPER FUNCTIONS
-- =====================================================

-- Function to refresh comparison view
CREATE OR REPLACE FUNCTION refresh_prediction_comparison()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY prediction_comparison;
END;
$$ LANGUAGE plpgsql;

-- Function to get latest performance summary
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

-- =====================================================
-- DATA RETENTION POLICIES
-- =====================================================

-- Keep raw data for 2 years, then compress older chunks
SELECT add_retention_policy('electricity_consumption', INTERVAL '730 days', if_not_exists => TRUE);
SELECT add_retention_policy('electricity_prices', INTERVAL '730 days', if_not_exists => TRUE);
SELECT add_retention_policy('weather_observations', INTERVAL '730 days', if_not_exists => TRUE);

-- Keep forecasts for 90 days (we mainly care about comparing recent forecasts)
SELECT add_retention_policy('weather_forecasts', INTERVAL '90 days', if_not_exists => TRUE);
SELECT add_retention_policy('fingrid_forecasts', INTERVAL '90 days', if_not_exists => TRUE);

-- Keep predictions for 1 year for long-term analysis
SELECT add_retention_policy('model_predictions', INTERVAL '365 days', if_not_exists => TRUE);

-- Enable compression on older chunks (after 7 days)
ALTER TABLE electricity_consumption SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'time'
);
SELECT add_compression_policy('electricity_consumption', INTERVAL '7 days', if_not_exists => TRUE);

ALTER TABLE electricity_prices SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'area,price_type'
);
SELECT add_compression_policy('electricity_prices', INTERVAL '7 days', if_not_exists => TRUE);

-- =====================================================
-- SAMPLE QUERIES (for documentation)
-- =====================================================

-- Example 1: Get latest prediction performance for last 24 hours
-- SELECT * FROM get_latest_performance_summary(24);

-- Example 2: Find conditions where ML model wins
-- SELECT
--     EXTRACT(hour FROM time) AS hour_of_day,
--     AVG(ml_absolute_error) AS avg_ml_error,
--     AVG(fingrid_absolute_error) AS avg_fingrid_error,
--     COUNT(*) AS num_predictions,
--     SUM(CASE WHEN ml_vs_fingrid_winner = 1 THEN 1 ELSE 0 END)::FLOAT / COUNT(*) * 100 AS win_rate
-- FROM prediction_comparison
-- WHERE actual_price IS NOT NULL
--   AND fingrid_price_forecast IS NOT NULL
--   AND time >= NOW() - INTERVAL '30 days'
-- GROUP BY EXTRACT(hour FROM time)
-- ORDER BY win_rate DESC;

-- Example 3: Latest predictions with comparison
-- SELECT
--     time,
--     actual_price,
--     ml_prediction,
--     fingrid_price_forecast,
--     ml_absolute_error,
--     CASE ml_vs_fingrid_winner
--         WHEN 1 THEN 'ML Wins'
--         WHEN -1 THEN 'Fingrid Wins'
--         ELSE 'Tie/Unknown'
--     END AS winner
-- FROM prediction_comparison
-- ORDER BY time DESC
-- LIMIT 24;

COMMIT;
