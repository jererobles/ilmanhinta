-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Tables for electricity data
CREATE TABLE IF NOT EXISTS electricity_consumption (
    time TIMESTAMPTZ NOT NULL,
    consumption_mw DOUBLE PRECISION,
    production_mw DOUBLE PRECISION,
    net_import_mw DOUBLE PRECISION,
    UNIQUE(time)
);

CREATE TABLE IF NOT EXISTS electricity_prices (
    time TIMESTAMPTZ NOT NULL,
    price_eur_mwh DOUBLE PRECISION,
    area TEXT DEFAULT 'FI',
    UNIQUE(time, area)
);

-- Weather observations table
CREATE TABLE IF NOT EXISTS weather_observations (
    time TIMESTAMPTZ NOT NULL,
    station_id TEXT,
    temperature_c DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    cloud_cover INTEGER,
    humidity_percent DOUBLE PRECISION,
    pressure_hpa DOUBLE PRECISION
);

-- Predictions table
CREATE TABLE IF NOT EXISTS predictions (
    timestamp TIMESTAMPTZ NOT NULL,
    predicted_consumption_mw DOUBLE PRECISION NOT NULL,
    confidence_lower DOUBLE PRECISION NOT NULL,
    confidence_upper DOUBLE PRECISION NOT NULL,
    model_type TEXT NOT NULL,
    model_version TEXT,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, model_type)
);

-- Convert tables to TimescaleDB hypertables for time-series optimization
SELECT create_hypertable('electricity_consumption', 'time', if_not_exists => TRUE);
SELECT create_hypertable('electricity_prices', 'time', if_not_exists => TRUE);
SELECT create_hypertable('weather_observations', 'time', if_not_exists => TRUE);
SELECT create_hypertable('predictions', 'timestamp', if_not_exists => TRUE);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_predictions_generated ON predictions(generated_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_station ON weather_observations(station_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_prices_area ON electricity_prices(area, time DESC);

-- Create materialized views for common aggregations
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_consumption_stats AS
SELECT
    time_bucket('1 hour', time) AS hour,
    AVG(consumption_mw) AS avg_consumption,
    MAX(consumption_mw) AS max_consumption,
    MIN(consumption_mw) AS min_consumption
FROM electricity_consumption
GROUP BY hour
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_price_stats AS
SELECT
    time_bucket('1 day', time) AS day,
    area,
    AVG(price_eur_mwh) AS avg_price,
    MAX(price_eur_mwh) AS max_price,
    MIN(price_eur_mwh) AS min_price,
    STDDEV(price_eur_mwh) AS price_stddev
FROM electricity_prices
GROUP BY day, area
WITH DATA;

-- Create continuous aggregates (TimescaleDB feature for real-time materialized views)
CREATE MATERIALIZED VIEW IF NOT EXISTS consumption_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) AS bucket,
    AVG(consumption_mw) AS avg_consumption,
    SUM(consumption_mw) AS total_consumption,
    COUNT(*) AS sample_count
FROM electricity_consumption
GROUP BY bucket
WITH NO DATA;

-- Add refresh policies
SELECT add_continuous_aggregate_policy('consumption_1h',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
