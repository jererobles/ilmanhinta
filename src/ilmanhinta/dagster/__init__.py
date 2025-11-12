"""Dagster orchestration for ETL pipeline.

Unified definitions combining:
1. Consumption Prediction Pipeline (LightGBM)
2. Price Prediction Pipeline
"""

from dagster import Definitions

from ilmanhinta.dagster import consumption_pipeline as consumption
from ilmanhinta.dagster import price_pipeline as price

# Consumption pipeline components
CONSUMPTION_ASSETS = [
    consumption.consumption_collect_power_actuals,
    consumption.consumption_collect_weather_observations,
    consumption.consumption_build_training_dataset,
    consumption.consumption_train_lightgbm_model,
    consumption.consumption_generate_hourly_predictions,
    consumption.consumption_short_interval_backfill,
]
CONSUMPTION_JOBS = [
    consumption.consumption_ingest_job,
    consumption.consumption_train_job,
    consumption.consumption_forecast_job,
    consumption.consumption_bootstrap_job,
    consumption.consumption_short_interval_job,
]
CONSUMPTION_SCHEDULES = [
    consumption.consumption_ingest_schedule,
    consumption.consumption_train_schedule,
    consumption.consumption_forecast_schedule,
]
CONSUMPTION_SENSORS = [consumption.consumption_bootstrap_sensor]

# Price pipeline components
PRICE_ASSETS = [
    price.price_backfill_recent_history,
    price.price_collect_actual_prices,
    price.price_collect_power_forecasts,
    price.price_collect_weather_forecasts,
    price.price_generate_historical_predictions,
    price.price_generate_predictions,
    price.price_refresh_prediction_views,
    price.price_retrain_model,
    price.price_generate_performance_report,
]
PRICE_JOBS = [
    price.price_hourly_prediction_job,
    price.price_daily_analysis_job,
    price.price_weekly_retraining_job,
    price.price_bootstrap_job,
]
PRICE_SCHEDULES = [
    price.price_hourly_schedule,
    price.price_daily_schedule,
    price.price_weekly_schedule,
]
PRICE_SENSORS = [price.price_bootstrap_sensor]

defs = Definitions(
    assets=[*CONSUMPTION_ASSETS, *PRICE_ASSETS],
    jobs=[*CONSUMPTION_JOBS, *PRICE_JOBS],
    schedules=[*CONSUMPTION_SCHEDULES, *PRICE_SCHEDULES],
    sensors=[*CONSUMPTION_SENSORS, *PRICE_SENSORS],
)

__all__ = ["defs"]
