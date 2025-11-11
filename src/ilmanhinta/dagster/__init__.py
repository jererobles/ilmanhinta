"""Dagster orchestration for ETL pipeline.

Unified definitions combining:
1. Consumption Prediction Pipeline (LightGBM)
2. Price Prediction Pipeline
"""

from dagster import Definitions

# =====================================================
# Import Consumption Prediction Components
# =====================================================
from ilmanhinta.dagster.jobs import (
    bootstrap_job,
    # Sensors
    bootstrap_sensor,
    # Assets
    fingrid_consumption_data,
    fmi_weather_data,
    forecast_job,
    forecast_schedule,
    hourly_forecast_predictions,
    # Jobs
    ingest_job,
    # Schedules
    ingest_schedule,
    processed_training_data,
    train_job,
    train_schedule,
    trained_lightgbm_model,
)

# =====================================================
# Import Price Prediction Components
# =====================================================
from ilmanhinta.dagster.price_prediction_jobs import (
    # Assets
    collect_actual_prices,
    collect_fingrid_power_forecasts,
    collect_fmi_weather_forecasts,
    daily_analysis_job,
    generate_performance_report,
    generate_price_predictions,
    # Jobs
    hourly_prediction_job,
    refresh_comparison_views,
    retrain_price_model,
    weekly_retraining_job,
)
from ilmanhinta.dagster.price_prediction_jobs import (
    daily_schedule as price_daily_schedule,
)
from ilmanhinta.dagster.price_prediction_jobs import (
    # Schedules
    hourly_schedule as price_hourly_schedule,
)
from ilmanhinta.dagster.price_prediction_jobs import (
    weekly_schedule as price_weekly_schedule,
)

# =====================================================
# Unified Definitions
# =====================================================

defs = Definitions(
    assets=[
        # === Consumption Prediction Assets ===
        fingrid_consumption_data,
        fmi_weather_data,
        processed_training_data,
        trained_lightgbm_model,
        hourly_forecast_predictions,
        # === Price Prediction Assets ===
        collect_actual_prices,
        collect_fingrid_power_forecasts,
        collect_fmi_weather_forecasts,
        generate_price_predictions,
        refresh_comparison_views,
        retrain_price_model,
        generate_performance_report,
    ],
    jobs=[
        # === Consumption Prediction Jobs ===
        ingest_job,
        train_job,
        forecast_job,
        bootstrap_job,
        # === Price Prediction Jobs ===
        hourly_prediction_job,
        daily_analysis_job,
        weekly_retraining_job,
    ],
    schedules=[
        # === Consumption Prediction Schedules ===
        ingest_schedule,  # Hourly: Fetch consumption + weather data
        train_schedule,  # Daily 2 AM: Retrain consumption models
        forecast_schedule,  # Hourly: Generate consumption predictions
        # === Price Prediction Schedules ===
        price_hourly_schedule,  # Hourly: Collect forecasts + generate price predictions
        price_daily_schedule,  # Daily 1 AM: Refresh comparison views + performance reports
        price_weekly_schedule,  # Weekly Sun 2 AM: Retrain price model
    ],
    sensors=[
        bootstrap_sensor,  # Auto-bootstrap consumption predictions when missing
    ],
)

__all__ = ["defs"]
