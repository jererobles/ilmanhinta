"""Dagster jobs for data ingestion, training, and prediction."""

import asyncio
from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    asset,
    define_asset_job,
    sensor,
)

from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.db.prediction_store import get_prediction_status, store_predictions
from ilmanhinta.logging import get_logger
from ilmanhinta.ml.model import ConsumptionModel
from ilmanhinta.ml.predict import Predictor
from ilmanhinta.processing.features import FeatureEngineer
from ilmanhinta.processing.joins import TemporalJoiner
from ilmanhinta.scripts.backfill_data import BackfillService

# Data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)
logger = get_logger(__name__)


@asset(group_name="consumption_ingest")
async def consumption_collect_power_actuals(context: AssetExecutionContext) -> Path:
    """Fetch all electricity data from Fingrid (consumption, production, wind, nuclear).

    This replaces the old single-dataset approach with a comprehensive fetch
    that ensures all features are available for both training and prediction.
    """
    logger.info("Fetching all Fingrid electricity data (consumption, production, wind, nuclear)")

    from datetime import timedelta

    # Use BackfillService to fetch + insert actuals for the last 30 days
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=24 * 30)
    async with BackfillService() as svc:
        df, inserted_rows = await svc.ingest_actuals(start_time, end_time)

    # Save to parquet (for backup/historical)
    output_path = DATA_DIR / "raw" / f"fingrid_electricity_{datetime.now(UTC).date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    # Rows were already upserted by BackfillService
    rows_inserted = inserted_rows

    context.log.info(
        f"Saved {len(df)} electricity records to {output_path} and {rows_inserted} to PostgreSQL"
    )
    logger.info(
        f"Fetched features: {df.columns} - ensuring feature parity for training & prediction"
    )
    return output_path


@asset(group_name="consumption_backfill")
async def consumption_short_interval_backfill(context: AssetExecutionContext) -> dict[str, int]:
    """On-demand short-interval scrape of Fingrid/FMI into PostgreSQL.

    Uses BackfillService to fetch a small, recent window and persist it. Designed
    for manual triggering or high-frequency runs without heavy history.
    """
    from datetime import timedelta

    hours = 6  # short window; adjust as needed
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=hours)

    summary: dict[str, int] = {}
    async with BackfillService() as svc:
        _df, inserted = await svc.ingest_actuals(start_time, end_time)
        summary["actuals"] = inserted
        _df, inserted = await svc.ingest_weather(start_time, end_time)
        summary["weather"] = inserted

    context.log.info(f"Short-interval backfill complete: {summary}")
    return summary


@asset(group_name="consumption_ingest")
async def consumption_collect_weather_observations(context: AssetExecutionContext) -> Path:
    """Fetch weather observations from FMI."""
    logger.info("Fetching FMI weather data")

    # Fetch + insert last 30 days via BackfillService
    from datetime import timedelta

    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=24 * 30)
    async with BackfillService() as svc:
        df, rows_inserted = await svc.ingest_weather(start_time, end_time)

    # Save to parquet (for backup/historical)
    output_path = DATA_DIR / "raw" / f"fmi_weather_{datetime.now(UTC).date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    # Rows were already upserted by BackfillService

    context.log.info(
        f"Saved {len(df)} weather records to {output_path} and {rows_inserted} to PostgreSQL"
    )
    return output_path


@asset(
    group_name="consumption_processing",
    deps=[consumption_collect_power_actuals, consumption_collect_weather_observations],
)
def consumption_build_training_dataset(context: AssetExecutionContext) -> Path:
    """Join and process data for model training using PostgreSQL + TimescaleDB."""
    logger.info("Processing training data from PostgreSQL")
    from datetime import timedelta

    # Query last 30 days from PostgreSQL (uses LATERAL JOIN for temporal alignment)
    db = PostgresClient()
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=30)

    # Debug: Check what's in the database
    stats = db.get_stats()
    context.log.info(f"PostgreSQL stats before join: {stats}")

    # PostgreSQL LATERAL JOIN does the temporal join for us
    joined_df = db.get_joined_training_data(start_time, end_time)
    context.log.info(
        f"Joined {len(joined_df)} rows from PostgreSQL (time range: {start_time} to {end_time})"
    )

    # Align to hourly (still needed for consistent intervals)
    aligned_df = TemporalJoiner.align_to_hourly(joined_df)

    # Create features
    features_df = FeatureEngineer.create_all_features(aligned_df)

    db.close()

    # Save processed data
    output_path = DATA_DIR / "processed" / f"training_data_{datetime.now(UTC).date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    features_df.write_parquet(output_path)

    context.log.info(f"Saved {len(features_df)} processed records to {output_path}")
    return output_path


@asset(group_name="consumption_training", deps=[consumption_build_training_dataset])
def consumption_train_lightgbm_model(context: AssetExecutionContext) -> Path:
    """Train LightGBM model on processed data with engineered features."""
    logger.info("Training LightGBM consumption prediction model")

    # Load latest processed data
    processed_files = sorted((DATA_DIR / "processed").glob("training_data_*.parquet"))

    if not processed_files:
        raise ValueError("No processed data files found")

    import polars as pl

    training_df = pl.read_parquet(processed_files[-1])

    # Train model
    model = ConsumptionModel()
    metrics = model.train(training_df)

    context.log.info(f"LightGBM training complete: {metrics}")

    # Save model
    output_path = DATA_DIR / "models" / f"lightgbm_{model.model_version}.pkl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    model.save(output_path)

    # Also save as "latest"
    latest_path = DATA_DIR / "models" / "lightgbm_latest.pkl"
    model.save(latest_path)

    context.log.info(f"LightGBM model saved to {output_path}")
    return output_path


@asset(group_name="consumption_prediction", deps=[consumption_train_lightgbm_model])
def consumption_generate_hourly_predictions(context: AssetExecutionContext) -> int:
    """Generate the next 24-hour forecast and persist it for the API."""

    logger.info("Generating scheduled 24h forecast")

    model_path = DATA_DIR / "models" / "lightgbm_latest.pkl"
    if not model_path.exists():
        raise ValueError(f"LightGBM model not found at {model_path}")

    predictor = Predictor(model_path)

    # Run async function in event loop
    async def _generate_predictions() -> Any:
        try:
            return await predictor.predict_next_24h()
        finally:
            await predictor.close()

    predictions = asyncio.run(_generate_predictions())

    if not predictions:
        raise ValueError("Prediction pipeline returned no data")

    stored = store_predictions(predictions, model_type="lightgbm")
    context.log.info(f"Stored {stored} scheduled predictions")
    return stored


# Define jobs
consumption_ingest_job = define_asset_job(
    name="consumption_ingest_job",
    selection=[consumption_collect_power_actuals, consumption_collect_weather_observations],
)

consumption_train_job = define_asset_job(
    name="consumption_train_job",
    selection=[
        consumption_collect_power_actuals,
        consumption_collect_weather_observations,
        consumption_build_training_dataset,
        consumption_train_lightgbm_model,
    ],
)

consumption_forecast_job = define_asset_job(
    name="consumption_forecast_job",
    selection=[consumption_generate_hourly_predictions],
)

consumption_short_interval_job = define_asset_job(
    name="consumption_short_interval_job",
    selection=[consumption_short_interval_backfill],
)

# Bootstrap job - runs full pipeline from scratch
consumption_bootstrap_job = define_asset_job(
    name="consumption_bootstrap_job",
    selection=[
        consumption_collect_power_actuals,
        consumption_collect_weather_observations,
        consumption_build_training_dataset,
        consumption_train_lightgbm_model,
        consumption_generate_hourly_predictions,
    ],
)

# Define schedules
consumption_ingest_schedule = ScheduleDefinition(
    job=consumption_ingest_job,
    cron_schedule="0 * * * *",  # Every hour
)

consumption_train_schedule = ScheduleDefinition(
    job=consumption_train_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
)

consumption_forecast_schedule = ScheduleDefinition(
    job=consumption_forecast_job,
    cron_schedule="5 * * * *",  # Hourly, shortly after ingestion
)


# Sensor to detect missing predictions and auto-bootstrap
@sensor(
    job=consumption_bootstrap_job,
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.RUNNING,  # Auto-enabled on startup
)
def consumption_bootstrap_sensor(
    context: SensorEvaluationContext,
) -> Generator[RunRequest, None, None]:
    """Automatically bootstrap the system when predictions are missing.

    This sensor checks if predictions exist and triggers the bootstrap job
    if none are found. It's useful for:
    - First-time setup
    - After database resets
    - Recovery from data loss
    """
    try:
        status = get_prediction_status(model_type="lightgbm")

        if not status["available"]:
            context.log.warning("No predictions found - triggering bootstrap job")
            yield RunRequest(
                run_key=f"consumption_bootstrap_{datetime.now(UTC).isoformat()}",
                tags={
                    "reason": "missing_predictions",
                    "auto_triggered": "true",
                },
            )
        else:
            context.log.info(
                f"Predictions available (latest: {status['latest_timestamp']}, "
                f"version: {status['model_version']})"
            )
    except Exception as e:
        context.log.error(f"Error checking prediction status: {e}")
        # Don't trigger on errors to avoid infinite loops


# Definitions
"""Note: Unified Definitions are composed in ilmanhinta.dagster.__init__.py.

This module intentionally does not export a module-level `defs` to avoid
multiple Dagster entry points. Import assets/jobs into the aggregator instead.
"""
