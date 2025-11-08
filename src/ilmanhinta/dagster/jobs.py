"""Dagster jobs for data ingestion, training, and prediction."""

from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    asset,
    define_asset_job,
    sensor,
)

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.db.prediction_store import get_prediction_status, store_predictions
from ilmanhinta.logging import logfire
from ilmanhinta.ml.ensemble import EnsemblePredictor
from ilmanhinta.ml.model import ConsumptionModel
from ilmanhinta.ml.predict import Predictor
from ilmanhinta.ml.prophet_model import ProphetConsumptionModel
from ilmanhinta.processing.features import FeatureEngineer
from ilmanhinta.processing.joins import TemporalJoiner

# Data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


@asset
async def fingrid_consumption_data(context: AssetExecutionContext) -> Path:
    """Fetch all electricity data from Fingrid (consumption, production, wind, nuclear).

    This replaces the old single-dataset approach with a comprehensive fetch
    that ensures all features are available for both training and prediction.
    """
    logfire.info("Fetching all Fingrid electricity data (consumption, production, wind, nuclear)")

    async with FingridClient() as client:
        # Fetch last 30 days for training - ALL datasets in parallel
        all_data = await client.fetch_all_electricity_data(hours=24 * 30)

    # Merge all datasets into a single DataFrame with all features
    df = TemporalJoiner.merge_fingrid_datasets(all_data)

    # Save to parquet (for backup/historical)
    output_path = DATA_DIR / "raw" / f"fingrid_electricity_{datetime.now(UTC).date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    # Write to PostgreSQL + TimescaleDB
    db = PostgresClient()
    rows_inserted = db.insert_consumption(df)
    db.close()

    context.log.info(
        f"Saved {len(df)} electricity records to {output_path} and {rows_inserted} to PostgreSQL"
    )
    logfire.info(
        f"Fetched features: {df.columns} - ensuring feature parity for training & prediction"
    )
    return output_path


@asset
def fmi_weather_data(context: AssetExecutionContext) -> Path:
    """Fetch weather observations from FMI."""
    logfire.info("Fetching FMI weather data")

    client = FMIClient()

    # Fetch last 30 days
    data = client.fetch_realtime_observations(hours=24 * 30)

    # Convert to Polars DataFrame
    df = TemporalJoiner.fmi_to_polars(data.observations)

    # Add station_id column (required by database schema)
    import polars as pl

    df = df.with_columns(pl.lit(client.station_id).alias("station_id"))

    # Save to parquet (for backup/historical)
    output_path = DATA_DIR / "raw" / f"fmi_weather_{datetime.now(UTC).date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    # Write to PostgreSQL + TimescaleDB
    db = PostgresClient()
    rows_inserted = db.insert_weather(df, station_id=client.station_id)
    db.close()

    context.log.info(
        f"Saved {len(df)} weather records to {output_path} and {rows_inserted} to PostgreSQL"
    )
    return output_path


@asset(deps=[fingrid_consumption_data, fmi_weather_data])
def processed_training_data(context: AssetExecutionContext) -> Path:
    """Join and process data for model training using PostgreSQL + TimescaleDB."""
    logfire.info("Processing training data from PostgreSQL")
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


@asset(deps=[processed_training_data])
def trained_lightgbm_model(context: AssetExecutionContext) -> Path:
    """Train LightGBM model on processed data with engineered features."""
    logfire.info("Training LightGBM consumption prediction model")

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


@asset(deps=[processed_training_data])
def trained_prophet_model(context: AssetExecutionContext) -> Path:
    """Train Prophet model for seasonal forecasting."""
    logfire.info("Training Prophet seasonal forecasting model")
    from datetime import timedelta

    # Query raw data from PostgreSQL for Prophet (needs timestamp + target)
    db = PostgresClient()
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=30)

    # Get joined data for Prophet
    training_df = db.get_joined_training_data(start_time, end_time)
    db.close()

    # Prophet can use weather as regressors
    weather_features = ["temperature", "humidity", "wind_speed", "pressure"]

    # Train Prophet model
    model = ProphetConsumptionModel()
    metrics = model.train(training_df, weather_features=weather_features)

    context.log.info(f"Prophet training complete: {metrics}")

    # Save model
    output_path = DATA_DIR / "models" / f"prophet_{model.model_version}.pkl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    model.save(output_path)

    # Also save as "latest"
    latest_path = DATA_DIR / "models" / "prophet_latest.pkl"
    model.save(latest_path)

    context.log.info(f"Prophet model saved to {output_path}")
    return output_path


@asset(deps=[trained_lightgbm_model, trained_prophet_model])
def trained_ensemble_model(context: AssetExecutionContext) -> Path:
    """Create ensemble model combining Prophet + LightGBM."""
    logfire.info("Creating ensemble model")

    # Load both models
    lightgbm_path = DATA_DIR / "models" / "lightgbm_latest.pkl"
    prophet_path = DATA_DIR / "models" / "prophet_latest.pkl"

    if not lightgbm_path.exists() or not prophet_path.exists():
        raise ValueError("Required models not found for ensemble")

    ensemble = EnsemblePredictor.load_from_paths(
        prophet_path=prophet_path,
        lightgbm_path=lightgbm_path,
        prophet_weight=0.4,  # 40% Prophet, 60% LightGBM
    )

    # Save ensemble metadata
    output_path = DATA_DIR / "models" / "ensemble_latest.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    output_path.write_text(
        f"Ensemble Model\n"
        f"Prophet: {prophet_path}\n"
        f"LightGBM: {lightgbm_path}\n"
        f"Prophet weight: 0.4\n"
        f"LightGBM weight: 0.6\n"
    )

    context.log.info(f"Ensemble model created: {output_path}")
    return output_path


@asset(deps=[trained_lightgbm_model])
def hourly_forecast_predictions(context: AssetExecutionContext) -> int:
    """Generate the next 24-hour forecast and persist it for the API."""
    import asyncio

    logfire.info("Generating scheduled 24h forecast")

    model_path = DATA_DIR / "models" / "lightgbm_latest.pkl"
    if not model_path.exists():
        raise ValueError(f"LightGBM model not found at {model_path}")

    predictor = Predictor(model_path)

    # Run async function in event loop
    async def _generate_predictions():
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
ingest_job = define_asset_job(
    name="ingest_data",
    selection=[fingrid_consumption_data, fmi_weather_data],
)

train_job = define_asset_job(
    name="train_models",
    selection=[
        fingrid_consumption_data,
        fmi_weather_data,
        processed_training_data,
        trained_lightgbm_model,
        trained_prophet_model,
        trained_ensemble_model,
    ],
)

forecast_job = define_asset_job(
    name="forecast_next_24h",
    selection=[hourly_forecast_predictions],
)

# Bootstrap job - runs full pipeline from scratch
bootstrap_job = define_asset_job(
    name="bootstrap_system",
    selection=[
        fingrid_consumption_data,
        fmi_weather_data,
        processed_training_data,
        trained_lightgbm_model,
        trained_prophet_model,
        trained_ensemble_model,
        hourly_forecast_predictions,
    ],
)

# Define schedules
ingest_schedule = ScheduleDefinition(
    job=ingest_job,
    cron_schedule="0 * * * *",  # Every hour
)

train_schedule = ScheduleDefinition(
    job=train_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
)

forecast_schedule = ScheduleDefinition(
    job=forecast_job,
    cron_schedule="5 * * * *",  # Hourly, shortly after ingestion
)


# Sensor to detect missing predictions and auto-bootstrap
@sensor(
    job=bootstrap_job,
    minimum_interval_seconds=300,  # Check every 5 minutes
    default_status=DefaultSensorStatus.RUNNING,  # Auto-enabled on startup
)
def bootstrap_sensor(context: SensorEvaluationContext):
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
                run_key=f"bootstrap_{datetime.now(UTC).isoformat()}",
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
defs = Definitions(
    assets=[
        fingrid_consumption_data,
        fmi_weather_data,
        processed_training_data,
        trained_lightgbm_model,
        trained_prophet_model,
        trained_ensemble_model,
        hourly_forecast_predictions,
    ],
    jobs=[ingest_job, train_job, forecast_job, bootstrap_job],
    schedules=[ingest_schedule, train_schedule, forecast_schedule],
    sensors=[bootstrap_sensor],
)
