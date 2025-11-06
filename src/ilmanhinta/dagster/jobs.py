"""Dagster jobs for data ingestion, training, and prediction."""

from datetime import datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
)

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.db import DuckDBClient
from ilmanhinta.logging import logfire
from ilmanhinta.ml.ensemble import EnsemblePredictor
from ilmanhinta.ml.model import ConsumptionModel
from ilmanhinta.ml.prophet_model import ProphetConsumptionModel
from ilmanhinta.processing.features import FeatureEngineer
from ilmanhinta.processing.joins import TemporalJoiner

# Data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


@asset
async def fingrid_consumption_data(context: AssetExecutionContext) -> Path:
    """Fetch electricity consumption data from Fingrid."""
    logfire.info("Fetching Fingrid consumption data")

    async with FingridClient() as client:
        # Fetch last 30 days for training
        data = await client.fetch_realtime_consumption(hours=24 * 30)

    # Convert to Polars DataFrame
    df = TemporalJoiner.fingrid_to_polars(data)

    # Save to parquet (for backup/historical)
    output_path = DATA_DIR / "raw" / f"fingrid_consumption_{datetime.utcnow().date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    # Also write to DuckDB for OLAP queries
    db = DuckDBClient()
    rows_inserted = db.insert_consumption(df)
    db.close()

    context.log.info(
        f"Saved {len(df)} consumption records to {output_path} and {rows_inserted} to DuckDB"
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

    # Save to parquet (for backup/historical)
    output_path = DATA_DIR / "raw" / f"fmi_weather_{datetime.utcnow().date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)

    # Also write to DuckDB for OLAP queries
    db = DuckDBClient()
    rows_inserted = db.insert_weather(df, data_type="observation")
    db.close()

    context.log.info(
        f"Saved {len(df)} weather records to {output_path} and {rows_inserted} to DuckDB"
    )
    return output_path


@asset(deps=[fingrid_consumption_data, fmi_weather_data])
def processed_training_data(context: AssetExecutionContext) -> Path:
    """Join and process data for model training using DuckDB."""
    logfire.info("Processing training data from DuckDB")
    from datetime import timedelta

    # Query last 30 days from DuckDB (replaces manual Parquet scanning)
    db = DuckDBClient()
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(days=30)

    # DuckDB does the temporal join for us (replaces TemporalJoiner.temporal_join)
    joined_df = db.get_joined_training_data(start_time, end_time)

    # Align to hourly (still needed for consistent intervals)
    aligned_df = TemporalJoiner.align_to_hourly(joined_df)

    # Create features
    features_df = FeatureEngineer.create_all_features(aligned_df)

    db.close()

    # Save processed data
    output_path = DATA_DIR / "processed" / f"training_data_{datetime.utcnow().date()}.parquet"
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
    import polars as pl
    from datetime import timedelta

    # Query raw data from DuckDB for Prophet (needs timestamp + target)
    db = DuckDBClient()
    end_time = datetime.utcnow()
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

# Define schedules
ingest_schedule = ScheduleDefinition(
    job=ingest_job,
    cron_schedule="0 * * * *",  # Every hour
)

train_schedule = ScheduleDefinition(
    job=train_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
)

# Definitions
defs = Definitions(
    assets=[
        fingrid_consumption_data,
        fmi_weather_data,
        processed_training_data,
        trained_lightgbm_model,
        trained_prophet_model,
        trained_ensemble_model,
    ],
    jobs=[ingest_job, train_job],
    schedules=[ingest_schedule, train_schedule],
)
