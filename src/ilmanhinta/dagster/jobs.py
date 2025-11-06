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
from loguru import logger

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.ml.model import ConsumptionModel
from ilmanhinta.processing.features import FeatureEngineer
from ilmanhinta.processing.joins import TemporalJoiner

# Data directory
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)


@asset
async def fingrid_consumption_data(context: AssetExecutionContext) -> Path:
    """Fetch electricity consumption data from Fingrid."""
    logger.info("Fetching Fingrid consumption data")

    async with FingridClient() as client:
        # Fetch last 30 days for training
        data = await client.fetch_realtime_consumption(hours=24 * 30)

    # Save to parquet
    df = TemporalJoiner.fingrid_to_polars(data)
    output_path = DATA_DIR / "raw" / f"fingrid_consumption_{datetime.utcnow().date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(output_path)

    context.log.info(f"Saved {len(df)} consumption records to {output_path}")
    return output_path


@asset
def fmi_weather_data(context: AssetExecutionContext) -> Path:
    """Fetch weather observations from FMI."""
    logger.info("Fetching FMI weather data")

    client = FMIClient()

    # Fetch last 30 days
    data = client.fetch_realtime_observations(hours=24 * 30)

    # Save to parquet
    df = TemporalJoiner.fmi_to_polars(data.observations)
    output_path = DATA_DIR / "raw" / f"fmi_weather_{datetime.utcnow().date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.write_parquet(output_path)

    context.log.info(f"Saved {len(df)} weather records to {output_path}")
    return output_path


@asset(deps=[fingrid_consumption_data, fmi_weather_data])
def processed_training_data(context: AssetExecutionContext) -> Path:
    """Join and process data for model training."""
    logger.info("Processing training data")
    import polars as pl

    # Load latest raw data
    consumption_files = sorted((DATA_DIR / "raw").glob("fingrid_consumption_*.parquet"))
    weather_files = sorted((DATA_DIR / "raw").glob("fmi_weather_*.parquet"))

    if not consumption_files or not weather_files:
        raise ValueError("No raw data files found")

    # Read and combine latest consumption files
    consumption_frames = [pl.read_parquet(file) for file in consumption_files[-7:]]
    consumption_df = pl.concat(consumption_frames) if consumption_frames else pl.DataFrame()

    # Read and combine latest weather files
    weather_frames = [pl.read_parquet(file) for file in weather_files[-7:]]
    weather_df = pl.concat(weather_frames) if weather_frames else pl.DataFrame()

    # Temporal join
    joined_df = TemporalJoiner.temporal_join(consumption_df, weather_df)

    # Align to hourly
    aligned_df = TemporalJoiner.align_to_hourly(joined_df)

    # Create features
    features_df = FeatureEngineer.create_all_features(aligned_df)

    # Save processed data
    output_path = DATA_DIR / "processed" / f"training_data_{datetime.utcnow().date()}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    features_df.write_parquet(output_path)

    context.log.info(f"Saved {len(features_df)} processed records to {output_path}")
    return output_path


@asset(deps=[processed_training_data])
def trained_model(context: AssetExecutionContext) -> Path:
    """Train LightGBM model on processed data."""
    logger.info("Training consumption prediction model")

    # Load latest processed data
    processed_files = sorted((DATA_DIR / "processed").glob("training_data_*.parquet"))

    if not processed_files:
        raise ValueError("No processed data files found")

    import polars as pl

    training_df = pl.read_parquet(processed_files[-1])

    # Train model
    model = ConsumptionModel()
    metrics = model.train(training_df)

    context.log.info(f"Model training complete: {metrics}")

    # Save model
    output_path = DATA_DIR / "models" / f"model_{model.model_version}.pkl"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    model.save(output_path)

    # Also save as "latest"
    latest_path = DATA_DIR / "models" / "model_latest.pkl"
    model.save(latest_path)

    context.log.info(f"Model saved to {output_path}")
    return output_path


# Define jobs
ingest_job = define_asset_job(
    name="ingest_data",
    selection=[fingrid_consumption_data, fmi_weather_data],
)

train_job = define_asset_job(
    name="train_model",
    selection=[fingrid_consumption_data, fmi_weather_data, processed_training_data, trained_model],
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
        trained_model,
    ],
    jobs=[ingest_job, train_job],
    schedules=[ingest_schedule, train_schedule],
)
