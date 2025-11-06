"""Bootstrap script to ensure a trained model exists on disk.

This is designed to run in Fly.io's release_command or locally.
It will:
  - Check for `data/models/model_latest.pkl`
  - If missing, fetch the last 30 days of data, train a model, and save it

Idempotent and safe to run multiple times.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.ml.model import ConsumptionModel
from ilmanhinta.processing.features import FeatureEngineer
from ilmanhinta.processing.joins import TemporalJoiner
from loguru import logger

MODEL_LATEST_PATH = Path("data/models/model_latest.pkl")


async def _train_model_last_30_days() -> Path:
    """Fetch data for ~30 days, create features, train and persist the model."""
    logger.info("Bootstrapping: fetching last 30 days of data for training")

    # 1) Fetch data
    async with FingridClient() as fg:
        consumption = await fg.fetch_realtime_consumption(hours=24 * 30)

    fmi = FMIClient()
    observations = fmi.fetch_realtime_observations(hours=24 * 30)

    # 2) Convert to Polars
    consumption_df = TemporalJoiner.fingrid_to_polars(consumption)
    weather_df = TemporalJoiner.fmi_to_polars(observations.observations)

    if consumption_df.is_empty() or weather_df.is_empty():
        raise RuntimeError("Insufficient data to bootstrap model (empty consumption or weather)")

    # 3) Join + align hourly + features
    joined_df = TemporalJoiner.temporal_join(consumption_df, weather_df)
    hourly_df = TemporalJoiner.align_to_hourly(joined_df)
    features_df = FeatureEngineer.create_all_features(hourly_df)

    if features_df.is_empty():
        raise RuntimeError("No training rows after feature engineering; aborting bootstrap")

    # 4) Train
    model = ConsumptionModel()
    metrics = model.train(features_df)
    logger.info(f"Bootstrap training complete. Metrics: {metrics}")

    # 5) Persist
    timestamped = Path("data/models") / f"model_{model.model_version}.pkl"
    timestamped.parent.mkdir(parents=True, exist_ok=True)
    model.save(timestamped)
    model.save(MODEL_LATEST_PATH)

    logger.info(f"Model saved to {timestamped} and {MODEL_LATEST_PATH}")
    return MODEL_LATEST_PATH


async def ensure_model_exists() -> Path:
    """Ensure latest model exists, training if needed."""
    if MODEL_LATEST_PATH.exists():
        logger.info(f"Model already present at {MODEL_LATEST_PATH}")
        return MODEL_LATEST_PATH

    # Create base directories early (esp. for mounted volumes)
    MODEL_LATEST_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger.warning(f"Model missing at {MODEL_LATEST_PATH}; starting bootstrap training")
    return await _train_model_last_30_days()


async def train_and_save_model_for_last_30_days() -> Path:
    """Always train a fresh model from the last 30 days and save it as latest.

    Returns the path to the saved latest model.
    """
    return await _train_model_last_30_days()


def _human_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


async def _main_async() -> int:
    try:
        logger.info(f"Bootstrap start {_human_now()}")
        path = await ensure_model_exists()
        logger.info(f"Bootstrap done {_human_now()} — model at {path}")
        return 0
    except Exception as e:
        logger.error(f"Bootstrap failed: {e}")
        return 1


def main() -> None:
    raise SystemExit(asyncio.run(_main_async()))


if __name__ == "__main__":
    main()
