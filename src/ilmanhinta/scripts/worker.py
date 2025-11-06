"""Worker process to handle ingestion and scheduled retraining.

Runs as a separate Fly.io process group (no HTTP service). It:
  - Ensures a model exists at startup (bootstrap)
  - Ingests recent raw data hourly into /app/data/raw (for observability)
  - Retrains daily (02:00 UTC) and writes model to /app/data/models

The API watches the model file and hot-reloads when it changes.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from pathlib import Path

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.processing.joins import TemporalJoiner
from ilmanhinta.scripts.bootstrap_model import (
    ensure_model_exists,
    train_and_save_model_for_last_30_days,
)
from loguru import logger


async def _hourly_ingestion_loop() -> None:
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            logger.info("[worker] Starting hourly ingestion of recent data")

            async with FingridClient() as fg:
                cons = await fg.fetch_realtime_consumption(hours=2)
            fmi = FMIClient()
            obs = fmi.fetch_realtime_observations(hours=2)

            cdf = TemporalJoiner.fingrid_to_polars(cons)
            wdf = TemporalJoiner.fmi_to_polars(obs.observations)

            c_path = raw_dir / f"fingrid_consumption_{now:%Y%m%d_%H}.parquet"
            w_path = raw_dir / f"fmi_weather_{now:%Y%m%d_%H}.parquet"
            if not cdf.is_empty():
                cdf.write_parquet(c_path)
            if not wdf.is_empty():
                wdf.write_parquet(w_path)

            logger.info(
                f"[worker] Hourly ingestion saved to {c_path.name if cdf.height else '(no consumption)'} and "
                f"{w_path.name if wdf.height else '(no weather)'}"
            )
        except Exception as e:
            logger.error(f"[worker] Hourly ingestion error: {e}")
        await asyncio.sleep(3600)


async def _daily_retrain_loop() -> None:
    while True:
        try:
            now = datetime.utcnow()
            target = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if target <= now:
                target = target + timedelta(days=1)
            delay = (target - now).total_seconds()
            logger.info(f"[worker] Daily retrain scheduled in {int(delay)}s (at {target} UTC)")
            await asyncio.sleep(delay)

            logger.info("[worker] Starting daily retraining...")
            await train_and_save_model_for_last_30_days()
            logger.info("[worker] Daily retraining complete; model saved as latest")
        except Exception as e:
            logger.error(f"[worker] Daily retraining error: {e}")
            await asyncio.sleep(300)


async def _main_async() -> int:
    try:
        # Ensure a model exists before loops.
        await ensure_model_exists()

        # Start loops concurrently.
        await asyncio.gather(_hourly_ingestion_loop(), _daily_retrain_loop())
        return 0
    except Exception as e:
        logger.error(f"[worker] Fatal error: {e}")
        return 1


def main() -> None:
    raise SystemExit(asyncio.run(_main_async()))


if __name__ == "__main__":
    main()
