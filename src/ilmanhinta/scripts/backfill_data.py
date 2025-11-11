"""On-demand backfill for Fingrid/FMI datasets used by Ilmanhinta."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import polars as pl

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.logging import get_logger
from ilmanhinta.models.fingrid import FingridDataPoint, FingridDatasets
from ilmanhinta.processing.joins import TemporalJoiner

DEFAULT_HOURS = 24 * 30  # 1 month
DEFAULT_CHUNK_HOURS = 240  # keep Fingrid requests <10k data points
DEFAULT_PAGE_SIZE = 10_000
DEFAULT_PRICE_BUFFER_HOURS = 168  # lag features need trailing week
TARGET_CHOICES: tuple[str, ...] = ("actuals", "prices", "forecasts", "weather")
logger = get_logger(__name__)


@dataclass(frozen=True)
class ForecastSource:
    """Metadata required to backfill a Fingrid forecast dataset."""

    forecast_type: str
    dataset_id: int
    unit: str
    update_frequency: str


FORECAST_SOURCES: dict[str, ForecastSource] = {
    "consumption": ForecastSource(
        forecast_type="consumption",
        dataset_id=FingridDatasets.FORECAST_CONSUMPTION_RT,
        unit="MW",
        update_frequency="15min",
    ),
    "production": ForecastSource(
        forecast_type="production",
        dataset_id=FingridDatasets.FORECAST_PRODUCTION,
        unit="MW",
        update_frequency="15min",
    ),
    "wind": ForecastSource(
        forecast_type="wind",
        dataset_id=FingridDatasets.FORECAST_WIND,
        unit="MW",
        update_frequency="15min",
    ),
}

ACTUAL_DATASETS: dict[str, int] = {
    "consumption": FingridDatasets.CONSUMPTION,
    "production": FingridDatasets.PRODUCTION,
    "wind": FingridDatasets.WIND_PRODUCTION,
    "nuclear": FingridDatasets.NUCLEAR_PRODUCTION,
    "net_import": FingridDatasets.NET_IMPORT,
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Fingrid/FMI tables for historical training and evaluation."
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=DEFAULT_HOURS,
        help=f"How many hours to backfill (default: {DEFAULT_HOURS}).",
    )
    parser.add_argument(
        "--chunk-hours",
        type=int,
        default=DEFAULT_CHUNK_HOURS,
        help=f"Chunk size for Fingrid API requests (default: {DEFAULT_CHUNK_HOURS}).",
    )
    parser.add_argument(
        "--price-buffer-hours",
        type=int,
        default=DEFAULT_PRICE_BUFFER_HOURS,
        help=(
            "Extra hours to fetch before the main window for prices "
            "to cover lag features (default: 168)."
        ),
    )
    parser.add_argument(
        "--targets",
        nargs="+",
        choices=TARGET_CHOICES,
        default=list(TARGET_CHOICES),
        help="Subset of data domains to backfill (default: all).",
    )
    parser.add_argument(
        "--forecast-types",
        nargs="+",
        choices=sorted(FORECAST_SOURCES.keys()),
        default=list(FORECAST_SOURCES.keys()),
        help="Fingrid forecast types to backfill (default: consumption production wind).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Page size used for Fingrid API calls (default: 10000).",
    )
    parser.add_argument(
        "--station-id",
        type=str,
        default=None,
        help="Override FMI station id for weather backfill (defaults to settings.fmi_station_id).",
    )
    return parser.parse_args()


def _chunk_ranges(
    start_time: datetime, end_time: datetime, chunk_hours: int
) -> Iterable[tuple[datetime, datetime]]:
    """Yield inclusive-exclusive intervals covering the requested window."""
    if chunk_hours <= 0:
        raise ValueError("chunk_hours must be positive")

    cursor = start_time
    delta = timedelta(hours=chunk_hours)
    while cursor < end_time:
        chunk_end = min(cursor + delta, end_time)
        yield cursor, chunk_end
        cursor = chunk_end


async def _fetch_dataset_points(
    client: FingridClient,
    dataset_id: int,
    start_time: datetime,
    end_time: datetime,
    *,
    chunk_hours: int,
    page_size: int,
    label: str,
) -> list[FingridDataPoint]:
    """Fetch a Fingrid dataset in manageable chunks to avoid API hard limits."""
    points: list[FingridDataPoint] = []
    for chunk_start, chunk_end in _chunk_ranges(start_time, end_time, chunk_hours):
        logger.info(
            f"Fetching {label} dataset {dataset_id} "
            f"from {chunk_start.isoformat()} to {chunk_end.isoformat()}"
        )
        data = await client.fetch_data(
            dataset_id=dataset_id,
            start_time=chunk_start,
            end_time=chunk_end,
            page_size=page_size,
        )
        points.extend(data)

        # Gentle pause between chunks to dodge 429s
        if chunk_end < end_time:
            await asyncio.sleep(0.25)

    points.sort(key=lambda p: p.start_time)
    logger.info(f"Fetched {len(points)} points for {label}")
    return points


async def _backfill_actuals(
    client: FingridClient,
    db: PostgresClient,
    start_time: datetime,
    end_time: datetime,
    chunk_hours: int,
    page_size: int,
) -> int:
    """Backfill electricity actuals (consumption/production/wind/etc.)."""
    dataset_map: dict[str, list[FingridDataPoint]] = {}

    for key, dataset_id in ACTUAL_DATASETS.items():
        points = await _fetch_dataset_points(
            client,
            dataset_id,
            start_time,
            end_time,
            chunk_hours=chunk_hours,
            page_size=page_size,
            label=f"{key} actuals",
        )
        if points:
            dataset_map[key] = points
        else:
            logger.warning(f"No {key} data returned for requested window")

    if not dataset_map:
        logger.warning("Skipping consumption insert: no actual datasets fetched")
        return 0

    df = TemporalJoiner.merge_fingrid_datasets(dataset_map)
    if df.is_empty():
        logger.warning("Merged actuals dataframe is empty")
        return 0

    inserted = db.insert_consumption(df)
    logger.info(f"Inserted {inserted} merged actual rows")
    return inserted


async def _backfill_prices(
    client: FingridClient,
    db: PostgresClient,
    start_time: datetime,
    end_time: datetime,
    *,
    chunk_hours: int,
    page_size: int,
    buffer_hours: int,
) -> int:
    """Backfill imbalance prices with extra history for lag features."""
    if buffer_hours < 0:
        raise ValueError("price buffer must be non-negative")

    price_start = start_time - timedelta(hours=buffer_hours)
    points = await _fetch_dataset_points(
        client,
        FingridDatasets.PRICE_IMBALANCE,
        price_start,
        end_time,
        chunk_hours=chunk_hours,
        page_size=page_size,
        label="imbalance prices",
    )

    if not points:
        logger.warning("No price data returned for requested window")
        return 0

    df = pl.DataFrame(
        {
            "time": [point.start_time for point in points],
            "price_eur_mwh": [point.value for point in points],
        }
    )
    inserted = db.insert_prices(df, area="FI", price_type="imbalance", source="fingrid")
    logger.info(f"Inserted {inserted} price rows ({buffer_hours} buffer hrs)")
    return inserted


async def _backfill_forecasts(
    client: FingridClient,
    db: PostgresClient,
    start_time: datetime,
    end_time: datetime,
    *,
    chunk_hours: int,
    page_size: int,
    forecast_types: list[str],
) -> int:
    """Backfill Fingrid official forecasts used as ML features."""
    generation_timestamp = datetime.now(UTC)
    total = 0

    for forecast_type in forecast_types:
        source = FORECAST_SOURCES[forecast_type]
        points = await _fetch_dataset_points(
            client,
            source.dataset_id,
            start_time,
            end_time,
            chunk_hours=chunk_hours,
            page_size=page_size,
            label=f"{forecast_type} forecasts",
        )

        if not points:
            logger.warning(f"No {forecast_type} forecasts returned")
            continue

        df = pl.DataFrame(
            {
                "forecast_time": [point.start_time for point in points],
                "value": [point.value for point in points],
            }
        )
        inserted = db.insert_fingrid_forecast(
            df,
            forecast_type=source.forecast_type,
            unit=source.unit,
            update_frequency=source.update_frequency,
            generated_at=generation_timestamp,
        )
        logger.info(f"Inserted {inserted} {forecast_type} forecast rows")
        total += inserted

    return total


async def _backfill_weather(
    db: PostgresClient,
    start_time: datetime,
    end_time: datetime,
    *,
    station_id: str | None,
) -> int:
    """Backfill FMI weather observations (actuals)."""
    client = FMIClient(station_id=station_id)
    logger.info(
        f"Fetching FMI observations for station {client.station_id} "
        f"({start_time.isoformat()} -> {end_time.isoformat()})"
    )

    weather_data = await asyncio.to_thread(client.fetch_observations, start_time, end_time)
    if not weather_data.observations:
        logger.warning(f"No weather observations returned for {client.station_id}")
        return 0

    df = TemporalJoiner.fmi_to_polars(weather_data.observations)
    if df.is_empty():
        logger.warning("Weather dataframe empty after conversion")
        return 0

    df = df.with_columns(pl.lit(client.station_id).alias("station_id"))
    inserted = db.insert_weather(df, station_id=client.station_id)
    logger.info(f"Inserted {inserted} weather rows for station {client.station_id}")
    return inserted


async def run_backfill() -> None:
    args = _parse_args()
    if args.hours <= 0:
        raise ValueError("hours must be positive")
    if args.chunk_hours <= 0:
        raise ValueError("chunk-hours must be positive")
    if args.price_buffer_hours < 0:
        raise ValueError("price-buffer-hours must be non-negative")

    targets = list(dict.fromkeys(args.targets))  # preserve CLI order, drop duplicates

    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=args.hours)
    logger.info(
        f"Backfilling targets {targets} "
        f"for window {start_time.isoformat()} -> {end_time.isoformat()}"
    )

    summary: dict[str, int] = {}
    db = PostgresClient()

    try:
        fingrid_targets = {"actuals", "prices", "forecasts"}
        need_fingrid = any(target in fingrid_targets for target in targets)

        if need_fingrid:
            async with FingridClient() as fingrid_client:
                if "actuals" in targets:
                    summary["actuals"] = await _backfill_actuals(
                        fingrid_client,
                        db,
                        start_time,
                        end_time,
                        args.chunk_hours,
                        args.page_size,
                    )

                if "prices" in targets:
                    summary["prices"] = await _backfill_prices(
                        fingrid_client,
                        db,
                        start_time,
                        end_time,
                        chunk_hours=args.chunk_hours,
                        page_size=args.page_size,
                        buffer_hours=args.price_buffer_hours,
                    )

                if "forecasts" in targets:
                    summary["forecasts"] = await _backfill_forecasts(
                        fingrid_client,
                        db,
                        start_time,
                        end_time,
                        chunk_hours=args.chunk_hours,
                        page_size=args.page_size,
                        forecast_types=args.forecast_types,
                    )
        elif {"actuals", "prices", "forecasts"} & set(targets):
            raise RuntimeError("Fingrid targets requested but Fingrid client unavailable")

        if "weather" in targets:
            summary["weather"] = await _backfill_weather(
                db, start_time, end_time, station_id=args.station_id
            )
    finally:
        db.close()

    logger.info(f"Backfill complete: {summary}")


def main() -> None:
    asyncio.run(run_backfill())


if __name__ == "__main__":
    main()
