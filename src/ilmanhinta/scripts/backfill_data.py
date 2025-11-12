"""Data ingestion/backfill service for Fingrid/FMI used by Ilmanhinta.

This module exposes a BackfillService class that centralizes fetching from
Fingrid/FMI lower-level clients and persisting into PostgreSQL via
PostgresClient. It can be used in two ways:
- As a standalone CLI for long-interval, one-off backfills
- As a library from Dagster for short-interval, on-demand scraping
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import polars as pl

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.logging import configure_observability, get_logger
from ilmanhinta.models.fingrid import FingridDataPoint, FingridDatasets
from ilmanhinta.processing.joins import TemporalJoiner

DEFAULT_HOURS = 24 * 30  # 1 month
DEFAULT_CHUNK_HOURS = 240  # keep Fingrid requests <10k data points
DEFAULT_PAGE_SIZE = 10_000
DEFAULT_PRICE_BUFFER_HOURS = 168  # lag features need trailing week
TARGET_CHOICES: tuple[str, ...] = ("actuals", "prices", "forecasts", "weather")
REQUESTS_PER_MINUTE = 10

configure_observability()
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
        "--requests-per-minute",
        type=int,
        default=REQUESTS_PER_MINUTE,
        help="Max Fingrid requests per minute for pacing (default: 10).",
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


class BackfillService:
    """High-level data ingestion/backfill orchestrator.

    Wraps Fingrid/FMI clients and handles persistence to PostgreSQL.
    """

    def __init__(
        self,
        *,
        db: PostgresClient | None = None,
        fingrid_client: FingridClient | None = None,
        fmi_client: FMIClient | None = None,
        chunk_hours: int = DEFAULT_CHUNK_HOURS,
        page_size: int = DEFAULT_PAGE_SIZE,
        price_buffer_hours: int = DEFAULT_PRICE_BUFFER_HOURS,
        requests_per_minute: int = REQUESTS_PER_MINUTE,
    ) -> None:
        self.db = db or PostgresClient()
        self._owns_db = db is None

        self.fingrid_client = fingrid_client
        self._owns_fg = fingrid_client is None
        self.fmi_client = fmi_client or FMIClient()

        self.chunk_hours = chunk_hours
        self.page_size = page_size
        self.price_buffer_hours = price_buffer_hours

        # pacing
        self._req_interval = 60 / max(1, requests_per_minute)

    async def __aenter__(self) -> BackfillService:
        if self.fingrid_client is None:
            self.fingrid_client = FingridClient()
        # Enter fingrid client context if we own it
        if self._owns_fg and hasattr(self.fingrid_client, "__aenter__"):
            await self.fingrid_client.__aenter__()
        return self

    async def __aexit__(self, *exc: object) -> None:
        # Close fingrid client if we created it
        if self._owns_fg and self.fingrid_client is not None:
            with contextlib.suppress(Exception):
                await self.fingrid_client.__aexit__(None, None, None)
        # Close DB if we created it
        if self._owns_db:
            with contextlib.suppress(Exception):
                self.db.close()

    async def _fetch_dataset_points(
        self,
        dataset_id: int,
        start_time: datetime,
        end_time: datetime,
        *,
        label: str,
    ) -> list[FingridDataPoint]:
        """Fetch a Fingrid dataset in manageable chunks to avoid API hard limits."""
        assert self.fingrid_client is not None, "Fingrid client not initialized"
        points: list[FingridDataPoint] = []
        for chunk_start, chunk_end in _chunk_ranges(start_time, end_time, self.chunk_hours):
            logger.info(
                f"Fetching {label} dataset {dataset_id} "
                f"from {chunk_start.isoformat()} to {chunk_end.isoformat()}"
            )
            request_start = asyncio.get_running_loop().time()
            data = await self.fingrid_client.fetch_data(
                dataset_id=dataset_id,
                start_time=chunk_start,
                end_time=chunk_end,
                page_size=self.page_size,
            )
            elapsed = asyncio.get_running_loop().time() - request_start
            remaining_quota = self._req_interval - elapsed
            points.extend(data)

            if chunk_end < end_time and remaining_quota > 0:
                await asyncio.sleep(remaining_quota)

        points.sort(key=lambda p: p.start_time)
        logger.info(f"Fetched {len(points)} points for {label}")
        return points

    async def ingest_actuals(
        self, start_time: datetime, end_time: datetime
    ) -> tuple[pl.DataFrame, int]:
        """Fetch and insert electricity actuals into DB; return merged DataFrame and rows inserted."""
        dataset_map: dict[str, list[FingridDataPoint]] = {}
        for key, dataset_id in ACTUAL_DATASETS.items():
            points = await self._fetch_dataset_points(
                dataset_id,
                start_time,
                end_time,
                label=f"{key} actuals",
            )
            if points:
                dataset_map[key] = points
            else:
                logger.warning(f"No {key} data returned for requested window")

        if not dataset_map:
            logger.warning("Skipping consumption insert: no actual datasets fetched")
            return pl.DataFrame(), 0

        df = TemporalJoiner.merge_fingrid_datasets(dataset_map)
        if df.is_empty():
            logger.warning("Merged actuals dataframe is empty")
            return df, 0

        inserted = self.db.insert_consumption(df)
        logger.info(f"Inserted {inserted} merged actual rows")
        return df, inserted

    async def ingest_prices(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        buffer_hours: int | None = None,
    ) -> tuple[pl.DataFrame, int]:
        """Fetch and insert imbalance prices; return DataFrame and rows inserted."""
        if buffer_hours is None:
            buffer_hours = self.price_buffer_hours
        if buffer_hours < 0:
            raise ValueError("price buffer must be non-negative")

        price_start = start_time - timedelta(hours=buffer_hours)
        points = await self._fetch_dataset_points(
            FingridDatasets.PRICE_IMBALANCE,
            price_start,
            end_time,
            label="imbalance prices",
        )

        if not points:
            logger.warning("No price data returned for requested window")
            return pl.DataFrame(), 0

        df = pl.DataFrame(
            {
                "time": [point.start_time for point in points],
                "price_eur_mwh": [point.value for point in points],
            }
        )
        inserted = self.db.insert_prices(df, area="FI", price_type="imbalance", source="fingrid")
        logger.info(f"Inserted {inserted} price rows ({buffer_hours} buffer hrs)")
        return df, inserted

    async def ingest_forecasts(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        forecast_types: list[str] | None = None,
    ) -> int:
        """Fetch and insert Fingrid forecasts; return total rows inserted."""
        generation_timestamp = datetime.now(UTC)
        total = 0

        types = list(forecast_types or FORECAST_SOURCES.keys())
        for forecast_type in types:
            source = FORECAST_SOURCES[forecast_type]
            points = await self._fetch_dataset_points(
                source.dataset_id,
                start_time,
                end_time,
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
            inserted = self.db.insert_fingrid_forecast(
                df,
                forecast_type=source.forecast_type,
                unit=source.unit,
                update_frequency=source.update_frequency,
                generated_at=generation_timestamp,
            )
            logger.info(f"Inserted {inserted} {forecast_type} forecast rows")
            total += inserted

        return total

    async def ingest_weather(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        station_id: str | None = None,
    ) -> tuple[pl.DataFrame, int]:
        """Fetch and insert FMI weather observations; return DataFrame and rows inserted."""
        client = self.fmi_client if station_id is None else FMIClient(station_id=station_id)
        logger.info(
            f"Fetching FMI observations for station {client.station_id} "
            f"({start_time.isoformat()} -> {end_time.isoformat()})"
        )

        weather_data = await asyncio.to_thread(client.fetch_observations, start_time, end_time)
        if not weather_data.observations:
            logger.warning(f"No weather observations returned for {client.station_id}")
            return pl.DataFrame(), 0

        df = TemporalJoiner.fmi_to_polars(weather_data.observations)
        if df.is_empty():
            logger.warning("Weather dataframe empty after conversion")
            return df, 0

        df = df.with_columns(pl.lit(client.station_id).alias("station_id"))
        inserted = self.db.insert_weather(df, station_id=client.station_id)
        logger.info(f"Inserted {inserted} weather rows for station {client.station_id}")
        return df, inserted

    async def ingest_weather_forecast(
        self,
        *,
        horizon_hours: int = 48,
        station_id: str | None = None,
        forecast_model: str = "harmonie",
    ) -> tuple[pl.DataFrame, int]:
        """Fetch and insert FMI weather forecasts; return DataFrame and rows inserted.

        Args:
            horizon_hours: How many hours ahead to fetch
            station_id: Optional station override (defaults to settings)
            forecast_model: Model name metadata for storage
        """
        client = self.fmi_client if station_id is None else FMIClient(station_id=station_id)
        weather_fcst = await asyncio.to_thread(client.fetch_forecast, hours=horizon_hours)

        if not weather_fcst.observations:
            logger.warning("No FMI weather forecasts available")
            return pl.DataFrame(), 0

        # Build DataFrame similar to existing usage in jobs
        records = [
            {
                "forecast_time": obs.timestamp,
                "temperature_c": obs.temperature,
                "wind_speed_ms": obs.wind_speed,
                "pressure_hpa": obs.pressure,
                "cloud_cover": obs.cloud_cover,
                "humidity_percent": obs.humidity,
            }
            for obs in weather_fcst.observations
            if obs.temperature is not None
        ]
        df = pl.DataFrame(records) if records else pl.DataFrame()
        if df.is_empty():
            logger.warning("Weather forecast dataframe is empty after conversion")
            return df, 0

        rows = self.db.insert_weather_forecast(
            df,
            station_id=client.station_id,
            forecast_model=forecast_model,
        )
        logger.info(f"Inserted {rows} weather forecast rows for station {client.station_id}")
        return df, rows


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

    async with BackfillService(
        chunk_hours=args.chunk_hours,
        page_size=args.page_size,
        price_buffer_hours=args.price_buffer_hours,
        requests_per_minute=args.requests_per_minute,
    ) as svc:
        if "actuals" in targets:
            _df, inserted = await svc.ingest_actuals(start_time, end_time)
            summary["actuals"] = inserted
        if "prices" in targets:
            _df, inserted = await svc.ingest_prices(start_time, end_time)
            summary["prices"] = inserted
        if "forecasts" in targets:
            inserted = await svc.ingest_forecasts(
                start_time, end_time, forecast_types=args.forecast_types
            )
            summary["forecasts"] = inserted
        if "weather" in targets:
            _df, inserted = await svc.ingest_weather(
                start_time, end_time, station_id=args.station_id
            )
            summary["weather"] = inserted

    logger.info(f"Backfill complete: {summary}")


def main() -> None:
    asyncio.run(run_backfill())


if __name__ == "__main__":
    main()
