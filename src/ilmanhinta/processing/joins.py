"""Temporal joins between weather and electricity data using Polars."""

from datetime import timedelta

import polars as pl

from ilmanhinta.logging import logfire
from ilmanhinta.models.fingrid import FingridDataPoint
from ilmanhinta.models.fmi import FMIObservation


class TemporalJoiner:
    """Handle temporal alignment and joining of weather and electricity data."""

    @staticmethod
    def fingrid_to_polars(data: list[FingridDataPoint]) -> pl.DataFrame:
        """Convert Fingrid data to Polars DataFrame."""
        if not data:
            return pl.DataFrame()

        records = [
            {
                "timestamp": point.start_time,
                "consumption_mw": point.value,
            }
            for point in data
        ]

        return pl.DataFrame(records).with_columns(
            pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
        )

    @staticmethod
    def fmi_to_polars(observations: list[FMIObservation]) -> pl.DataFrame:
        """Convert FMI observations to Polars DataFrame."""
        if not observations:
            return pl.DataFrame()

        records = [
            {
                "timestamp": obs.timestamp,
                "temperature": obs.temperature,
                "humidity": obs.humidity,
                "wind_speed": obs.wind_speed,
                "wind_direction": obs.wind_direction,
                "pressure": obs.pressure,
                "precipitation": obs.precipitation,
                "cloud_cover": obs.cloud_cover,
            }
            for obs in observations
        ]

        return pl.DataFrame(records).with_columns(
            pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
        )

    @staticmethod
    def temporal_join(
        consumption_df: pl.DataFrame,
        weather_df: pl.DataFrame,
        tolerance: timedelta = timedelta(minutes=30),
    ) -> pl.DataFrame:
        """
        Perform temporal join between consumption and weather data.

        Matches each consumption timestamp to the nearest weather observation
        within the specified tolerance window.
        """
        if consumption_df.is_empty() or weather_df.is_empty():
            logfire.warning("Empty dataframe provided for temporal join")
            return pl.DataFrame()

        logfire.info(
            f"Performing temporal join: {len(consumption_df)} consumption records, "
            f"{len(weather_df)} weather records"
        )

        # Sort both dataframes by timestamp
        consumption_df = consumption_df.sort("timestamp")
        weather_df = weather_df.sort("timestamp")

        # Use join_asof for temporal join (nearest match within tolerance)
        tolerance_seconds = int(tolerance.total_seconds())
        joined = consumption_df.join_asof(
            weather_df,
            on="timestamp",
            strategy="nearest",
            tolerance=str(tolerance_seconds) + "s",
        )

        # Drop rows with missing weather data (outside tolerance)
        initial_count = len(joined)
        joined = joined.drop_nulls(subset=["temperature"])
        final_count = len(joined)

        if initial_count > final_count:
            logfire.warning(
                f"Dropped {initial_count - final_count} rows due to missing weather data"
            )

        logfire.info(f"Temporal join completed: {len(joined)} matched records")

        return joined

    @staticmethod
    def align_to_hourly(df: pl.DataFrame) -> pl.DataFrame:
        """
        Align data to hourly resolution by flooring timestamps.

        FMI typically provides hourly data, Fingrid provides 3-minute data.
        This aligns everything to hourly intervals.
        """
        if df.is_empty():
            return df

        # Floor timestamps to the start of the hour to avoid spilling into the next hour
        df = df.with_columns(pl.col("timestamp").dt.truncate("1h").alias("timestamp_hourly"))

        # Group by hourly timestamp and aggregate
        numeric_cols = [
            col
            for col in df.columns
            if col not in ["timestamp", "timestamp_hourly"] and df[col].dtype.is_numeric()
        ]

        agg_exprs = [pl.col(col).mean().alias(col) for col in numeric_cols]

        aligned = (
            df.group_by("timestamp_hourly")
            .agg(agg_exprs)
            .sort("timestamp_hourly")
            .rename({"timestamp_hourly": "timestamp"})
        )

        logfire.info(f"Aligned to hourly resolution: {len(aligned)} records")

        return aligned
