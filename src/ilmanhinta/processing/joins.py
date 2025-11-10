"""Temporal joins between weather and electricity data using Polars."""

from datetime import timedelta

import polars as pl

from ilmanhinta.logging import logfire
from ilmanhinta.models.fingrid import FingridDataPoint
from ilmanhinta.models.fmi import FMIObservation


class TemporalJoiner:
    """Handle temporal alignment and joining of weather and electricity data."""

    @staticmethod
    def fingrid_to_polars(
        data: list[FingridDataPoint], column_name: str = "consumption_mw"
    ) -> pl.DataFrame:
        """Convert Fingrid data to Polars DataFrame with a specified column name.

        Args:
            data: List of Fingrid data points
            column_name: Name for the value column (e.g., 'consumption_mw', 'production_mw')
        """
        if not data:
            return pl.DataFrame()

        records = [
            {
                "timestamp": point.start_time,
                column_name: point.value,
            }
            for point in data
        ]

        return pl.DataFrame(records).with_columns(
            pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
        )

    @staticmethod
    def merge_fingrid_datasets(datasets: dict[str, list[FingridDataPoint]]) -> pl.DataFrame:
        """Merge multiple Fingrid datasets into a single DataFrame with all features.

        Args:
            datasets: Dict mapping column names to Fingrid data points.
                     Expected keys: 'consumption', 'production', 'wind', 'nuclear', 'net_import'

        Returns:
            Polars DataFrame with columns: timestamp, consumption_mw, production_mw, wind_mw, nuclear_mw, net_import_mw
        """
        if not datasets:
            return pl.DataFrame()

        # Convert each dataset to a DataFrame with the appropriate column name
        column_mapping = {
            "consumption": "consumption_mw",
            "production": "production_mw",
            "wind": "wind_mw",
            "nuclear": "nuclear_mw",
            "net_import": "net_import_mw",
        }

        dfs = []
        for key, data in datasets.items():
            if not data:
                logfire.warning(f"No data for {key} dataset")
                continue

            column_name = column_mapping.get(key, f"{key}_mw")
            df = TemporalJoiner.fingrid_to_polars(data, column_name=column_name)
            dfs.append((key, df))

        if not dfs:
            return pl.DataFrame()

        # Start with the first dataset
        _, merged = dfs[0]

        # Join all other datasets on timestamp using coalesce strategy
        for key, df in dfs[1:]:
            # Use outer join with coalesce to handle duplicate timestamp columns
            merged = merged.join(df, on="timestamp", how="outer", coalesce=True)
            logfire.info(f"Merged {key} dataset: {len(merged)} total records")

        # Sort by timestamp
        merged = merged.sort("timestamp")

        # Forward fill nulls caused by different update frequencies
        # (e.g., wind updates every 15 min, consumption every 3 min)
        # This ensures we have values for all timestamps
        numeric_cols = [col for col in merged.columns if col != "timestamp"]
        for col in numeric_cols:
            if col in merged.columns:
                merged = merged.with_columns(pl.col(col).forward_fill().alias(col))

        # Drop any rows where consumption_mw is still null (no data available)
        # This can happen at the start of the time series
        if "consumption_mw" in merged.columns:
            merged = merged.drop_nulls(subset=["consumption_mw"])

        logfire.info(f"Merged all Fingrid datasets: {len(merged)} final records after forward-fill")

        return merged

    @staticmethod
    def fmi_to_polars(observations: list[FMIObservation]) -> pl.DataFrame:
        """Convert FMI observations to Polars DataFrame."""
        if not observations:
            return pl.DataFrame()

        records = [
            {
                "timestamp": obs.timestamp,
                "temperature_c": obs.temperature,
                "humidity_percent": obs.humidity,
                "wind_speed_ms": obs.wind_speed,
                "wind_direction": obs.wind_direction,
                "pressure_hpa": obs.pressure,
                "precipitation_mm": obs.precipitation,
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

        # Determine which time column is present (handle both 'time' and 'timestamp')
        time_col = "time" if "time" in df.columns else "timestamp"

        # Floor timestamps to the start of the hour to avoid spilling into the next hour
        df = df.with_columns(pl.col(time_col).dt.truncate("1h").alias("timestamp_hourly"))

        # Group by hourly timestamp and aggregate
        numeric_cols = [
            col
            for col in df.columns
            if col not in [time_col, "timestamp_hourly"] and df[col].dtype.is_numeric()
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
