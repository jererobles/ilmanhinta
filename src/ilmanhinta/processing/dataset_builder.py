"""Dataset builders for training and prediction.

These orchestrate fetching data from the database and creating feature-rich datasets.
"""

from datetime import datetime, timedelta

import polars as pl

from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.logging import logfire
from ilmanhinta.processing.price_features import PriceFeatureEngineer


class TrainingDatasetBuilder:
    """Build training datasets from historical data."""

    def __init__(self, db: PostgresClient | None = None):
        """Initialize with database client."""
        self.db = db or PostgresClient()

    def build(
        self, start_time: datetime, end_time: datetime, resample_freq: str = "1h"
    ) -> pl.DataFrame:
        """Build complete training dataset with features and target.

        Args:
            start_time: Start of training period
            end_time: End of training period
            resample_freq: Resample frequency (default: 1h for hourly)

        Returns:
            DataFrame with features + target (price_eur_mwh)
        """
        logfire.info(f"Building training dataset from {start_time} to {end_time}")

        # Fetch all required data
        consumption = self.db.get_consumption(start_time, end_time)
        prices = self.db.get_prices(start_time, end_time, area="FI")
        weather = self.db.get_weather(start_time, end_time)

        # Check if we have data
        if len(consumption) == 0:
            raise ValueError("No consumption data found for training period")
        if len(prices) == 0:
            raise ValueError("No price data found for training period")
        if len(weather) == 0:
            logfire.warning("No weather data found - proceeding without weather features")

        # Resample to consistent frequency (handle missing data)
        consumption = self._resample_consumption(consumption, resample_freq)
        prices = self._resample_prices(prices, resample_freq)
        if len(weather) > 0:
            weather = self._resample_weather(weather, resample_freq)

        # Join all data sources on time
        df = consumption

        # Join prices
        df = df.join(prices.select(["time", "price_eur_mwh"]), on="time", how="left")

        # Join weather if available
        if len(weather) > 0:
            weather_cols = [
                "time",
                "temperature_c",
                "wind_speed_ms",
                "wind_direction",
                "cloud_cover",
                "humidity_percent",
                "pressure_hpa",
                "precipitation_mm",
            ]
            # Only select columns that exist
            existing_cols = [col for col in weather_cols if col in weather.columns]
            df = df.join(weather.select(existing_cols), on="time", how="left")

        # Create features
        df = PriceFeatureEngineer.create_training_features(df, time_col="time")

        # Drop rows with null target (can't train on these)
        initial_count = len(df)
        df = df.filter(pl.col("price_eur_mwh").is_not_null())
        final_count = len(df)

        if final_count < initial_count:
            logfire.info(f"Dropped {initial_count - final_count} rows with null prices")

        # Drop rows with too many null features (from lag features)
        # Keep rows where at least 80% of columns are non-null
        null_threshold = int(len(df.columns) * 0.2)  # Allow 20% nulls
        df = df.filter(pl.sum_horizontal(pl.all().is_null()) <= null_threshold)

        logfire.info(f"Training dataset ready: {len(df)} samples, {len(df.columns)} features")

        return df

    def _resample_consumption(self, df: pl.DataFrame, freq: str) -> pl.DataFrame:
        """Resample consumption data to consistent frequency."""
        if df.is_empty():
            return df

        # Group by time bucket and take mean
        df = (
            df.sort("time")
            .group_by_dynamic("time", every=freq)
            .agg(
                [
                    pl.col("consumption_mw").mean(),
                    pl.col("production_mw").mean(),
                    pl.col("wind_mw").mean(),
                    pl.col("nuclear_mw").mean(),
                    pl.col("net_import_mw").mean(),
                ]
            )
        )

        return df

    def _resample_prices(self, df: pl.DataFrame, freq: str) -> pl.DataFrame:
        """Resample price data to consistent frequency."""
        if df.is_empty():
            return df

        df = (
            df.sort("time")
            .group_by_dynamic("time", every=freq)
            .agg(
                [
                    pl.col("price_eur_mwh").mean(),
                ]
            )
        )

        return df

    def _resample_weather(self, df: pl.DataFrame, freq: str) -> pl.DataFrame:
        """Resample weather data to consistent frequency."""
        if df.is_empty():
            return df

        # Take first station if multiple (could be improved to aggregate)
        df = df.filter(pl.col("station_id") == df["station_id"][0])

        # Aggregate columns that exist
        agg_exprs = []
        numeric_cols = [
            "temperature_c",
            "wind_speed_ms",
            "wind_direction",
            "cloud_cover",
            "humidity_percent",
            "pressure_hpa",
            "precipitation_mm",
        ]

        for col in numeric_cols:
            if col in df.columns:
                if col == "precipitation_mm":
                    # Sum precipitation
                    agg_exprs.append(pl.col(col).sum().alias(col))
                else:
                    # Average other metrics
                    agg_exprs.append(pl.col(col).mean().alias(col))

        df = df.sort("time").group_by_dynamic("time", every=freq).agg(agg_exprs)

        return df


class PredictionDatasetBuilder:
    """Build prediction datasets from forecasts."""

    def __init__(self, db: PostgresClient | None = None):
        """Initialize with database client."""
        self.db = db or PostgresClient()

    def build(
        self,
        forecast_start: datetime,
        forecast_end: datetime,
        lookback_hours: int = 720,  # 30 days of history for lag features
    ) -> pl.DataFrame:
        """Build prediction dataset using forecasts + historical prices.

        Args:
            forecast_start: Start of forecast period
            forecast_end: End of forecast period
            lookback_hours: Hours of historical data to fetch for lag features

        Returns:
            DataFrame ready for model.predict()
        """
        logfire.info(f"Building prediction dataset for {forecast_start} to {forecast_end}")

        # Fetch forecasts
        weather_fcst = self._get_weather_forecasts(forecast_start, forecast_end)
        fingrid_fcst = self._get_fingrid_forecasts(forecast_start, forecast_end)

        # Fetch historical prices for lag features
        hist_start = forecast_start - timedelta(hours=lookback_hours)
        historical_prices = self.db.get_prices(hist_start, forecast_start, area="FI")

        # Join forecasts
        df = weather_fcst

        if not fingrid_fcst.is_empty():
            # Pivot Fingrid forecasts to wide format
            fingrid_wide = self._pivot_fingrid_forecasts(fingrid_fcst)
            df = df.join(fingrid_wide, on="forecast_time", how="left")

        # Create features
        df = PriceFeatureEngineer.create_prediction_features(
            df, historical_prices=historical_prices, time_col="forecast_time"
        )

        logfire.info(f"Prediction dataset ready: {len(df)} samples, {len(df.columns)} features")

        return df

    def _get_weather_forecasts(self, _start_time: datetime, _end_time: datetime) -> pl.DataFrame:
        """Get weather forecasts from database."""
        # This would query the weather_forecasts table
        # For now, return a placeholder structure

        # In production, you'd query:
        # SELECT forecast_time, temperature_c, wind_speed_ms, ...
        # FROM weather_forecasts
        # WHERE forecast_time >= _start_time AND forecast_time <= _end_time
        # ORDER BY forecast_time

        logfire.warning("weather_forecasts query not yet implemented - using placeholder")

        # Placeholder: return empty DataFrame with expected columns
        return pl.DataFrame(
            {
                "forecast_time": [],
                "temperature": [],
                "wind_speed": [],
                "pressure": [],
                "cloud_cover": [],
                "humidity": [],
            }
        ).cast(
            {
                "forecast_time": pl.Datetime,
                "temperature": pl.Float64,
                "wind_speed": pl.Float64,
                "pressure": pl.Float64,
                "cloud_cover": pl.Float64,
                "humidity": pl.Float64,
            }
        )

    def _get_fingrid_forecasts(self, _start_time: datetime, _end_time: datetime) -> pl.DataFrame:
        """Get Fingrid forecasts from database."""
        # This would query the fingrid_forecasts table
        # For now, return a placeholder

        # In production:
        # SELECT forecast_time, forecast_type, value
        # FROM fingrid_forecasts
        # WHERE forecast_time >= _start_time AND forecast_time <= _end_time
        # ORDER BY forecast_time

        logfire.warning("fingrid_forecasts query not yet implemented - using placeholder")

        return pl.DataFrame(
            {
                "forecast_time": [],
                "forecast_type": [],
                "value": [],
            }
        ).cast(
            {
                "forecast_time": pl.Datetime,
                "forecast_type": pl.Utf8,
                "value": pl.Float64,
            }
        )

    def _pivot_fingrid_forecasts(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pivot Fingrid forecasts from long to wide format.

        Input:
            forecast_time | forecast_type | value
            2024-11-08    | consumption   | 10000
            2024-11-08    | production    | 9500

        Output:
            forecast_time | consumption_forecast_mw | production_forecast_mw | ...
            2024-11-08    | 10000                   | 9500                   | ...
        """
        if df.is_empty():
            return df

        # Pivot using polars
        df_pivot = df.pivot(on="forecast_type", index="forecast_time", values="value")

        # Rename columns to match expected format
        rename_map = {
            "consumption": "consumption_forecast_mw",
            "production": "production_forecast_mw",
            "wind": "wind_forecast_mw",
        }

        for old_name, new_name in rename_map.items():
            if old_name in df_pivot.columns:
                df_pivot = df_pivot.rename({old_name: new_name})

        return df_pivot


def split_train_test_temporal(
    df: pl.DataFrame, test_size: float = 0.2, time_col: str = "time"
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Split dataset into train/test preserving temporal order.

    IMPORTANT: For time series, we split chronologically, not randomly!

    Args:
        df: Input dataset
        test_size: Fraction of data for test (default: 20%)
        time_col: Name of time column

    Returns:
        (train_df, test_df)
    """
    df = df.sort(time_col)

    split_idx = int(len(df) * (1 - test_size))

    train = df[:split_idx]
    test = df[split_idx:]

    logfire.info(
        f"Split dataset: {len(train)} train, {len(test)} test (test size: {test_size:.1%})"
    )

    return train, test


def get_feature_names(df: pl.DataFrame, exclude: list[str] | None = None) -> list[str]:
    """Get feature column names (excluding target and metadata).

    Args:
        df: Dataset DataFrame
        exclude: Additional columns to exclude

    Returns:
        List of feature column names
    """
    exclude_defaults = [
        "time",
        "forecast_time",
        "prediction_time",
        "price_eur_mwh",  # Target
        "station_id",
        "station_name",
        "generated_at",
        "created_at",
    ]

    if exclude:
        exclude_defaults.extend(exclude)

    feature_cols = [col for col in df.columns if col not in exclude_defaults]

    return feature_cols
