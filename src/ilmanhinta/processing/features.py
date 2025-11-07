"""Feature engineering with sliding windows and time-based features."""

import polars as pl

from ilmanhinta.logging import logfire


class FeatureEngineer:
    """Create features for ML model using sliding windows and time-based transformations."""

    @staticmethod
    def add_time_features(df: pl.DataFrame) -> pl.DataFrame:
        """Add time-based features (hour, day of week, weekend, etc.)."""
        if df.is_empty():
            return df

        df = df.with_columns(
            [
                pl.col("timestamp").dt.hour().alias("hour_of_day"),
                pl.col("timestamp").dt.weekday().alias("day_of_week"),
                pl.col("timestamp").dt.month().alias("month"),
                pl.col("timestamp").dt.day().alias("day_of_month"),
                (pl.col("timestamp").dt.weekday() >= 5).alias("is_weekend"),
            ]
        )

        logfire.debug("Added time-based features")
        return df

    @staticmethod
    def add_lag_features(
        df: pl.DataFrame,
        target_col: str = "consumption_mw",
        lags: list[int] | None = None,
    ) -> pl.DataFrame:
        """Add lagged features (previous consumption values)."""
        if df.is_empty():
            return df

        if lags is None:
            # Default lags: 1h, 3h, 6h, 12h, 24h, 48h, 168h (1 week)
            lags = [1, 3, 6, 12, 24, 48, 168]

        lag_exprs = [
            pl.col(target_col).shift(lag).alias(f"{target_col}_lag_{lag}h") for lag in lags
        ]

        df = df.with_columns(lag_exprs)

        logfire.debug(f"Added lag features for {target_col}: {lags}")
        return df

    @staticmethod
    def add_rolling_features(
        df: pl.DataFrame,
        target_col: str = "consumption_mw",
        windows: list[int] | None = None,
    ) -> pl.DataFrame:
        """Add rolling window statistics (mean, std, min, max)."""
        if df.is_empty():
            return df

        if windows is None:
            # Default windows: 6h, 12h, 24h, 168h (1 week)
            windows = [6, 12, 24, 168]

        rolling_exprs = []
        for window in windows:
            rolling_exprs.extend(
                [
                    pl.col(target_col)
                    .rolling_mean(window_size=window, min_periods=1)
                    .alias(f"{target_col}_rolling_mean_{window}h"),
                    pl.col(target_col)
                    .rolling_std(window_size=window, min_periods=1)
                    .alias(f"{target_col}_rolling_std_{window}h"),
                    pl.col(target_col)
                    .rolling_min(window_size=window, min_periods=1)
                    .alias(f"{target_col}_rolling_min_{window}h"),
                    pl.col(target_col)
                    .rolling_max(window_size=window, min_periods=1)
                    .alias(f"{target_col}_rolling_max_{window}h"),
                ]
            )

        df = df.with_columns(rolling_exprs)

        logfire.debug(f"Added rolling window features for {target_col}: {windows}")
        return df

    @staticmethod
    def add_weather_interactions(df: pl.DataFrame) -> pl.DataFrame:
        """Add interaction features between weather variables."""
        if df.is_empty():
            return df

        # Temperature-based features
        if "temperature" in df.columns:
            df = df.with_columns(
                [
                    # Heating degree days (base 18°C)
                    pl.when(pl.col("temperature") < 18)
                    .then(18 - pl.col("temperature"))
                    .otherwise(0)
                    .alias("heating_degree_days"),
                    # Cooling degree days (base 22°C)
                    pl.when(pl.col("temperature") > 22)
                    .then(pl.col("temperature") - 22)
                    .otherwise(0)
                    .alias("cooling_degree_days"),
                    # Temperature squared (non-linear effects)
                    (pl.col("temperature") ** 2).alias("temperature_squared"),
                ]
            )

        # Wind chill effect
        if "temperature" in df.columns and "wind_speed" in df.columns:
            # Simplified wind chill formula
            df = df.with_columns(
                (
                    13.12
                    + 0.6215 * pl.col("temperature")
                    - 11.37 * (pl.col("wind_speed") * 3.6) ** 0.16
                    + 0.3965 * pl.col("temperature") * (pl.col("wind_speed") * 3.6) ** 0.16
                ).alias("wind_chill")
            )

        logfire.debug("Added weather interaction features")
        return df

    @staticmethod
    def create_all_features(
        df: pl.DataFrame,
        target_col: str = "consumption_mw",
        drop_nulls: bool = True,
    ) -> pl.DataFrame:
        """
        Apply all feature engineering steps.

        Parameters
        - df: Input dataframe with at least `timestamp` and target column.
        - target_col: Name of the target column used for lags/rolling.
        - drop_nulls: When True (default), drop rows with any nulls. For
          inference where the target is unknown for the forecast row, set to
          False and handle row selection downstream.
        """
        logfire.info(f"Creating features from {len(df)} records")

        df = FeatureEngineer.add_time_features(df)
        df = FeatureEngineer.add_lag_features(df, target_col)
        df = FeatureEngineer.add_rolling_features(df, target_col)
        df = FeatureEngineer.add_weather_interactions(df)

        # Optionally drop rows with NaN values (from lags/rolling windows)
        if drop_nulls:
            initial_count = len(df)
            df = df.drop_nulls()
            final_count = len(df)

            if initial_count > final_count:
                logfire.info(
                    f"Dropped {initial_count - final_count} rows with missing values "
                    f"(from lag/rolling features)"
                )

        logfire.info(
            f"Feature engineering complete: {len(df)} records with {len(df.columns)} features"
        )

        return df
