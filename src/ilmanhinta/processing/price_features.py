"""Feature engineering for electricity price prediction.

This module creates features for training and prediction with proper temporal alignment:
- Training: Uses historical actuals (weather observations, consumption, prices)
- Prediction: Uses forecasts only (weather forecasts, Fingrid forecasts)

Key principle: NEVER use future information in training features!
"""

from typing import Literal

import polars as pl

from ilmanhinta.logging import get_logger
from ilmanhinta.processing.holiday_features import add_holiday_features

logger = get_logger(__name__)


class PriceFeatureEngineer:
    """Feature engineering for electricity price prediction."""

    @staticmethod
    def add_calendar_features(df: pl.DataFrame, time_col: str = "time") -> pl.DataFrame:
        """Add calendar-based features.

        These are available for both training and prediction.
        """
        if df.is_empty():
            return df

        df = df.with_columns(
            [
                # Time of day features
                pl.col(time_col).dt.hour().alias("hour"),
                pl.col(time_col).dt.minute().alias("minute"),
                # Day features
                pl.col(time_col).dt.weekday().alias("day_of_week"),  # 1=Monday, 7=Sunday
                pl.col(time_col).dt.ordinal_day().alias("day_of_year"),
                # Week features
                pl.col(time_col).dt.week().alias("week_of_year"),
                # Month features
                pl.col(time_col).dt.month().alias("month"),
                pl.col(time_col).dt.quarter().alias("quarter"),
                # Weekend indicator
                (pl.col(time_col).dt.weekday() >= 6).alias("is_weekend"),
                # Business hours (7 AM - 6 PM on weekdays)
                (
                    (pl.col(time_col).dt.weekday() < 6)
                    & (pl.col(time_col).dt.hour() >= 7)
                    & (pl.col(time_col).dt.hour() < 18)
                ).alias("is_business_hours"),
                # Peak hours (typically 7-10 AM and 5-8 PM)
                (
                    ((pl.col(time_col).dt.hour() >= 7) & (pl.col(time_col).dt.hour() < 10))
                    | ((pl.col(time_col).dt.hour() >= 17) & (pl.col(time_col).dt.hour() < 20))
                ).alias("is_peak_hours"),
                # Night hours (11 PM - 5 AM) - typically low prices
                ((pl.col(time_col).dt.hour() >= 23) | (pl.col(time_col).dt.hour() < 5)).alias(
                    "is_night_hours"
                ),
            ]
        )

        # Add cyclical encoding for hour (sine/cosine)
        # This helps the model understand that hour 23 is close to hour 0
        df = df.with_columns(
            [
                (2 * 3.14159 * pl.col("hour") / 24).sin().alias("hour_sin"),
                (2 * 3.14159 * pl.col("hour") / 24).cos().alias("hour_cos"),
                (2 * 3.14159 * pl.col("day_of_week") / 7).sin().alias("day_sin"),
                (2 * 3.14159 * pl.col("day_of_week") / 7).cos().alias("day_cos"),
                (2 * 3.14159 * pl.col("month") / 12).sin().alias("month_sin"),
                (2 * 3.14159 * pl.col("month") / 12).cos().alias("month_cos"),
            ]
        )

        # Add Finnish holiday features (using holidays library)
        # This replaces hardcoded FINNISH_HOLIDAYS with dynamic lookup
        df = add_holiday_features(df, time_col=time_col)

        logger.debug("Added calendar features")
        return df

    @staticmethod
    def add_weather_features(
        df: pl.DataFrame, mode: Literal["observation", "forecast"] = "observation"
    ) -> pl.DataFrame:
        """Add weather-based features with interaction terms.

        Args:
            df: DataFrame with weather data
            mode: "observation" for training, "forecast" for prediction
        """
        if df.is_empty():
            return df

        # Expected column names differ slightly between observations and forecasts
        temp_col = "temperature_c" if mode == "observation" else "temperature"
        wind_col = "wind_speed_ms" if mode == "observation" else "wind_speed"

        feature_exprs = []

        # Temperature features (high impact on electricity demand and pricing)
        if temp_col in df.columns:
            feature_exprs.extend(
                [
                    # Heating degree days (base 18°C) - Finland needs heating most of the year
                    pl.when(pl.col(temp_col) < 18)
                    .then(18 - pl.col(temp_col))
                    .otherwise(0)
                    .alias("heating_degree_days"),
                    # Extreme cold (below -10°C) - massive heating demand
                    (pl.col(temp_col) < -10).alias("is_extreme_cold"),
                    # Mild cold (0-10°C) - moderate heating
                    ((pl.col(temp_col) >= 0) & (pl.col(temp_col) < 10)).alias("is_mild_cold"),
                    # Freezing point crossing - affects systems
                    (pl.col(temp_col) < 0).alias("is_freezing"),
                    # Temperature squared for non-linear effects
                    (pl.col(temp_col) ** 2).alias("temperature_squared"),
                ]
            )

        # Wind features (affects wind power production = lower prices)
        if wind_col in df.columns:
            feature_exprs.extend(
                [
                    # Wind power categories (typical turbine cut-in/rated/cut-out)
                    (pl.col(wind_col) < 3).alias("is_no_wind"),  # Below cut-in
                    ((pl.col(wind_col) >= 3) & (pl.col(wind_col) < 12)).alias(
                        "is_good_wind"
                    ),  # Productive
                    (pl.col(wind_col) >= 12).alias("is_high_wind"),  # Near rated/cut-out
                    # Wind speed squared (power output is cubic with wind speed)
                    (pl.col(wind_col) ** 2).alias("wind_speed_squared"),
                    (pl.col(wind_col) ** 3).alias("wind_speed_cubed"),
                ]
            )

        # Pressure (low pressure = storms = high demand)
        if "pressure_hpa" in df.columns or "pressure" in df.columns:
            pressure_col = "pressure_hpa" if "pressure_hpa" in df.columns else "pressure"
            feature_exprs.extend(
                [
                    (pl.col(pressure_col) < 1000).alias("is_low_pressure"),  # Storm conditions
                    (pl.col(pressure_col) > 1020).alias("is_high_pressure"),  # Stable weather
                ]
            )

        # Cloud cover (affects solar potential, though minor in Finland)
        cloud_col = "cloud_cover"
        if cloud_col in df.columns:
            feature_exprs.extend(
                [
                    (pl.col(cloud_col) > 6).alias("is_overcast"),  # >6 oktas = mostly cloudy
                ]
            )

        # Precipitation (snow = heating demand)
        precip_col = "precipitation_mm"
        if precip_col in df.columns:
            feature_exprs.extend(
                [
                    (pl.col(precip_col) > 0).alias("is_precipitation"),
                    (pl.col(precip_col) > 5).alias("is_heavy_precipitation"),
                ]
            )

        # Interaction features (these often have strong effects on prices)
        if temp_col in df.columns and wind_col in df.columns:
            feature_exprs.extend(
                [
                    # Wind chill (feels colder = more heating)
                    (
                        13.12
                        + 0.6215 * pl.col(temp_col)
                        - 11.37 * (pl.col(wind_col) * 3.6) ** 0.16
                        + 0.3965 * pl.col(temp_col) * (pl.col(wind_col) * 3.6) ** 0.16
                    ).alias("wind_chill"),
                    # Cold + wind = high demand + high wind production = price uncertainty
                    (pl.col(temp_col) * pl.col(wind_col)).alias("temp_wind_interaction"),
                ]
            )

        if feature_exprs:
            df = df.with_columns(feature_exprs)
            logger.debug(f"Added {len(feature_exprs)} weather features (mode={mode})")

        return df

    @staticmethod
    def add_consumption_production_features(
        df: pl.DataFrame, mode: Literal["actual", "forecast"] = "actual"
    ) -> pl.DataFrame:
        """Add consumption and production features.

        Args:
            mode: "actual" for training (uses historical actuals),
                  "forecast" for prediction (uses Fingrid forecasts)
        """
        if df.is_empty():
            return df

        # Column names differ between actuals and forecasts
        if mode == "actual":
            cons_col = "consumption_mw"
            prod_col = "production_mw"
            wind_col = "wind_mw"
            nuclear_col = "nuclear_mw"
            import_col = "net_import_mw"
        else:  # forecast
            cons_col = "consumption_forecast_mw"
            prod_col = "production_forecast_mw"
            wind_col = "wind_forecast_mw"
            nuclear_col = "nuclear_forecast_mw"  # May not exist
            import_col = "import_forecast_mw"  # May not exist

        feature_exprs = []

        # Supply-demand balance (critical for prices!)
        if cons_col in df.columns and prod_col in df.columns:
            feature_exprs.extend(
                [
                    # Net position (negative = shortage = high prices)
                    (pl.col(prod_col) - pl.col(cons_col)).alias("net_supply_mw"),
                    # Supply-demand ratio (< 1 = shortage)
                    (pl.col(prod_col) / pl.col(cons_col).clip(lower_bound=1)).alias(
                        "supply_demand_ratio"
                    ),
                    # Shortage indicator
                    (pl.col(prod_col) < pl.col(cons_col)).alias("is_shortage"),
                    # Tight margin (< 5% surplus)
                    (
                        (pl.col(prod_col) - pl.col(cons_col)) / pl.col(cons_col).clip(lower_bound=1)
                        < 0.05
                    ).alias("is_tight_margin"),
                ]
            )

        # Wind penetration (high wind = lower prices)
        if wind_col in df.columns and prod_col in df.columns:
            feature_exprs.extend(
                [
                    (pl.col(wind_col) / pl.col(prod_col).clip(lower_bound=1)).alias(
                        "wind_penetration"
                    ),
                    (pl.col(wind_col) > pl.col(prod_col) * 0.3).alias("is_high_wind_penetration"),
                ]
            )

        # Nuclear as baseload indicator
        if nuclear_col in df.columns and prod_col in df.columns:
            feature_exprs.extend(
                [
                    (pl.col(nuclear_col) / pl.col(prod_col).clip(lower_bound=1)).alias(
                        "nuclear_share"
                    ),
                ]
            )

        # Import dependency (high imports = potentially higher prices)
        if import_col in df.columns and cons_col in df.columns:
            feature_exprs.extend(
                [
                    (pl.col(import_col) / pl.col(cons_col).clip(lower_bound=1)).alias(
                        "import_dependency"
                    ),
                    (pl.col(import_col) > 0).alias("is_importing"),
                    (pl.col(import_col) < 0).alias("is_exporting"),
                ]
            )

        if feature_exprs:
            df = df.with_columns(feature_exprs)
            logger.debug(
                f"Added {len(feature_exprs)} consumption/production features (mode={mode})"
            )

        return df

    @staticmethod
    def add_price_lag_features(
        df: pl.DataFrame, price_col: str = "price_eur_mwh", lags: list[int] | None = None
    ) -> pl.DataFrame:
        """Add lagged price features.

        Important: Only use lags that are truly historical!
        For hourly data predicting hour H:
        - lag_1h = price at H-1 (available)
        - lag_24h = price at H-24 (same hour yesterday)
        - lag_168h = price at H-168 (same hour last week)
        """
        if df.is_empty() or price_col not in df.columns:
            return df

        if lags is None:
            # Default: 1h, 2h, 3h, 24h, 48h, 168h
            lags = [1, 2, 3, 24, 48, 168]

        lag_exprs = [pl.col(price_col).shift(lag).alias(f"price_lag_{lag}h") for lag in lags]

        df = df.with_columns(lag_exprs)
        logger.debug(f"Added price lag features: {lags}")
        return df

    @staticmethod
    def add_price_rolling_features(
        df: pl.DataFrame, price_col: str = "price_eur_mwh", windows: list[int] | None = None
    ) -> pl.DataFrame:
        """Add rolling statistics for price (volatility indicators).

        Important: Rolling windows look BACKWARD, so they're safe to use!
        """
        if df.is_empty() or price_col not in df.columns:
            return df

        if windows is None:
            # Default windows capture intraday + weekly trends
            windows = [6, 12, 24, 168]

        rolling_exprs = []
        for window in windows:
            rolling_exprs.extend(
                [
                    pl.col(price_col)
                    .rolling_mean(window_size=window)
                    .alias(f"price_rolling_mean_{window}h"),
                    pl.col(price_col)
                    .rolling_std(window_size=window)
                    .alias(f"price_rolling_std_{window}h"),
                    pl.col(price_col)
                    .rolling_min(window_size=window)
                    .alias(f"price_rolling_min_{window}h"),
                    pl.col(price_col)
                    .rolling_max(window_size=window)
                    .alias(f"price_rolling_max_{window}h"),
                ]
            )

        df = df.with_columns(rolling_exprs)
        logger.debug(f"Added price rolling features: {windows}")
        return df

    @staticmethod
    def add_price_diff_features(
        df: pl.DataFrame,
        fast_lag: str = "price_lag_1h",
        slower_lag: str = "price_lag_2h",
        day_lag: str = "price_lag_24h",
        week_lag: str = "price_lag_48h",
    ) -> pl.DataFrame:
        """Add price change features using existing lag columns."""

        diff_exprs = []
        if fast_lag in df.columns and slower_lag in df.columns:
            diff_exprs.append((pl.col(fast_lag) - pl.col(slower_lag)).alias("price_diff_1h"))
        if day_lag in df.columns and week_lag in df.columns:
            diff_exprs.append((pl.col(day_lag) - pl.col(week_lag)).alias("price_diff_24h"))

        if diff_exprs:
            df = df.with_columns(diff_exprs)
            logger.debug("Added price diff features")

        return df

    @staticmethod
    def add_price_history_features(
        df: pl.DataFrame,
        historical_prices: pl.DataFrame,
        time_col: str = "forecast_time",
    ) -> pl.DataFrame:
        """Add lag/rolling/diff features derived from historical price data."""

        if df.is_empty():
            return df

        if historical_prices.is_empty():
            logger.warning("No historical prices provided - skipping price lag features")
            return df

        required_cols = {"time", "price_eur_mwh"}
        missing = required_cols - set(historical_prices.columns)
        if missing:
            logger.warning("Missing columns %s in historical prices", sorted(missing))
            return df

        history = (
            historical_prices.select(["time", "price_eur_mwh"])
            .sort("time")
            .unique(subset=["time"], keep="last")
            .with_columns(pl.lit(False).alias("_is_forecast"))
        )

        forecast_stub = df.select(pl.col(time_col).alias("time")).with_columns(
            [
                pl.lit(True).alias("_is_forecast"),
                pl.lit(None, dtype=pl.Float64).alias("price_eur_mwh"),
            ]
        )

        combined = pl.concat([history, forecast_stub], how="diagonal").sort("time")
        combined = PriceFeatureEngineer.add_price_lag_features(combined)
        combined = PriceFeatureEngineer.add_price_rolling_features(combined)
        combined = PriceFeatureEngineer.add_price_diff_features(combined)

        feature_cols = [
            col
            for col in combined.columns
            if col.startswith("price_lag_")
            or col.startswith("price_rolling_")
            or col.startswith("price_diff_")
        ]

        if not feature_cols:
            logger.warning("Price history features could not be generated")
            return df

        features_df = (
            combined.filter(pl.col("_is_forecast"))
            .select(["time", *feature_cols])
            .rename({"time": time_col})
        )

        return df.join(features_df, on=time_col, how="left")

    @staticmethod
    def create_training_features(
        df: pl.DataFrame, time_col: str = "time", price_col: str = "price_eur_mwh"
    ) -> pl.DataFrame:
        """Create all features for MODEL TRAINING using historical actuals.

        Input DataFrame should have columns from joined query:
        - time, price_eur_mwh (target)
        - consumption_mw, production_mw, wind_mw, nuclear_mw, net_import_mw
        - temperature_c, wind_speed_ms, pressure_hpa, cloud_cover, etc.

        Returns DataFrame with all features + target.
        """
        logger.info(f"Creating training features from {len(df)} records")

        # 1. Calendar features (always available)
        df = PriceFeatureEngineer.add_calendar_features(df, time_col)

        # 2. Weather features (observations)
        df = PriceFeatureEngineer.add_weather_features(df, mode="observation")

        # 3. Consumption/production features (actuals)
        df = PriceFeatureEngineer.add_consumption_production_features(df, mode="actual")

        # 4. Price lag features (learn from past prices)
        df = PriceFeatureEngineer.add_price_lag_features(df, price_col)

        # 5. Price rolling statistics (volatility)
        df = PriceFeatureEngineer.add_price_rolling_features(df, price_col)

        # 6. Price diffs (momentum signals)
        df = PriceFeatureEngineer.add_price_diff_features(df)

        logger.info(f"Training features complete: {len(df.columns)} columns")
        return df

    @staticmethod
    def create_prediction_features(
        df: pl.DataFrame,
        historical_prices: pl.DataFrame,
        time_col: str = "forecast_time",
    ) -> pl.DataFrame:
        """Create features for PREDICTION using forecasts only.

        CRITICAL: Only use data that would actually be available at prediction time!

        Input DataFrame should have columns from forecasts:
        - forecast_time
        - temperature, wind_speed, pressure, cloud_cover (FMI HARMONIE forecast)
        - consumption_forecast_mw, production_forecast_mw, wind_forecast_mw (Fingrid forecast)

        historical_prices: DataFrame with past prices for lag features
        - time, price_eur_mwh

        Returns DataFrame ready for model.predict().
        """
        logger.info(f"Creating prediction features for {len(df)} forecast periods")

        # 1. Calendar features (available for future times)
        df = PriceFeatureEngineer.add_calendar_features(df, time_col)

        # 2. Weather features (from forecasts)
        df = PriceFeatureEngineer.add_weather_features(df, mode="forecast")

        # 3. Consumption/production features (from Fingrid forecasts)
        df = PriceFeatureEngineer.add_consumption_production_features(df, mode="forecast")

        # 4. Price lag/rolling features via historical prices
        df = PriceFeatureEngineer.add_price_history_features(df, historical_prices, time_col)

        logger.info(f"Prediction features complete: {len(df.columns)} columns")
        return df
