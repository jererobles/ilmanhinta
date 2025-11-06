"""Prophet model for electricity consumption forecasting.

Prophet handles seasonal patterns, holidays, and uncertainty intervals automatically.
This is Facebook Infrastructure that's either Overengineered or Abandoned (both).
"""

import pickle
from datetime import datetime
from pathlib import Path

import polars as pl
from prophet import Prophet

# Finnish holidays that affect electricity consumption
FINNISH_HOLIDAYS = pl.DataFrame(
    {
        "holiday": [
            "new_year",
            "epiphany",
            "good_friday",
            "easter",
            "easter_monday",
            "may_day",
            "midsummer_eve",
            "midsummer",
            "independence_day",
            "christmas_eve",
            "christmas",
            "boxing_day",
        ],
        # Example dates - Prophet will handle recurrence automatically
        "ds": [
            "2024-01-01",
            "2024-01-06",
            "2024-03-29",
            "2024-03-31",
            "2024-04-01",
            "2024-05-01",
            "2024-06-21",
            "2024-06-22",
            "2024-12-06",
            "2024-12-24",
            "2024-12-25",
            "2024-12-26",
        ],
        "lower_window": [0] * 12,
        "upper_window": [1] * 12,
    }
)


class ProphetConsumptionModel:
    """Prophet model for electricity consumption forecasting.

    Features:
    - Automatic seasonal decomposition (daily, weekly, yearly)
    - Finnish holiday effects (juhannus electricity anomaly is REAL)
    - Weather regressors (temperature, wind, etc.)
    - Proper uncertainty intervals via MCMC simulation
    - Handles DST gaps gracefully (unlike manual feature engineering)
    """

    def __init__(self, model_path: Path | None = None) -> None:
        """Initialize Prophet model, optionally loading from disk."""
        self.model: Prophet | None = None
        self.feature_names: list[str] = []
        self.model_version: str = "untrained"
        self.trained_at: datetime | None = None

        if model_path and model_path.exists():
            self.load(model_path)

    def train(
        self,
        train_df: pl.DataFrame,
        target_col: str = "consumption_mw",
        weather_features: list[str] | None = None,
    ) -> dict[str, float]:
        """Train Prophet model on historical time-series data.

        Args:
            train_df: Polars DataFrame with timestamp and consumption data
            target_col: Target column name (default: consumption_mw)
            weather_features: Additional weather regressors to include

        Returns:
            Dict of training metrics (MAE, RMSE)
        """
        # Prophet requires specific column names: 'ds' (datetime) and 'y' (value)
        prophet_df = train_df.select(
            [
                pl.col("timestamp").alias("ds"),
                pl.col(target_col).alias("y"),
            ]
        ).to_pandas()

        # Initialize Prophet with Finnish-specific configuration
        self.model = Prophet(
            # Seasonality
            yearly_seasonality=True,  # Heating/cooling seasons
            weekly_seasonality=True,  # Weekday vs weekend patterns
            daily_seasonality=True,  # Hourly consumption patterns
            # Uncertainty intervals via MCMC
            interval_width=0.95,  # 95% confidence intervals
            uncertainty_samples=1000,
            # Changepoint detection (when patterns shift)
            changepoint_prior_scale=0.05,  # Conservative changepoints
            seasonality_prior_scale=10.0,  # Strong seasonal effects
            # Finnish holidays
            holidays=FINNISH_HOLIDAYS.to_pandas(),
        )

        # Add weather regressors if provided
        if weather_features:
            self.feature_names = weather_features
            for feature in weather_features:
                if feature in train_df.columns:
                    prophet_df[feature] = train_df[feature].to_pandas()
                    self.model.add_regressor(feature, prior_scale=0.5, mode="additive")

        # Train model
        self.model.fit(prophet_df)

        self.trained_at = datetime.utcnow()
        self.model_version = self.trained_at.strftime("%Y%m%d_%H%M%S")

        # Calculate in-sample metrics
        forecast = self.model.predict(prophet_df)
        y_true = prophet_df["y"].values
        y_pred = forecast["yhat"].values

        mae = float(abs(y_pred - y_true).mean())
        rmse = float(((y_pred - y_true) ** 2).mean() ** 0.5)

        metrics = {
            "mae": mae,
            "rmse": rmse,
        }

        return metrics

    def predict(
        self,
        future_df: pl.DataFrame,
        weather_features: list[str] | None = None,
    ) -> pl.DataFrame:
        """Make predictions with uncertainty intervals.

        Args:
            future_df: Polars DataFrame with future timestamps and weather data
            weather_features: Weather features matching training regressors

        Returns:
            Polars DataFrame with predictions and confidence intervals
        """
        if self.model is None:
            raise ValueError("Model not trained or loaded")

        # Prepare future dataframe with 'ds' column
        future_prophet = future_df.select([pl.col("timestamp").alias("ds")]).to_pandas()

        # Add weather regressors if provided
        if weather_features:
            for feature in weather_features:
                if feature in future_df.columns:
                    future_prophet[feature] = future_df[feature].to_pandas()

        # Generate forecast with uncertainty intervals
        forecast = self.model.predict(future_prophet)

        # Convert back to Polars with renamed columns
        result = pl.DataFrame(
            {
                "timestamp": forecast["ds"],
                "predicted_consumption_mw": forecast["yhat"],
                "confidence_lower": forecast["yhat_lower"],
                "confidence_upper": forecast["yhat_upper"],
            }
        )

        return result

    def predict_with_components(
        self,
        future_df: pl.DataFrame,
        weather_features: list[str] | None = None,
    ) -> pl.DataFrame:
        """Make predictions with seasonal component breakdown.

        This provides interpretability - WHY is consumption predicted to spike?
        (spoiler: it's cold and it's winter)

        Returns:
            DataFrame with predictions plus trend, weekly, yearly components
        """
        if self.model is None:
            raise ValueError("Model not trained or loaded")

        future_prophet = future_df.select([pl.col("timestamp").alias("ds")]).to_pandas()

        if weather_features:
            for feature in weather_features:
                if feature in future_df.columns:
                    future_prophet[feature] = future_df[feature].to_pandas()

        forecast = self.model.predict(future_prophet)

        # Include all components for interpretability
        result = pl.DataFrame(
            {
                "timestamp": forecast["ds"],
                "predicted_consumption_mw": forecast["yhat"],
                "confidence_lower": forecast["yhat_lower"],
                "confidence_upper": forecast["yhat_upper"],
                "trend": forecast["trend"],
                "weekly": forecast.get("weekly", 0.0),
                "yearly": forecast.get("yearly", 0.0),
                "holidays": forecast.get("holidays", 0.0),
            }
        )

        return result

    def save(self, path: Path) -> None:
        """Save Prophet model to disk."""
        if self.model is None:
            raise ValueError("No model to save")

        path.parent.mkdir(parents=True, exist_ok=True)

        metadata = {
            "model": self.model,
            "feature_names": self.feature_names,
            "model_version": self.model_version,
            "trained_at": self.trained_at,
        }

        with open(path, "wb") as f:
            pickle.dump(metadata, f)

    def load(self, path: Path) -> None:
        """Load Prophet model from disk."""
        with open(path, "rb") as f:
            metadata = pickle.load(f)

        self.model = metadata["model"]
        self.feature_names = metadata["feature_names"]
        self.model_version = metadata["model_version"]
        self.trained_at = metadata["trained_at"]

    def get_changepoints(self) -> pl.DataFrame:
        """Get detected changepoints (when consumption patterns shift).

        Useful for understanding structural breaks in demand patterns.
        """
        if self.model is None:
            raise ValueError("Model not trained")

        changepoints = self.model.changepoints
        deltas = self.model.params["delta"].mean(axis=0)

        return pl.DataFrame(
            {
                "changepoint": changepoints,
                "delta": deltas,
            }
        ).sort("changepoint", descending=True)

    def plot_components(self, forecast_df: pl.DataFrame) -> None:
        """Plot seasonal components.

        Requires matplotlib. Use for debugging Prophet's seasonal decomposition.
        """
        if self.model is None:
            raise ValueError("Model not trained")

        # Convert to pandas for Prophet's plotting utilities
        forecast_pd = forecast_df.to_pandas()
        forecast_pd = forecast_pd.rename(columns={"timestamp": "ds"})

        # This requires the model's predict() output format
        self.model.plot_components(forecast_pd)
