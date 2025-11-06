"""Ensemble predictor combining Prophet and LightGBM.

The theory:
- Prophet captures trend + seasonality + holidays
- LightGBM learns complex feature interactions
- Ensemble combines their strengths via weighted averaging

The practice:
- Prophet provides the baseline seasonal pattern
- LightGBM corrects for weather-dependent deviations
- Combined uncertainty intervals via variance pooling
"""

from datetime import datetime
from pathlib import Path

import polars as pl

from .model import ConsumptionModel
from .prophet_model import ProphetConsumptionModel


class EnsemblePredictor:
    """Ensemble of Prophet and LightGBM for electricity consumption forecasting.

    Architecture:
    1. Prophet predicts base consumption (trend + seasonality + holidays)
    2. LightGBM predicts with engineered features
    3. Weighted average: 0.4 * Prophet + 0.6 * LightGBM
       (LightGBM weighted higher bc it has access to more features)
    4. Uncertainty intervals combine both models' confidence bounds
    """

    def __init__(
        self,
        prophet_model: ProphetConsumptionModel | None = None,
        lightgbm_model: ConsumptionModel | None = None,
        prophet_weight: float = 0.4,
    ) -> None:
        """Initialize ensemble predictor.

        Args:
            prophet_model: Prophet forecasting model
            lightgbm_model: LightGBM model with engineered features
            prophet_weight: Weight for Prophet (0-1, LightGBM gets 1-weight)
        """
        self.prophet_model = prophet_model or ProphetConsumptionModel()
        self.lightgbm_model = lightgbm_model or ConsumptionModel()
        self.prophet_weight = prophet_weight
        self.lightgbm_weight = 1.0 - prophet_weight
        self.model_version: str = "ensemble_untrained"
        self.trained_at: datetime | None = None

    @classmethod
    def load_from_paths(
        cls,
        prophet_path: Path,
        lightgbm_path: Path,
        prophet_weight: float = 0.4,
    ) -> "EnsemblePredictor":
        """Load ensemble from saved model files.

        Args:
            prophet_path: Path to Prophet model pickle
            lightgbm_path: Path to LightGBM model pickle
            prophet_weight: Weight for Prophet predictions

        Returns:
            Initialized ensemble predictor
        """
        prophet_model = ProphetConsumptionModel(prophet_path)
        lightgbm_model = ConsumptionModel(lightgbm_path)
        return cls(prophet_model, lightgbm_model, prophet_weight)

    def train_prophet(
        self,
        train_df: pl.DataFrame,
        target_col: str = "consumption_mw",
        weather_features: list[str] | None = None,
    ) -> dict[str, float]:
        """Train Prophet component.

        Args:
            train_df: Training data with timestamp and consumption
            target_col: Target column name
            weather_features: Weather regressors to include

        Returns:
            Prophet training metrics
        """
        return self.prophet_model.train(train_df, target_col, weather_features)

    def train_lightgbm(
        self,
        train_df: pl.DataFrame,
        target_col: str = "consumption_mw",
    ) -> dict[str, float]:
        """Train LightGBM component.

        Args:
            train_df: Training data with engineered features
            target_col: Target column name

        Returns:
            LightGBM training metrics
        """
        return self.lightgbm_model.train(train_df, target_col)

    def predict(
        self,
        prophet_df: pl.DataFrame,
        lightgbm_df: pl.DataFrame,
        weather_features: list[str] | None = None,
    ) -> pl.DataFrame:
        """Make ensemble predictions.

        Args:
            prophet_df: DataFrame for Prophet (timestamp + weather)
            lightgbm_df: DataFrame for LightGBM (timestamp + engineered features)
            weather_features: Weather features for Prophet regressors

        Returns:
            DataFrame with ensemble predictions and combined uncertainty intervals
        """
        # Get predictions from both models
        prophet_pred = self.prophet_model.predict(prophet_df, weather_features)
        lightgbm_pred = self.lightgbm_model.predict(lightgbm_df)

        # Weighted average of predictions
        ensemble_pred = (
            self.prophet_weight * prophet_pred["predicted_consumption_mw"]
            + self.lightgbm_weight * lightgbm_pred["predicted_consumption_mw"]
        )

        # Combine uncertainty intervals using variance pooling
        # Formula: combined_variance = w1²*σ1² + w2²*σ2² + w1*w2*cov(x1,x2)
        # Assuming independence (cov=0) for simplicity
        prophet_lower = prophet_pred["confidence_lower"]
        prophet_upper = prophet_pred["confidence_upper"]
        lightgbm_lower = lightgbm_pred["confidence_lower"]
        lightgbm_upper = lightgbm_pred["confidence_upper"]

        # Approximate variance from confidence intervals
        # CI width ≈ 3.92 * std (for 95% CI)
        prophet_std = (prophet_upper - prophet_lower) / 3.92
        lightgbm_std = (lightgbm_upper - lightgbm_lower) / 3.92

        # Combined standard deviation
        combined_std = (
            (self.prophet_weight**2 * prophet_std**2)
            + (self.lightgbm_weight**2 * lightgbm_std**2)
        ) ** 0.5

        # Construct result DataFrame
        result = pl.DataFrame(
            {
                "timestamp": prophet_pred["timestamp"],
                "predicted_consumption_mw": ensemble_pred,
                "confidence_lower": ensemble_pred - 1.96 * combined_std,
                "confidence_upper": ensemble_pred + 1.96 * combined_std,
                "prophet_prediction": prophet_pred["predicted_consumption_mw"],
                "lightgbm_prediction": lightgbm_pred["predicted_consumption_mw"],
            }
        )

        return result

    def predict_with_components(
        self,
        prophet_df: pl.DataFrame,
        lightgbm_df: pl.DataFrame,
        weather_features: list[str] | None = None,
    ) -> pl.DataFrame:
        """Make predictions with component breakdown for interpretability.

        This shows:
        - Prophet's seasonal decomposition (trend, weekly, yearly)
        - LightGBM's prediction
        - Final ensemble prediction
        - Combined uncertainty

        Use this endpoint for "why did you predict a peak?" questions.
        """
        # Get Prophet predictions with components
        prophet_pred = self.prophet_model.predict_with_components(
            prophet_df, weather_features
        )

        # Get LightGBM predictions
        lightgbm_pred = self.lightgbm_model.predict(lightgbm_df)

        # Weighted ensemble
        ensemble_pred = (
            self.prophet_weight * prophet_pred["predicted_consumption_mw"]
            + self.lightgbm_weight * lightgbm_pred["predicted_consumption_mw"]
        )

        # Combine uncertainty
        prophet_lower = prophet_pred["confidence_lower"]
        prophet_upper = prophet_pred["confidence_upper"]
        lightgbm_lower = lightgbm_pred["confidence_lower"]
        lightgbm_upper = lightgbm_pred["confidence_upper"]

        prophet_std = (prophet_upper - prophet_lower) / 3.92
        lightgbm_std = (lightgbm_upper - lightgbm_lower) / 3.92

        combined_std = (
            (self.prophet_weight**2 * prophet_std**2)
            + (self.lightgbm_weight**2 * lightgbm_std**2)
        ) ** 0.5

        # Construct result with all components
        result = pl.DataFrame(
            {
                "timestamp": prophet_pred["timestamp"],
                "predicted_consumption_mw": ensemble_pred,
                "confidence_lower": ensemble_pred - 1.96 * combined_std,
                "confidence_upper": ensemble_pred + 1.96 * combined_std,
                # Prophet components
                "prophet_prediction": prophet_pred["predicted_consumption_mw"],
                "prophet_trend": prophet_pred["trend"],
                "prophet_weekly": prophet_pred["weekly"],
                "prophet_yearly": prophet_pred["yearly"],
                "prophet_holidays": prophet_pred["holidays"],
                # LightGBM prediction
                "lightgbm_prediction": lightgbm_pred["predicted_consumption_mw"],
            }
        )

        return result

    def save(self, prophet_path: Path, lightgbm_path: Path) -> None:
        """Save both ensemble components.

        Args:
            prophet_path: Path to save Prophet model
            lightgbm_path: Path to save LightGBM model
        """
        self.prophet_model.save(prophet_path)
        self.lightgbm_model.save(lightgbm_path)

    def get_model_info(self) -> dict[str, str]:
        """Get ensemble model metadata."""
        return {
            "ensemble_version": self.model_version,
            "prophet_version": self.prophet_model.model_version,
            "lightgbm_version": self.lightgbm_model.model_version,
            "prophet_weight": str(self.prophet_weight),
            "lightgbm_weight": str(self.lightgbm_weight),
        }
