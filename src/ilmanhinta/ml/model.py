"""LightGBM model for electricity consumption prediction."""

import pickle
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import lightgbm as lgb
import numpy as np
import polars as pl

from ilmanhinta.logging import get_logger

logger = get_logger(__name__)


class ConsumptionModel:
    """LightGBM model for predicting electricity consumption."""

    def __init__(self, model_path: Path | None = None) -> None:
        """Initialize model, optionally loading from disk."""
        self.model: lgb.Booster | None = None
        self.feature_names: list[str] = []
        self.model_version: str = "untrained"
        self.trained_at: datetime | None = None

        if model_path and model_path.exists():
            self.load(model_path)

    def train(
        self,
        train_df: pl.DataFrame,
        target_col: str = "consumption_mw",
        params: dict[str, Any] | None = None,
    ) -> dict[str, float]:
        """Train LightGBM model on historical data."""
        logger.info(f"Training model on {len(train_df)} records")

        # Separate features and target
        feature_cols = [col for col in train_df.columns if col not in [target_col, "timestamp"]]

        X = train_df.select(feature_cols).to_numpy()
        y = train_df[target_col].to_numpy()

        self.feature_names = feature_cols

        # Default LightGBM params optimized for time series
        default_params = {
            "objective": "regression",
            "metric": "rmse",
            "boosting_type": "gbdt",
            "num_leaves": 31,
            "learning_rate": 0.05,
            "feature_fraction": 0.9,
            "bagging_fraction": 0.8,
            "bagging_freq": 5,
            "verbose": -1,
            "num_threads": 4,
        }

        if params:
            default_params.update(params)

        # Create LightGBM dataset
        train_data = lgb.Dataset(X, label=y, feature_name=feature_cols)

        logger.info(f"Training with params: {default_params}")

        # Train model
        self.model = lgb.train(
            default_params,
            train_data,
            num_boost_round=1000,
            valid_sets=[train_data],
            callbacks=[
                lgb.early_stopping(stopping_rounds=50),
                lgb.log_evaluation(period=100),
            ],
        )

        self.trained_at = datetime.now(UTC)
        self.model_version = self.trained_at.strftime("%Y%m%d_%H%M%S")

        # Calculate metrics
        predictions = self.model.predict(X)
        mse = float(((predictions - y) ** 2).mean())
        rmse = mse**0.5
        mae = float(abs(predictions - y).mean())

        metrics = {
            "rmse": rmse,
            "mae": mae,
            "mse": mse,
        }

        logger.info(f"Training complete: RMSE={rmse:.2f}, MAE={mae:.2f}")

        return metrics

    def predict(self, features_df: pl.DataFrame) -> pl.DataFrame:
        """Make predictions on new data."""
        if self.model is None:
            raise ValueError("Model not trained or loaded")

        logger.info(f"Making predictions for {len(features_df)} records")

        # Ensure all required features are present
        missing_features = set(self.feature_names) - set(features_df.columns)
        if missing_features:
            raise ValueError(
                f"Missing required features for prediction: {missing_features}. "
                f"Ensure training and inference use the same feature set."
            )

        # Ensure feature order matches training
        X = features_df.select(self.feature_names).to_numpy()

        # Predict
        predictions = self.model.predict(X)

        # Add predictions to dataframe
        result = features_df.with_columns(pl.Series("predicted_consumption_mw", predictions))

        # Add confidence intervals (simple approximation using std)
        # Cast to numpy array to ensure .std() is available
        predictions_arr = np.asarray(predictions)
        std = predictions_arr.std()
        result = result.with_columns(
            [
                (pl.col("predicted_consumption_mw") - 1.96 * std).alias("confidence_lower"),
                (pl.col("predicted_consumption_mw") + 1.96 * std).alias("confidence_upper"),
            ]
        )

        logger.info("Predictions complete")

        return result

    def save(self, path: Path) -> None:
        """Save model to disk."""
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

        logger.info(f"Model saved to {path}")

    def load(self, path: Path) -> None:
        """Load model from disk."""
        with open(path, "rb") as f:
            metadata = pickle.load(f)

        self.model = metadata["model"]
        self.feature_names = metadata["feature_names"]
        self.model_version = metadata["model_version"]
        self.trained_at = metadata["trained_at"]

        logger.info(f"Model loaded from {path} (version: {self.model_version})")

    def get_feature_importance(self) -> dict[str, float]:
        """Get feature importance scores."""
        if self.model is None:
            raise ValueError("Model not trained or loaded")

        importance = self.model.feature_importance(importance_type="gain")
        return dict(zip(self.feature_names, importance.tolist(), strict=False))
