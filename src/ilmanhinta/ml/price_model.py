"""Price prediction model for electricity spot prices.

Uses gradient boosting (XGBoost/LightGBM) or ensemble approaches.
Prophet is less suitable for price prediction due to complex feature interactions.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import joblib
import numpy as np
import polars as pl
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

try:
    import xgboost as xgb

    HAS_XGBOOST = True
except ImportError:
    HAS_XGBOOST = False

try:
    import lightgbm as lgb

    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False

from ilmanhinta.logging import get_logger
from ilmanhinta.processing.dataset_builder import get_feature_names, split_train_test_temporal

logger = get_logger(__name__)

# Map training-only column names to the names used during prediction.
TRAIN_TO_PREDICT_COLUMN_MAP: dict[str, str] = {
    # Electricity system metrics
    "consumption_mw": "consumption_forecast_mw",
    "production_mw": "production_forecast_mw",
    "wind_mw": "wind_forecast_mw",
    # Weather observations vs. forecast column names
    "temperature_c": "temperature",
    "wind_speed_ms": "wind_speed",
    "humidity_percent": "humidity",
    "pressure_hpa": "pressure",
}

# Columns/derived features that we cannot reproduce at prediction time with the current
# data sources (e.g. no nuclear or net import forecasts yet). Keep them out of training.
TRAIN_ONLY_FEATURES: list[str] = [
    "nuclear_mw",
    "net_import_mw",
    "nuclear_share",
    "import_dependency",
    "is_importing",
    "is_exporting",
]


class PricePredictionModel:
    """Electricity price prediction model."""

    def __init__(
        self,
        model_type: Literal[
            "xgboost", "lightgbm", "gradient_boosting", "random_forest"
        ] = "xgboost",
        model_params: dict[str, Any] | None = None,
    ):
        """Initialize price prediction model.

        Args:
            model_type: Type of model to use
            model_params: Model hyperparameters (None = use defaults)
        """
        self.model_type = model_type
        self.model_params = model_params or {}
        self.model: Any = None
        self.feature_names: list[str] = []
        self.feature_importance: dict[str, float] = {}
        self.training_metrics: dict[str, float] = {}

    def train(
        self,
        train_df: pl.DataFrame,
        target_col: str = "price_eur_mwh",
        validation_split: float = 0.2,
    ) -> dict[str, float]:
        """Train the price prediction model.

        Args:
            train_df: Training dataset with features + target
            target_col: Name of target column
            validation_split: Fraction for validation (temporal split)

        Returns:
            Dictionary of training metrics
        """
        logger.info(f"Training {self.model_type} model on {len(train_df)} samples")

        # Align column names so they match what PredictionDatasetBuilder produces.
        train_df = self._align_training_columns(train_df)

        # Split into train/validation (temporal)
        train_data, val_data = split_train_test_temporal(train_df, test_size=validation_split)

        # Get features and target
        excluded_cols = (
            list(TRAIN_TO_PREDICT_COLUMN_MAP.keys()) + TRAIN_ONLY_FEATURES + [target_col]
        )
        self.feature_names = get_feature_names(train_df, exclude=excluded_cols)

        X_train = train_data.select(self.feature_names).to_numpy()
        y_train = train_data.select(target_col).to_numpy().ravel()

        X_val = val_data.select(self.feature_names).to_numpy()
        y_val = val_data.select(target_col).to_numpy().ravel()

        logger.info(f"Training set: {len(X_train)} samples, {len(self.feature_names)} features")
        logger.info(f"Validation set: {len(X_val)} samples")

        # Create and train model
        self.model = self._create_model()

        if self.model_type == "xgboost" and HAS_XGBOOST:
            # XGBoost with early stopping
            self.model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
        elif self.model_type == "lightgbm" and HAS_LIGHTGBM:
            # LightGBM with early stopping
            self.model.fit(
                X_train,
                y_train,
                eval_set=[(X_val, y_val)],
                eval_names=["validation"],
                callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)],
            )
        else:
            # Sklearn models
            self.model.fit(X_train, y_train)

        # Calculate validation metrics
        y_pred = self.model.predict(X_val)

        self.training_metrics = {
            "train_samples": len(X_train),
            "val_samples": len(X_val),
            "val_mae": mean_absolute_error(y_val, y_pred),
            "val_rmse": np.sqrt(mean_squared_error(y_val, y_pred)),
            "val_r2": r2_score(y_val, y_pred),
            "val_mape": self._calculate_mape(y_val, y_pred),
        }

        logger.info(f"Validation MAE: {self.training_metrics['val_mae']:.2f} EUR/MWh")
        logger.info(f"Validation RMSE: {self.training_metrics['val_rmse']:.2f} EUR/MWh")
        logger.info(f"Validation R²: {self.training_metrics['val_r2']:.3f}")

        # Calculate feature importance
        self._calculate_feature_importance()

        return self.training_metrics

    def predict(self, df: pl.DataFrame, confidence_interval: float = 0.95) -> pl.DataFrame:
        """Make price predictions.

        Args:
            df: DataFrame with features (from PredictionDatasetBuilder)
            confidence_interval: Confidence level for intervals (0.95 = 95%)

        Returns:
            DataFrame with predictions and confidence intervals
        """
        if self.model is None:
            raise RuntimeError("Model not trained yet. Call train() first.")

        logger.info(f"Predicting prices for {len(df)} periods")

        missing_features = [col for col in self.feature_names if col not in df.columns]
        if missing_features:
            missing_list = ", ".join(sorted(missing_features))
            raise ValueError(
                "Prediction dataset is missing required features: "
                f"{missing_list}. Ensure PredictionDatasetBuilder generates the "
                "same feature set that was used during training."
            )

        # Extract features
        X = df.select(self.feature_names).to_numpy()

        # Get point predictions
        predictions = self.model.predict(X)

        # Calculate confidence intervals
        # For tree-based models, use quantile regression or bootstrap
        # For simplicity, use a heuristic based on validation RMSE
        rmse = self.training_metrics.get("val_rmse", predictions.std())

        # Assuming normal distribution (which isn't perfect for prices)
        from scipy import stats

        z_score = stats.norm.ppf((1 + confidence_interval) / 2)

        lower = predictions - z_score * rmse
        upper = predictions + z_score * rmse

        # Create result DataFrame
        result = pl.DataFrame(
            {
                "prediction_time": df.select(
                    "forecast_time" if "forecast_time" in df.columns else "time"
                ).to_series(),
                "predicted_price_eur_mwh": predictions,
                "confidence_lower": lower,
                "confidence_upper": upper,
            }
        )

        logger.info(
            f"Predictions: mean={predictions.mean():.2f}, "
            f"min={predictions.min():.2f}, max={predictions.max():.2f} EUR/MWh"
        )

        return result

    def save(self, path: str | Path) -> None:
        """Save trained model to disk."""
        if self.model is None:
            raise RuntimeError("No model to save. Train first.")

        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        # Save model and metadata
        model_data = {
            "model": self.model,
            "model_type": self.model_type,
            "feature_names": self.feature_names,
            "feature_importance": self.feature_importance,
            "training_metrics": self.training_metrics,
            "trained_at": datetime.now().isoformat(),
        }

        joblib.dump(model_data, path)
        logger.info(f"Model saved to {path}")

    def load(self, path: str | Path) -> None:
        """Load trained model from disk."""
        path = Path(path)

        if not path.exists():
            raise FileNotFoundError(f"Model file not found: {path}")

        model_data = joblib.load(path)

        self.model = model_data["model"]
        self.model_type = model_data["model_type"]
        self.feature_names = model_data["feature_names"]
        self.feature_importance = model_data.get("feature_importance", {})
        self.training_metrics = model_data.get("training_metrics", {})

        logger.info(f"Model loaded from {path}")

    def _create_model(self) -> Any:
        """Create model instance based on model_type."""
        if self.model_type == "xgboost":
            if not HAS_XGBOOST:
                raise ImportError("xgboost not installed. Install with: pip install xgboost")

            params = {
                "n_estimators": 500,
                "max_depth": 6,
                "learning_rate": 0.05,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "random_state": 42,
                "early_stopping_rounds": 50,
                **self.model_params,
            }

            return xgb.XGBRegressor(**params)

        elif self.model_type == "lightgbm":
            if not HAS_LIGHTGBM:
                raise ImportError("lightgbm not installed. Install with: pip install lightgbm")

            params = {
                "n_estimators": 500,
                "max_depth": 6,
                "learning_rate": 0.05,
                "subsample": 0.8,
                "colsample_bytree": 0.8,
                "random_state": 42,
                "verbose": -1,
                **self.model_params,
            }

            return lgb.LGBMRegressor(**params)

        elif self.model_type == "gradient_boosting":
            params = {
                "n_estimators": 500,
                "max_depth": 6,
                "learning_rate": 0.05,
                "subsample": 0.8,
                "random_state": 42,
                **self.model_params,
            }

            return GradientBoostingRegressor(**params)

        elif self.model_type == "random_forest":
            params = {
                "n_estimators": 200,
                "max_depth": 15,
                "min_samples_split": 5,
                "random_state": 42,
                "n_jobs": -1,
                **self.model_params,
            }

            return RandomForestRegressor(**params)

        else:
            raise ValueError(f"Unknown model type: {self.model_type}")

    def _calculate_feature_importance(self) -> None:
        """Calculate and store feature importance."""
        if self.model is None:
            return

        try:
            if hasattr(self.model, "feature_importances_"):
                importances = self.model.feature_importances_
                self.feature_importance = dict(zip(self.feature_names, importances, strict=False))

                # Log top 10 features
                top_features = sorted(
                    self.feature_importance.items(), key=lambda x: x[1], reverse=True
                )[:10]

                logger.info("Top 10 important features:")
                for feat, imp in top_features:
                    logger.info(f"  {feat}: {imp:.4f}")

        except Exception as e:
            logger.warning(f"Could not calculate feature importance: {e}")

    @staticmethod
    def _calculate_mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Calculate Mean Absolute Percentage Error."""
        # Avoid division by zero
        mask = y_true != 0
        if not mask.any():
            return np.nan

        return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)

    def get_feature_importance_df(self) -> pl.DataFrame:
        """Get feature importance as a DataFrame (for analysis/visualization).

        Returns:
            DataFrame with columns: feature, importance (sorted by importance)
        """
        if not self.feature_importance:
            return pl.DataFrame({"feature": [], "importance": []})

        df = pl.DataFrame(
            {
                "feature": list(self.feature_importance.keys()),
                "importance": list(self.feature_importance.values()),
            }
        ).sort("importance", descending=True)

        return df

    def _align_training_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Duplicate training columns so names match prediction-time features."""
        additional_columns = []
        aligned_names: list[str] = []

        for actual_name, prediction_name in TRAIN_TO_PREDICT_COLUMN_MAP.items():
            if actual_name in df.columns and prediction_name not in df.columns:
                additional_columns.append(pl.col(actual_name).alias(prediction_name))
                aligned_names.append(f"{actual_name}->{prediction_name}")

        if additional_columns:
            df = df.with_columns(additional_columns)
            logger.debug(
                "Aligned %d training columns to prediction naming: %s",
                len(additional_columns),
                aligned_names,
            )

        return df
