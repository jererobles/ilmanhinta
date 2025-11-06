"""High-level prediction interface combining data fetching and model inference."""

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
from loguru import logger

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.ml.model import ConsumptionModel
from ilmanhinta.models.fmi import PredictionOutput
from ilmanhinta.processing.features import FeatureEngineer
from ilmanhinta.processing.joins import TemporalJoiner


class Predictor:
    """High-level interface for making consumption predictions."""

    def __init__(self, model_path: Path) -> None:
        """Initialize predictor with trained model."""
        self.model = ConsumptionModel(model_path)
        self.fingrid_client = FingridClient()
        self.fmi_client = FMIClient()

    async def predict_next_24h(self) -> list[PredictionOutput]:
        """
        Predict electricity consumption for the next 24 hours.

        Returns hourly predictions with confidence intervals.
        """
        logger.info("Generating 24-hour consumption forecast")

        # 1. Fetch historical consumption (for lag features)
        historical_consumption = await self.fingrid_client.fetch_realtime_consumption(hours=24 * 7)
        consumption_df = TemporalJoiner.fingrid_to_polars(historical_consumption)

        # 2. Fetch weather forecast
        weather_forecast = self.fmi_client.fetch_forecast(hours=24)
        forecast_df = TemporalJoiner.fmi_to_polars(weather_forecast.observations)

        if forecast_df.is_empty():
            logger.error("No weather forecast available")
            return []

        # 3. Align to hourly resolution
        consumption_df = TemporalJoiner.align_to_hourly(consumption_df)
        forecast_df = TemporalJoiner.align_to_hourly(forecast_df)

        # 4. For each forecast hour, create features using historical data
        predictions: list[PredictionOutput] = []

        for i in range(len(forecast_df)):
            forecast_row = forecast_df[i]
            forecast_time = forecast_row["timestamp"][0]

            # Create a combined dataframe with history + current forecast point
            current_df = pl.concat([
                consumption_df,
                pl.DataFrame({
                    "timestamp": [forecast_time],
                    "consumption_mw": [None],  # This is what we're predicting
                    "temperature": [forecast_row["temperature"][0]],
                    "humidity": [forecast_row["humidity"][0]],
                    "wind_speed": [forecast_row["wind_speed"][0]],
                    "wind_direction": [forecast_row["wind_direction"][0]],
                    "pressure": [forecast_row["pressure"][0]],
                    "precipitation": [forecast_row["precipitation"][0]],
                    "cloud_cover": [forecast_row["cloud_cover"][0]],
                })
            ]).sort("timestamp")

            # Create features (this will use historical consumption for lags)
            features_df = FeatureEngineer.create_all_features(current_df)

            # Get the last row (our forecast point) - it should have lag features from history
            if not features_df.is_empty():
                forecast_features = features_df.tail(1)

                # Make prediction
                result = self.model.predict(forecast_features)

                if not result.is_empty():
                    row = result[0]
                    predictions.append(
                        PredictionOutput(
                            timestamp=forecast_time,
                            predicted_consumption_mw=float(row["predicted_consumption_mw"][0]),
                            confidence_lower=float(row["confidence_lower"][0]),
                            confidence_upper=float(row["confidence_upper"][0]),
                            model_version=self.model.model_version,
                        )
                    )

                # Update consumption_df with the prediction for next iteration
                consumption_df = pl.concat([
                    consumption_df,
                    pl.DataFrame({
                        "timestamp": [forecast_time],
                        "consumption_mw": [float(row["predicted_consumption_mw"][0])],
                    })
                ]).sort("timestamp")

        logger.info(f"Generated {len(predictions)} hourly predictions")

        return predictions

    async def find_peak_consumption(
        self, predictions: list[PredictionOutput]
    ) -> PredictionOutput | None:
        """Find the hour with peak predicted consumption in the next 24h."""
        if not predictions:
            return None

        peak = max(predictions, key=lambda p: p.predicted_consumption_mw)
        logger.info(
            f"Peak consumption predicted at {peak.timestamp}: {peak.predicted_consumption_mw:.2f} MW"
        )

        return peak

    async def close(self) -> None:
        """Close API clients."""
        await self.fingrid_client.close()
