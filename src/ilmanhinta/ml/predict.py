"""High-level prediction interface combining data fetching and model inference."""

from pathlib import Path

import polars as pl

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.logging import logfire
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
        logfire.info("Generating 24-hour consumption forecast")

        # 1. Fetch historical electricity data (consumption + production + wind + nuclear)
        # This ensures we have ALL features that were used during training
        historical_data = await self.fingrid_client.fetch_all_electricity_data(hours=24 * 7)
        consumption_df = TemporalJoiner.merge_fingrid_datasets(historical_data)

        # 2. Fetch weather forecast
        weather_forecast = self.fmi_client.fetch_forecast(hours=24)
        forecast_df = TemporalJoiner.fmi_to_polars(weather_forecast.observations)

        if forecast_df.is_empty():
            logfire.error("No weather forecast available")
            return []

        # Rename weather columns to match model training features
        # Note: Keep precipitation_mm as is since the model was trained with that name
        weather_column_mapping = {
            "temperature_c": "temperature",
            "humidity_percent": "humidity",
            "wind_speed_ms": "wind_speed",
            "pressure_hpa": "pressure",
            # precipitation_mm stays as precipitation_mm (model expects this name)
        }
        # Only rename columns that exist
        rename_dict = {k: v for k, v in weather_column_mapping.items() if k in forecast_df.columns}
        if rename_dict:
            forecast_df = forecast_df.rename(rename_dict)

        # 3. Align to hourly resolution
        consumption_df = TemporalJoiner.align_to_hourly(consumption_df)
        forecast_df = TemporalJoiner.align_to_hourly(forecast_df)

        # Ensure both DataFrames share the same schema before vertical concatenation.
        # consumption_df now has: [timestamp, consumption_mw, production_mw, wind_mw, nuclear_mw, net_import_mw]
        # forecast_df has weather columns: [temperature, humidity, wind_speed, wind_direction, pressure, precipitation_mm, cloud_cover]
        # Add missing weather columns to consumption_df as nulls so we can vstack later.
        weather_cols = [
            "temperature",
            "humidity",
            "wind_speed",
            "wind_direction",
            "pressure",
            "precipitation_mm",
            "cloud_cover",
        ]

        for col in weather_cols:
            if col not in consumption_df.columns:
                consumption_df = consumption_df.with_columns(
                    pl.lit(None, dtype=pl.Float64).alias(col)
                )

        logfire.info(
            f"Historical data columns: {consumption_df.columns} - includes all electricity features"
        )

        # 4. For each forecast hour, create features using historical data
        predictions: list[PredictionOutput] = []

        # Get the latest known values for electricity features to use in forecasts
        latest_production = (
            consumption_df.select("production_mw").tail(1)[0, 0]
            if "production_mw" in consumption_df.columns
            else None
        )
        latest_wind = (
            consumption_df.select("wind_mw").tail(1)[0, 0]
            if "wind_mw" in consumption_df.columns
            else None
        )
        latest_nuclear = (
            consumption_df.select("nuclear_mw").tail(1)[0, 0]
            if "nuclear_mw" in consumption_df.columns
            else None
        )
        latest_net_import = (
            consumption_df.select("net_import_mw").tail(1)[0, 0]
            if "net_import_mw" in consumption_df.columns
            else None
        )

        for i in range(len(forecast_df)):
            forecast_row = forecast_df[i]
            forecast_time = forecast_row["timestamp"][0]

            # Create a combined dataframe with history + current forecast point.
            # For electricity features (production, wind, nuclear, net_import), use latest known values
            # since we don't have forecasts for these (yet - could be added later)
            current_row = pl.DataFrame(
                {
                    "timestamp": [forecast_time],
                    "consumption_mw": [None],  # This is what we're predicting
                    "production_mw": [latest_production],  # Use latest known value
                    "wind_mw": [latest_wind],  # Use latest known value
                    "nuclear_mw": [latest_nuclear],  # Use latest known value
                    "net_import_mw": [latest_net_import],  # Use latest known value
                    "temperature": [forecast_row["temperature"][0]],
                    "humidity": [forecast_row["humidity"][0]],
                    "wind_speed": [forecast_row["wind_speed"][0]],
                    "wind_direction": [forecast_row["wind_direction"][0]],
                    "pressure": [forecast_row["pressure"][0]],
                    "precipitation_mm": [forecast_row["precipitation_mm"][0]],
                    "cloud_cover": [forecast_row["cloud_cover"][0]],
                }
            )

            current_df = pl.concat([consumption_df, current_row]).sort("timestamp")

            # Create features (this will use historical consumption for lags)
            # Keep nulls to preserve the forecast row even if target is None.
            features_df = FeatureEngineer.create_all_features(current_df, drop_nulls=False)

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

                # Update consumption_df with the prediction for next iteration.
                # Keep schema identical including electricity features and weather columns.
                consumption_update_row = pl.DataFrame(
                    {
                        "timestamp": [forecast_time],
                        "consumption_mw": [float(row["predicted_consumption_mw"][0])],
                        "production_mw": [latest_production],  # Keep using latest known value
                        "wind_mw": [latest_wind],  # Keep using latest known value
                        "nuclear_mw": [latest_nuclear],  # Keep using latest known value
                        "net_import_mw": [latest_net_import],  # Keep using latest known value
                        "temperature": [None],
                        "humidity": [None],
                        "wind_speed": [None],
                        "wind_direction": [None],
                        "pressure": [None],
                        "precipitation_mm": [None],
                        "cloud_cover": [None],
                    }
                )

                consumption_df = pl.concat([consumption_df, consumption_update_row]).sort(
                    "timestamp"
                )

        logfire.info(f"Generated {len(predictions)} hourly predictions")

        return predictions

    async def find_peak_consumption(
        self, predictions: list[PredictionOutput]
    ) -> PredictionOutput | None:
        """Find the hour with peak predicted consumption in the next 24h."""
        if not predictions:
            return None

        peak = max(predictions, key=lambda p: p.predicted_consumption_mw)
        logfire.info(
            f"Peak consumption predicted at {peak.timestamp}: {peak.predicted_consumption_mw:.2f} MW"
        )

        return peak

    async def close(self) -> None:
        """Close API clients."""
        await self.fingrid_client.close()
