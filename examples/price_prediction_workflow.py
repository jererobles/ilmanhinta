"""Complete workflow: Train price model → Make predictions → Compare vs Fingrid.

This example demonstrates the full hybrid forecasting pipeline:
1. Build training dataset from historical data
2. Train price prediction model
3. Generate predictions using forecasts
4. Compare your predictions vs Fingrid forecasts vs actuals
"""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.ml.price_model import PricePredictionModel
from ilmanhinta.processing.dataset_builder import (
    PredictionDatasetBuilder,
    TrainingDatasetBuilder,
)


async def collect_historical_data_for_training(days: int = 90):
    """Step 1: Collect historical data for model training.

    Fetches:
    - Actual prices (target variable)
    - Actual consumption/production (features)
    - Weather observations (features)
    """
    print("\n" + "=" * 60)
    print("STEP 1: Collecting Historical Data for Training")
    print("=" * 60)

    db = PostgresClient()
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=days)

    print(f"Fetching {days} days of historical data...")
    print(f"From: {start_time}")
    print(f"To:   {end_time}")

    # Fetch from Fingrid (actuals + prices)
    async with FingridClient() as fingrid:
        # Fetch in chunks to avoid rate limits
        chunk_size = 720  # 30 days at a time
        all_prices = []

        current = start_time
        while current < end_time:
            chunk_end = min(current + timedelta(hours=chunk_size), end_time)
            hours = int((chunk_end - current).total_seconds() / 3600)

            print(f"\nFetching chunk: {current} to {chunk_end} ({hours}h)")

            data = await fingrid.fetch_training_data(hours=hours)

            # Store prices
            if data["price_imbalance"]:
                import polars as pl

                prices = pl.DataFrame(
                    [
                        {
                            "time": p.start_time,
                            "price_eur_mwh": p.value,
                        }
                        for p in data["price_imbalance"]
                    ]
                )

                rows = db.insert_prices(prices, price_type="imbalance", source="fingrid")
                print(f"  Stored {rows} price records")
                all_prices.append(prices)

            # Store consumption/production
            if data["consumption"]:
                import polars as pl

                consumption = pl.DataFrame(
                    [
                        {
                            "time": p.start_time,
                            "consumption_mw": c.value if c else None,
                            "production_mw": pr.value if pr else None,
                            "wind_mw": w.value if w else None,
                            "nuclear_mw": n.value if n else None,
                            "net_import_mw": ni.value if ni else None,
                        }
                        for c, pr, w, n, ni in zip(
                            data["consumption"],
                            data["production"],
                            data["wind"],
                            data["nuclear"],
                            data["net_import"],
                            strict=False,
                        )
                    ]
                )

                rows = db.insert_consumption(consumption)
                print(f"  Stored {rows} consumption records")

            current = chunk_end + timedelta(seconds=1)
            await asyncio.sleep(1)  # Rate limiting

    # Fetch weather observations from FMI
    fmi = FMIClient(station_id="101004")  # Helsinki
    weather = fmi.fetch_observations(start_time, end_time)

    if weather.observations:
        import polars as pl

        weather_df = pl.DataFrame(
            [
                {
                    "time": obs.timestamp,
                    "temperature_c": obs.temperature,
                    "wind_speed_ms": obs.wind_speed,
                    "wind_direction": obs.wind_direction,
                    "humidity_percent": obs.humidity,
                    "pressure_hpa": obs.pressure,
                    "cloud_cover": obs.cloud_cover,
                    "precipitation_mm": obs.precipitation,
                }
                for obs in weather.observations
                if obs.temperature is not None
            ]
        )

        rows = db.insert_weather(weather_df, station_id="101004")
        print(f"\nStored {rows} weather observations")

    db.close()

    print("\n✓ Historical data collection complete!")
    print(f"Total prices stored: {sum(len(p) for p in all_prices)}")


def train_price_model(model_dir: Path = Path("models")):
    """Step 2: Train price prediction model on historical data."""
    print("\n" + "=" * 60)
    print("STEP 2: Training Price Prediction Model")
    print("=" * 60)

    db = PostgresClient()

    # Build training dataset
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=60)  # Use 60 days for training

    print("Building training dataset...")
    print(f"Period: {start_time} to {end_time}")

    builder = TrainingDatasetBuilder(db)
    train_df = builder.build(start_time, end_time, resample_freq="1h")

    print(f"\nTraining dataset: {len(train_df)} samples")
    print(f"Features: {len(train_df.columns)} columns")
    print("Sample data:")
    print(train_df.head(3))

    # Train model (try XGBoost, fallback to GradientBoosting)
    try:
        model = PricePredictionModel(model_type="xgboost")
        print("\nUsing XGBoost model")
    except ImportError:
        model = PricePredictionModel(model_type="gradient_boosting")
        print("\nUsing GradientBoosting model (XGBoost not available)")

    print("\nTraining model...")
    metrics = model.train(train_df, validation_split=0.2)

    print("\n✓ Training complete!")
    print(f"Validation MAE: {metrics['val_mae']:.2f} EUR/MWh")
    print(f"Validation RMSE: {metrics['val_rmse']:.2f} EUR/MWh")
    print(f"Validation R²: {metrics['val_r2']:.3f}")
    print(f"Validation MAPE: {metrics['val_mape']:.2f}%")

    # Show top features
    print("\nTop 10 Most Important Features:")
    importance_df = model.get_feature_importance_df()
    for row in importance_df.head(10).iter_rows(named=True):
        print(f"  {row['feature']}: {row['importance']:.4f}")

    # Save model
    model_dir.mkdir(parents=True, exist_ok=True)
    model_path = model_dir / f"price_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib"
    model.save(model_path)
    print(f"\n✓ Model saved to {model_path}")

    db.close()

    return model, model_path


async def generate_predictions(model_path: Path):
    """Step 3: Generate predictions using weather and Fingrid forecasts."""
    print("\n" + "=" * 60)
    print("STEP 3: Generating Price Predictions")
    print("=" * 60)

    db = PostgresClient()

    # Load trained model
    print(f"Loading model from {model_path}...")
    model = PricePredictionModel()
    model.load(model_path)

    # Collect forecast data
    print("\nCollecting forecast data...")

    # Get FMI weather forecasts
    fmi = FMIClient(station_id="101004")
    weather_fcst = fmi.fetch_forecast(hours=48)

    if weather_fcst.observations:
        import polars as pl

        weather_fcst_df = pl.DataFrame(
            [
                {
                    "forecast_time": obs.timestamp,
                    "temperature": obs.temperature,
                    "wind_speed": obs.wind_speed,
                    "pressure": obs.pressure,
                    "cloud_cover": obs.cloud_cover,
                    "humidity": obs.humidity,
                }
                for obs in weather_fcst.observations
                if obs.temperature is not None
            ]
        )

        # Store forecasts
        rows = db.insert_weather_forecast(
            weather_fcst_df, station_id="101004", forecast_model="harmonie"
        )
        print(f"Stored {rows} weather forecast records")

    # Get Fingrid forecasts
    async with FingridClient() as fingrid:
        forecasts = await fingrid.fetch_forecast_data(hours=48)

        # Store consumption forecast
        if forecasts["consumption_forecast_rt"]:
            import polars as pl

            cons_fcst = pl.DataFrame(
                [
                    {"forecast_time": p.start_time, "value": p.value}
                    for p in forecasts["consumption_forecast_rt"]
                ]
            )

            rows = db.insert_fingrid_forecast(
                cons_fcst, forecast_type="consumption", update_frequency="15min"
            )
            print(f"Stored {rows} consumption forecast records")

        # Store production forecast
        if forecasts["production_forecast"]:
            import polars as pl

            prod_fcst = pl.DataFrame(
                [
                    {"forecast_time": p.start_time, "value": p.value}
                    for p in forecasts["production_forecast"]
                ]
            )

            rows = db.insert_fingrid_forecast(
                prod_fcst, forecast_type="production", update_frequency="15min"
            )
            print(f"Stored {rows} production forecast records")

    # Build prediction dataset
    print("\nBuilding prediction dataset...")
    forecast_start = datetime.now(UTC)
    forecast_end = forecast_start + timedelta(hours=48)

    pred_builder = PredictionDatasetBuilder(db)
    pred_df = pred_builder.build(forecast_start, forecast_end)

    if len(pred_df) == 0:
        print("⚠ No forecast data available for prediction!")
        db.close()
        return

    print(f"Prediction dataset: {len(pred_df)} periods")

    # Generate predictions
    print("\nGenerating predictions...")
    predictions = model.predict(pred_df, confidence_interval=0.95)

    print(f"\n✓ Generated {len(predictions)} predictions")
    print(f"Mean predicted price: {predictions['predicted_price_eur_mwh'].mean():.2f} EUR/MWh")
    print(
        f"Min: {predictions['predicted_price_eur_mwh'].min():.2f}, Max: {predictions['predicted_price_eur_mwh'].max():.2f}"
    )

    # Store predictions in database
    rows = db.insert_predictions(
        predictions,
        model_type="production",
        model_version="v1.0_hybrid",
    )
    print(f"\nStored {rows} predictions in database")

    # Show sample predictions
    print("\nSample predictions:")
    print(predictions.head(10))

    db.close()


def compare_predictions():
    """Step 4: Compare ML predictions vs Fingrid forecasts vs actuals."""
    print("\n" + "=" * 60)
    print("STEP 4: Comparing Predictions vs Forecasts vs Actuals")
    print("=" * 60)

    db = PostgresClient()

    # Refresh comparison view
    print("Refreshing comparison materialized view...")
    try:
        db.refresh_materialized_views()
        print("✓ Views refreshed")
    except Exception as e:
        print(f"⚠ Could not refresh views: {e}")
        print("  Make sure the database schema is set up correctly")

    # Get comparison data
    print("\nFetching comparison data...")
    try:
        comparison = db.get_prediction_comparison(limit=50)

        if len(comparison) > 0:
            print(f"\n✓ Found {len(comparison)} comparison records")

            # Show summary statistics
            completed = comparison.filter(
                (comparison["actual_price"].is_not_null())
                & (comparison["ml_prediction"].is_not_null())
            )

            if len(completed) > 0:
                ml_errors = completed["ml_absolute_error"]
                fingrid_errors = completed["fingrid_absolute_error"].filter(
                    completed["fingrid_absolute_error"].is_not_null()
                )

                print("\nML Model Performance:")
                print(f"  Mean Error: {ml_errors.mean():.2f} EUR/MWh")
                print(f"  Median Error: {ml_errors.median():.2f} EUR/MWh")
                print(f"  Max Error: {ml_errors.max():.2f} EUR/MWh")

                if len(fingrid_errors) > 0:
                    print("\nFingrid Forecast Performance:")
                    print(f"  Mean Error: {fingrid_errors.mean():.2f} EUR/MWh")
                    print(f"  Median Error: {fingrid_errors.median():.2f} EUR/MWh")

                    # Calculate win rate
                    wins = len(comparison.filter(comparison["winner"] == "ML"))
                    total = len(completed)
                    win_rate = (wins / total) * 100 if total > 0 else 0

                    print(f"\nWin Rate: {win_rate:.1f}% ({wins}/{total} periods)")

            # Show recent predictions
            print("\nMost recent predictions:")
            print(
                comparison.select(
                    ["time", "ml_prediction", "fingrid_price_forecast", "actual_price", "winner"]
                ).head(10)
            )

        else:
            print("No comparison data available yet.")
            print("\nThis is normal if:")
            print("  1. You just generated predictions (actuals not available yet)")
            print("  2. No historical comparisons exist")
            print("\nWait for actual prices to come in, then run this again!")

    except Exception as e:
        print(f"⚠ Error querying comparison: {e}")

    # Get performance summary
    print("\nFetching performance summary...")
    try:
        summary = db.get_performance_summary(hours_back=168)  # Last week

        if summary:
            print("\nPerformance Summary (last 7 days):")
            for metric, values in summary.items():
                print(f"\n{metric}:")
                if values["ml"] is not None:
                    print(f"  ML Model: {values['ml']:.2f}")
                if values["fingrid"] is not None:
                    print(f"  Fingrid: {values['fingrid']:.2f}")
                if values["improvement_pct"] is not None:
                    print(f"  Improvement: {values['improvement_pct']:+.1f}%")

    except Exception as e:
        print(f"Note: Performance summary not available: {e}")

    db.close()


async def main():
    """Run complete hybrid forecasting workflow."""
    print("=" * 60)
    print("HYBRID FORECASTING WORKFLOW")
    print("Train → Predict → Compare")
    print("=" * 60)

    # Step 1: Collect historical data
    collect_data = input("\nCollect historical data? (y/n): ").lower() == "y"
    if collect_data:
        await collect_historical_data_for_training(days=30)  # Start with 30 days
    else:
        print("Skipping data collection (using existing data)")

    # Step 2: Train model
    train = input("\nTrain new model? (y/n): ").lower() == "y"
    if train:
        model, model_path = train_price_model()
    else:
        # Try to find most recent model
        model_dir = Path("models")
        if model_dir.exists():
            models = sorted(model_dir.glob("price_model_*.joblib"), reverse=True)
            if models:
                model_path = models[0]
                print(f"\nUsing existing model: {model_path}")
            else:
                print("\n⚠ No existing model found. Please train first.")
                return
        else:
            print("\n⚠ No models directory found. Please train first.")
            return

    # Step 3: Generate predictions
    predict = input("\nGenerate predictions? (y/n): ").lower() == "y"
    if predict:
        await generate_predictions(model_path)

    # Step 4: Compare results
    compare = input("\nCompare results? (y/n): ").lower() == "y"
    if compare:
        compare_predictions()

    print("\n" + "=" * 60)
    print("WORKFLOW COMPLETE!")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. Wait for actual prices to come in (usually T+1 or T+2)")
    print("  2. Run comparison again to see ML vs Fingrid performance")
    print("  3. Iterate: retrain with more data, tune hyperparameters")
    print("  4. Set up automated daily retraining (Phase 6)")


if __name__ == "__main__":
    asyncio.run(main())
