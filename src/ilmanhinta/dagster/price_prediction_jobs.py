"""Dagster jobs for automated price prediction workflow.

Schedules:
- Hourly: Collect forecasts, actual prices → Generate predictions
- Daily: Refresh comparison views, performance reports
- Weekly: Retrain model on updated historical data
"""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
from dagster import AssetExecutionContext, Definitions, ScheduleDefinition, asset, define_asset_job

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.logging import get_logger
from ilmanhinta.ml.price_model import PricePredictionModel
from ilmanhinta.processing.dataset_builder import (
    PredictionDatasetBuilder,
    TrainingDatasetBuilder,
)

logger = get_logger(__name__)

# =====================================================
# Assets (individual tasks)
# =====================================================


@asset(group_name="data_collection")
def collect_actual_prices(context: AssetExecutionContext) -> dict:
    """Collect actual prices from Fingrid (hourly).

    Fetches the latest actual imbalance prices for training data.
    """
    logger.info("Collecting actual prices from Fingrid")

    async def _fetch():
        async with FingridClient() as client:
            # Fetch last 24 hours (to catch any delayed updates)
            data = await client.fetch_imbalance_price(hours=24)
            return data

    price_data = asyncio.run(_fetch())

    if not price_data:
        context.log.warning("No price data fetched")
        return {"status": "no_data", "count": 0}

    # Store in database
    db = PostgresClient()
    try:
        price_df = pl.DataFrame(
            [{"time": p.start_time, "price_eur_mwh": p.value} for p in price_data]
        )

        rows = db.insert_prices(
            price_df,
            price_type="imbalance",
            source="fingrid",
        )

        context.log.info(f"Stored {rows} price records")
        return {"status": "success", "count": rows}

    finally:
        db.close()


@asset(group_name="data_collection")
def collect_fingrid_power_forecasts(context: AssetExecutionContext) -> dict:
    """Collect Fingrid's official forecasts (hourly).

    Fetches consumption, production, and wind forecasts.
    """
    logger.info("Collecting Fingrid forecasts")

    async def _fetch():
        async with FingridClient() as client:
            # Fetch next 48 hours of forecasts
            forecasts = await client.fetch_forecast_data(hours=48)
            return forecasts

    forecasts = asyncio.run(_fetch())

    db = PostgresClient()
    total_rows = 0

    try:
        generated_at = datetime.now(UTC)

        # Store consumption forecast
        if forecasts.get("consumption_forecast_rt"):
            df = pl.DataFrame(
                [
                    {"forecast_time": p.start_time, "value": p.value}
                    for p in forecasts["consumption_forecast_rt"]
                ]
            )
            rows = db.insert_fingrid_forecast(
                df,
                forecast_type="consumption",
                unit="MW",
                update_frequency="15min",
                generated_at=generated_at,
            )
            total_rows += rows
            context.log.info(f"Stored {rows} consumption forecasts")

        # Store production forecast
        if forecasts.get("production_forecast"):
            df = pl.DataFrame(
                [
                    {"forecast_time": p.start_time, "value": p.value}
                    for p in forecasts["production_forecast"]
                ]
            )
            rows = db.insert_fingrid_forecast(
                df,
                forecast_type="production",
                unit="MW",
                update_frequency="15min",
                generated_at=generated_at,
            )
            total_rows += rows
            context.log.info(f"Stored {rows} production forecasts")

        # Store wind forecast
        if forecasts.get("wind_forecast"):
            df = pl.DataFrame(
                [
                    {"forecast_time": p.start_time, "value": p.value}
                    for p in forecasts["wind_forecast"]
                ]
            )
            rows = db.insert_fingrid_forecast(
                df,
                forecast_type="wind",
                unit="MW",
                update_frequency="15min",
                generated_at=generated_at,
            )
            total_rows += rows
            context.log.info(f"Stored {rows} wind forecasts")

        return {"status": "success", "count": total_rows}

    finally:
        db.close()


@asset(group_name="data_collection")
def collect_fmi_weather_forecasts(context: AssetExecutionContext) -> dict:
    """Collect FMI HARMONIE weather forecasts (every 6 hours).

    Fetches temperature, wind, pressure, etc. for next 48 hours.
    """
    logger.info("Collecting FMI weather forecasts")

    fmi = FMIClient(station_id="101004")  # Helsinki
    weather_fcst = fmi.fetch_forecast(hours=48)

    if not weather_fcst.observations:
        context.log.warning("No weather forecasts available")
        return {"status": "no_data", "count": 0}

    # Store in database
    db = PostgresClient()
    try:
        fcst_df = pl.DataFrame(
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

        rows = db.insert_weather_forecast(
            fcst_df,
            station_id="101004",
            forecast_model="harmonie",
        )

        context.log.info(f"Stored {rows} weather forecast records")
        return {"status": "success", "count": rows}

    finally:
        db.close()


@asset(
    group_name="prediction",
    deps=["collect_fingrid_power_forecasts", "collect_fmi_weather_forecasts"],
)
def generate_price_predictions(context: AssetExecutionContext) -> dict:
    """Generate price predictions using latest forecasts (hourly).

    Uses the production model to predict next 48 hours.
    """
    logger.info("Generating price predictions")

    # Load production model
    model_dir = Path("models")
    model_files = sorted(model_dir.glob("price_model_*.joblib"), reverse=True)

    if not model_files:
        context.log.error("No trained model found. Run training job first.")
        return {"status": "error", "message": "No model found"}

    model_path = model_files[0]
    context.log.info(f"Loading model from {model_path}")

    model = PricePredictionModel()
    model.load(model_path)

    # Build prediction dataset
    db = PostgresClient()
    try:
        forecast_start = datetime.now(UTC)
        forecast_end = forecast_start + timedelta(hours=48)

        pred_builder = PredictionDatasetBuilder(db)
        pred_df = pred_builder.build(forecast_start, forecast_end)

        if len(pred_df) == 0:
            context.log.warning("No forecast data available for prediction")
            return {"status": "no_data", "count": 0}

        # Generate predictions
        predictions = model.predict(pred_df, confidence_interval=0.95)

        # Store predictions
        rows = db.insert_predictions(
            predictions,
            model_type="production",
            model_version="v1.0",
            training_end_date=datetime.now(UTC),
        )

        context.log.info(f"Generated and stored {rows} predictions")
        return {"status": "success", "count": rows}

    finally:
        db.close()


@asset(group_name="analysis", deps=["generate_price_predictions"])
def refresh_comparison_views(context: AssetExecutionContext) -> dict:
    """Refresh materialized views for comparison (daily).

    Updates the prediction_comparison view with latest data.
    """
    logger.info("Refreshing comparison materialized views")

    db = PostgresClient()
    try:
        db.refresh_materialized_views()
        context.log.info("Materialized views refreshed successfully")
        return {"status": "success"}

    except Exception as e:
        context.log.error(f"Error refreshing views: {e}")
        return {"status": "error", "message": str(e)}

    finally:
        db.close()


@asset(group_name="training")
def retrain_price_model(context: AssetExecutionContext) -> dict:
    """Retrain price prediction model on updated historical data (weekly).

    Trains a new model on the last 60 days of data.
    """
    logger.info("Retraining price prediction model")

    db = PostgresClient()
    try:
        # Build training dataset (last 60 days)
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(days=60)

        context.log.info(f"Building training dataset from {start_time} to {end_time}")

        builder = TrainingDatasetBuilder(db)
        train_df = builder.build(start_time, end_time, resample_freq="1h")

        if len(train_df) < 100:
            context.log.error(f"Insufficient training data: {len(train_df)} samples")
            return {"status": "error", "message": "Not enough data"}

        context.log.info(f"Training dataset: {len(train_df)} samples")

        # Train model
        try:
            model = PricePredictionModel(model_type="xgboost")
            context.log.info("Using XGBoost model")
        except ImportError:
            model = PricePredictionModel(model_type="gradient_boosting")
            context.log.info("Using GradientBoosting model (XGBoost not available)")

        metrics = model.train(train_df, validation_split=0.2)

        context.log.info(f"Training complete: MAE={metrics['val_mae']:.2f} EUR/MWh")

        # Save model
        model_dir = Path("models")
        model_dir.mkdir(parents=True, exist_ok=True)

        model_path = model_dir / f"price_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib"
        model.save(model_path)

        context.log.info(f"Model saved to {model_path}")

        return {
            "status": "success",
            "model_path": str(model_path),
            "val_mae": metrics["val_mae"],
            "val_r2": metrics["val_r2"],
            "train_samples": metrics["train_samples"],
        }

    finally:
        db.close()


@asset(group_name="monitoring")
def generate_performance_report(context: AssetExecutionContext) -> dict:
    """Generate performance report comparing ML vs Fingrid (daily).

    Analyzes the last 7 days of predictions.
    """
    logger.info("Generating performance report")

    db = PostgresClient()
    try:
        # Get performance summary for last 7 days
        summary = db.get_performance_summary(hours_back=168)

        if not summary:
            context.log.warning("No performance data available")
            return {"status": "no_data"}

        # Log key metrics
        for metric_name, values in summary.items():
            if values["ml"] and values["fingrid"]:
                improvement = values.get("improvement_pct", 0)
                context.log.info(
                    f"{metric_name}: ML={values['ml']:.2f}, "
                    f"Fingrid={values['fingrid']:.2f}, "
                    f"Improvement={improvement:+.1f}%"
                )

        # Get comparison data for win rate
        comparison_df = db.get_prediction_comparison(
            start_time=datetime.now(UTC) - timedelta(days=7),
            limit=1000,
        )

        with_actuals = comparison_df.filter(pl.col("actual_price").is_not_null())

        if len(with_actuals) > 0:
            ml_wins = len(with_actuals.filter(pl.col("winner") == "ML"))
            win_rate = (ml_wins / len(with_actuals)) * 100

            context.log.info(f"ML Win Rate: {win_rate:.1f}% ({ml_wins}/{len(with_actuals)})")

            # Alert if performance degraded
            if win_rate < 45:
                context.log.warning(
                    f"⚠ ML win rate below 45% ({win_rate:.1f}%). Consider retraining."
                )

        return {"status": "success", "summary": summary}

    finally:
        db.close()


# =====================================================
# Jobs (group assets into workflows)
# =====================================================


# Hourly job: Collect data + generate predictions
hourly_prediction_job = define_asset_job(
    name="hourly_prediction_job",
    selection=[
        "collect_actual_prices",
        "collect_fingrid_power_forecasts",
        "collect_fmi_weather_forecasts",
        "generate_price_predictions",
    ],
    description="Collect forecasts and actual prices, then generate predictions",
)

# Daily job: Refresh views + performance report
daily_analysis_job = define_asset_job(
    name="daily_analysis_job",
    selection=["refresh_comparison_views", "generate_performance_report"],
    description="Refresh comparison views and generate performance reports",
)

# Weekly job: Retrain model
weekly_retraining_job = define_asset_job(
    name="weekly_retraining_job",
    selection=["retrain_price_model"],
    description="Retrain price prediction model on updated historical data",
)


# =====================================================
# Schedules
# =====================================================


hourly_schedule = ScheduleDefinition(
    job=hourly_prediction_job,
    cron_schedule="0 * * * *",  # Every hour at :00
    description="Run hourly: collect forecasts + generate predictions",
)

daily_schedule = ScheduleDefinition(
    job=daily_analysis_job,
    cron_schedule="0 1 * * *",  # Daily at 1:00 AM
    description="Run daily: refresh views + performance report",
)

weekly_schedule = ScheduleDefinition(
    job=weekly_retraining_job,
    cron_schedule="0 2 * * 0",  # Weekly on Sunday at 2:00 AM
    description="Run weekly: retrain price prediction model",
)


# =====================================================
# Definitions (expose to Dagster)
# =====================================================


defs = Definitions(
    assets=[
        collect_actual_prices,
        collect_fingrid_power_forecasts,
        collect_fmi_weather_forecasts,
        generate_price_predictions,
        refresh_comparison_views,
        retrain_price_model,
        generate_performance_report,
    ],
    jobs=[hourly_prediction_job, daily_analysis_job, weekly_retraining_job],
    schedules=[hourly_schedule, daily_schedule, weekly_schedule],
)
