"""Dagster jobs for automated price prediction workflow.

Schedules:
- Hourly: Collect forecasts, actual prices → Generate predictions
- Daily: Refresh comparison views, performance reports
- Weekly: Retrain model on updated historical data
"""

from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl
from dagster import (
    AssetExecutionContext,
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    asset,
    define_asset_job,
    sensor,
)
from sqlalchemy import text

from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.logging import get_logger
from ilmanhinta.ml.price_model import PricePredictionModel
from ilmanhinta.processing.dataset_builder import (
    PredictionDatasetBuilder,
    TrainingDatasetBuilder,
)
from ilmanhinta.scripts.backfill_data import BackfillService

logger = get_logger(__name__)

PRICE_HISTORY_HOURS = 72
PRICE_RETRO_PREDICTION_HOURS = 48

# =====================================================
# Assets (individual tasks)
# =====================================================


@asset(group_name="price_data_collection")
async def price_collect_actual_prices(context: AssetExecutionContext) -> dict[str, Any]:
    """Collect actual prices from Fingrid (hourly).

    Fetches the latest actual imbalance prices for training data.
    """
    logger.info("Collecting actual prices from Fingrid via BackfillService")
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=PRICE_HISTORY_HOURS)
    async with BackfillService() as svc:
        _df, rows = await svc.ingest_prices(start_time, end_time)
    context.log.info(f"Stored {rows} price records")
    return {"status": "success", "count": rows}


@asset(group_name="price_data_collection")
async def price_collect_power_forecasts(context: AssetExecutionContext) -> dict[str, Any]:
    """Collect Fingrid's official forecasts (hourly).

    Fetches consumption, production, and wind forecasts.
    """
    logger.info("Collecting Fingrid forecasts")

    async with BackfillService() as svc:
        start = datetime.now(UTC)
        end = start + timedelta(hours=48)
        inserted = await svc.ingest_forecasts(start, end)
    context.log.info(f"Stored {inserted} Fingrid forecasts")
    return {"status": "success", "count": inserted}


@asset(group_name="price_data_collection")
async def price_collect_weather_forecasts(context: AssetExecutionContext) -> dict[str, Any]:
    """Collect FMI HARMONIE weather forecasts (every 6 hours).

    Fetches temperature, wind, pressure, etc. for next 48 hours.
    """
    logger.info("Collecting FMI weather forecasts via BackfillService")
    async with BackfillService() as svc:
        _df, rows = await svc.ingest_weather_forecast(horizon_hours=48)
    context.log.info(f"Stored {rows} weather forecast records")
    return {"status": "success", "count": rows}


@asset(group_name="price_data_collection")
async def price_backfill_recent_history(context: AssetExecutionContext) -> dict[str, int]:
    """Backfill the last few days of actuals/observations needed for bootstrap."""

    history_hours = PRICE_HISTORY_HOURS
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=history_hours)
    summary: dict[str, int] = {}

    logger.info(
        "Backfilling price history window %s -> %s", start_time.isoformat(), end_time.isoformat()
    )

    async with BackfillService() as svc:
        actuals_df, actual_rows = await svc.ingest_actuals(start_time, end_time)
        summary["actual_rows"] = actual_rows

        weather_df, weather_rows = await svc.ingest_weather(start_time, end_time)
        summary["weather_rows"] = weather_rows

        price_df, price_rows = await svc.ingest_prices(start_time, end_time)
        summary["price_rows"] = price_rows

        forecast_rows = await svc.ingest_forecasts(start_time, end_time)
        summary["forecast_rows"] = forecast_rows

    context.log.info(
        "Backfill summary: actual=%s weather=%s price=%s forecasts=%s",
        summary.get("actual_rows", 0),
        summary.get("weather_rows", 0),
        summary.get("price_rows", 0),
        summary.get("forecast_rows", 0),
    )

    return summary


@asset(
    group_name="price_prediction",
    deps=["price_collect_power_forecasts", "price_collect_weather_forecasts"],
)
def price_generate_predictions(context: AssetExecutionContext) -> dict[str, Any]:
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


@asset(
    group_name="price_prediction",
    deps=["price_backfill_recent_history"],
)
def price_generate_historical_predictions(context: AssetExecutionContext) -> dict[str, Any]:
    """Generate historical predictions on recent actual data to seed analytics."""

    history_hours = PRICE_RETRO_PREDICTION_HOURS
    history_end = datetime.now(UTC)
    history_start = history_end - timedelta(hours=history_hours)

    logger.info(
        "Generating historical price predictions for %d hours (%s -> %s)",
        history_hours,
        history_start,
        history_end,
    )

    model_dir = Path("models")
    model_files = sorted(model_dir.glob("price_model_*.joblib"), reverse=True)
    if not model_files:
        context.log.error("No trained model found for historical prediction")
        return {"status": "error", "message": "No model found"}

    model_path = model_files[0]
    model = PricePredictionModel()
    model.load(model_path)

    db = PostgresClient()
    try:
        builder = TrainingDatasetBuilder(db)
        history_df = builder.build(history_start, history_end, resample_freq="1h")

        if history_df.is_empty():
            context.log.warning("No historical training data available for prediction backfill")
            return {"status": "no_data", "count": 0}

        aligned_df = model._align_training_columns(history_df)
        predictions = model.predict(aligned_df, confidence_interval=0.95)
        if predictions.is_empty():
            context.log.warning("Model produced no historical predictions")
            return {"status": "no_data", "count": 0}

        filtered = predictions.filter(
            (pl.col("prediction_time") >= history_start)
            & (pl.col("prediction_time") <= history_end)
        )
        if filtered.is_empty():
            context.log.warning("Historical prediction window produced no records after filtering")
            return {"status": "no_data", "count": 0}

        rows = db.insert_predictions(
            filtered,
            model_type="production",
            model_version="v1.0",
            generated_at=history_end,
            training_end_date=history_end,
        )

        context.log.info("Inserted %d historical predictions", rows)
        return {"status": "success", "count": rows}

    finally:
        db.close()


@asset(
    group_name="price_analysis",
    deps=["price_generate_historical_predictions", "price_generate_predictions"],
)
def price_refresh_prediction_views(context: AssetExecutionContext) -> dict[str, Any]:
    """Refresh materialized views for price analytics (daily).

    Updates the price_prediction_comparison view and downstream aggregates.
    """
    logger.info("Refreshing comparison materialized views")

    db = PostgresClient()
    try:
        db.refresh_price_analytics_views()
        context.log.info("Materialized views refreshed successfully")
        return {"status": "success"}

    except Exception as e:
        context.log.error(f"Error refreshing views: {e}")
        return {"status": "error", "message": str(e)}

    finally:
        db.close()


@asset(group_name="price_training")
def price_retrain_model(context: AssetExecutionContext) -> dict[str, Any]:
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


@asset(group_name="price_monitoring")
def price_generate_performance_report(context: AssetExecutionContext) -> dict[str, Any]:
    """Generate performance report comparing ML vs Fingrid (daily).

    Analyzes the last 7 days of predictions.
    """
    logger.info("Generating performance report")

    db = PostgresClient()
    try:
        # Get performance summary for last 7 days
        summary = db.get_price_performance_summary(hours_back=168)

        if not summary:
            context.log.warning("No performance data available")
            return {"status": "no_data"}

        # Log key metrics
        for metric_name, values in summary.items():
            if isinstance(values, dict) and values.get("ml") and values.get("fingrid"):
                improvement = values.get("improvement_pct", 0)
                context.log.info(
                    f"{metric_name}: ML={values['ml']:.2f}, "
                    f"Fingrid={values['fingrid']:.2f}, "
                    f"Improvement={improvement:+.1f}%"
                )

        # Get comparison data for win rate
        comparison_df = db.get_price_prediction_comparison(
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
price_hourly_prediction_job = define_asset_job(
    name="price_hourly_prediction_job",
    selection=[
        price_collect_actual_prices,
        price_collect_power_forecasts,
        price_collect_weather_forecasts,
        price_generate_predictions,
    ],
    description="Collect forecasts and actual prices, then generate predictions",
)

# Daily job: Refresh views + performance report
price_daily_analysis_job = define_asset_job(
    name="price_daily_analysis_job",
    selection=[price_refresh_prediction_views, price_generate_performance_report],
    description="Refresh comparison views and generate performance reports",
)

# Weekly job: Retrain model
price_weekly_retraining_job = define_asset_job(
    name="price_weekly_retraining_job",
    selection=[price_retrain_model],
    description="Retrain price prediction model on updated historical data",
)

# Bootstrap job: run everything needed for dashboard parity
price_bootstrap_job = define_asset_job(
    name="price_bootstrap_job",
    selection=[
        price_backfill_recent_history,
        price_collect_actual_prices,
        price_collect_power_forecasts,
        price_collect_weather_forecasts,
        price_generate_historical_predictions,
        price_generate_predictions,
        price_refresh_prediction_views,
    ],
    description="Full price pipeline bootstrap to populate charts and analytics",
)


# =====================================================
# Schedules
# =====================================================


price_hourly_schedule = ScheduleDefinition(
    job=price_hourly_prediction_job,
    cron_schedule="0 * * * *",  # Every hour at :00
    description="Run hourly: collect forecasts + generate predictions",
)

price_daily_schedule = ScheduleDefinition(
    job=price_daily_analysis_job,
    cron_schedule="0 1 * * *",  # Daily at 1:00 AM
    description="Run daily: refresh views + performance report",
)

price_weekly_schedule = ScheduleDefinition(
    job=price_weekly_retraining_job,
    cron_schedule="0 2 * * 0",  # Weekly on Sunday at 2:00 AM
    description="Run weekly: retrain price prediction model",
)

# =====================================================
# Sensors
# =====================================================


@sensor(
    job=price_bootstrap_job,
    minimum_interval_seconds=600,
    default_status=DefaultSensorStatus.RUNNING,
)
def price_bootstrap_sensor(
    context: SensorEvaluationContext,
) -> Generator[RunRequest, None, None]:
    """Automatically bootstrap price predictions when missing or stale."""
    db = PostgresClient()
    try:
        with db.session() as session:
            has_recent_predictions = bool(
                session.execute(
                    text(
                        """
                        SELECT EXISTS (
                            SELECT 1
                            FROM price_model_predictions
                            WHERE generated_at >= NOW() - INTERVAL '6 hours'
                        )
                        """
                    )
                ).scalar()
            )
            comparison_rows = (
                session.execute(
                    text(
                        """
                    SELECT COUNT(*)
                    FROM price_prediction_comparison
                    WHERE time >= NOW() - INTERVAL '12 hours'
                    """
                    )
                ).scalar()
                or 0
            )
    except Exception as exc:
        context.log.error(f"Error checking price prediction status: {exc}")
        return
    finally:
        db.close()

    has_recent_view = comparison_rows > 0

    if not has_recent_predictions or not has_recent_view:
        reason_parts = []
        if not has_recent_predictions:
            reason_parts.append("no recent price predictions")
        if not has_recent_view:
            reason_parts.append("comparison view empty")
        reason = ", ".join(reason_parts)

        context.log.warning(
            "Price bootstrap required (%s) - triggering job", reason or "unknown reason"
        )
        yield RunRequest(
            run_key=f"price_bootstrap_{datetime.now(UTC).isoformat()}",
            tags={"auto_triggered": "true", "reason": reason or "price_bootstrap"},
        )
    else:
        context.log.info("Price predictions and comparison view look healthy")


# =====================================================
# Definitions (expose to Dagster)
# =====================================================


"""Note: Unified Definitions are composed in ilmanhinta.dagster.__init__.py.

This module intentionally does not export a module-level `defs` to avoid
multiple Dagster entry points. Import assets/jobs into the aggregator instead.
"""
