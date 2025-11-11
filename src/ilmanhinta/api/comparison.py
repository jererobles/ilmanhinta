"""Comparison API endpoints for price predictions.

Provides REST API for accessing:
- Latest price predictions
- Comparison of ML vs Fingrid vs Actuals
- Performance metrics and insights
- Win condition analysis
"""

from datetime import UTC, datetime, timedelta
from typing import Literal

import polars as pl
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ilmanhinta.db.postgres_client import PostgresClient
from ilmanhinta.logging import get_logger

router = APIRouter(prefix="/api/v1", tags=["price_predictions"])
logger = get_logger(__name__)


# =====================================================
# Response Models
# =====================================================


class PricePrediction(BaseModel):
    """Single price prediction."""

    prediction_time: datetime = Field(..., description="Time this prediction is for")
    predicted_price_eur_mwh: float = Field(..., description="Predicted price in EUR/MWh")
    confidence_lower: float = Field(..., description="Lower bound of 95% CI")
    confidence_upper: float = Field(..., description="Upper bound of 95% CI")
    model_type: str = Field(..., description="Model identifier")
    model_version: str | None = Field(None, description="Model version")
    generated_at: datetime = Field(..., description="When prediction was generated")


class PredictionComparison(BaseModel):
    """Comparison of ML prediction vs Fingrid forecast vs actual outcome."""

    time: datetime
    ml_prediction: float | None = Field(None, description="Your ML model's prediction")
    ml_lower: float | None = Field(None, description="ML confidence lower bound")
    ml_upper: float | None = Field(None, description="ML confidence upper bound")
    fingrid_forecast: float | None = Field(None, description="Fingrid's official forecast")
    actual_price: float | None = Field(None, description="Actual realized price")
    ml_error: float | None = Field(None, description="ML absolute error (EUR/MWh)")
    ml_error_pct: float | None = Field(None, description="ML percentage error (%)")
    fingrid_error: float | None = Field(None, description="Fingrid absolute error (EUR/MWh)")
    fingrid_error_pct: float | None = Field(None, description="Fingrid percentage error (%)")
    winner: str | None = Field(None, description="Who won: 'ML', 'Fingrid', or 'Tie'")
    model_type: str | None = None
    model_version: str | None = None


class PerformanceMetric(BaseModel):
    """Single performance metric with ML and Fingrid values."""

    metric_name: str
    ml_value: float | None
    fingrid_value: float | None
    improvement_pct: float | None = Field(
        None, description="% improvement of ML over Fingrid (positive = ML better)"
    )


class PerformanceSummary(BaseModel):
    """Performance summary comparing ML vs Fingrid."""

    period_hours: int
    metrics: list[PerformanceMetric]
    total_predictions: int
    predictions_with_actuals: int
    ml_win_rate: float | None = Field(None, description="% of hours where ML beat Fingrid")


class WinCondition(BaseModel):
    """Condition under which ML model wins."""

    condition: str = Field(..., description="Condition description")
    total_cases: int = Field(..., description="Number of occurrences")
    ml_wins: int = Field(..., description="Times ML won")
    win_rate: float = Field(..., description="ML win rate (%)")
    avg_ml_error: float = Field(..., description="Average ML error")
    avg_fingrid_error: float = Field(..., description="Average Fingrid error")


class HourlyPerformance(BaseModel):
    """Performance metrics aggregated by hour."""

    hour: datetime
    num_predictions: int
    ml_avg_error: float | None
    ml_median_error: float | None
    fingrid_avg_error: float | None
    ml_win_rate_pct: float | None
    avg_actual_price: float | None


# =====================================================
# Endpoints
# =====================================================


@router.get("/predictions/latest", response_model=list[PricePrediction])
async def get_latest_predictions(
    hours_ahead: int = Query(48, ge=1, le=168, description="Number of hours to return"),
    model_type: str = Query("production", description="Model type to fetch"),
) -> list[PricePrediction]:
    """Get latest price predictions from the ML model.

    Returns the most recent prediction run for the specified model.

    **Example:**
    ```
    GET /api/v1/predictions/latest?hours_ahead=24&model_type=production
    ```
    """
    db = PostgresClient()

    try:
        # Query latest predictions
        query = """
            WITH latest_run AS (
                SELECT MAX(generated_at) as latest
                FROM price_model_predictions
                WHERE model_type = :model_type
            )
            SELECT
                prediction_time,
                predicted_price_eur_mwh,
                confidence_lower,
                confidence_upper,
                model_type,
                model_version,
                generated_at
            FROM price_model_predictions, latest_run
            WHERE generated_at = latest_run.latest
              AND model_type = :model_type
              AND prediction_time >= NOW()
            ORDER BY prediction_time
            LIMIT :limit
        """

        with db.session() as session:
            from sqlalchemy import text

            result = session.execute(text(query), {"model_type": model_type, "limit": hours_ahead})

            predictions = [
                PricePrediction(
                    prediction_time=row[0],
                    predicted_price_eur_mwh=row[1],
                    confidence_lower=row[2],
                    confidence_upper=row[3],
                    model_type=row[4],
                    model_version=row[5],
                    generated_at=row[6],
                )
                for row in result
            ]

        if not predictions:
            raise HTTPException(
                status_code=404,
                detail=f"No predictions found for model_type='{model_type}'. "
                "Have you generated predictions yet?",
            )

        logger.info(f"Served {len(predictions)} latest predictions for {model_type}")
        return predictions

    finally:
        db.close()


@router.get("/comparison", response_model=list[PredictionComparison])
async def get_comparison(
    hours_back: int = Query(168, ge=1, le=720, description="Hours of historical data to return"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records"),
    only_with_actuals: bool = Query(
        False, description="Only return predictions with actual outcomes"
    ),
) -> list[PredictionComparison]:
    """Get comparison of ML predictions vs Fingrid forecasts vs actual prices.

    This endpoint shows:
    - Your ML model's predictions
    - Fingrid's official forecasts
    - Actual realized prices (when available)
    - Who won (ML vs Fingrid)

    **Example:**
    ```
    GET /api/v1/comparison?hours_back=168&only_with_actuals=true
    ```
    """
    db = PostgresClient()

    try:
        start_time = datetime.now(UTC) - timedelta(hours=hours_back)
        comparison_df = db.get_prediction_comparison(
            start_time=start_time, end_time=datetime.now(UTC), limit=limit
        )

        if comparison_df.is_empty():
            # Try to give helpful error message
            raise HTTPException(
                status_code=404,
                detail="No comparison data available. Make sure you have: "
                "1) Generated ML predictions, "
                "2) Stored Fingrid forecasts, "
                "3) Refreshed materialized views (db.refresh_materialized_views())",
            )

        # Filter if requested
        if only_with_actuals:
            comparison_df = comparison_df.filter(pl.col("actual_price").is_not_null())

            if comparison_df.is_empty():
                raise HTTPException(
                    status_code=404,
                    detail="No predictions with actual outcomes yet. "
                    "Actual prices are typically available T+1 or T+2 hours.",
                )

        # Convert to response models
        comparisons = [
            PredictionComparison(
                time=row["time"],
                ml_prediction=row["ml_prediction"],
                ml_lower=row["ml_lower"],
                ml_upper=row["ml_upper"],
                fingrid_forecast=row["fingrid_price_forecast"],
                actual_price=row["actual_price"],
                ml_error=row["ml_absolute_error"],
                ml_error_pct=row["ml_percentage_error"],
                fingrid_error=row["fingrid_absolute_error"],
                fingrid_error_pct=row["fingrid_percentage_error"],
                winner=row["winner"],
                model_type=row["model_type"],
                model_version=row["model_version"],
            )
            for row in comparison_df.iter_rows(named=True)
        ]

        logger.info(f"Served {len(comparisons)} comparison records")
        return comparisons

    finally:
        db.close()


@router.get("/performance/summary", response_model=PerformanceSummary)
async def get_performance_summary(
    hours_back: int = Query(168, ge=1, le=720, description="Hours to analyze"),
) -> PerformanceSummary:
    """Get performance summary comparing ML model vs Fingrid forecasts.

    Returns key metrics like MAE, win rate, etc.

    **Example:**
    ```
    GET /api/v1/performance/summary?hours_back=168
    ```
    """
    db = PostgresClient()

    try:
        summary_dict = db.get_performance_summary(hours_back=hours_back)

        if not summary_dict:
            raise HTTPException(
                status_code=404,
                detail="No performance data available for the specified period. "
                "Ensure predictions with actual outcomes exist.",
            )

        # Convert to response model
        metrics = [
            PerformanceMetric(
                metric_name=metric_name,
                ml_value=values["ml"],
                fingrid_value=values["fingrid"],
                improvement_pct=values["improvement_pct"],
            )
            for metric_name, values in summary_dict.items()
        ]

        # Get additional stats
        start_time = datetime.now(UTC) - timedelta(hours=hours_back)
        comparison_df = db.get_prediction_comparison(
            start_time=start_time,
            limit=10000,  # Get all
        )

        total_predictions = len(comparison_df)
        with_actuals = len(comparison_df.filter(pl.col("actual_price").is_not_null()))

        # Calculate win rate
        ml_wins = len(comparison_df.filter(pl.col("winner") == "ML"))
        win_rate = (ml_wins / with_actuals * 100) if with_actuals > 0 else None

        return PerformanceSummary(
            period_hours=hours_back,
            metrics=metrics,
            total_predictions=total_predictions,
            predictions_with_actuals=with_actuals,
            ml_win_rate=win_rate,
        )

    finally:
        db.close()


@router.get("/performance/hourly", response_model=list[HourlyPerformance])
async def get_hourly_performance(
    hours_back: int = Query(168, ge=1, le=720, description="Hours of data to return"),
) -> list[HourlyPerformance]:
    """Get hourly aggregated performance metrics.

    Shows performance trends over time.

    **Example:**
    ```
    GET /api/v1/performance/hourly?hours_back=168
    ```
    """
    db = PostgresClient()

    try:
        start_time = datetime.now(UTC) - timedelta(hours=hours_back)
        hourly_df = db.get_hourly_performance(start_time=start_time)

        if hourly_df.is_empty():
            raise HTTPException(status_code=404, detail="No hourly performance data available")

        # Convert to response models
        performance = [
            HourlyPerformance(
                hour=row["hour"],
                num_predictions=row["num_predictions"],
                ml_avg_error=row["ml_avg_error"],
                ml_median_error=row["ml_median_error"],
                fingrid_avg_error=row["fingrid_avg_error"],
                ml_win_rate_pct=row["ml_win_rate_pct"],
                avg_actual_price=row["avg_actual_price"],
            )
            for row in hourly_df.iter_rows(named=True)
        ]

        logger.info(f"Served {len(performance)} hourly performance records")
        return performance

    finally:
        db.close()


@router.get("/insights/win-conditions", response_model=list[WinCondition])
async def get_win_conditions(
    hours_back: int = Query(720, ge=24, le=8760, description="Hours to analyze"),
    group_by: Literal["hour", "day_of_week", "temperature", "wind"] = Query(
        "hour", description="How to group conditions"
    ),
    min_cases: int = Query(10, ge=1, description="Minimum cases to include"),
) -> list[WinCondition]:
    """Find conditions where ML model wins against Fingrid.

    Analyzes historical comparisons to identify patterns where your ML model
    consistently outperforms Fingrid's forecasts.

    **Example:**
    ```
    GET /api/v1/insights/win-conditions?group_by=hour&min_cases=20
    ```

    **Returns conditions like:**
    - "Hour 7-9 AM" - ML wins 75% of the time
    - "Weekends" - ML wins 68% of the time
    - "Temperature < 0°C" - ML wins 72% of the time
    """
    db = PostgresClient()

    try:
        # Build query based on grouping
        if group_by == "hour":
            condition_expr = "EXTRACT(hour FROM time) || ':00'"
            condition_label = "Hour"
        elif group_by == "day_of_week":
            condition_expr = """
                CASE EXTRACT(dow FROM time)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END
            """
            condition_label = "Day"
        elif group_by == "temperature":
            # Would need to join with weather data - simplified for now
            condition_expr = """
                CASE
                    WHEN EXTRACT(hour FROM time) BETWEEN 0 AND 5 THEN 'Night (likely cold)'
                    WHEN EXTRACT(hour FROM time) BETWEEN 6 AND 11 THEN 'Morning'
                    WHEN EXTRACT(hour FROM time) BETWEEN 12 AND 17 THEN 'Afternoon'
                    ELSE 'Evening'
                END
            """
            condition_label = "Period"
        else:  # wind - also simplified
            condition_expr = """
                CASE
                    WHEN EXTRACT(month FROM time) IN (11, 12, 1, 2) THEN 'Winter (high wind)'
                    WHEN EXTRACT(month FROM time) IN (3, 4, 5) THEN 'Spring'
                    WHEN EXTRACT(month FROM time) IN (6, 7, 8) THEN 'Summer (low wind)'
                    ELSE 'Autumn'
                END
            """
            condition_label = "Season"

        query = f"""
            SELECT
                {condition_expr} as condition,
                COUNT(*) as total_cases,
                SUM(CASE WHEN ml_vs_fingrid_winner = 1 THEN 1 ELSE 0 END) as ml_wins,
                SUM(CASE WHEN ml_vs_fingrid_winner = 1 THEN 1 ELSE 0 END)::FLOAT /
                    NULLIF(COUNT(*), 0) * 100 as win_rate,
                AVG(ml_absolute_error) as avg_ml_error,
                AVG(fingrid_absolute_error) as avg_fingrid_error
            FROM prediction_comparison
            WHERE actual_price IS NOT NULL
              AND fingrid_price_forecast IS NOT NULL
              AND time >= NOW() - INTERVAL '{hours_back} hours'
            GROUP BY condition
            HAVING COUNT(*) >= :min_cases
            ORDER BY win_rate DESC
        """

        with db.session() as session:
            from sqlalchemy import text

            result = session.execute(text(query), {"min_cases": min_cases})

            conditions = [
                WinCondition(
                    condition=f"{condition_label}: {row[0]}",
                    total_cases=row[1],
                    ml_wins=row[2],
                    win_rate=row[3],
                    avg_ml_error=row[4],
                    avg_fingrid_error=row[5],
                )
                for row in result
            ]

        if not conditions:
            raise HTTPException(
                status_code=404,
                detail=f"No win conditions found with at least {min_cases} cases. "
                "Try reducing min_cases or increasing hours_back.",
            )

        logger.info(f"Served {len(conditions)} win conditions grouped by {group_by}")
        return conditions

    finally:
        db.close()


@router.post("/admin/refresh-views")
async def refresh_comparison_views() -> dict[str, str]:
    """Refresh materialized views (admin endpoint).

    Call this after:
    - Generating new predictions
    - Actual prices have been updated
    - You want latest comparison data

    **Example:**
    ```
    POST /api/v1/admin/refresh-views
    ```
    """
    db = PostgresClient()

    try:
        db.refresh_materialized_views()
        logger.info("Materialized views refreshed successfully")

        return {
            "status": "success",
            "message": "Comparison views refreshed",
            "timestamp": datetime.now(UTC).isoformat(),
        }

    except Exception as e:
        logger.error(f"Error refreshing views: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to refresh views: {str(e)}") from e

    finally:
        db.close()
