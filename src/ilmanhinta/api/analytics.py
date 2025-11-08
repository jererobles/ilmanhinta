"""Analytics API endpoints powered by TimescaleDB continuous aggregates.

These endpoints provide real-time model performance metrics and insights
without expensive computation - all data is pre-computed and auto-refreshed.
"""

from datetime import datetime
from typing import Any

import psycopg
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ilmanhinta.db.prediction_store import _get_database_url

router = APIRouter(prefix="/analytics", tags=["analytics"])


# Response models
class ModelAccuracyMetrics(BaseModel):
    """Model accuracy metrics for a specific day."""

    day: datetime
    model_type: str
    total_predictions: int
    avg_actual_mw: float
    avg_predicted_mw: float
    bias_mw: float = Field(..., description="Average prediction error (positive = over-prediction)")
    mae_mw: float = Field(..., description="Mean Absolute Error in MW")
    rmse_mw: float = Field(..., description="Root Mean Squared Error in MW")
    mape_percent: float = Field(..., description="Mean Absolute Percentage Error")
    coverage_percent: float = Field(..., description="% of actuals within confidence intervals")
    avg_interval_width_mw: float


class ModelComparison(BaseModel):
    """Comparison of models over the last 24 hours."""

    model_type: str
    prediction_count: int
    mae_mw: float
    rmse_mw: float
    coverage_percent: float
    latest_version: str | None
    latest_prediction_time: datetime


class ConsumptionStats(BaseModel):
    """Hourly consumption statistics."""

    hour: datetime
    avg_consumption_mw: float
    max_consumption_mw: float
    min_consumption_mw: float
    std_consumption_mw: float
    avg_production_mw: float | None
    avg_wind_mw: float | None
    avg_nuclear_mw: float | None
    sample_count: int


# Endpoints
@router.get("/accuracy/daily", response_model=list[ModelAccuracyMetrics])
async def get_daily_accuracy(
    days: int = Query(default=30, ge=1, le=365, description="Number of days to retrieve"),
    model_type: str | None = Query(default=None, description="Filter by model type"),
) -> list[ModelAccuracyMetrics]:
    """Get daily prediction accuracy metrics from continuous aggregates.

    This endpoint returns pre-computed accuracy metrics updated daily.
    No expensive computation at query time!

    Args:
        days: Number of days to retrieve (1-365)
        model_type: Optional filter by model type (lightgbm, prophet, ensemble)

    Returns:
        List of daily accuracy metrics ordered by day
    """
    params: list[Any] = [days]
    where_clause = ""
    if model_type:
        where_clause = "AND model_type = %s"
        params.append(model_type)

    query = f"""
        SELECT
            day,
            model_type,
            total_predictions,
            avg_actual_mw,
            avg_predicted_mw,
            avg_bias_mw,
            avg_mae_mw,
            avg_rmse_mw,
            avg_mape_percent,
            avg_coverage_percent,
            avg_interval_width_mw
        FROM prediction_accuracy_daily
        WHERE day > NOW() - INTERVAL '%s days'
        {where_clause}
        ORDER BY day DESC, model_type
    """

    try:
        with psycopg.connect(_get_database_url()) as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

        return [
            ModelAccuracyMetrics(
                day=row[0],
                model_type=row[1],
                total_predictions=row[2],
                avg_actual_mw=row[3] or 0.0,
                avg_predicted_mw=row[4] or 0.0,
                bias_mw=row[5] or 0.0,
                mae_mw=row[6] or 0.0,
                rmse_mw=row[7] or 0.0,
                mape_percent=row[8] or 0.0,
                coverage_percent=row[9] or 0.0,
                avg_interval_width_mw=row[10] or 0.0,
            )
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/models/comparison", response_model=list[ModelComparison])
async def compare_models() -> list[ModelComparison]:
    """Compare all models over the last 24 hours.

    Uses the pre-computed model_comparison_24h continuous aggregate
    for instant response time.

    Returns:
        List of model performance metrics ordered by MAE (best first)
    """
    query = """
        SELECT
            model_type,
            prediction_count,
            mae_mw,
            rmse_mw,
            coverage_percent,
            latest_version,
            latest_prediction_time
        FROM model_comparison_24h
        ORDER BY mae_mw ASC
    """

    try:
        with psycopg.connect(_get_database_url()) as conn, conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()

        if not rows:
            raise HTTPException(
                status_code=404, detail="No predictions available for the last 24 hours"
            )

        return [
            ModelComparison(
                model_type=row[0],
                prediction_count=row[1],
                mae_mw=row[2],
                rmse_mw=row[3],
                coverage_percent=row[4],
                latest_version=row[5],
                latest_prediction_time=row[6],
            )
            for row in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/consumption/hourly", response_model=list[ConsumptionStats])
async def get_hourly_consumption(
    hours: int = Query(default=24, ge=1, le=168, description="Number of hours to retrieve"),
) -> list[ConsumptionStats]:
    """Get hourly consumption statistics from continuous aggregates.

    Returns pre-computed hourly statistics including consumption, production,
    wind, and nuclear power generation.

    Args:
        hours: Number of hours to retrieve (1-168 = 1 week)

    Returns:
        List of hourly statistics ordered by hour
    """
    query = """
        SELECT
            hour,
            avg_consumption_mw,
            max_consumption_mw,
            min_consumption_mw,
            std_consumption_mw,
            avg_production_mw,
            avg_wind_mw,
            avg_nuclear_mw,
            sample_count
        FROM consumption_hourly
        WHERE hour > NOW() - INTERVAL '%s hours'
        ORDER BY hour DESC
    """

    try:
        with psycopg.connect(_get_database_url()) as conn, conn.cursor() as cur:
            cur.execute(query, (hours,))
            rows = cur.fetchall()

        return [
            ConsumptionStats(
                hour=row[0],
                avg_consumption_mw=row[1],
                max_consumption_mw=row[2],
                min_consumption_mw=row[3],
                std_consumption_mw=row[4],
                avg_production_mw=row[5],
                avg_wind_mw=row[6],
                avg_nuclear_mw=row[7],
                sample_count=row[8],
            )
            for row in rows
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/summary")
async def get_analytics_summary() -> dict[str, Any]:
    """Get a high-level summary of model performance and data availability.

    Returns:
        Summary statistics including best model, data coverage, and health metrics
    """
    try:
        with psycopg.connect(_get_database_url()) as conn, conn.cursor() as cur:
            # Get best performing model from last 24h
            cur.execute(
                """
                SELECT model_type, mae_mw, prediction_count
                FROM model_comparison_24h
                ORDER BY mae_mw ASC
                LIMIT 1
            """
            )
            best_model = cur.fetchone()

            # Get total data points
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM electricity_consumption) as consumption_count,
                    (SELECT COUNT(*) FROM weather_observations) as weather_count,
                    (SELECT COUNT(*) FROM predictions) as predictions_count
            """
            )
            counts = cur.fetchone()

            # Get latest prediction timestamp
            cur.execute(
                """
                SELECT MAX(timestamp) FROM predictions
            """
            )
            latest_prediction = cur.fetchone()[0]

        return {
            "best_model_24h": {
                "model_type": best_model[0] if best_model else None,
                "mae_mw": best_model[1] if best_model else None,
                "predictions": best_model[2] if best_model else 0,
            },
            "data_coverage": {
                "consumption_rows": counts[0] if counts else 0,
                "weather_rows": counts[1] if counts else 0,
                "predictions_rows": counts[2] if counts else 0,
            },
            "latest_prediction_time": latest_prediction,
            "data_freshness_hours": (
                (datetime.utcnow() - latest_prediction).total_seconds() / 3600
                if latest_prediction
                else None
            ),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
