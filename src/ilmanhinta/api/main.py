"""FastAPI application exposing precomputed energy consumption predictions."""

import os
import time
from datetime import UTC, datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from ilmanhinta.config import settings
from ilmanhinta.db.prediction_store import fetch_latest_predictions, get_prediction_status
from ilmanhinta.logging import logfire
from ilmanhinta.models.fmi import PredictionOutput

from .analytics import router as analytics_router
from .comparison import router as comparison_router
from .metrics import (
    api_request_duration_seconds,
    api_requests_total,
    model_version_info,
    prediction_value_mw,
    predictions_total,
)

# Initialize FastAPI
app = FastAPI(
    title="Ilmanhinta - Finnish Energy Prediction & Price Forecasting",
    description="API for predicting electricity consumption and prices based on weather data and market forecasts. "
    "Includes comparison framework for ML predictions vs Fingrid forecasts.",
    version="0.3.0",  # Bumped version for price prediction + comparison API
)

# Instrument FastAPI with Logfire for automatic tracing
logfire.instrument_fastapi(app)

# Include routers
app.include_router(analytics_router)
app.include_router(comparison_router)  # NEW: Price prediction comparison API

DEFAULT_MODEL_TYPE = os.getenv("PREDICTION_MODEL_TYPE", "lightgbm")


class PeakPrediction(BaseModel):
    """Peak consumption prediction for next 24h."""

    peak_timestamp: datetime
    peak_consumption_mw: float
    confidence_lower: float
    confidence_upper: float
    model_version: str
    generated_at: datetime


class HealthCheck(BaseModel):
    """Health check response."""

    status: str
    predictions_available: bool
    latest_prediction_timestamp: datetime | None
    model_version: str | None


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):  # type: ignore
    """Record metrics for all requests."""
    start_time = time.time()

    response = await call_next(request)

    # Record metrics
    duration = time.time() - start_time
    api_request_duration_seconds.labels(
        method=request.method,
        endpoint=request.url.path,
    ).observe(duration)

    api_requests_total.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code,
    ).inc()

    return response


def _prediction_status() -> HealthCheck:
    """Build a health response from stored predictions."""
    status = get_prediction_status(model_type=DEFAULT_MODEL_TYPE)
    if status["model_version"]:
        model_version_info.labels(version=status["model_version"]).set(1)
    return HealthCheck(
        status="healthy",
        predictions_available=status["available"],
        latest_prediction_timestamp=status["latest_timestamp"],
        model_version=status["model_version"],
    )


def _ensure_predictions(limit: int) -> list[PredictionOutput]:
    """Fetch predictions or raise a 503 if none exist."""
    predictions = fetch_latest_predictions(limit=limit, model_type=DEFAULT_MODEL_TYPE)
    if not predictions:
        raise HTTPException(status_code=503, detail="Predictions not available yet")
    return predictions


def _observe_predictions(predictions: list[PredictionOutput]) -> None:
    """Update Prometheus metrics for the latest fetch."""
    predictions_total.inc(len(predictions))
    peak = max(predictions, key=lambda p: p.predicted_consumption_mw)
    prediction_value_mw.set(peak.predicted_consumption_mw)
    if peak.model_version:
        model_version_info.labels(version=peak.model_version).set(1)


@app.get("/", response_model=HealthCheck)
async def root() -> HealthCheck:
    """Root endpoint with health check."""
    return _prediction_status()


@app.get("/health", response_model=HealthCheck)
async def health() -> HealthCheck:
    """Health check endpoint."""
    return _prediction_status()


@app.get("/predict/peak", response_model=PeakPrediction)
async def predict_peak_consumption() -> PeakPrediction:
    """
    Predict peak electricity consumption in the next 24 hours.

    Returns the hour with the highest predicted consumption.
    """
    predictions = _ensure_predictions(settings.prediction_horizon_hours)
    _observe_predictions(predictions)

    peak = max(predictions, key=lambda p: p.predicted_consumption_mw)
    logfire.info(f"Peak prediction: {peak.predicted_consumption_mw:.2f} MW at {peak.timestamp}")

    return PeakPrediction(
        peak_timestamp=peak.timestamp,
        peak_consumption_mw=peak.predicted_consumption_mw,
        confidence_lower=peak.confidence_lower,
        confidence_upper=peak.confidence_upper,
        model_version=peak.model_version,
        generated_at=datetime.now(UTC),
    )


@app.get("/predict/forecast", response_model=list[PredictionOutput])
async def predict_24h_forecast() -> list[PredictionOutput]:
    """
    Get full 24-hour hourly consumption forecast.

    Returns hourly predictions for the next 24 hours.
    """
    predictions = _ensure_predictions(settings.prediction_horizon_hours)
    _observe_predictions(predictions)
    logfire.info(f"Serving {len(predictions)} cached hourly predictions")
    return predictions


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    """Prometheus metrics endpoint."""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
