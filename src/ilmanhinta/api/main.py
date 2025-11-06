"""FastAPI application for serving energy consumption predictions."""

import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from loguru import logger
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from ilmanhinta.config import settings
from ilmanhinta.ml.predict import Predictor
from ilmanhinta.models.fmi import PredictionOutput

from .metrics import (
    api_request_duration_seconds,
    api_requests_total,
    model_version_info,
    prediction_value_mw,
    predictions_total,
)

# Initialize FastAPI
app = FastAPI(
    title="Ilmanhinta - Finnish Energy Consumption Prediction",
    description="API for predicting electricity consumption based on weather data",
    version="0.1.0",
)

# Global predictor instance (loaded once at startup)
predictor: Predictor | None = None


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
    model_loaded: bool
    model_version: str | None


@app.on_event("startup")
async def startup_event() -> None:
    """Load model on startup."""
    global predictor

    logger.info("Starting Ilmanhinta API")

    model_path = Path("data/models/model_latest.pkl")
    if not model_path.exists():
        logger.warning(f"Model not found at {model_path}, API will not serve predictions")
        return

    try:
        predictor = Predictor(model_path)
        logger.info(f"Model loaded: version {predictor.model.model_version}")

        # Set model version metric
        model_version_info.labels(version=predictor.model.model_version).set(1)

    except Exception as e:
        logger.error(f"Failed to load model: {e}")


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


@app.get("/", response_model=HealthCheck)
async def root() -> HealthCheck:
    """Root endpoint with health check."""
    return HealthCheck(
        status="healthy",
        model_loaded=predictor is not None and predictor.model.model is not None,
        model_version=predictor.model.model_version if predictor else None,
    )


@app.get("/health", response_model=HealthCheck)
async def health() -> HealthCheck:
    """Health check endpoint."""
    return HealthCheck(
        status="healthy",
        model_loaded=predictor is not None and predictor.model.model is not None,
        model_version=predictor.model.model_version if predictor else None,
    )


@app.get("/predict/peak", response_model=PeakPrediction)
async def predict_peak_consumption() -> PeakPrediction:
    """
    Predict peak electricity consumption in the next 24 hours.

    Returns the hour with the highest predicted consumption.
    """
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        logger.info("Generating 24h peak consumption forecast")

        # Get all predictions for next 24h
        predictions = await predictor.predict_next_24h()

        if not predictions:
            raise HTTPException(status_code=500, detail="Failed to generate predictions")

        # Find peak
        peak = await predictor.find_peak_consumption(predictions)

        if peak is None:
            raise HTTPException(status_code=500, detail="No peak found in predictions")

        # Update metrics
        predictions_total.inc(len(predictions))
        prediction_value_mw.set(peak.predicted_consumption_mw)

        logger.info(
            f"Peak prediction: {peak.predicted_consumption_mw:.2f} MW at {peak.timestamp}"
        )

        return PeakPrediction(
            peak_timestamp=peak.timestamp,
            peak_consumption_mw=peak.predicted_consumption_mw,
            confidence_lower=peak.confidence_lower,
            confidence_upper=peak.confidence_upper,
            model_version=peak.model_version,
            generated_at=datetime.utcnow(),
        )

    except Exception as e:
        logger.error(f"Prediction error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/predict/forecast", response_model=list[PredictionOutput])
async def predict_24h_forecast() -> list[PredictionOutput]:
    """
    Get full 24-hour hourly consumption forecast.

    Returns hourly predictions for the next 24 hours.
    """
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        logger.info("Generating 24h hourly forecast")

        predictions = await predictor.predict_next_24h()

        if not predictions:
            raise HTTPException(status_code=500, detail="Failed to generate predictions")

        # Update metrics
        predictions_total.inc(len(predictions))

        if predictions:
            peak = max(predictions, key=lambda p: p.predicted_consumption_mw)
            prediction_value_mw.set(peak.predicted_consumption_mw)

        logger.info(f"Generated {len(predictions)} hourly predictions")

        return predictions

    except Exception as e:
        logger.error(f"Forecast error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


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
