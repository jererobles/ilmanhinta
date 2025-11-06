"""FastAPI application for serving energy consumption predictions."""

import asyncio
import os
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, Response
from loguru import logger
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel

from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.config import settings
from ilmanhinta.ml.predict import Predictor
from ilmanhinta.models.fmi import PredictionOutput
from ilmanhinta.processing.joins import TemporalJoiner
from ilmanhinta.scripts.bootstrap_model import (
    ensure_model_exists,
    train_and_save_model_for_last_30_days,
)

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
_background_tasks: list[asyncio.Task] = []
_enable_in_process_scheduler = os.getenv("ENABLE_IN_PROCESS_SCHEDULER", "false").lower() in (
    "1",
    "true",
    "yes",
)


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


async def _load_model_from_path(model_path: Path) -> None:
    """Instantiate the global predictor from a saved model path and set metrics."""
    global predictor
    # Close previous predictor's clients if present to avoid leaks
    try:
        if predictor is not None:
            await predictor.close()
    except Exception:
        pass
    predictor = Predictor(model_path)
    logger.info(f"Model loaded: version {predictor.model.model_version}")
    model_version_info.labels(version=predictor.model.model_version).set(1)


async def _bootstrap_then_load() -> None:
    """Ensure a model exists (training if missing) and load it."""
    try:
        path = await ensure_model_exists()
        await _load_model_from_path(path)
    except Exception as e:
        logger.error(f"Bootstrap training failed: {e}")


async def _daily_retrain_loop() -> None:
    """Retrain the model daily around 02:00 UTC and hot-reload predictor."""
    while True:
        try:
            now = datetime.utcnow()
            # Next 02:00 UTC
            target = now.replace(hour=2, minute=0, second=0, microsecond=0)
            if target <= now:
                from datetime import timedelta

                target = target + timedelta(days=1)
            delay = (target - now).total_seconds()
            logger.info(f"Daily retrain scheduled in {int(delay)} seconds (at {target} UTC)")
            await asyncio.sleep(delay)

            logger.info("Starting daily retraining...")
            path = await train_and_save_model_for_last_30_days()
            await _load_model_from_path(path)
            logger.info("Daily retraining complete and model reloaded")
        except Exception as e:
            logger.error(f"Daily retraining error: {e}")
            # Wait a bit before retrying to avoid tight loop
            await asyncio.sleep(300)


async def _hourly_ingestion_loop() -> None:
    """Ingest recent raw data hourly into `/app/data/raw` for observability/debug.

    This is a lightweight complement to the daily retraining which fetches full windows.
    """
    raw_dir = Path("data/raw")
    raw_dir.mkdir(parents=True, exist_ok=True)
    while True:
        try:
            now = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
            logger.info("Starting hourly ingestion of recent data")

            async with FingridClient() as fg:
                cons = await fg.fetch_realtime_consumption(hours=2)
            fmi = FMIClient()
            obs = fmi.fetch_realtime_observations(hours=2)

            cdf = TemporalJoiner.fingrid_to_polars(cons)
            wdf = TemporalJoiner.fmi_to_polars(obs.observations)

            c_path = raw_dir / f"fingrid_consumption_{now:%Y%m%d_%H}.parquet"
            w_path = raw_dir / f"fmi_weather_{now:%Y%m%d_%H}.parquet"
            if not cdf.is_empty():
                cdf.write_parquet(c_path)
            if not wdf.is_empty():
                wdf.write_parquet(w_path)

            logger.info(
                f"Hourly ingestion saved to {c_path.name if cdf.height else '(no consumption)'} and "
                f"{w_path.name if wdf.height else '(no weather)'}"
            )
        except Exception as e:
            logger.error(f"Hourly ingestion error: {e}")
        # Sleep roughly one hour between ingestions
        await asyncio.sleep(3600)


async def _model_reload_watcher(model_path: Path, interval_seconds: int = 300) -> None:
    """Watch model file and hot‑reload predictor when it changes."""
    last_mtime = None
    while True:
        try:
            if model_path.exists():
                mtime = model_path.stat().st_mtime
                if last_mtime is None:
                    last_mtime = mtime
                elif mtime != last_mtime:
                    logger.info("Model file changed; reloading predictor")
                    await _load_model_from_path(model_path)
                    last_mtime = mtime
        except Exception as e:
            logger.error(f"Model watcher error: {e}")
        await asyncio.sleep(interval_seconds)


@app.on_event("startup")
async def startup_event() -> None:
    """Load or bootstrap model on startup and start background schedules."""
    logger.info("Starting Ilmanhinta API")

    model_path = Path("data/models/model_latest.pkl")

    # Try loading existing model; if missing, bootstrap in background.
    if model_path.exists():
        try:
            await _load_model_from_path(model_path)
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
    else:
        logger.warning(f"Model not found at {model_path}; starting background bootstrap training")
        _background_tasks.append(asyncio.create_task(_bootstrap_then_load()))

    # Start daily retraining loop
    if _enable_in_process_scheduler:
        _background_tasks.append(asyncio.create_task(_daily_retrain_loop()))
        _background_tasks.append(asyncio.create_task(_hourly_ingestion_loop()))

    # Model file watcher (enabled by default to pick up worker-trained models)
    _background_tasks.append(asyncio.create_task(_model_reload_watcher(model_path)))


@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Cancel background tasks on shutdown."""
    for t in _background_tasks:
        t.cancel()


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

        logger.info(f"Peak prediction: {peak.predicted_consumption_mw:.2f} MW at {peak.timestamp}")

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
        raise HTTPException(status_code=500, detail=str(e)) from None


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
        raise HTTPException(status_code=500, detail=str(e)) from None


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    """Prometheus metrics endpoint."""
    return PlainTextResponse(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> HTMLResponse:
    """Lightweight public dashboard with a 24h forecast chart (read-only)."""
    html = """
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="utf-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1" />
      <title>Ilmanhinta – 24h Forecast</title>
      <style>
        body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji"; margin: 16px; color: #111; }
        .wrap { max-width: 980px; margin: 0 auto; }
        h1 { font-size: 1.25rem; margin: 0 0 8px; }
        .muted { color: #666; font-size: 0.9rem; }
        .card { border: 1px solid #eee; border-radius: 8px; padding: 12px; box-shadow: 0 1px 2px rgba(0,0,0,0.03); }
        canvas { max-width: 100%; }
      </style>
    </head>
    <body>
      <div class="wrap">
        <h1>Ilmanhinta – 24h Electricity Consumption Forecast</h1>
        <p class="muted">Auto-updates on refresh. Data source: Fingrid + FMI. Model: LightGBM.</p>
        <div class="card">
          <canvas id="chart" height="120"></canvas>
        </div>
        <p class="muted" id="meta"></p>
      </div>
      <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
      <script>
      async function run() {
        const resp = await fetch('/predict/forecast');
        if (!resp.ok) throw new Error('Failed to load forecast');
        const data = await resp.json();
        const labels = data.map(p => new Date(p.timestamp));
        const values = data.map(p => p.predicted_consumption_mw);
        const peak = data.reduce((a,b) => a.predicted_consumption_mw > b.predicted_consumption_mw ? a : b, data[0]);
        const ctx = document.getElementById('chart').getContext('2d');
        const gradient = ctx.createLinearGradient(0,0,0,200);
        gradient.addColorStop(0,'rgba(33,150,243,0.25)');
        gradient.addColorStop(1,'rgba(33,150,243,0.02)');
        new Chart(ctx, {
          type: 'line',
          data: {
            labels,
            datasets: [{
              label: 'Predicted consumption (MW)',
              data: values,
              borderColor: '#2196f3',
              backgroundColor: gradient,
              fill: true,
              tension: 0.25,
              pointRadius: 0
            }]
          },
          options: {
            animation: false,
            responsive: true,
            scales: {
              x: { type: 'time', time: { unit: 'hour' }, grid: { display: false } },
              y: { beginAtZero: false, ticks: { callback: v => v.toLocaleString() + ' MW' } }
            },
            plugins: { legend: { display: false } }
          }
        });
        const meta = document.getElementById('meta');
        meta.textContent = `Peak: ${peak.predicted_consumption_mw.toFixed(0)} MW at ${new Date(peak.timestamp).toLocaleString()}`;
      }
      run().catch(err => {
        document.querySelector('.wrap').insertAdjacentHTML('beforeend', `<p style="color:#b00020">${err}</p>`);
      });
      </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/plot/forecast.svg")
async def plot_forecast_svg() -> Response:
    """SVG line chart for 24h forecast, suitable for README embedding."""
    if predictor is None:
        svg = """<svg xmlns='http://www.w3.org/2000/svg' width='900' height='300'><text x='20' y='40' font-family='sans-serif' font-size='16'>Model not loaded</text></svg>"""
        return Response(
            content=svg, media_type="image/svg+xml", headers={"Cache-Control": "no-store"}
        )

    data = await predictor.predict_next_24h()
    if not data:
        svg = """<svg xmlns='http://www.w3.org/2000/svg' width='900' height='300'><text x='20' y='40' font-family='sans-serif' font-size='16'>No data</text></svg>"""
        return Response(
            content=svg, media_type="image/svg+xml", headers={"Cache-Control": "no-store"}
        )

    # Dimensions and padding
    W, H, P = 900, 300, 40
    xs = list(range(len(data)))
    ys = [p.predicted_consumption_mw for p in data]
    ymin, ymax = min(ys), max(ys)
    # Avoid zero division
    yr = (ymax - ymin) or 1.0

    # Scale function
    def sx(i: int) -> float:
        return P + i * (W - 2 * P) / max(1, len(xs) - 1)

    def sy(v: float) -> float:
        return H - P - (v - ymin) * (H - 2 * P) / yr

    points = " ".join(f"{sx(i):.1f},{sy(v):.1f}" for i, v in zip(xs, ys, strict=False))

    # Axes labels (min/max)
    y_min_lbl = f"{ymin:,.0f} MW".replace(",", "\u2009")
    y_max_lbl = f"{ymax:,.0f} MW".replace(",", "\u2009")

    svg = f"""
    <svg xmlns='http://www.w3.org/2000/svg' width='{W}' height='{H}'>
      <defs>
        <linearGradient id='g' x1='0' x2='0' y1='0' y2='1'>
          <stop offset='0%' stop-color='#2196f3' stop-opacity='0.25'/>
          <stop offset='100%' stop-color='#2196f3' stop-opacity='0.02'/>
        </linearGradient>
      </defs>
      <rect x='0' y='0' width='{W}' height='{H}' fill='white'/>
      <polyline fill='none' stroke='#2196f3' stroke-width='2' points='{points}'/>
      <polygon fill='url(#g)' points='{points} {P},{H-P} {W-P},{H-P}'/>
      <text x='{P}' y='{H-P+24}' font-family='sans-serif' font-size='12' fill='#555'>now</text>
      <text x='{W-P-40}' y='{H-P+24}' font-family='sans-serif' font-size='12' fill='#555'>+24h</text>
      <text x='8' y='{H-P}' font-family='sans-serif' font-size='12' fill='#555'>{y_min_lbl}</text>
      <text x='8' y='{P+4}' font-family='sans-serif' font-size='12' fill='#555'>{y_max_lbl}</text>
    </svg>
    """
    return Response(
        content=svg,
        media_type="image/svg+xml",
        headers={"Cache-Control": "public, max-age=300"},
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
    )
