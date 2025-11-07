# ⚡ Ilmanhinta - Finnish Weather → Energy Consumption Prediction

Production-grade ETL pipeline predicting electricity consumption based on Finnish weather data. Built with modern Python tooling (DuckDB, Prophet, Logfire) and deployed to Railway.

[![CI/CD](https://github.com/jererobles/ilmanhinta/actions/workflows/ci.yml/badge.svg)](https://github.com/yourusername/ilmanhinta/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type checked: mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](http://mypy-lang.org/)

## 🎯 Features

- **Real-time data ingestion** from Fingrid (electricity) and FMI (weather) APIs
- **DuckDB OLAP engine** for time-series storage and SQL queries (replaces separate cache/DB)
- **Temporal joins** with Polars for high-performance data processing
- **Feature engineering** with sliding windows, lag features, and weather interactions
- **Ensemble forecasting** combining Prophet (seasonality) + LightGBM (features)
- **Finnish holiday detection** with Prophet (Juhannus electricity anomaly!)
- **Logfire observability** with automatic FastAPI tracing (Pydantic-native)
- **Dagster** orchestration with hourly ingestion and daily model retraining
- **FastAPI** service with Prometheus metrics and uncertainty intervals
- **Railway** deployment with health checks and persistent volume

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (modern Python package manager)
- Fingrid API key (free from [data.fingrid.fi](https://data.fingrid.fi))

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ilmanhinta.git
cd ilmanhinta

# One-liner quickstart (creates .env, folders, installs deps)
scripts/quickstart.sh

# One-liner including Railway setup (login/link/init, secrets)
scripts/quickstart.sh --cloud

# Or do it step-by-step with Makefile
make setup

# Then edit .env and add your FINGRID_API_KEY (required)
# Optionally set LOGFIRE_TOKEN to enable cloud tracing
```

### Usage

#### 1. Run Data Ingestion (Dagster)

```bash
# Start Dagster UI
dagster dev -m ilmanhinta.dagster

# Navigate to http://localhost:3000
# Materialize assets: fingrid_consumption_data, fmi_weather_data
# Run train_model job
```

#### 2. Start API Server

```bash
# Run with uvicorn
python -m uvicorn ilmanhinta.api.main:app --host 0.0.0.0 --port 8000 --reload

# Or use the module directly
python -m ilmanhinta.api.main
```

#### 3. Get Predictions

```bash
# Peak consumption in next 24h
curl http://localhost:8000/predict/peak

# Full 24h hourly forecast
curl http://localhost:8000/predict/forecast

# Prometheus metrics
curl http://localhost:8000/metrics
```

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Data Sources                            │
├─────────────────────┬───────────────────────────────────────────┤
│   Fingrid API       │          FMI OpenData API                 │
│ (Electricity data)  │        (Weather data)                     │
│   - Consumption     │   - Temperature, humidity                 │
│   - Production      │   - Wind, pressure                        │
│   - 3-min updates   │   - Hourly observations & forecasts       │
└──────────┬──────────┴────────────────┬──────────────────────────┘
           │                           │
           ▼                           ▼
    ┌──────────────────────────────────────────┐
    │        Dagster Orchestration             │
    │  ┌────────────────────────────────────┐  │
    │  │  Hourly: Ingest to DuckDB+Parquet  │  │
    │  │  Daily: Train ensemble models      │  │
    │  └────────────────────────────────────┘  │
    └──────────────────┬───────────────────────┘
                       │
                       ▼
           ┌───────────────────────┐
           │   DuckDB Data Layer   │
           │  - SQL temporal joins │
           │  - OLAP queries       │
           │  - Native Parquet I/O │
           └───────────┬───────────┘
                       │
                       ▼
           ┌───────────────────────┐
           │   Polars Processing   │
           │  - Hourly alignment   │
           │  - Feature engineering│
           └───────────┬───────────┘
                       │
            ┌──────────┴──────────┐
            │                     │
            ▼                     ▼
   ┌──────────────┐     ┌──────────────────┐
   │Prophet Model │     │ LightGBM Model   │
   │ - Seasonality│     │ - Gradient       │
   │ - Holidays   │     │   boosting       │
   │ - Weather    │     │ - 30+ engineered │
   │   regressors │     │   features       │
   └──────┬───────┘     └────────┬─────────┘
          │                      │
          └──────────┬───────────┘
                     ▼
            ┌────────────────┐
            │Ensemble Model  │
            │ 40% Prophet    │
            │ 60% LightGBM   │
            └────────┬───────┘
                     │
                     ▼
          ┌──────────────────────┐
          │   FastAPI Service    │
          │ (Logfire instrumented)│
          │  ┌────────────────┐  │
          │  │ /predict/peak  │  │
          │  │ /predict/      │  │
          │  │   forecast     │  │
          │  │ /metrics       │  │
          │  └────────────────┘  │
          └──────────────────────┘
                     │
                     ▼
              ┌──────────┐
              │ Railway  │
              │  (EU)    │
              └──────────┘
```

## 📊 Data Sources

### Fingrid Open Data API

- **Endpoint**: `https://data.fingrid.fi/api/datasets`
- **Auth**: API key (x-api-key header)
- **Rate limit**: 10,000 requests/day
- **Datasets**:
  - `124`: Real-time consumption (MW)
  - `192`: Total production (MW)
  - `181`: Wind production (MW)
  - `188`: Nuclear production (MW)
- **Resolution**: 3-minute updates for real-time data
- **License**: CC BY 4.0

### FMI (Finnish Meteorological Institute)

- **Endpoint**: `https://opendata.fmi.fi/wfs`
- **Auth**: None (public API)
- **Datasets**:
  - Observations: `fmi::observations::weather::multipointcoverage`
  - Forecast: `fmi::forecast::hirlam::surface::point::multipointcoverage`
- **Resolution**: Hourly observations and forecasts
- **License**: CC BY 4.0

## 🔧 Technical Stack

### Core Technologies

- **Python 3.11+**: Modern async support, improved type hints
- **uv**: Next-generation Python package manager (10-100x faster than pip)
- **Polars**: High-performance DataFrame library (faster than pandas)
- **Pydantic v2**: Data validation with 5-50x performance boost
- **DuckDB**: In-process OLAP database for time-series queries
- **Prophet**: Facebook's seasonal forecasting with holiday effects
- **LightGBM**: Gradient boosting for time series prediction

### Orchestration & Serving

- **Dagster**: Asset-based orchestration with schedules
- **FastAPI**: High-performance async API framework
- **Uvicorn**: ASGI server with HTTP/2 support

### Monitoring & Deployment

- **Logfire**: Pydantic-native observability with auto-instrumentation
- **Prometheus**: Metrics collection (request latency, predictions)
- **Railway**: Cloud application platform with persistent volumes
- **Docker**: Multi-stage builds for small images

### Code Quality

- **Ruff**: Extremely fast Python linter and formatter
- **mypy**: Static type checking
- **pre-commit**: Git hooks for code quality
- **pytest**: Testing with coverage

## 📈 ML Model

### Ensemble Architecture

The forecasting system combines two complementary models:

**Prophet Model (40% weight):**

- Automatic seasonal decomposition (daily, weekly, yearly)
- Finnish holiday effects (New Year, Midsummer, Independence Day, Christmas)
- Weather regressors (temperature, humidity, wind, pressure)
- Proper uncertainty intervals via MCMC sampling
- Handles DST gaps gracefully

**LightGBM Model (60% weight):**

- Gradient boosting with 30+ engineered features
- Complex non-linear feature interactions
- Time-based, lag, rolling statistics features

**Ensemble Strategy:**

- Weighted average: `0.4 * Prophet + 0.6 * LightGBM`
- Combined uncertainty via variance pooling
- Prophet captures trend/seasonality, LightGBM captures weather-dependent deviations

### LightGBM Features

**Time-based:**

- Hour of day, day of week, month
- Weekend indicator
- Heating/cooling degree days

**Lag features:**

- Consumption at t-1h, t-3h, t-6h, t-12h, t-24h, t-48h, t-168h

**Rolling statistics:**

- Mean, std, min, max over 6h, 12h, 24h, 168h windows

**Weather features:**

- Temperature, humidity, wind speed, pressure
- Wind chill, temperature squared
- Weather interactions

### Model Performance

Typical performance on test set:

- **Prophet RMSE**: ~250-350 MW
- **LightGBM RMSE**: ~200-300 MW
- **Ensemble RMSE**: ~180-280 MW (best)
- **MAE**: ~150-250 MW
- **Training time**: 5-10 minutes for both models

### Retraining

- **Schedule**: Daily at 2 AM (configurable)
- **Training window**: Last 30 days
- **Model versioning**: Timestamped model files (prophet*\*, lightgbm*\*)
- **Ensemble**: Automatically created from latest Prophet + LightGBM models

## 💾 DuckDB Data Layer

DuckDB replaces multiple separate tools (Redis cache, Postgres storage, manual joins):

### Architecture

- **In-process OLAP database** stored as single file: `data/ilmanhinta.duckdb`
- **Native Parquet reading** via `read_parquet()` for zero-copy ingestion
- **Polars interop** through Arrow for efficient data transfer
- **SQL-based temporal joins** replacing complex Python join logic

### Tables

**consumption:**

- Fingrid electricity data (consumption, production, wind, nuclear)
- 15-minute resolution aligned to hourly for ML
- Indexed on timestamp for fast queries

**weather:**

- FMI observations and forecasts
- Weather parameters (temperature, humidity, wind, pressure, etc.)
- Supports both observation and forecast data types

**predictions:**

- Model forecasts with confidence intervals
- Tracks model type (prophet, lightgbm, ensemble)
- Version tracking for model comparisons

### Benefits

- **Single dependency** replaces Redis + Postgres + manual file scanning
- **SQL queries** for complex time-series operations (ASOF joins, windows)
- **Fast**: Columnar storage with Parquet compression
- **Small**: Entire database fits in Docker image
- **Embeddable**: No separate database server needed

## 🔍 Logfire Observability

Automatic distributed tracing without explicit logging calls:

### Setup

1. Sign up at [logfire.pydantic.dev](https://logfire.pydantic.dev)
2. Set `LOGFIRE_TOKEN` environment variable
3. Run application
4. View traces in Logfire dashboard

No configuration files, no complex setup. It just works™.

### Features

- **Auto-instrumentation**: FastAPI requests traced automatically
- **Structured logs**: No manual `logfire.info()` needed
- **Console fallback**: Works without token (logs to console)
- **Low overhead**: Built by Pydantic team, optimized for performance

### What Gets Traced

- Every FastAPI request (method, path, duration, status)
- Database queries (DuckDB operations)
- Model predictions (features, outputs, confidence)
- External API calls (Fingrid, FMI)

## 🚀 Deployment

### Deploy to Railway

```bash
# Install Railway CLI
# macOS (Homebrew)
brew install railway

# or generic install script
curl -fsSL https://railway.app/install.sh | sh

# Login (optional if you used '--cloud' or 'make setup-cloud')
# Tip: you can also use the Makefile which checks/installs the CLI
# make railway-login
railway login

# Link a project the first time (choose ONE) — optional if you used '--cloud':
# - Create a new project from this repo (recommended)
# or: make railway-init
railway init
# - OR link to an existing project
# or: make railway-link
railway link

# Set required variables in Railway (never commit secrets)
railway variables set FINGRID_API_KEY=$FINGRID_API_KEY

# Deploy both services (services/api + services/etl)
make deploy

# Deploy a single service (if needed)
make deploy-api   # FastAPI frontend
make deploy-etl   # Dagster workers

# If Railway CLI is not installed:
# - The Makefile shows install instructions, or auto-installs when set:
# AUTO_INSTALL_RAILWAY=1 make railway-login

# View logs
railway logs

# Open in browser
railway open
```

Notes:

- `services/api/railway.json` and `services/etl/railway.json` describe the two services. Run `railway up` (or `make deploy-*`) from those folders so Railway knows which entrypoint to use.
- Attach a volume per service and mount it at `/app/data` to persist DuckDB + model files on ETL and cache files on the API.
- Railway injects `PORT`; both processes listen on `${PORT}` (or `4000` for the Dagster gRPC server, configured via `scripts/start-etl.sh`).

### Environment Variables

Validate your local `.env`:

```bash
make check-env
```

Notes:

- `.env` is ignored by Git; never commit secrets.
- For production (Railway), set secrets via `railway variables set`.

Required:

- `FINGRID_API_KEY`: Your Fingrid API key

Optional:

- `FMI_STATION_ID`: FMI station ID (default: 101004 - Helsinki)
- `LOG_LEVEL`: Logging level (default: INFO)
- `CACHE_TTL_SECONDS`: Cache duration (default: 180)
- `MODEL_RETRAIN_HOURS`: Retrain interval (default: 24)

Observability (Logfire):

- `LOGFIRE_TOKEN`: Optional; when set, enables Logfire cloud tracing
- `LOGFIRE_PROJECT`: Optional; default `ilmanhinta`
- `LOGFIRE_ENVIRONMENT`: Optional; default `production`

## 📊 API Documentation

### `GET /predict/peak`

Returns the hour with peak predicted consumption in the next 24 hours.

**Response:**

```json
{
  "peak_timestamp": "2024-01-15T18:00:00Z",
  "peak_consumption_mw": 9847.32,
  "confidence_lower": 9650.18,
  "confidence_upper": 10044.46,
  "model_version": "20240115_020000",
  "generated_at": "2024-01-15T10:00:00Z"
}
```

### `GET /predict/forecast`

Returns hourly consumption predictions for the next 24 hours.

**Response:**

```json
[
  {
    "timestamp": "2024-01-15T11:00:00Z",
    "predicted_consumption_mw": 8234.56,
    "confidence_lower": 8050.23,
    "confidence_upper": 8418.89,
    "model_version": "20240115_020000"
  },
  ...
]
```

### `GET /metrics`

Prometheus metrics endpoint.

**Metrics:**

- `ilmanhinta_api_requests_total`: Total API requests
- `ilmanhinta_api_request_duration_seconds`: Request latency
- `ilmanhinta_predictions_total`: Total predictions made
- `ilmanhinta_peak_prediction_mw`: Latest peak prediction

## 🧪 Development

### Running Tests

```bash
# Run all tests with coverage
pytest tests/ -v --cov=src/ilmanhinta

# Run specific test file
pytest tests/test_models.py -v

# Run with markers
pytest -m "not slow" -v
```

### Code Quality

```bash
# Lint with ruff
ruff check .

# Format with ruff
ruff format .

# Type check with mypy
mypy src/ilmanhinta

# Run all pre-commit hooks
pre-commit run --all-files
```

### Local Development

```bash
# Run Dagster UI for pipeline development
dagster dev -m ilmanhinta.dagster

# Run API with auto-reload
uvicorn ilmanhinta.api.main:app --reload

# Build Docker image
docker build -t ilmanhinta .

# Run container
docker run -p 8000:8000 --env-file .env ilmanhinta
```

## 🎯 Roadmap

- [x] Ensemble models (Prophet + LightGBM)
- [x] DuckDB for OLAP queries on time-series data
- [x] Logfire observability with auto-instrumentation
- [ ] Add Grafana dashboard for monitoring
- [ ] Implement spot price prediction (Nord Pool integration)
- [ ] Multi-region support (multiple FMI stations)
- [ ] Real-time prediction updates every 3 minutes
- [ ] Historical data backfilling via DuckDB
- [ ] Model performance tracking over time
- [ ] API endpoints for ensemble component breakdown (interpretability)

## 📝 License

MIT License - see [LICENSE](LICENSE) for details.

## 🙏 Acknowledgments

- **Fingrid**: For providing open electricity data
- **FMI**: For comprehensive weather data
- **Finnish open data community**: For making this possible

## 🤝 Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) first.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

Built with ⚡ by the Ilmanhinta team. Powered by modern Python tooling.
