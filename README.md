# ⚡ Ilmanhinta - Finnish Weather → Energy Consumption Prediction

Production-grade ETL pipeline predicting electricity consumption based on Finnish weather data. Built with modern Python tooling and deployed to Fly.io.

[![CI/CD](https://github.com/jererobles/ilmanhinta/actions/workflows/ci.yml/badge.svg)](https://github.com/yourusername/ilmanhinta/actions)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)
[![Type checked: mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](http://mypy-lang.org/)

## 🎯 Features

- **Real-time data ingestion** from Fingrid (electricity) and FMI (weather) APIs
- **Temporal joins** with Polars for high-performance data processing
- **Feature engineering** with sliding windows, lag features, and weather interactions
- **LightGBM** model for consumption prediction
- **Dagster** orchestration with hourly ingestion and daily model retraining
- **FastAPI** service with caching and Prometheus metrics
- **Fly.io** deployment with health checks and auto-scaling

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

# Install with uv (much faster than pip)
uv pip install -e ".[dev]"

# Set up pre-commit hooks
pre-commit install

# Configure environment
cp .env.example .env
# Edit .env and add your FINGRID_API_KEY
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
python -m uvicorn ilmanhinta.api.main:app --host 0.0.0.0 --port 8000

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
    │  │  Hourly: Ingest data               │  │
    │  │  Daily: Train model                │  │
    │  └────────────────────────────────────┘  │
    └──────────────────┬───────────────────────┘
                       │
                       ▼
           ┌───────────────────────┐
           │   Polars Processing   │
           │  - Temporal joins     │
           │  - Hourly alignment   │
           │  - Feature engineering│
           └───────────┬───────────┘
                       │
                       ▼
              ┌────────────────┐
              │  LightGBM Model │
              │  - Gradient     │
              │    boosting     │
              │  - Time series  │
              │    features     │
              └────────┬───────┘
                       │
                       ▼
            ┌──────────────────────┐
            │    FastAPI Service    │
            │  ┌────────────────┐   │
            │  │ /predict/peak  │   │
            │  │ /predict/      │   │
            │  │   forecast     │   │
            │  │ /metrics       │   │
            │  └────────────────┘   │
            └──────────────────────┘
                       │
                       ▼
                ┌──────────┐
                │  Fly.io  │
                │ (Stockholm)│
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
- **LightGBM**: Gradient boosting for time series prediction

### Orchestration & Serving

- **Dagster**: Asset-based orchestration with schedules
- **FastAPI**: High-performance async API framework
- **Uvicorn**: ASGI server with HTTP/2 support

### Monitoring & Deployment

- **Prometheus**: Metrics collection (request latency, predictions)
- **Fly.io**: Global application platform (Stockholm region)
- **Docker**: Multi-stage builds for small images

### Code Quality

- **Ruff**: Extremely fast Python linter and formatter
- **mypy**: Static type checking
- **pre-commit**: Git hooks for code quality
- **pytest**: Testing with coverage

## 📈 ML Model

### Features

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

- **RMSE**: ~200-300 MW
- **MAE**: ~150-250 MW
- **Training time**: 2-5 minutes on historical data

### Retraining

- **Schedule**: Daily at 2 AM (configurable)
- **Training window**: Last 30 days
- **Model versioning**: Timestamped model files

## 🚀 Deployment

### Deploy to Fly.io

```bash
# Install flyctl
curl -L https://fly.io/install.sh | sh

# Login
flyctl auth login

# Create app (first time)
flyctl apps create ilmanhinta

# Create volume for persistent data
flyctl volumes create ilmanhinta_data --region arn --size 10

# Set secrets
flyctl secrets set FINGRID_API_KEY=your_api_key_here

# Deploy
flyctl deploy

# Check status
flyctl status

# View logs
flyctl logs

# Open in browser
flyctl open
```

### Environment Variables

Required:

- `FINGRID_API_KEY`: Your Fingrid API key

Optional:

- `FMI_PLACE`: FMI station (default: 101004 - Helsinki)
- `LOG_LEVEL`: Logging level (default: INFO)
- `CACHE_TTL_SECONDS`: Cache duration (default: 180)
- `MODEL_RETRAIN_HOURS`: Retrain interval (default: 24)

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

- [ ] Add Grafana dashboard for monitoring
- [ ] Implement spot price prediction (Nord Pool integration)
- [ ] Multi-region support (multiple FMI stations)
- [ ] Ensemble models (Prophet + LightGBM)
- [ ] Real-time prediction updates every 3 minutes
- [ ] Historical data backfilling
- [ ] Model performance tracking over time

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
