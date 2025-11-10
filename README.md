# ⚡ Ilmanhinta

Finnish electricity consumption prediction using weather data. Built with TimescaleDB and LightGBM.

## What this does

Pulls electricity data from Fingrid and weather data from FMI, then predicts next 24h consumption using LightGBM with 30+ engineered features. The predictions can beat Fingrid's own forecasts.

## Quick start

```bash
# Needs Python 3.11+ and uv
git clone https://github.com/jererobles/ilmanhinta.git
cd ilmanhinta

# Setup everything
make setup

# Add your Fingrid API key to .env (get one from data.fingrid.fi)
# Run migrations
./scripts/run_migrations.sh

# Start the stack
make docker-up
```

Then hit `http://localhost:8000/predict/peak` for predictions.

## Architecture

```
Fingrid API →
              → Dagster → TimescaleDB → LightGBM → FastAPI
FMI API     →
```

The pipeline:

1. Dagster fetches data every hour
2. TimescaleDB stores it (with hypertables for fast queries)
3. Models retrain daily at 2 AM
4. FastAPI serves predictions with confidence intervals

## The ML stuff

LightGBM with 30+ engineered features:

- Time-based: hour, day of week, month, weekend indicator
- Lag features: consumption at t-1h, t-3h, t-6h, t-12h, t-24h, t-48h, t-168h
- Rolling statistics: mean/std/min/max over various windows
- Weather features: temperature, humidity, wind, pressure, wind chill

Performance: ~200-300 MW RMSE on test set. The weather features make a big difference - especially temperature.

## Stack

**Core:**

- TimescaleDB (PostgreSQL with time-series superpowers)
- Polars (way faster than pandas for our use case)
- LightGBM (gradient boosting)
- Dagster for orchestration
- FastAPI

**DevOps:**

- Docker Compose with two profiles (lite for dev, full with SigNoz)
- Alembic for migrations
- Logfire for tracing (optional, works without token)
- Pre-commit hooks with ruff

## Database

TimescaleDB gives us:

- Automatic partitioning by time
- Continuous aggregates (pre-computed stats)
- 70-95% compression on old data
- Regular PostgreSQL compatibility

Key tables:

- `electricity_consumption`: 3-minute resolution Fingrid data
- `weather_observations`: Hourly FMI data
- `predictions`: Model outputs with confidence intervals

## API

```bash
# Peak hour in next 24h
GET /predict/peak

# Full 24h forecast
GET /predict/forecast

# Prometheus metrics
GET /metrics
```

## Development

```bash
# Run tests
make test

# Lint/format
make lint
make format

# Run locally
make run-api        # API on :8000
make run-dagster    # Dagster UI on :3000

# Docker commands
make docker-up      # Lite mode (console logs)
make docker-up-full # With SigNoz UI on :8080
make docker-down
```

## Config

Required:

- `FINGRID_API_KEY`: From data.fingrid.fi

Optional:

- `LOGFIRE_TOKEN`: For cloud tracing
- `FMI_STATION_ID`: Default is Helsinki (101004)
- `MODEL_RETRAIN_HOURS`: Default 24

## Data sources

**Fingrid**:

- Datasets: 124 (consumption), 192 (production), 181 (wind), 188 (nuclear)
- 3-minute updates
- 10k requests/day limit

**FMI**:

- Weather observations and HIRLAM forecasts
- Hourly resolution
- No auth needed

## TODO

- [ ] Grafana dashboards
- [ ] Add Prophet for better seasonality handling
- [ ] Multi-region support
- [ ] Real-time updates (3-min instead of hourly)
- [ ] Model drift detection
- [ ] Actual price predictions (currently only consumption)

## License

MIT

## Notes

Built this because I wanted accurate consumption forecasts for spot price optimization. Turns out weather features + gradient boosting works pretty well for this use case.

Originally planned an ensemble with Prophet for seasonality, but LightGBM alone with good feature engineering gets solid results. Might add Prophet later if seasonal patterns need better handling.

TimescaleDB was chosen over vanilla Postgres because continuous aggregates make the analytics queries instant, and compression saves a ton of space on historical data.

For deployments I explored serveral cloud-hosted services (Fly.io, Railway, Render) but in the end i opted for plain old `docker-compose.yaml` locally or on a VPS.
