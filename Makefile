.PHONY: help install lint format test clean run-api run-dagster docker-build docker-run deploy

help:
	@echo "Ilmanhinta - Finnish Weather → Energy ETL Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make install        Install dependencies with uv"
	@echo "  make lint          Run ruff linter"
	@echo "  make format        Format code with ruff"
	@echo "  make test          Run tests with pytest"
	@echo "  make clean         Clean build artifacts"
	@echo "  make run-api       Start FastAPI server"
	@echo "  make run-dagster   Start Dagster UI"
	@echo "  make docker-build  Build Docker image"
	@echo "  make docker-run    Run Docker container"
	@echo "  make deploy        Deploy to Fly.io"

install:
	@echo "📦 Installing dependencies..."
	uv pip install -e ".[dev]"
	pre-commit install
	@echo "✅ Installation complete"

lint:
	@echo "🔍 Running linter..."
	ruff check .

format:
	@echo "✨ Formatting code..."
	ruff format .
	ruff check --fix .

test:
	@echo "🧪 Running tests..."
	pytest tests/ -v --cov=src/ilmanhinta --cov-report=term-missing

clean:
	@echo "🧹 Cleaning up..."
	rm -rf build dist *.egg-info
	rm -rf .pytest_cache .coverage htmlcov
	rm -rf .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
	@echo "✅ Cleanup complete"

run-api:
	@echo "🚀 Starting FastAPI server..."
	uvicorn ilmanhinta.api.main:app --reload --host 0.0.0.0 --port 8000

run-dagster:
	@echo "🔧 Starting Dagster UI..."
	dagster dev -m ilmanhinta.dagster

docker-build:
	@echo "🐳 Building Docker image..."
	docker build -t ilmanhinta:latest .

docker-run:
	@echo "🐳 Running Docker container..."
	docker run -p 8000:8000 --env-file .env ilmanhinta:latest

deploy:
	@echo "🚀 Deploying to Fly.io..."
	flyctl deploy
	@echo "✅ Deployment complete"
