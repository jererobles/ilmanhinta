.PHONY: help install lint format test clean run-api run-dagster docker-build docker-run setup check-env ensure-dirs \
	docker-up docker-up-full docker-down docker-restart docker-logs docker-ps docker-exec docker-shell docker-build-fresh \
	db-migrate db-backup db-restore prod-up prod-down

SKIP_PRE_COMMIT_INSTALL ?= 0

help:
	@echo "Ilmanhinta - Finnish Weather → Energy ETL Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make install        Install dependencies with uv"
	@echo "  make setup          First-time setup (.env, dirs, install)"
	@echo "  make lint          Run ruff linter"
	@echo "  make format        Format code with ruff"
	@echo "  make test          Run tests with pytest"
	@echo "  make clean         Clean build artifacts"
	@echo "  make run-api       Start FastAPI server"
	@echo "  make run-dagster   Start Dagster UI"
	@echo "  make check-env     Validate required env vars"
	@echo ""
	@echo "Docker Compose commands:"
	@echo "  make docker-up          Start lite mode (console logs only)"
	@echo "  make docker-up-full     Start full SigNoz stack (ClickHouse + UI)"
	@echo "  make docker-down        Stop all Docker Compose services"
	@echo "  make docker-restart     Restart all services"
	@echo "  make docker-logs        Tail logs from all services"
	@echo "  make docker-ps          Show running containers"
	@echo "  make docker-shell       Open shell in API container"
	@echo "  make docker-build-fresh Rebuild images from scratch"
	@echo "  make prod-up            Start production stack"
	@echo "  make prod-down          Stop production stack"
	@echo ""
	@echo "Database commands:"
	@echo "  make db-migrate    Run database migrations"
	@echo "  make db-backup     Backup PostgreSQL database"
	@echo "  make db-restore    Restore database from backup"

install:
	@echo "📦 Installing dependencies..."
	uv pip install -e ".[dev]"
	@if [ "$(SKIP_PRE_COMMIT_INSTALL)" = "1" ]; then \
		echo "⏭️  Skipping pre-commit install (SKIP_PRE_COMMIT_INSTALL=1)"; \
	else \
		uv run pre-commit install; \
	fi
	@echo "✅ Installation complete"

setup: ensure-dirs
	@echo "🛠  First-time project setup..."
	@if [ ! -f .env ]; then \
		cp .env.example .env && \
		echo "📝 Created .env from .env.example" && \
		echo "⚠️  Edit .env and set FINGRID_API_KEY"; \
	else \
		echo "✅ .env already exists"; \
	fi
	make install
	@echo "✨ Setup complete"

ensure-dirs:
	@echo "📁 Ensuring data and Dagster dirs..."
	mkdir -p data/{raw,processed,models} dagster_home
	@echo "✅ Directories ready"

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

run-api: check-env
	@echo "🚀 Starting FastAPI server..."
	uvicorn ilmanhinta.api.main:app --reload --host 0.0.0.0 --port 8000

run-dagster: check-env
	@echo "🔧 Starting Dagster UI..."
	dagster dev -m ilmanhinta.dagster

docker-build:
	@echo "🐳 Building Docker image..."
	docker build -t ilmanhinta:latest .

docker-run:
	@echo "🐳 Running Docker container..."
	docker run -p 8000:8000 --env-file .env ilmanhinta:latest

check-env:
	@echo "🔐 Validating environment configuration..."
	@if [ ! -f .env ]; then \
		echo "❌ .env not found. Run 'cp .env.example .env' and set FINGRID_API_KEY."; \
		exit 1; \
	fi

# ------------------- Docker Compose commands -------------------
docker-up:
	@echo "🐳 Starting Docker Compose services (lite mode, console logs only)..."
	@echo "📋 Stopping signoz profile first (if running)..."
	docker-compose --profile signoz down 2>/dev/null || true
	docker-compose --profile lite up -d
	@echo "✅ Services started. Check status with 'make docker-ps'"
	@echo ""
	@echo "🌐 Access points:"
	@echo "   API: http://localhost:8000"
	@echo "   Dagster: http://localhost:3000"
	@echo ""
	@echo "💡 For persistent storage + SigNoz UI:"
	@echo "   make docker-up-full"

docker-up-full:
	@echo "🔍 Starting with full SigNoz observability stack..."
	@echo "📋 Stopping lite profile first..."
	docker-compose --profile lite down
	@echo "🚀 Starting SigNoz profile..."
	docker-compose --profile signoz up -d
	@echo ""
	@echo "✅ Full SigNoz stack running:"
	@echo "   API: http://localhost:8000"
	@echo "   Dagster: http://localhost:3000"
	@echo "   SigNoz UI: http://localhost:8080"

docker-down:
	@echo "🛑 Stopping Docker Compose services..."
	docker-compose --profile lite --profile signoz down
	@echo "✅ Services stopped"

docker-restart:
	@echo "🔄 Restarting Docker Compose services..."
	docker-compose restart
	@echo "✅ Services restarted"

docker-logs:
	@echo "📋 Tailing logs from all services..."
	docker-compose logs -f

docker-ps:
	@echo "🐳 Docker Compose service status:"
	docker-compose ps

docker-exec:
	@echo "🐚 Opening shell in API container..."
	docker-compose exec api bash

docker-shell: docker-exec

docker-build-fresh:
	@echo "🔨 Rebuilding Docker images from scratch..."
	docker-compose build --no-cache
	@echo "✅ Images rebuilt"

# Production stack
prod-up:
	@echo "🚀 Starting production stack..."
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml up -d
	@echo "✅ Production stack started"

prod-down:
	@echo "🛑 Stopping production stack..."
	docker-compose -f docker-compose.yml -f docker-compose.prod.yml down
	@echo "✅ Production stack stopped"

# Database management
db-migrate:
	@echo "🗄️  Running database migrations..."
	docker-compose exec postgres psql -U api -d ilmanhinta -f /docker-entrypoint-initdb.d/init.sql
	@echo "✅ Migrations complete"

db-backup:
	@echo "💾 Backing up database..."
	@TIMESTAMP=$$(date +%Y%m%d_%H%M%S); \
	docker-compose exec postgres pg_dump -U api ilmanhinta > backups/ilmanhinta_$$TIMESTAMP.sql
	@echo "✅ Database backed up to backups/ilmanhinta_$$TIMESTAMP.sql"

db-restore:
	@if [ -z "$(BACKUP_FILE)" ]; then \
		echo "❌ Usage: make db-restore BACKUP_FILE=backups/ilmanhinta_20240101_120000.sql"; \
		exit 1; \
	fi
	@echo "⚠️  This will overwrite the current database. Continue? [y/N]"
	@read -r CONFIRM; \
	if [ "$$CONFIRM" = "y" ] || [ "$$CONFIRM" = "Y" ]; then \
		docker-compose exec -T postgres psql -U api ilmanhinta < $(BACKUP_FILE); \
		echo "✅ Database restored from $(BACKUP_FILE)"; \
	else \
		echo "❌ Restore cancelled"; \
	fi
