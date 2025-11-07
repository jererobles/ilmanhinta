.PHONY: help install lint format test clean run-api run-dagster docker-build docker-run deploy setup setup-cloud check-env ensure-dirs \
	railway-help railway-login railway-init railway-link railway-open railway-logs railway-set-fingrid railway-deploy railway-check \
	railway-auth railway-project-setup railway-service-setup railway-secrets deploy-api deploy-etl

RAILWAY_PROJECT_NAME ?= ilmanhinta
RAILWAY_SERVICE_NAME ?= app
SKIP_PRE_COMMIT_INSTALL ?= 0

# ---- helper functions ----
define get_env_var
$(shell if [ -n "$(1)" ]; then echo "$(1)"; elif [ -f .env ]; then sed -n 's/^\s*$(2)\s*=\s*//p' .env | sed 's/[[:space:]]*\#.*$$//' | tail -n1; fi)
endef

define prompt_if_missing
$(shell if [ -z "$(1)" ]; then read -s -p "Enter $(2): " VAL; echo $$VAL; else echo "$(1)"; fi)
endef

define railway_var_set
@railway variables -s "$(RAILWAY_SERVICE_NAME)" --set "$(1)=$(2)" >/dev/null && echo "✅ Set $(1)"
endef

# lazy eval for env vars
FINGRID_KEY = $(call get_env_var,$(FINGRID_API_KEY),FINGRID_API_KEY)
LOGFIRE_TOKEN_VAL = $(call get_env_var,$(LOGFIRE_TOKEN),LOGFIRE_TOKEN)

help:
	@echo "Ilmanhinta - Finnish Weather → Energy ETL Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make install        Install dependencies with uv"
	@echo "  make setup          First-time setup (.env, dirs, install)"
	@echo "  make setup-cloud    Setup Railway (login, link/init, secrets)"
	@echo "  make lint          Run ruff linter"
	@echo "  make format        Format code with ruff"
	@echo "  make test          Run tests with pytest"
	@echo "  make clean         Clean build artifacts"
	@echo "  make run-api       Start FastAPI server"
	@echo "  make run-dagster   Start Dagster UI"
	@echo "  make docker-build  Build Docker image"
	@echo "  make docker-run    Run Docker container"
	@echo "  make deploy        Deploy both services to Railway"
	@echo "  make deploy-api    Deploy only the API service"
	@echo "  make deploy-etl    Deploy only the ETL service"
	@echo "  make check-env     Validate required env vars"
	@echo ""
	@echo "Railway helpers:"
	@echo "  make railway-help          Show Railway helper commands"
	@echo "  make railway-login         Railway login"
	@echo "  make railway-init          Create new Railway project from this repo"
	@echo "  make railway-link          Link to existing Railway project"
	@echo "  make railway-set-fingrid   Set FINGRID_API_KEY in Railway"
	@echo "  make railway-deploy        Deploy (alias of 'deploy')"
	@echo "  make railway-logs          Tail logs"
	@echo "  make railway-open          Open project in browser"
	@echo ""
	@echo "Tip: set AUTO_INSTALL_RAILWAY=1 to auto-install the Railway CLI"

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

setup-cloud: setup railway-check railway-auth railway-project-setup railway-service-setup railway-secrets
	@if [ "$(AUTO_DEPLOY)" = "1" ]; then \
		echo "🚀 Auto-deploy enabled (AUTO_DEPLOY=1)"; \
		make deploy; \
	else \
		echo "✨ Cloud setup complete. Run 'make deploy' to deploy."; \
	fi

railway-auth:
	@echo "🔐 Checking Railway authentication..."
	@if railway whoami >/dev/null 2>&1; then \
		echo "✅ Railway authenticated"; \
	else \
		echo "🔐 Logging into Railway..."; \
		railway login || { echo "❌ Railway login failed"; exit 1; }; \
	fi

railway-project-setup:
	@echo "🔗 Linking project '$(RAILWAY_PROJECT_NAME)'..."
	@if railway link --project "$(RAILWAY_PROJECT_NAME)" >/dev/null 2>&1 || \
		railway link -p "$(RAILWAY_PROJECT_NAME)" >/dev/null 2>&1 || \
		railway link "$(RAILWAY_PROJECT_NAME)" >/dev/null 2>&1; then \
		echo "✅ Project linked: $(RAILWAY_PROJECT_NAME)"; \
	else \
		echo "ℹ️  Project not found; creating '$(RAILWAY_PROJECT_NAME)'..."; \
		if railway init --name "$(RAILWAY_PROJECT_NAME)" >/dev/null 2>&1 || \
		   railway init -n "$(RAILWAY_PROJECT_NAME)" >/dev/null 2>&1; then \
			echo "✅ Project created: $(RAILWAY_PROJECT_NAME)"; \
		else \
			echo "⚠️  Non-interactive create failed. Launching interactive 'railway init'..."; \
			railway init || { echo "❌ Railway init failed"; exit 1; }; \
		fi; \
	fi

railway-service-setup:
	@echo "🧩 Selecting service '$(RAILWAY_SERVICE_NAME)'..."
	@if railway service "$(RAILWAY_SERVICE_NAME)" >/dev/null 2>&1; then \
		echo "✅ Service selected: $(RAILWAY_SERVICE_NAME)"; \
	else \
		echo "⚠️  Service not found. Creating..."; \
		railway add -s "$(RAILWAY_SERVICE_NAME)" \
			--variables "" >/dev/null || { echo "❌ Service creation failed"; exit 1; }; \
	fi

railway-volumes:
	@echo "📦 Configuring volumes in Railway..."
	@railway volumes -s "$(RAILWAY_SERVICE_NAME)" \
		$(if $(FINGRID_KEY),--set "FINGRID_API_KEY=$(FINGRID_KEY)") \
		$(if $(LOGFIRE_TOKEN_VAL),--set "LOGFIRE_TOKEN=$(LOGFIRE_TOKEN_VAL)") >/dev/null && echo "✅ Volumes configured"

railway-secrets:
	@echo "🔐 Configuring secrets in Railway..."
	@railway variables -s "$(RAILWAY_SERVICE_NAME)" \
		$(if $(FINGRID_KEY),--set "FINGRID_API_KEY=$(FINGRID_KEY)") \
		$(if $(LOGFIRE_TOKEN_VAL),--set "LOGFIRE_TOKEN=$(LOGFIRE_TOKEN_VAL)") >/dev/null && echo "✅ Secrets configured"

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

deploy:
	@$(MAKE) deploy-api
	@$(MAKE) deploy-etl
	@echo "✅ Deployment complete"

deploy-api:
	@$(MAKE) railway-check
	@echo "🚀 Deploying API service..."
	railway up --path-as-root services/api

deploy-etl:
	@$(MAKE) railway-check
	@echo "🚀 Deploying ETL service..."
	cd services/etl && railway up --path-as-root ../..

railway-help:
	@echo "Railway helper commands:"
	@echo "  make railway-login         Railway login"
	@echo "  make railway-init          Create new Railway project from this repo"
	@echo "  make railway-link          Link to existing Railway project"
	@echo "  make railway-set-fingrid   Set FINGRID_API_KEY in Railway"
	@echo "  make railway-deploy        Deploy (alias of 'deploy')"
	@echo "  make railway-logs          Tail logs"
	@echo "  make railway-open          Open project in browser"

railway-login: railway-check
	railway login

railway-init: railway-check
	railway init

railway-link: railway-check
	railway link

railway-open: railway-check
	railway open

railway-logs: railway-check
	railway logs

railway-set-fingrid: railway-check
	@echo "🔐 Setting FINGRID_API_KEY in Railway..."
	@FINGRID_VAL="$(FINGRID_KEY)"; \
	if [ -z "$$FINGRID_VAL" ]; then \
		echo "❌ Missing FINGRID_API_KEY. Usage: make railway-set-fingrid FINGRID_API_KEY=..."; \
		exit 1; \
	fi; \
	railway variables -s "$(RAILWAY_SERVICE_NAME)" --set "FINGRID_API_KEY=$$FINGRID_VAL" >/dev/null && echo "✅ Set FINGRID_API_KEY in Railway"

railway-deploy: deploy

railway-check:
	@if ! command -v railway >/dev/null 2>&1; then \
		echo "❌ Railway CLI not found."; \
		if [ "$(AUTO_INSTALL_RAILWAY)" = "1" ]; then \
			if command -v brew >/dev/null 2>&1; then \
				echo "📦 Installing Railway via Homebrew..."; \
				brew install railway || { echo "Failed to install Railway via brew"; exit 1; }; \
			else \
				echo "📦 Installing Railway via official install script..."; \
				curl -fsSL https://railway.app/install.sh | sh || { echo "Failed to install Railway via script"; exit 1; }; \
			fi; \
		else \
			echo "➡️  Install it with: brew install railway"; \
			echo "    or: curl -fsSL https://railway.app/install.sh | sh"; \
			exit 1; \
		fi; \
	fi

check-env:
	@echo "🔐 Validating environment configuration..."
	@if [ ! -f .env ]; then \
		echo "❌ .env not found. Run 'cp .env.example .env' and set FINGRID_API_KEY."; \
		exit 1; \
	fi
	@FINGRID_VAL="$(FINGRID_KEY)"; \
	if [ -z "$$FINGRID_VAL" ]; then \
		echo "❌ FINGRID_API_KEY is missing in .env"; \
		echo "   Edit .env and set a valid key from https://data.fingrid.fi"; \
		exit 1; \
	else \
		echo "✅ Required env vars present"; \
	fi
