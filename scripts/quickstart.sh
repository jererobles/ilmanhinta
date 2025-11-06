#!/bin/bash
set -euo pipefail

echo "🚀 Ilmanhinta Quick Start"
echo "=========================="
echo ""

# Ensure uv exists (Makefile relies on it)
if ! command -v uv &> /dev/null; then
  echo "📦 Installing uv..."
  curl -LsSf https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.cargo/bin:$PATH"
else
  echo "✅ uv is installed"
fi

MODE=${1:-}
if [ "$MODE" = "--cloud" ]; then
  echo "🛠  Running 'make setup-cloud' (env, dirs, install, Railway)"
  make setup-cloud
else
  echo "🛠  Running 'make setup' (env, dirs, install)"
  make setup
fi

echo ""
echo "Next steps:"
echo "1) Edit .env and add your Fingrid API key from https://data.fingrid.fi"
echo "2) Start Dagster:  make run-dagster"
echo "3) Start the API:  make run-api"
echo "4) Deploy to Railway:  make deploy"
echo "   Tip: use 'scripts/quickstart.sh --cloud' or 'make setup-cloud' to set up Railway"
echo "   Set AUTO_INSTALL_RAILWAY=1 to auto-install the Railway CLI"
echo ""
echo "📚 Read README.md for full details"
