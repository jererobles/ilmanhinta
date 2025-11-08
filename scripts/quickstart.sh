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

echo "🛠  Running 'make setup' (env, dirs, install)"
make setup

echo ""
echo "Next steps:"
echo "1) Edit .env and add your Fingrid API key from https://data.fingrid.fi"
echo "2) Start Dagster:  make run-dagster"
echo "3) Start the API:  make run-api"
echo ""
echo "📚 Read README.md for full details"
