#!/bin/bash
set -e

echo "🚀 Ilmanhinta Quick Start"
echo "=========================="
echo ""

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo "📦 Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
fi

echo "✅ uv is installed"
echo ""

# Install dependencies
echo "📦 Installing dependencies..."
uv pip install -e ".[dev]"
echo "✅ Dependencies installed"
echo ""

# Set up pre-commit
echo "🔧 Setting up pre-commit hooks..."
pre-commit install
echo "✅ Pre-commit hooks installed"
echo ""

# Create .env if it doesn't exist
if [ ! -f .env ]; then
    echo "📝 Creating .env file..."
    cp .env.example .env
    echo "⚠️  Please edit .env and add your FINGRID_API_KEY"
else
    echo "✅ .env file already exists"
fi
echo ""

# Create data directories
echo "📁 Creating data directories..."
mkdir -p data/{raw,processed,models} dagster_home
echo "✅ Data directories created"
echo ""

echo "✨ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env and add your Fingrid API key from https://data.fingrid.fi"
echo "2. Run the data ingestion: dagster dev -m ilmanhinta.dagster"
echo "3. Start the API: uvicorn ilmanhinta.api.main:app --reload"
echo ""
echo "📚 Read the README.md for more information"
