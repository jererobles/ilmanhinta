# Multi-stage build with uv for modern Python packaging
FROM python:3.11-slim AS builder

# Install system dependencies for Prophet (needs C++ compiler)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./

# Install dependencies using uv (much faster than pip)
RUN uv pip install --system -r pyproject.toml

# Production stage
FROM python:3.11-slim

WORKDIR /app

# Copy Python packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages

# Copy application code
COPY src/ilmanhinta ./ilmanhinta

# Create data directories
RUN mkdir -p /app/data/{raw,processed,models} /app/dagster_home

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/app/dagster_home

# Expose ports
EXPOSE 8000 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD python -c "import httpx; httpx.get('http://localhost:8000/health')"

# Default command (can be overridden)
CMD ["python", "-m", "uvicorn", "ilmanhinta.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
