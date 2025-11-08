FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Install uv for faster package management
RUN pip install uv

# Copy project files needed for installation (for layer caching)
COPY pyproject.toml README.md ./

# Copy source code (needed for editable install)
COPY src/ /app/src/

# Install package with dependencies using uv
RUN uv pip install --system -e .

# Create dagster home directory
RUN mkdir -p /opt/dagster/dagster_home

ENV PYTHONPATH=/app
ENV DAGSTER_HOME=/opt/dagster/dagster_home

EXPOSE 8000 3000
