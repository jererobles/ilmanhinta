#!/usr/bin/env bash
set -euo pipefail

export DAGSTER_HOME="${DAGSTER_HOME:-/app/dagster_home}"
export DAGSTER_GRPC_PORT="${DAGSTER_GRPC_PORT:-4000}"

mkdir -p "${DAGSTER_HOME}"

echo "Starting Dagster gRPC server on port ${DAGSTER_GRPC_PORT}..."
dagster api grpc -m ilmanhinta.dagster --host 0.0.0.0 --port "${DAGSTER_GRPC_PORT}" &
GRPC_PID=$!

cleanup() {
  echo "Shutting down Dagster services..."
  kill "${GRPC_PID}" >/dev/null 2>&1 || true
}

trap cleanup EXIT

echo "Starting Dagster daemon..."
dagster-daemon run
