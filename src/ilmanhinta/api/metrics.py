"""Prometheus metrics for API monitoring."""

from prometheus_client import Counter, Gauge, Histogram

# Request metrics
api_requests_total = Counter(
    "ilmanhinta_api_requests_total",
    "Total API requests",
    ["method", "endpoint", "status"],
)

api_request_duration_seconds = Histogram(
    "ilmanhinta_api_request_duration_seconds",
    "API request latency",
    ["method", "endpoint"],
)

# Prediction metrics
predictions_total = Counter(
    "ilmanhinta_predictions_total",
    "Total predictions made",
)

prediction_value_mw = Gauge(
    "ilmanhinta_peak_prediction_mw",
    "Peak predicted consumption (MW) in next 24h",
)

# Model metrics
model_version_info = Gauge(
    "ilmanhinta_model_version_info",
    "Model version information",
    ["version"],
)

# Data fetching metrics
data_fetch_duration_seconds = Histogram(
    "ilmanhinta_data_fetch_duration_seconds",
    "Data fetching duration",
    ["source"],
)

data_fetch_errors_total = Counter(
    "ilmanhinta_data_fetch_errors_total",
    "Data fetching errors",
    ["source"],
)
