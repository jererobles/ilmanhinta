"""OpenTelemetry-backed logging and tracing utilities."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk._logs import LoggerProvider as OtelLoggerProvider
from opentelemetry.sdk._logs import LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .config import settings

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(settings.observability_service_name)
tracer = trace.get_tracer(settings.observability_service_name)

_configured = False
_fastapi_instrumented = False


def _resource() -> Resource:
    """Build OTEL resource attributes shared by traces/logs."""
    attributes = {
        "service.name": settings.observability_service_name,
        "deployment.environment": settings.observability_environment,
    }
    return Resource(attributes={k: v for k, v in attributes.items() if v})


def configure_observability() -> None:
    """Initialize OpenTelemetry tracing + logging exporters."""
    global _configured, logger, tracer

    if _configured:
        return

    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    resource = _resource()

    trace_provider = TracerProvider(resource=resource)
    trace_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(trace_provider)
    tracer = trace.get_tracer(settings.observability_service_name)

    logger_provider = OtelLoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))

    otel_handler = LoggingHandler(logger_provider=logger_provider)
    otel_handler.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    )

    logging.basicConfig(
        level=log_level,
        handlers=[console_handler, otel_handler],
        force=True,
    )
    logging.captureWarnings(True)

    logger = logging.getLogger(settings.observability_service_name)
    _configured = True


def instrument_fastapi(app: FastAPI) -> None:
    """Attach OpenTelemetry instrumentation to the FastAPI app."""
    global _fastapi_instrumented

    if _fastapi_instrumented:
        return

    FastAPIInstrumentor.instrument_app(
        app, excluded_urls="health,metrics", exclude_spans=["send", "receive"]
    )
    _fastapi_instrumented = True


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a module-specific logger."""
    return logging.getLogger(name or settings.observability_service_name)


__all__ = ["configure_observability", "instrument_fastapi", "get_logger", "logger", "tracer"]
