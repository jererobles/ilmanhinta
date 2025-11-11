"""Logfire instrumentation - structured observability that auto-traces everything."""

import sys

import logfire

from .config import settings

# Initialize Logfire
logfire.configure(
    send_to_logfire=False,
    service_name=settings.logfire_project,
    additional_span_processors=[],  # Use default OTLP processors
    console=logfire.ConsoleOptions(
        verbose=settings.log_level.upper() == "DEBUG",
        colors="auto" if sys.stderr.isatty() else "never",
    ),
)

# Re-export logfire for use throughout the codebase
__all__ = ["logfire"]
