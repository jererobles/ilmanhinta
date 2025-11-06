"""Logfire instrumentation - structured observability that auto-traces everything.

This replaces loguru's manual logging with Pydantic's Logfire.
The magic: it auto-instruments without explicit logger.info() calls.
The reality: you still need to configure it properly.
"""

import sys

import logfire

from .config import settings

# Initialize Logfire
logfire.configure(
    token=settings.logfire_token,
    project_name=settings.logfire_project,
    environment=settings.logfire_environment,
    # Send to console if no token provided (local development)
    send_to_logfire=settings.logfire_token is not None,
    console=logfire.ConsoleOptions(
        verbose=settings.log_level.upper() == "DEBUG",
        colors="auto" if sys.stderr.isatty() else "never",
    ),
)

# Re-export logfire for use throughout the codebase
__all__ = ["logfire"]
