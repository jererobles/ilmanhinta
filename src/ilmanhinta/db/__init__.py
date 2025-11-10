"""PostgreSQL + TimescaleDB data layer for time-series storage and querying.

This module provides PostgreSQL clients optimized for time-series data with
TimescaleDB extensions including compression, hypertables, and continuous aggregates.
"""

from .postgres_client import PostgresClient, get_database_url

__all__ = ["PostgresClient", "get_database_url"]
