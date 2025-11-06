"""DuckDB data layer for time-series storage and querying."""

from .duckdb_client import DuckDBClient, get_db_path

__all__ = ["DuckDBClient", "get_db_path"]
