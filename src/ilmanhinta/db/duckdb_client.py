"""DuckDB client for OLAP queries on time-series data.

This replaces direct Parquet scans with proper SQL queries over DuckDB tables.
DuckDB acts as both storage engine and compute layer - the Roided Cousin of SQLite.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator

import duckdb
import polars as pl


def get_db_path() -> Path:
    """Get the DuckDB database file path."""
    data_dir = Path("/app/data")
    if not data_dir.exists():
        data_dir = Path.cwd() / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir / "ilmanhinta.duckdb"


class DuckDBClient:
    """DuckDB client for time-series data storage and queries.

    Architecture:
    - Replaces direct Parquet scans with SQL queries
    - Acts as both OLAP engine AND storage (no need for separate cache/DB)
    - Reads Parquet files natively via DuckDB's parquet_scan
    - Polars interop via arrow for zero-copy data transfer

    Tables:
    - consumption: Fingrid electricity consumption (15-min intervals)
    - weather: FMI weather observations and forecasts (hourly)
    - predictions: Model predictions with confidence intervals
    """

    def __init__(self, db_path: Path | None = None) -> None:
        """Initialize DuckDB client.

        Args:
            db_path: Path to DuckDB file. Defaults to /app/data/ilmanhinta.duckdb
        """
        self.db_path = db_path or get_db_path()
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._init_schema()

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = duckdb.connect(str(self.db_path))
            # Optimize for time-series queries
            self._conn.execute("SET memory_limit='1GB'")
            self._conn.execute("SET threads=2")
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @contextmanager
    def transaction(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Context manager for transactions."""
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def _init_schema(self) -> None:
        """Initialize database schema if not exists."""
        with self.transaction():
            # Consumption table: Fingrid electricity data
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS consumption (
                    timestamp TIMESTAMP NOT NULL,
                    consumption_mw DOUBLE NOT NULL,
                    production_mw DOUBLE,
                    wind_mw DOUBLE,
                    nuclear_mw DOUBLE,
                    source VARCHAR DEFAULT 'fingrid',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (timestamp)
                )
            """)

            # Weather table: FMI observations and forecasts
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS weather (
                    timestamp TIMESTAMP NOT NULL,
                    temperature DOUBLE,
                    humidity DOUBLE,
                    wind_speed DOUBLE,
                    wind_direction DOUBLE,
                    pressure DOUBLE,
                    precipitation DOUBLE,
                    cloud_cover DOUBLE,
                    station_id VARCHAR,
                    data_type VARCHAR,  -- 'observation' or 'forecast'
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (timestamp, station_id, data_type)
                )
            """)

            # Predictions table: Model forecasts with confidence intervals
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS predictions (
                    timestamp TIMESTAMP NOT NULL,
                    predicted_consumption_mw DOUBLE NOT NULL,
                    confidence_lower DOUBLE NOT NULL,
                    confidence_upper DOUBLE NOT NULL,
                    model_type VARCHAR NOT NULL,  -- 'prophet', 'lightgbm', 'ensemble'
                    model_version VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (timestamp, model_type, created_at)
                )
            """)

            # Create indexes for time-series queries
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_consumption_ts ON consumption(timestamp)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_weather_ts ON weather(timestamp)"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_predictions_ts ON predictions(timestamp)"
            )

    def ingest_consumption_parquet(self, parquet_path: Path | str) -> int:
        """Ingest consumption data from Parquet file.

        Args:
            parquet_path: Path to Parquet file

        Returns:
            Number of rows inserted
        """
        with self.transaction():
            result = self.conn.execute(f"""
                INSERT INTO consumption (timestamp, consumption_mw, production_mw, wind_mw, nuclear_mw)
                SELECT
                    timestamp,
                    consumption_mw,
                    production_mw,
                    wind_mw,
                    nuclear_mw
                FROM read_parquet('{parquet_path}')
                ON CONFLICT (timestamp) DO UPDATE SET
                    consumption_mw = EXCLUDED.consumption_mw,
                    production_mw = EXCLUDED.production_mw,
                    wind_mw = EXCLUDED.wind_mw,
                    nuclear_mw = EXCLUDED.nuclear_mw,
                    created_at = CURRENT_TIMESTAMP
            """)
            return result.fetchone()[0] if result else 0

    def ingest_weather_parquet(self, parquet_path: Path | str, data_type: str = "observation") -> int:
        """Ingest weather data from Parquet file.

        Args:
            parquet_path: Path to Parquet file
            data_type: 'observation' or 'forecast'

        Returns:
            Number of rows inserted
        """
        with self.transaction():
            result = self.conn.execute(f"""
                INSERT INTO weather (
                    timestamp, temperature, humidity, wind_speed, wind_direction,
                    pressure, precipitation, cloud_cover, station_id, data_type
                )
                SELECT
                    timestamp,
                    temperature,
                    humidity,
                    wind_speed,
                    wind_direction,
                    pressure,
                    precipitation,
                    cloud_cover,
                    station_id,
                    '{data_type}' as data_type
                FROM read_parquet('{parquet_path}')
                ON CONFLICT (timestamp, station_id, data_type) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_direction = EXCLUDED.wind_direction,
                    pressure = EXCLUDED.pressure,
                    precipitation = EXCLUDED.precipitation,
                    cloud_cover = EXCLUDED.cloud_cover,
                    created_at = CURRENT_TIMESTAMP
            """)
            return result.fetchone()[0] if result else 0

    def insert_consumption(self, df: pl.DataFrame) -> int:
        """Insert consumption data from Polars DataFrame.

        Args:
            df: Polars DataFrame with consumption data

        Returns:
            Number of rows inserted
        """
        with self.transaction():
            # Convert Polars to Arrow for zero-copy transfer
            arrow_table = df.to_arrow()
            result = self.conn.execute("""
                INSERT INTO consumption (timestamp, consumption_mw, production_mw, wind_mw, nuclear_mw)
                SELECT * FROM arrow_table
                ON CONFLICT (timestamp) DO UPDATE SET
                    consumption_mw = EXCLUDED.consumption_mw,
                    production_mw = EXCLUDED.production_mw,
                    wind_mw = EXCLUDED.wind_mw,
                    nuclear_mw = EXCLUDED.nuclear_mw,
                    created_at = CURRENT_TIMESTAMP
            """)
            return result.fetchone()[0] if result else 0

    def insert_weather(self, df: pl.DataFrame, data_type: str = "observation") -> int:
        """Insert weather data from Polars DataFrame.

        Args:
            df: Polars DataFrame with weather data
            data_type: 'observation' or 'forecast'

        Returns:
            Number of rows inserted
        """
        # Add data_type column if not present
        if "data_type" not in df.columns:
            df = df.with_columns(pl.lit(data_type).alias("data_type"))

        with self.transaction():
            arrow_table = df.to_arrow()
            result = self.conn.execute("""
                INSERT INTO weather (
                    timestamp, temperature, humidity, wind_speed, wind_direction,
                    pressure, precipitation, cloud_cover, station_id, data_type
                )
                SELECT * FROM arrow_table
                ON CONFLICT (timestamp, station_id, data_type) DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    wind_speed = EXCLUDED.wind_speed,
                    wind_direction = EXCLUDED.wind_direction,
                    pressure = EXCLUDED.pressure,
                    precipitation = EXCLUDED.precipitation,
                    cloud_cover = EXCLUDED.cloud_cover,
                    created_at = CURRENT_TIMESTAMP
            """)
            return result.fetchone()[0] if result else 0

    def insert_predictions(
        self,
        df: pl.DataFrame,
        model_type: str,
        model_version: str | None = None,
    ) -> int:
        """Insert predictions from Polars DataFrame.

        Args:
            df: Polars DataFrame with predictions
            model_type: Model type ('prophet', 'lightgbm', 'ensemble')
            model_version: Optional model version string

        Returns:
            Number of rows inserted
        """
        # Add model metadata columns
        if "model_type" not in df.columns:
            df = df.with_columns(pl.lit(model_type).alias("model_type"))
        if "model_version" not in df.columns and model_version:
            df = df.with_columns(pl.lit(model_version).alias("model_version"))

        with self.transaction():
            arrow_table = df.to_arrow()
            self.conn.execute("""
                INSERT INTO predictions (
                    timestamp, predicted_consumption_mw, confidence_lower,
                    confidence_upper, model_type, model_version
                )
                SELECT * FROM arrow_table
            """)
            return len(df)

    def get_consumption(
        self,
        start_time: datetime,
        end_time: datetime | None = None,
    ) -> pl.DataFrame:
        """Query consumption data for time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp (defaults to now)

        Returns:
            Polars DataFrame with consumption data
        """
        end_time = end_time or datetime.now()
        result = self.conn.execute("""
            SELECT timestamp, consumption_mw, production_mw, wind_mw, nuclear_mw
            FROM consumption
            WHERE timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp
        """, [start_time, end_time])

        return pl.from_arrow(result.fetch_arrow_table())

    def get_weather(
        self,
        start_time: datetime,
        end_time: datetime | None = None,
        data_type: str | None = None,
    ) -> pl.DataFrame:
        """Query weather data for time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp (defaults to now)
            data_type: Filter by 'observation' or 'forecast' (optional)

        Returns:
            Polars DataFrame with weather data
        """
        end_time = end_time or datetime.now()

        if data_type:
            result = self.conn.execute("""
                SELECT timestamp, temperature, humidity, wind_speed, wind_direction,
                       pressure, precipitation, cloud_cover, station_id, data_type
                FROM weather
                WHERE timestamp >= ? AND timestamp <= ? AND data_type = ?
                ORDER BY timestamp
            """, [start_time, end_time, data_type])
        else:
            result = self.conn.execute("""
                SELECT timestamp, temperature, humidity, wind_speed, wind_direction,
                       pressure, precipitation, cloud_cover, station_id, data_type
                FROM weather
                WHERE timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp
            """, [start_time, end_time])

        return pl.from_arrow(result.fetch_arrow_table())

    def get_latest_predictions(
        self,
        model_type: str | None = None,
        limit: int = 24,
    ) -> pl.DataFrame:
        """Get latest predictions.

        Args:
            model_type: Filter by model type (optional)
            limit: Number of predictions to return

        Returns:
            Polars DataFrame with predictions
        """
        if model_type:
            result = self.conn.execute("""
                SELECT timestamp, predicted_consumption_mw, confidence_lower,
                       confidence_upper, model_type, model_version, created_at
                FROM predictions
                WHERE model_type = ?
                ORDER BY timestamp DESC
                LIMIT ?
            """, [model_type, limit])
        else:
            result = self.conn.execute("""
                SELECT timestamp, predicted_consumption_mw, confidence_lower,
                       confidence_upper, model_type, model_version, created_at
                FROM predictions
                ORDER BY timestamp DESC
                LIMIT ?
            """, [limit])

        return pl.from_arrow(result.fetch_arrow_table())

    def get_joined_training_data(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> pl.DataFrame:
        """Get joined consumption + weather data for ML training.

        This replaces the manual temporal joins in processing/joins.py.
        DuckDB handles the complex temporal alignment automatically.

        Args:
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            Polars DataFrame with joined data
        """
        result = self.conn.execute("""
            SELECT
                c.timestamp,
                c.consumption_mw,
                c.production_mw,
                c.wind_mw,
                c.nuclear_mw,
                w.temperature,
                w.humidity,
                w.wind_speed,
                w.wind_direction,
                w.pressure,
                w.precipitation,
                w.cloud_cover
            FROM consumption c
            LEFT JOIN LATERAL (
                SELECT * FROM weather w2
                WHERE w2.timestamp <= c.timestamp
                  AND w2.data_type = 'observation'
                ORDER BY w2.timestamp DESC
                LIMIT 1
            ) w ON true
            WHERE c.timestamp >= ? AND c.timestamp <= ?
            ORDER BY c.timestamp
        """, [start_time, end_time])

        return pl.from_arrow(result.fetch_arrow_table())

    def vacuum(self) -> None:
        """Optimize database (vacuum and analyze)."""
        self.conn.execute("VACUUM")
        self.conn.execute("ANALYZE")

    def get_stats(self) -> dict[str, int]:
        """Get database statistics.

        Returns:
            Dict with table row counts
        """
        consumption_count = self.conn.execute("SELECT COUNT(*) FROM consumption").fetchone()[0]
        weather_count = self.conn.execute("SELECT COUNT(*) FROM weather").fetchone()[0]
        predictions_count = self.conn.execute("SELECT COUNT(*) FROM predictions").fetchone()[0]

        return {
            "consumption_rows": consumption_count,
            "weather_rows": weather_count,
            "predictions_rows": predictions_count,
            "db_size_bytes": self.db_path.stat().st_size if self.db_path.exists() else 0,
        }
