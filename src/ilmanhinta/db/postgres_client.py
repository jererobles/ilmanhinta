"""PostgreSQL/TimescaleDB client for time-series data.

This replaces the DuckDB client with PostgreSQL + TimescaleDB for better
production compatibility and concurrent access.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime

import pandas as pd
import polars as pl
from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker


def get_database_url() -> str:
    """Get PostgreSQL database URL from environment."""
    return os.getenv("DATABASE_URL", "postgresql://api:hunter2@localhost:5432/ilmanhinta")


class PostgresClient:
    """PostgreSQL/TimescaleDB client for time-series data storage and queries.

    Architecture:
    - Uses PostgreSQL with TimescaleDB extension for time-series optimization
    - SQLAlchemy for connection management
    - Polars/Pandas interop for data manipulation
    - DuckDB can connect via postgres_scanner for analytical queries

    Tables:
    - electricity_consumption: Fingrid electricity data (15-min intervals)
    - electricity_prices: Spot prices per area
    - weather_observations: FMI weather data (hourly)
    - predictions: Model predictions with confidence intervals
    """

    def __init__(self, database_url: str | None = None) -> None:
        """Initialize PostgreSQL client.

        Args:
            database_url: PostgreSQL connection string
        """
        self.database_url = database_url or get_database_url()
        self._engine: Engine | None = None
        self._session_factory: sessionmaker | None = None

    @property
    def engine(self) -> Engine:
        """Get or create database engine."""
        if self._engine is None:
            self._engine = create_engine(
                self.database_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
            )
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine)
        return self._session_factory

    def close(self) -> None:
        """Close database connections."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

    @contextmanager
    def session(self) -> Iterator[Session]:
        """Context manager for database sessions."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def insert_consumption(self, df: pl.DataFrame) -> int:
        """Insert electricity consumption data from Polars DataFrame.

        Args:
            df: Polars DataFrame with consumption data
                Required columns: time, consumption_mw
                Optional: production_mw, net_import_mw

        Returns:
            Number of rows inserted
        """
        # Convert Polars to pandas for SQLAlchemy
        pdf = df.to_pandas()

        # Rename columns to match database schema
        column_mapping = {
            "timestamp": "time",
            "consumption": "consumption_mw",
            "production": "production_mw",
            "net_import": "net_import_mw",
        }
        pdf = pdf.rename(columns=column_mapping)

        with self.session() as session:
            # Use PostgreSQL's ON CONFLICT for upsert
            rows_affected = pdf.to_sql(
                "electricity_consumption",
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            return len(pdf)

    def insert_prices(self, df: pl.DataFrame, area: str = "FI") -> int:
        """Insert electricity prices from Polars DataFrame.

        Args:
            df: Polars DataFrame with price data
                Required columns: time, price_eur_mwh
            area: Price area (default: FI)

        Returns:
            Number of rows inserted
        """
        pdf = df.to_pandas()
        pdf["area"] = area

        with self.session() as session:
            rows_affected = pdf.to_sql(
                "electricity_prices",
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            return len(pdf)

    def insert_weather(self, df: pl.DataFrame, station_id: str) -> int:
        """Insert weather observations from Polars DataFrame.

        Args:
            df: Polars DataFrame with weather data
                Required columns: time, temperature_c
                Optional: wind_speed_ms, cloud_cover, humidity_percent, pressure_hpa
            station_id: Weather station identifier

        Returns:
            Number of rows inserted
        """
        pdf = df.to_pandas()
        pdf["station_id"] = station_id

        with self.session() as session:
            rows_affected = pdf.to_sql(
                "weather_observations",
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            return len(pdf)

    def insert_predictions(
        self,
        df: pl.DataFrame,
        model_type: str = "prophet",
        model_version: str | None = None,
    ) -> int:
        """Insert predictions from Polars DataFrame.

        Args:
            df: Polars DataFrame with predictions
                Required columns: prediction_time, predicted_consumption_mw,
                                confidence_lower, confidence_upper
            model_type: Model type identifier
            model_version: Optional model version string

        Returns:
            Number of rows inserted
        """
        pdf = df.to_pandas()

        # Rename columns to match database schema
        column_mapping = {
            "timestamp": "prediction_time",
            "yhat": "predicted_consumption_mw",
            "yhat_lower": "confidence_lower",
            "yhat_upper": "confidence_upper",
        }
        pdf = pdf.rename(columns=column_mapping)

        pdf["model_type"] = model_type
        if model_version:
            pdf["model_version"] = model_version

        with self.session() as session:
            rows_affected = pdf.to_sql(
                "predictions", con=self.engine, if_exists="append", index=False, method="multi"
            )
            return len(pdf)

    def get_consumption(
        self,
        start_time: datetime,
        end_time: datetime | None = None,
    ) -> pl.DataFrame:
        """Query electricity consumption data for time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp (defaults to now)

        Returns:
            Polars DataFrame with consumption data
        """
        end_time = end_time or datetime.now()

        query = text(
            """
            SELECT time, consumption_mw, production_mw, net_import_mw
            FROM electricity_consumption
            WHERE time >= :start_time AND time <= :end_time
            ORDER BY time
        """
        )

        with self.session() as session:
            result = session.execute(query, {"start_time": start_time, "end_time": end_time})
            pdf = pd.DataFrame(result.fetchall(), columns=result.keys())
            return pl.from_pandas(pdf)

    def get_prices(
        self,
        start_time: datetime,
        end_time: datetime | None = None,
        area: str = "FI",
    ) -> pl.DataFrame:
        """Query electricity prices for time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp (defaults to now)
            area: Price area (default: FI)

        Returns:
            Polars DataFrame with price data
        """
        end_time = end_time or datetime.now()

        query = text(
            """
            SELECT time, price_eur_mwh, area
            FROM electricity_prices
            WHERE time >= :start_time AND time <= :end_time
              AND area = :area
            ORDER BY time
        """
        )

        with self.session() as session:
            result = session.execute(
                query, {"start_time": start_time, "end_time": end_time, "area": area}
            )
            pdf = pd.DataFrame(result.fetchall(), columns=result.keys())
            return pl.from_pandas(pdf)

    def get_weather(
        self,
        start_time: datetime,
        end_time: datetime | None = None,
        station_id: str | None = None,
    ) -> pl.DataFrame:
        """Query weather data for time range.

        Args:
            start_time: Start timestamp
            end_time: End timestamp (defaults to now)
            station_id: Filter by station (optional)

        Returns:
            Polars DataFrame with weather data
        """
        end_time = end_time or datetime.now()

        if station_id:
            query = text(
                """
                SELECT time, station_id, temperature_c, wind_speed_ms,
                       cloud_cover, humidity_percent, pressure_hpa
                FROM weather_observations
                WHERE time >= :start_time AND time <= :end_time
                  AND station_id = :station_id
                ORDER BY time
            """
            )
            params = {"start_time": start_time, "end_time": end_time, "station_id": station_id}
        else:
            query = text(
                """
                SELECT time, station_id, temperature_c, wind_speed_ms,
                       cloud_cover, humidity_percent, pressure_hpa
                FROM weather_observations
                WHERE time >= :start_time AND time <= :end_time
                ORDER BY time
            """
            )
            params = {"start_time": start_time, "end_time": end_time}

        with self.session() as session:
            result = session.execute(query, params)
            pdf = pd.DataFrame(result.fetchall(), columns=result.keys())
            return pl.from_pandas(pdf)

    def get_latest_predictions(
        self,
        model_type: str = "prophet",
        limit: int = 24,
    ) -> pl.DataFrame:
        """Get latest predictions.

        Args:
            model_type: Filter by model type
            limit: Number of predictions to return

        Returns:
            Polars DataFrame with predictions
        """
        query = text(
            """
            WITH latest_run AS (
                SELECT MAX(created_at) as latest_created
                FROM predictions
                WHERE model_type = :model_type
            )
            SELECT
                p.prediction_time,
                p.predicted_consumption_mw,
                p.predicted_price_eur_mwh,
                p.confidence_lower,
                p.confidence_upper,
                p.model_type,
                p.model_version,
                p.created_at
            FROM predictions p, latest_run
            WHERE p.created_at = latest_run.latest_created
              AND p.model_type = :model_type
            ORDER BY p.prediction_time
            LIMIT :limit
        """
        )

        with self.session() as session:
            result = session.execute(query, {"model_type": model_type, "limit": limit})
            pdf = pd.DataFrame(result.fetchall(), columns=result.keys())
            return pl.from_pandas(pdf)

    def get_joined_training_data(
        self,
        start_time: datetime,
        end_time: datetime,
    ) -> pl.DataFrame:
        """Get joined consumption + weather data for ML training.

        Uses PostgreSQL's LATERAL JOIN for efficient temporal alignment.

        Args:
            start_time: Start timestamp
            end_time: End timestamp

        Returns:
            Polars DataFrame with joined data
        """
        query = text(
            """
            SELECT
                c.time,
                c.consumption_mw,
                c.production_mw,
                c.net_import_mw,
                w.temperature_c,
                w.wind_speed_ms,
                w.cloud_cover,
                w.humidity_percent,
                w.pressure_hpa,
                p.price_eur_mwh
            FROM electricity_consumption c
            LEFT JOIN LATERAL (
                SELECT * FROM weather_observations w2
                WHERE w2.time <= c.time
                ORDER BY w2.time DESC
                LIMIT 1
            ) w ON true
            LEFT JOIN LATERAL (
                SELECT * FROM electricity_prices p2
                WHERE p2.time = c.time
                  AND p2.area = 'FI'
                LIMIT 1
            ) p ON true
            WHERE c.time >= :start_time AND c.time <= :end_time
            ORDER BY c.time
        """
        )

        with self.session() as session:
            result = session.execute(query, {"start_time": start_time, "end_time": end_time})
            pdf = pd.DataFrame(result.fetchall(), columns=result.keys())
            return pl.from_pandas(pdf)

    def refresh_materialized_views(self) -> None:
        """Refresh all materialized views for better query performance."""
        with self.session() as session:
            session.execute(text("REFRESH MATERIALIZED VIEW hourly_consumption_stats"))
            session.execute(text("REFRESH MATERIALIZED VIEW daily_price_stats"))
            session.commit()

    def get_stats(self) -> dict[str, int]:
        """Get database statistics.

        Returns:
            Dict with table row counts and database info
        """
        with self.session() as session:
            consumption_count = session.execute(
                text("SELECT COUNT(*) FROM electricity_consumption")
            ).scalar()
            prices_count = session.execute(text("SELECT COUNT(*) FROM electricity_prices")).scalar()
            weather_count = session.execute(
                text("SELECT COUNT(*) FROM weather_observations")
            ).scalar()
            predictions_count = session.execute(text("SELECT COUNT(*) FROM predictions")).scalar()

            # Get database size
            db_size = session.execute(
                text(
                    """
                    SELECT pg_database_size(current_database()) as size
                """
                )
            ).scalar()

            return {
                "consumption_rows": consumption_count,
                "prices_rows": prices_count,
                "weather_rows": weather_count,
                "predictions_rows": predictions_count,
                "db_size_bytes": db_size,
            }

    def connect_duckdb(self) -> None:
        """Allow DuckDB to connect to this PostgreSQL instance for analytical queries.

        This enables hybrid OLTP/OLAP workflows where PostgreSQL handles
        transactional workloads and DuckDB handles analytical queries.
        """
        import duckdb

        conn = duckdb.connect()
        conn.install_extension("postgres_scanner")
        conn.load_extension("postgres_scanner")

        # Parse connection string
        db_parts = self.database_url.replace("postgresql://", "").split("@")
        user_pass = db_parts[0].split(":")
        host_db = db_parts[1].split("/")
        host_port = host_db[0].split(":")

        # Attach PostgreSQL database
        conn.execute(
            f"""
            ATTACH 'dbname={host_db[1]} host={host_port[0]} port={host_port[1] if len(host_port) > 1 else '5432'}
                   user={user_pass[0]} password={user_pass[1]}' AS postgres_db (TYPE postgres)
        """
        )

        return conn
