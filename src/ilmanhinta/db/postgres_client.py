"""PostgreSQL/TimescaleDB client for time-series data.

This replaces the DuckDB client with PostgreSQL + TimescaleDB for better
production compatibility and concurrent access.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

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

    def insert_prices(
        self,
        df: pl.DataFrame,
        area: str = "FI",
        price_type: str = "imbalance",
        source: str = "fingrid",
    ) -> int:
        """Insert electricity prices from Polars DataFrame.

        Args:
            df: Polars DataFrame with price data
                Required columns: time, price_eur_mwh
            area: Price area (default: FI)
            price_type: Type of price (imbalance, day_ahead, intraday)
            source: Data source (fingrid, nord_pool, etc.)

        Returns:
            Number of rows inserted
        """
        pdf = df.to_pandas()
        pdf["area"] = area
        pdf["price_type"] = price_type
        pdf["source"] = source

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

    def insert_weather_forecast(
        self,
        df: pl.DataFrame,
        station_id: str,
        forecast_model: str = "harmonie",
        generated_at: datetime | None = None,
    ) -> int:
        """Insert weather forecasts from Polars DataFrame (FMI HARMONIE).

        Args:
            df: Polars DataFrame with forecast data
                Required columns: forecast_time (or time), temperature_c
                Optional: wind_speed_ms, cloud_cover, humidity_percent, pressure_hpa,
                         global_radiation_wm2
            station_id: Weather station identifier
            forecast_model: Model name (harmonie, ecmwf)
            generated_at: When forecast was generated (defaults to now)

        Returns:
            Number of rows inserted
        """
        pdf = df.to_pandas()

        # Rename time column if needed
        if "time" in pdf.columns and "forecast_time" not in pdf.columns:
            pdf = pdf.rename(columns={"time": "forecast_time"})

        pdf["station_id"] = station_id
        pdf["forecast_model"] = forecast_model
        pdf["generated_at"] = generated_at or datetime.now(UTC)

        with self.session() as session:
            rows_affected = pdf.to_sql(
                "weather_forecasts",
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            return len(pdf)

    def insert_fingrid_forecast(
        self,
        df: pl.DataFrame,
        forecast_type: str,
        unit: str = "MW",
        update_frequency: str | None = None,
        generated_at: datetime | None = None,
    ) -> int:
        """Insert Fingrid forecasts from Polars DataFrame.

        Args:
            df: Polars DataFrame with forecast data
                Required columns: forecast_time (or time), value
            forecast_type: Type of forecast (consumption, production, wind, price)
            unit: Unit of measurement (MW or EUR/MWh)
            update_frequency: How often updated (15min, hourly, daily)
            generated_at: When forecast was generated (defaults to now)

        Returns:
            Number of rows inserted
        """
        pdf = df.to_pandas()

        # Rename columns if needed
        if "time" in pdf.columns and "forecast_time" not in pdf.columns:
            pdf = pdf.rename(columns={"time": "forecast_time"})

        pdf["forecast_type"] = forecast_type
        pdf["unit"] = unit
        if update_frequency:
            pdf["update_frequency"] = update_frequency
        pdf["generated_at"] = generated_at or datetime.now(UTC)

        with self.session() as session:
            rows_affected = pdf.to_sql(
                "fingrid_forecasts",
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
            )
            return len(pdf)

    def insert_predictions(
        self,
        df: pl.DataFrame,
        model_type: str = "production",
        model_version: str | None = None,
        generated_at: datetime | None = None,
        training_end_date: datetime | None = None,
    ) -> int:
        """Insert price predictions from Polars DataFrame.

        Args:
            df: Polars DataFrame with predictions
                Required columns: prediction_time (or time), predicted_price_eur_mwh
                Optional: confidence_lower, confidence_upper, predicted_consumption_mw
            model_type: Model identifier (production, staging, experimental, etc.)
            model_version: Version string (e.g., 'v1.0', '2024-11-08')
            generated_at: When predictions were generated (defaults to now)
            training_end_date: Last date in training data

        Returns:
            Number of rows inserted
        """
        pdf = df.to_pandas()

        # Rename columns if needed
        if "time" in pdf.columns and "prediction_time" not in pdf.columns:
            pdf = pdf.rename(columns={"time": "prediction_time"})

        # Handle Prophet column names
        column_mapping = {
            "yhat": "predicted_price_eur_mwh",
            "yhat_lower": "confidence_lower",
            "yhat_upper": "confidence_upper",
        }
        pdf = pdf.rename(columns=column_mapping)

        pdf["model_type"] = model_type
        if model_version:
            pdf["model_version"] = model_version
        pdf["generated_at"] = generated_at or datetime.now(UTC)
        if training_end_date:
            pdf["training_end_date"] = training_end_date

        with self.session() as session:
            rows_affected = pdf.to_sql(
                "model_predictions",
                con=self.engine,
                if_exists="append",
                index=False,
                method="multi",
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
        end_time = end_time or datetime.now(UTC)

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
        end_time = end_time or datetime.now(UTC)

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
        end_time = end_time or datetime.now(UTC)

        if station_id:
            query = text(
                """
                SELECT time, station_id,
                       temperature_c AS temperature,
                       wind_speed_ms AS wind_speed,
                       wind_direction,
                       cloud_cover,
                       humidity_percent AS humidity,
                       pressure_hpa AS pressure,
                       precipitation_mm
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
                SELECT time, station_id,
                       temperature_c AS temperature,
                       wind_speed_ms AS wind_speed,
                       wind_direction,
                       cloud_cover,
                       humidity_percent AS humidity,
                       pressure_hpa AS pressure,
                       precipitation_mm
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
                SELECT MAX(generated_at) as latest_generated
                FROM predictions
                WHERE model_type = :model_type
            )
            SELECT
                p.timestamp,
                p.predicted_consumption_mw,
                p.confidence_lower,
                p.confidence_upper,
                p.model_type,
                p.model_version,
                p.generated_at
            FROM predictions p, latest_run
            WHERE p.generated_at = latest_run.latest_generated
              AND p.model_type = :model_type
            ORDER BY p.timestamp
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
                c.wind_mw,
                c.nuclear_mw,
                c.net_import_mw,
                w.temperature_c AS temperature,
                w.wind_speed_ms AS wind_speed,
                w.wind_direction,
                w.cloud_cover,
                w.humidity_percent AS humidity,
                w.pressure_hpa AS pressure,
                w.precipitation_mm,
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

    def get_prediction_comparison(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> pl.DataFrame:
        """Get prediction comparison data (ML vs Fingrid vs Actual).

        Args:
            start_time: Start timestamp (defaults to 7 days ago)
            end_time: End timestamp (defaults to now)
            limit: Maximum number of rows to return

        Returns:
            Polars DataFrame with comparison metrics
        """
        start_time = start_time or (datetime.now(UTC) - timedelta(days=7))
        end_time = end_time or datetime.now(UTC)

        query = text(
            """
            SELECT
                time,
                ml_prediction,
                ml_lower,
                ml_upper,
                fingrid_price_forecast,
                actual_price,
                ml_absolute_error,
                ml_percentage_error,
                fingrid_absolute_error,
                fingrid_percentage_error,
                CASE ml_vs_fingrid_winner
                    WHEN 1 THEN 'ML'
                    WHEN -1 THEN 'Fingrid'
                    ELSE 'Tie'
                END as winner,
                model_type,
                model_version
            FROM prediction_comparison
            WHERE time >= :start_time AND time <= :end_time
            ORDER BY time DESC
            LIMIT :limit
        """
        )

        with self.session() as session:
            result = session.execute(
                query, {"start_time": start_time, "end_time": end_time, "limit": limit}
            )
            pdf = pd.DataFrame(result.fetchall(), columns=result.keys())
            return pl.from_pandas(pdf)

    def get_performance_summary(self, hours_back: int = 24) -> dict[str, any]:
        """Get performance summary comparing ML model vs Fingrid forecasts.

        Args:
            hours_back: Number of hours to look back

        Returns:
            Dict with performance metrics
        """
        query = text("SELECT * FROM get_latest_performance_summary(:hours_back)")

        with self.session() as session:
            result = session.execute(query, {"hours_back": hours_back})
            rows = result.fetchall()

            # Convert to dictionary
            summary = {}
            for row in rows:
                metric = row[0]
                ml_value = row[1]
                fingrid_value = row[2]
                improvement = row[3]

                summary[metric] = {
                    "ml": float(ml_value) if ml_value else None,
                    "fingrid": float(fingrid_value) if fingrid_value else None,
                    "improvement_pct": float(improvement) if improvement else None,
                }

            return summary

    def get_hourly_performance(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> pl.DataFrame:
        """Get hourly aggregated performance metrics.

        Args:
            start_time: Start timestamp (defaults to 7 days ago)
            end_time: End timestamp (defaults to now)

        Returns:
            Polars DataFrame with hourly performance
        """
        start_time = start_time or (datetime.now(UTC) - timedelta(days=7))
        end_time = end_time or datetime.now(UTC)

        query = text(
            """
            SELECT
                hour,
                num_predictions,
                ml_avg_error,
                ml_median_error,
                ml_avg_pct_error,
                fingrid_avg_error,
                fingrid_avg_pct_error,
                ml_win_rate_pct,
                avg_actual_price,
                min_actual_price,
                max_actual_price
            FROM hourly_prediction_performance
            WHERE hour >= :start_time AND hour <= :end_time
            ORDER BY hour DESC
        """
        )

        with self.session() as session:
            result = session.execute(query, {"start_time": start_time, "end_time": end_time})
            pdf = pd.DataFrame(result.fetchall(), columns=result.keys())
            return pl.from_pandas(pdf)

    def refresh_materialized_views(self) -> None:
        """Refresh all materialized views for better query performance."""
        with self.session() as session:
            # Refresh comparison view
            session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY prediction_comparison"))

            # Legacy views (if they exist)
            try:
                session.execute(text("REFRESH MATERIALIZED VIEW hourly_consumption_stats"))
                session.execute(text("REFRESH MATERIALIZED VIEW daily_price_stats"))
            except Exception:
                pass  # Views might not exist yet

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
