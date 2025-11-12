"""PostgreSQL/TimescaleDB client for time-series data.

This replaces the DuckDB client with PostgreSQL + TimescaleDB for better
production compatibility and concurrent access.
"""

from __future__ import annotations

import math
import os
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any

import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql.expression import TextClause


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
    - fingrid_power_actuals: Fingrid electricity data (15-min intervals)
    - fingrid_price_actuals: Spot prices per area
    - fmi_weather_observations: FMI weather data (hourly)
    - consumption_model_predictions: Consumption model outputs with confidence intervals
    - price_model_predictions: Price model outputs with confidence intervals
    """

    def __init__(self, database_url: str | None = None) -> None:
        """Initialize PostgreSQL client.

        Args:
            database_url: PostgreSQL connection string
        """
        self.database_url = database_url or get_database_url()
        self._engine: Engine | None = None
        self._session_factory: sessionmaker[Session] | None = None

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
    def session_factory(self) -> sessionmaker[Session]:
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

    def _read_frame(
        self,
        query: str | TextClause,
        params: Mapping[str, object] | None = None,
    ) -> pl.DataFrame:
        """Execute a query and return the result as a Polars DataFrame."""
        stmt = query if isinstance(query, TextClause) else text(query)
        if params:
            stmt = stmt.bindparams(**params)
        with self.engine.connect() as connection:
            return pl.read_database(stmt, connection=connection)

    def insert_consumption(self, df: pl.DataFrame) -> int:
        """Insert electricity consumption data from Polars DataFrame.

        Args:
            df: Polars DataFrame with consumption data
                Required columns: time, consumption_mw
                Optional: production_mw, net_import_mw

        Returns:
            Number of rows inserted/updated
        """
        column_mapping = {
            "timestamp": "time",
            "consumption": "consumption_mw",
            "production": "production_mw",
            "net_import": "net_import_mw",
        }
        rename_map = {old: new for old, new in column_mapping.items() if old in df.columns}
        if rename_map:
            df = df.rename(rename_map)

        target_columns = [
            "time",
            "consumption_mw",
            "production_mw",
            "wind_mw",
            "nuclear_mw",
            "net_import_mw",
        ]
        missing = [col for col in target_columns if col not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(col) for col in missing])

        values_list = df.select(target_columns).to_dicts()

        if not values_list:
            return 0

        upsert_sql = text(
            """
            INSERT INTO fingrid_power_actuals (time, consumption_mw, production_mw, wind_mw, nuclear_mw, net_import_mw)
            VALUES (:time, :consumption_mw, :production_mw, :wind_mw, :nuclear_mw, :net_import_mw)
            ON CONFLICT (time) DO UPDATE SET
                consumption_mw = EXCLUDED.consumption_mw,
                production_mw = EXCLUDED.production_mw,
                wind_mw = EXCLUDED.wind_mw,
                nuclear_mw = EXCLUDED.nuclear_mw,
                net_import_mw = EXCLUDED.net_import_mw
        """
        )
        with self.session() as session:
            session.execute(upsert_sql, values_list)
            session.commit()

        return len(values_list)

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
        if "timestamp" in df.columns and "time" not in df.columns:
            df = df.rename({"timestamp": "time"})

        df = df.with_columns(
            [
                pl.lit(area).alias("area"),
                pl.lit(price_type).alias("price_type"),
                pl.lit(source).alias("source"),
            ]
        )

        target_columns = ["time", "price_eur_mwh", "area", "price_type", "source"]
        missing = [col for col in target_columns if col not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(col) for col in missing])

        records = df.select(target_columns).to_dicts()
        if not records:
            return 0

        insert_sql = text(
            """
            INSERT INTO fingrid_price_actuals (time, price_eur_mwh, area, price_type, source)
            VALUES (:time, :price_eur_mwh, :area, :price_type, :source)
            ON CONFLICT DO NOTHING
        """
        )

        with self.session() as session:
            session.execute(insert_sql, records)
            session.commit()

        return len(records)

    def insert_weather(self, df: pl.DataFrame, station_id: str) -> int:
        """Insert weather observations from Polars DataFrame.

        Args:
            df: Polars DataFrame with weather data
                Required columns: timestamp or time, temperature_c
                Optional: wind_speed_ms, cloud_cover, humidity_percent, pressure_hpa
            station_id: Weather station identifier

        Returns:
            Number of rows inserted/updated
        """
        if "timestamp" in df.columns and "time" not in df.columns:
            df = df.rename({"timestamp": "time"})

        df = df.with_columns(pl.lit(station_id).alias("station_id"))

        target_columns = [
            "time",
            "station_id",
            "temperature_c",
            "humidity_percent",
            "wind_speed_ms",
            "wind_direction",
            "pressure_hpa",
            "precipitation_mm",
            "cloud_cover",
        ]
        missing = [col for col in target_columns if col not in df.columns]
        if missing:
            df = df.with_columns([pl.lit(None).alias(col) for col in missing])

        values_list: list[dict[str, object]] = []
        for row in df.select(target_columns).to_dicts():
            cloud_cover = row.get("cloud_cover")
            if isinstance(cloud_cover, float):
                cloud_cover = None if math.isnan(cloud_cover) else int(cloud_cover)
            elif isinstance(cloud_cover, int):
                cloud_cover = int(cloud_cover)
            else:
                cloud_cover = None if cloud_cover is None else cloud_cover

            row["cloud_cover"] = cloud_cover
            values_list.append(row)

        if not values_list:
            return 0

        upsert_sql = text(
            """
            INSERT INTO fmi_weather_observations
            (time, station_id, temperature_c, humidity_percent, wind_speed_ms,
             wind_direction, pressure_hpa, precipitation_mm, cloud_cover)
            VALUES (:time, :station_id, :temperature_c, :humidity_percent, :wind_speed_ms,
                    :wind_direction, :pressure_hpa, :precipitation_mm, :cloud_cover)
            ON CONFLICT (time, station_id, data_type) DO UPDATE SET
                temperature_c = EXCLUDED.temperature_c,
                humidity_percent = EXCLUDED.humidity_percent,
                wind_speed_ms = EXCLUDED.wind_speed_ms,
                wind_direction = EXCLUDED.wind_direction,
                pressure_hpa = EXCLUDED.pressure_hpa,
                precipitation_mm = EXCLUDED.precipitation_mm,
                cloud_cover = EXCLUDED.cloud_cover
        """
        )
        with self.session() as session:
            session.execute(upsert_sql, values_list)
            session.commit()

        return len(values_list)

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
        if "time" in df.columns and "forecast_time" not in df.columns:
            df = df.rename({"time": "forecast_time"})

        generated_at = generated_at or datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit(station_id).alias("station_id"),
                pl.lit(forecast_model).alias("forecast_model"),
                pl.lit(generated_at).alias("generated_at"),
            ]
        )

        if df.is_empty():
            return 0

        return df.write_database(
            table_name="fmi_weather_forecasts",
            connection=self.engine,
            if_table_exists="append",
        )

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
        if "time" in df.columns and "forecast_time" not in df.columns:
            df = df.rename({"time": "forecast_time"})

        generated_at = generated_at or datetime.now(UTC)
        additions = [
            pl.lit(forecast_type).alias("forecast_type"),
            pl.lit(unit).alias("unit"),
            pl.lit(generated_at).alias("generated_at"),
        ]
        if update_frequency:
            additions.append(pl.lit(update_frequency).alias("update_frequency"))

        df = df.with_columns(additions)

        if df.is_empty():
            return 0

        return df.write_database(
            table_name="fingrid_power_forecasts",
            connection=self.engine,
            if_table_exists="append",
        )

    def insert_predictions(
        self,
        df: pl.DataFrame,
        model_type: str = "production",
        model_version: str | None = None,
        generated_at: datetime | None = None,
        training_end_date: datetime | None = None,
    ) -> int:
        """Insert price-model predictions from a Polars DataFrame.

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
        if "time" in df.columns and "prediction_time" not in df.columns:
            df = df.rename({"time": "prediction_time"})

        column_mapping = {
            "yhat": "predicted_price_eur_mwh",
            "yhat_lower": "confidence_lower",
            "yhat_upper": "confidence_upper",
        }
        rename_map = {old: new for old, new in column_mapping.items() if old in df.columns}
        if rename_map:
            df = df.rename(rename_map)

        generated_at = generated_at or datetime.now(UTC)
        additions = [
            pl.lit(model_type).alias("model_type"),
            pl.lit(generated_at).alias("generated_at"),
        ]
        if model_version:
            additions.append(pl.lit(model_version).alias("model_version"))
        if training_end_date:
            additions.append(pl.lit(training_end_date).alias("training_end_date"))

        df = df.with_columns(additions)

        if df.is_empty():
            return 0

        return df.write_database(
            table_name="price_model_predictions",
            connection=self.engine,
            if_table_exists="append",
        )

    def get_fmi_weather_forecasts(
        self,
        start_time: datetime,
        end_time: datetime,
        station_id: str | None = None,
        forecast_model: str | None = None,
    ) -> pl.DataFrame:
        """Fetch weather forecasts for a time window.

        Returns the latest forecast per (forecast_time, station_id).
        """

        query = """
            SELECT DISTINCT ON (forecast_time, station_id)
                forecast_time,
                station_id,
                station_name,
                temperature_c,
                wind_speed_ms,
                wind_direction,
                cloud_cover,
                humidity_percent,
                pressure_hpa,
                precipitation_mm,
                global_radiation_wm2,
                forecast_model,
                generated_at
            FROM fmi_weather_forecasts
            WHERE forecast_time >= :start_time AND forecast_time <= :end_time
        """

        params: dict[str, object] = {
            "start_time": start_time,
            "end_time": end_time,
        }

        if station_id:
            query += " AND station_id = :station_id"
            params["station_id"] = station_id

        if forecast_model:
            query += " AND forecast_model = :forecast_model"
            params["forecast_model"] = forecast_model

        query += " ORDER BY forecast_time, station_id, generated_at DESC"

        return self._read_frame(query, params if params else None)

    def get_fingrid_power_forecasts(
        self,
        start_time: datetime,
        end_time: datetime,
        forecast_types: Sequence[str] | None = None,
    ) -> pl.DataFrame:
        """Fetch Fingrid forecasts (latest per forecast_time + type)."""

        query = """
            SELECT DISTINCT ON (forecast_time, forecast_type)
                forecast_time,
                forecast_type,
                value,
                unit,
                update_frequency,
                generated_at
            FROM fingrid_power_forecasts
            WHERE forecast_time >= :start_time AND forecast_time <= :end_time
        """

        params: dict[str, object] = {
            "start_time": start_time,
            "end_time": end_time,
        }

        if forecast_types:
            placeholders = ", ".join(f":forecast_type_{idx}" for idx in range(len(forecast_types)))
            query += f" AND forecast_type IN ({placeholders})"
            params.update({f"forecast_type_{idx}": ft for idx, ft in enumerate(forecast_types)})

        query += " ORDER BY forecast_time, forecast_type, generated_at DESC"

        return self._read_frame(query, params if params else None)

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
            FROM fingrid_power_actuals
            WHERE time >= :start_time AND time <= :end_time
            ORDER BY time
        """
        )

        return self._read_frame(query, {"start_time": start_time, "end_time": end_time})

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
            FROM fingrid_price_actuals
            WHERE time >= :start_time AND time <= :end_time
              AND area = :area
            ORDER BY time
        """
        )

        return self._read_frame(
            query, {"start_time": start_time, "end_time": end_time, "area": area}
        )

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
                FROM fmi_weather_observations
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
                FROM fmi_weather_observations
                WHERE time >= :start_time AND time <= :end_time
                ORDER BY time
            """
            )
            params = {"start_time": start_time, "end_time": end_time}

        return self._read_frame(query, params)

    def get_latest_predictions(
        self,
        model_type: str = "prophet",
        limit: int = 24,
    ) -> pl.DataFrame:
        """Get latest consumption-model predictions.

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
                FROM consumption_model_predictions
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
            FROM consumption_model_predictions p, latest_run
            WHERE p.generated_at = latest_run.latest_generated
              AND p.model_type = :model_type
            ORDER BY p.timestamp
            LIMIT :limit
        """
        )

        return self._read_frame(query, {"model_type": model_type, "limit": limit})

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
            FROM fingrid_power_actuals c
            LEFT JOIN LATERAL (
                SELECT * FROM fmi_weather_observations w2
                WHERE w2.time <= c.time
                ORDER BY w2.time DESC
                LIMIT 1
            ) w ON true
            LEFT JOIN LATERAL (
                SELECT * FROM fingrid_price_actuals p2
                WHERE p2.time = c.time
                  AND p2.area = 'FI'
                LIMIT 1
            ) p ON true
            WHERE c.time >= :start_time AND c.time <= :end_time
            ORDER BY c.time
        """
        )

        return self._read_frame(query, {"start_time": start_time, "end_time": end_time})

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

        return self._read_frame(
            query, {"start_time": start_time, "end_time": end_time, "limit": limit}
        )

    def get_performance_summary(self, hours_back: int = 24) -> dict[str, Any]:
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
            summary: dict[str, Any] = {}
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

        return self._read_frame(query, {"start_time": start_time, "end_time": end_time})

    def refresh_materialized_views(self) -> None:
        """Refresh all materialized views for better query performance."""
        with self.session() as session:
            # Keep prediction comparison views in sync before downstream aggregates
            session.execute(text("SELECT refresh_prediction_comparison()"))

            # Refresh prediction accuracy views (non-continuous due to multi-hypertable joins)
            session.execute(
                text("REFRESH MATERIALIZED VIEW CONCURRENTLY prediction_accuracy_hourly")
            )
            session.execute(
                text("REFRESH MATERIALIZED VIEW CONCURRENTLY prediction_accuracy_daily")
            )
            session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY model_comparison_24h"))

            # Legacy views (if they exist)
            try:
                session.execute(
                    text("REFRESH MATERIALIZED VIEW CONCURRENTLY hourly_consumption_stats")
                )
                session.execute(text("REFRESH MATERIALIZED VIEW CONCURRENTLY daily_price_stats"))
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
                text("SELECT COUNT(*) FROM fingrid_power_actuals")
            ).scalar()
            prices_count = session.execute(
                text("SELECT COUNT(*) FROM fingrid_price_actuals")
            ).scalar()
            weather_count = session.execute(
                text("SELECT COUNT(*) FROM fmi_weather_observations")
            ).scalar()
            predictions_count = session.execute(
                text("SELECT COUNT(*) FROM consumption_model_predictions")
            ).scalar()

            # Get database size
            db_size = session.execute(
                text(
                    """
                    SELECT pg_database_size(current_database()) as size
                """
                )
            ).scalar()

            return {
                "consumption_rows": int(consumption_count or 0),
                "prices_rows": int(prices_count or 0),
                "weather_rows": int(weather_count or 0),
                "predictions_rows": int(predictions_count or 0),
                "db_size_bytes": int(db_size or 0),
            }
