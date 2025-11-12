"""Shared prediction storage using PostgreSQL + TimescaleDB.

This module provides a simplified interface for storing and retrieving consumption-model
predictions. All consumption predictions are stored in PostgreSQL with TimescaleDB for efficient
time-series queries.
"""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from ilmanhinta.db.postgres_client import get_database_url
from ilmanhinta.models.fmi import PredictionOutput

try:
    import psycopg
except ModuleNotFoundError as e:  # pragma: no cover
    raise ImportError(
        "psycopg is required for prediction storage. Install with: pip install psycopg[binary]"
    ) from e

POSTGRES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS consumption_model_predictions (
    timestamp TIMESTAMPTZ NOT NULL,
    predicted_consumption_mw DOUBLE PRECISION NOT NULL,
    confidence_lower DOUBLE PRECISION NOT NULL,
    confidence_upper DOUBLE PRECISION NOT NULL,
    model_type TEXT NOT NULL,
    model_version TEXT,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, model_type, generated_at)
);
"""


@contextmanager
def _pg_conn() -> Generator[Any, None, None]:
    """Yield a Postgres connection."""
    conn = psycopg.connect(get_database_url())
    try:
        yield conn
    finally:
        conn.close()


def _ensure_postgres_schema() -> None:
    """Create the consumption_model_predictions table if it doesn't exist."""
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(POSTGRES_TABLE_SQL)
        conn.commit()


def store_predictions(
    predictions: list[PredictionOutput],
    model_type: str,
) -> int:
    """Persist consumption-model predictions to PostgreSQL + TimescaleDB.

    Args:
        predictions: List of upcoming consumption predictions
        model_type: Model identifier (lightgbm, ensemble, prophet, etc.)

    Returns:
        Number of upserted rows
    """
    if not predictions:
        return 0

    _ensure_postgres_schema()

    rows = [
        (
            prediction.timestamp,
            prediction.predicted_consumption_mw,
            prediction.confidence_lower,
            prediction.confidence_upper,
            model_type,
            prediction.model_version,
        )
        for prediction in predictions
    ]

    with _pg_conn() as conn, conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO consumption_model_predictions (
                timestamp,
                predicted_consumption_mw,
                confidence_lower,
                confidence_upper,
                model_type,
                model_version
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, model_type, generated_at)
            DO UPDATE SET
                predicted_consumption_mw = EXCLUDED.predicted_consumption_mw,
                confidence_lower = EXCLUDED.confidence_lower,
                confidence_upper = EXCLUDED.confidence_upper,
                model_version = EXCLUDED.model_version
            """,
            rows,
        )
        conn.commit()

    return len(rows)


def fetch_latest_predictions(
    *,
    limit: int = 24,
    model_type: str | None = None,
) -> list[PredictionOutput]:
    """Fetch most recent consumption-model predictions for the given model.

    Returns predictions from the latest run (latest generated_at), ordered chronologically.

    Args:
        limit: Maximum number of predictions to return
        model_type: Filter by model type (optional)

    Returns:
        List of predictions ordered by timestamp (chronological)
    """
    _ensure_postgres_schema()

    params: list[Any] = []
    where_clause = ""
    cte_where_clause = ""
    if model_type:
        where_clause = "AND model_type = %s"
        cte_where_clause = "WHERE model_type = %s"
        params.append(model_type)  # For CTE
        params.append(model_type)  # For main query

    params.append(limit)

    query = f"""
        WITH latest_run AS (
            SELECT MAX(generated_at) as latest_generated
            FROM consumption_model_predictions
            {cte_where_clause}
        )
        SELECT
            timestamp,
            predicted_consumption_mw,
            confidence_lower,
            confidence_upper,
            model_type,
            COALESCE(model_version, 'unknown') AS model_version
        FROM consumption_model_predictions p, latest_run
        WHERE p.generated_at = latest_run.latest_generated
        {where_clause}
        ORDER BY timestamp ASC
        LIMIT %s
    """

    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    return [
        PredictionOutput(
            timestamp=row[0],
            predicted_consumption_mw=row[1],
            confidence_lower=row[2],
            confidence_upper=row[3],
            model_version=row[5],
        )
        for row in rows
    ]


def get_prediction_status(model_type: str | None = None) -> dict[str, Any]:
    """Return availability metadata for latest consumption-model predictions.

    Args:
        model_type: Filter by model type (optional)

    Returns:
        Dict with keys: available, latest_timestamp, model_version
    """
    _ensure_postgres_schema()

    params: list[Any] = []
    where_clause = ""
    if model_type:
        where_clause = "WHERE model_type = %s"
        params.append(model_type)

    query = f"""
        SELECT
            timestamp,
            model_version,
            generated_at
        FROM consumption_model_predictions
        {where_clause}
        ORDER BY generated_at DESC, timestamp DESC
        LIMIT 1
    """

    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()

    if not row:
        return {
            "available": False,
            "latest_timestamp": None,
            "model_version": None,
        }

    return {
        "available": True,
        "latest_timestamp": row[0],
        "model_version": row[1],
    }
