"""Shared prediction storage with optional Postgres backend."""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Iterable

import polars as pl

from ilmanhinta.db import DuckDBClient
from ilmanhinta.models.fmi import PredictionOutput

try:
    import psycopg  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency for local dev
    psycopg = None  # type: ignore

POSTGRES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS predictions (
    timestamp TIMESTAMPTZ NOT NULL,
    predicted_consumption_mw DOUBLE PRECISION NOT NULL,
    confidence_lower DOUBLE PRECISION NOT NULL,
    confidence_upper DOUBLE PRECISION NOT NULL,
    model_type TEXT NOT NULL,
    model_version TEXT,
    generated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (timestamp, model_type)
);
"""


def _use_postgres() -> bool:
    """Return True if a Postgres connection string is available."""
    return bool(os.getenv("DATABASE_URL")) and psycopg is not None


@contextmanager
def _pg_conn():
    """Yield a Postgres connection when DATABASE_URL is set."""
    if not _use_postgres():
        raise RuntimeError("Postgres connection requested but DATABASE_URL is missing")
    conn = psycopg.connect(os.environ["DATABASE_URL"])
    try:
        yield conn
    finally:
        conn.close()


def _ensure_postgres_schema() -> None:
    """Create the predictions table when using Postgres."""
    if not _use_postgres():
        return
    with _pg_conn() as conn, conn.cursor() as cur:
        cur.execute(POSTGRES_TABLE_SQL)
        conn.commit()


def _as_dataframe(predictions: Iterable[PredictionOutput]) -> pl.DataFrame:
    """Convert predictions into a Polars DataFrame for DuckDB ingestion."""
    rows = [prediction.model_dump() for prediction in predictions]
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows)


def store_predictions(
    predictions: list[PredictionOutput],
    model_type: str,
) -> int:
    """Persist predictions to Postgres when available, otherwise DuckDB.

    Args:
        predictions: List of predictions for the next horizon
        model_type: Model identifier (lightgbm, ensemble, etc.)

    Returns:
        Number of upserted rows
    """
    if not predictions:
        return 0

    if _use_postgres():
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
                INSERT INTO predictions (
                    timestamp,
                    predicted_consumption_mw,
                    confidence_lower,
                    confidence_upper,
                    model_type,
                    model_version
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, model_type)
                DO UPDATE SET
                    predicted_consumption_mw = EXCLUDED.predicted_consumption_mw,
                    confidence_lower = EXCLUDED.confidence_lower,
                    confidence_upper = EXCLUDED.confidence_upper,
                    model_version = EXCLUDED.model_version,
                    generated_at = NOW()
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    # DuckDB fallback (local dev or tests)
    df = _as_dataframe(predictions)
    if df.is_empty():
        return 0

    client = DuckDBClient()
    try:
        return client.insert_predictions(
            df,
            model_type=model_type,
            model_version=predictions[0].model_version,
        )
    finally:
        client.close()


def fetch_latest_predictions(
    *,
    limit: int = 24,
    model_type: str | None = None,
) -> list[PredictionOutput]:
    """Fetch most recent predictions for the given model."""
    if _use_postgres():
        _ensure_postgres_schema()
        params: list[Any] = []
        where_clause = ""
        if model_type:
            where_clause = "WHERE model_type = %s"
            params.append(model_type)
        params.append(limit)
        query = f"""
            SELECT
                timestamp,
                predicted_consumption_mw,
                confidence_lower,
                confidence_upper,
                model_type,
                COALESCE(model_version, 'unknown') AS model_version
            FROM predictions
            {where_clause}
            ORDER BY timestamp DESC
            LIMIT %s
        """
        with _pg_conn() as conn, conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
        # Reverse to chronological order for API consumers
        rows.reverse()
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

    client = DuckDBClient()
    try:
        df = client.get_latest_predictions(model_type=model_type, limit=limit)
    finally:
        client.close()
    if df.is_empty():
        return []
    df = df.sort("timestamp")
    return [
        PredictionOutput(
            timestamp=row["timestamp"],
            predicted_consumption_mw=row["predicted_consumption_mw"],
            confidence_lower=row["confidence_lower"],
            confidence_upper=row["confidence_upper"],
            model_version=row.get("model_version") or "unknown",
        )
        for row in df.iter_rows(named=True)
    ]


def get_prediction_status(model_type: str | None = None) -> dict[str, Any]:
    """Return availability metadata for latest predictions."""
    if _use_postgres():
        _ensure_postgres_schema()
        params: list[Any] = []
        where_clause = ""
        if model_type:
            where_clause = "WHERE model_type = %s"
            params.append(model_type)
        query = f"""
            SELECT
                timestamp,
                model_version
            FROM predictions
            {where_clause}
            ORDER BY timestamp DESC
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

    client = DuckDBClient()
    try:
        df = client.get_latest_predictions(model_type=model_type, limit=1)
    finally:
        client.close()
    if df.is_empty():
        return {"available": False, "latest_timestamp": None, "model_version": None}
    row = df[0]
    return {
        "available": True,
        "latest_timestamp": row["timestamp"],
        "model_version": row.get("model_version"),
    }
