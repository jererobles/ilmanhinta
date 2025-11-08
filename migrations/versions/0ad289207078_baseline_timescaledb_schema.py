"""baseline_timescaledb_schema

Revision ID: 0ad289207078
Revises:
Create Date: 2025-11-08 07:01:59.227160

Creates the initial TimescaleDB schema with:
- Extensions (timescaledb, timescaledb_toolkit, pg_stat_monitor)
- Hypertables (electricity_consumption, electricity_prices, weather_observations, predictions)
- Compression policies
- Basic indexes (without forecast index - see next migration)
- Continuous aggregates for analytics
- Helper views for stats extraction

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0ad289207078"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create baseline TimescaleDB schema.

    This migration establishes the complete database schema from init_timescaledb_enhanced.sql,
    EXCLUDING the forecast index which will be added in a subsequent migration.
    """
    # Read and execute the initialization SQL
    # We execute this in one go since it's the initial schema
    with open("init_timescaledb_enhanced.sql") as f:
        baseline_sql = f.read()

    # Remove the forecast index from baseline (it will be added in next migration)
    # We'll split and filter out that specific CREATE INDEX statement
    statements = baseline_sql.split(";")
    filtered_statements = [stmt for stmt in statements if "idx_weather_forecast_lookup" not in stmt]

    # Execute all statements
    for statement in filtered_statements:
        statement = statement.strip()
        if statement:  # Skip empty statements
            op.execute(statement)


def downgrade() -> None:
    """Drop all TimescaleDB objects."""
    # Drop all views first (they depend on tables)
    op.execute("DROP VIEW IF EXISTS slow_queries CASCADE")
    op.execute("DROP VIEW IF EXISTS query_performance_buckets CASCADE")
    op.execute("DROP VIEW IF EXISTS frequent_queries CASCADE")
    op.execute("DROP VIEW IF EXISTS consumption_hourly_stats CASCADE")
    op.execute("DROP VIEW IF EXISTS prices_daily_stats CASCADE")
    op.execute("DROP VIEW IF EXISTS prediction_accuracy_hourly_stats CASCADE")
    op.execute("DROP VIEW IF EXISTS prediction_accuracy_daily_stats CASCADE")
    op.execute("DROP VIEW IF EXISTS model_comparison_24h_stats CASCADE")
    op.execute("DROP VIEW IF EXISTS latest_predictions_per_model CASCADE")
    op.execute("DROP VIEW IF EXISTS current_weather CASCADE")

    # Drop materialized views (continuous aggregates)
    op.execute("DROP MATERIALIZED VIEW IF EXISTS consumption_hourly CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS prices_daily CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS prediction_accuracy_hourly CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS prediction_accuracy_daily CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS model_comparison_24h CASCADE")

    # Drop hypertables (this also drops indexes and compression policies)
    op.execute("DROP TABLE IF EXISTS electricity_consumption CASCADE")
    op.execute("DROP TABLE IF EXISTS electricity_prices CASCADE")
    op.execute("DROP TABLE IF EXISTS weather_observations CASCADE")
    op.execute("DROP TABLE IF EXISTS predictions CASCADE")

    # Extensions can be left (they don't hurt and other databases might use them)
    # If you want to remove them:
    # op.execute("DROP EXTENSION IF EXISTS pg_stat_monitor")
    # op.execute("DROP EXTENSION IF EXISTS timescaledb_toolkit")
    # op.execute("DROP EXTENSION IF EXISTS timescaledb")
