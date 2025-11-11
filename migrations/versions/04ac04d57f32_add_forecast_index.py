"""add_forecast_index

Revision ID: 04ac04d57f32
Revises: 0ad289207078
Create Date: 2025-11-08 07:02:38.827349

Adds a partial index for weather forecast lookups.

This index optimizes queries that filter by data_type='forecast',
which are common when fetching weather forecasts for predictions.
Using a partial index (WHERE clause) makes it smaller and faster
than indexing all rows.

Performance impact:
- Before: Full table scan or index scan on all rows
- After: Direct index lookup on ~50% of rows (forecasts only)
- Estimated speedup: 2-5x for forecast queries

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "04ac04d57f32"
down_revision: str | Sequence[str] | None = "0ad289207078"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add partial index for weather forecast lookups."""
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_weather_forecast_lookup
        ON fmi_weather_observations(station_id, data_type, time DESC)
        WHERE data_type = 'forecast'
        """
    )


def downgrade() -> None:
    """Remove forecast index."""
    op.execute("DROP INDEX IF EXISTS idx_weather_forecast_lookup")
