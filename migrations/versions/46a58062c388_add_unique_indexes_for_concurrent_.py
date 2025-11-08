"""add_unique_indexes_for_concurrent_refresh

Revision ID: 46a58062c388
Revises: 04ac04d57f32
Create Date: 2025-11-08 09:07:37.956803

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "46a58062c388"
down_revision: str | Sequence[str] | None = "04ac04d57f32"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add unique indexes to materialized views for CONCURRENT refresh support.

    These materialized views were changed from continuous aggregates due to
    TimescaleDB limitations (can't join multiple hypertables in continuous aggregates).
    They now require unique indexes for CONCURRENT refresh to work without locking.
    """
    # Add unique index for prediction_accuracy_hourly
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_accuracy_hourly_unique
        ON prediction_accuracy_hourly(hour, model_type);
        """
    )

    # Add unique index for prediction_accuracy_daily
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_prediction_accuracy_daily_unique
        ON prediction_accuracy_daily(day, model_type);
        """
    )

    # Add unique index for model_comparison_24h
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_model_comparison_24h_unique
        ON model_comparison_24h(model_type);
        """
    )


def downgrade() -> None:
    """Remove unique indexes from materialized views."""
    op.execute("DROP INDEX IF EXISTS idx_prediction_accuracy_hourly_unique;")
    op.execute("DROP INDEX IF EXISTS idx_prediction_accuracy_daily_unique;")
    op.execute("DROP INDEX IF EXISTS idx_model_comparison_24h_unique;")
