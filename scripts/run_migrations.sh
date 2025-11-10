#!/usr/bin/env bash
#
# Run Alembic database migrations
#
# Usage:
#   ./scripts/run_migrations.sh              # Run all pending migrations
#   ./scripts/run_migrations.sh upgrade +1   # Run one migration forward
#   ./scripts/run_migrations.sh downgrade -1 # Rollback one migration
#

set -euo pipefail

# Change to project root directory
cd "$(dirname "$0")/.."

# Check if DATABASE_URL is set
if [ -z "${DATABASE_URL:-}" ]; then
    echo "❌ ERROR: DATABASE_URL environment variable is not set"
    echo ""
    echo "Please set DATABASE_URL before running migrations:"
    echo "  export DATABASE_URL='postgresql://user:password@host:port/database'"
    echo ""
    echo "Or load from .env file:"
    echo "  export \$(cat .env | xargs)"
    exit 1
fi

echo "🔄 Running Alembic migrations..."
echo "   Database: ${DATABASE_URL%%@*}@***"  # Hide password in output
echo ""

# Run migrations with uv
if [ $# -eq 0 ]; then
    # No arguments: upgrade to head
    uv run alembic upgrade head
else
    # Pass through arguments to alembic
    uv run alembic "$@"
fi

echo ""
echo "✅ Migrations complete!"
echo ""
echo "To check migration status:"
echo "  ./scripts/check_migration_status.sh"
