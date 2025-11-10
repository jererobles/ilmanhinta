#!/usr/bin/env bash
#
# Check Alembic migration status
#
# Shows current database version and pending migrations
#

set -euo pipefail

# Change to project root directory
cd "$(dirname "$0")/.."

# Check if DATABASE_URL is set
if [ -z "${DATABASE_URL:-}" ]; then
    echo "❌ ERROR: DATABASE_URL environment variable is not set"
    echo ""
    echo "Please set DATABASE_URL before checking status:"
    echo "  export DATABASE_URL='postgresql://user:password@host:port/database'"
    echo ""
    echo "Or load from .env file:"
    echo "  export \$(cat .env | xargs)"
    exit 1
fi

echo "📊 Checking migration status..."
echo "   Database: ${DATABASE_URL%%@*}@***"  # Hide password in output
echo ""

# Show current version
echo "Current database version:"
uv run alembic current

echo ""
echo "Migration history:"
uv run alembic history --verbose

echo ""
echo "Available commands:"
echo "  ./scripts/run_migrations.sh              # Apply all pending migrations"
echo "  ./scripts/run_migrations.sh upgrade +1   # Apply one migration forward"
echo "  ./scripts/run_migrations.sh downgrade -1 # Rollback one migration"
echo "  uv run alembic history                   # Show migration history"
echo "  uv run alembic show <revision>           # Show details of a migration"
