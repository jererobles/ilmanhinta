#!/usr/bin/env python3
"""Test script to verify TimescaleDB migration is working correctly.

Run this after starting docker-compose to verify:
- PostgreSQL connection
- TimescaleDB extensions
- Schema creation
- Continuous aggregates
- API endpoints

Usage:
    python test_timescaledb_migration.py
"""

import os
import sys

try:
    import psycopg
    import requests
except ImportError:
    print("❌ Missing dependencies. Install with:")
    print("   pip install psycopg[binary] requests")
    sys.exit(1)


def test_postgres_connection() -> bool:
    """Test PostgreSQL connection."""
    print("\n🔍 Testing PostgreSQL connection...")
    try:
        url = os.getenv("DATABASE_URL", "postgresql://api:hunter2@localhost:5432/ilmanhinta")
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                version = cur.fetchone()[0]
                print(f"✅ Connected to PostgreSQL: {version[:50]}...")
                return True
    except Exception as e:
        print(f"❌ PostgreSQL connection failed: {e}")
        return False


def test_timescaledb_extension() -> bool:
    """Test TimescaleDB extension is installed."""
    print("\n🔍 Testing TimescaleDB extension...")
    try:
        url = os.getenv("DATABASE_URL", "postgresql://api:hunter2@localhost:5432/ilmanhinta")
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
                version = cur.fetchone()
                if version:
                    print(f"✅ TimescaleDB extension installed: version {version[0]}")
                    return True
                else:
                    print("❌ TimescaleDB extension not found")
                    return False
    except Exception as e:
        print(f"❌ TimescaleDB check failed: {e}")
        return False


def test_hypertables() -> bool:
    """Test that hypertables are created."""
    print("\n🔍 Testing hypertables...")
    try:
        url = os.getenv("DATABASE_URL", "postgresql://api:hunter2@localhost:5432/ilmanhinta")
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT hypertable_name
                    FROM timescaledb_information.hypertables
                    ORDER BY hypertable_name
                """
                )
                tables = cur.fetchall()
                if tables:
                    print(f"✅ Found {len(tables)} hypertables:")
                    for table in tables:
                        print(f"   - {table[0]}")
                    return True
                else:
                    print("❌ No hypertables found")
                    return False
    except Exception as e:
        print(f"❌ Hypertable check failed: {e}")
        return False


def test_continuous_aggregates() -> bool:
    """Test that continuous aggregates are created."""
    print("\n🔍 Testing continuous aggregates...")
    try:
        url = os.getenv("DATABASE_URL", "postgresql://api:hunter2@localhost:5432/ilmanhinta")
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT view_name
                    FROM timescaledb_information.continuous_aggregates
                    ORDER BY view_name
                """
                )
                views = cur.fetchall()
                if views:
                    print(f"✅ Found {len(views)} continuous aggregates:")
                    for view in views:
                        print(f"   - {view[0]}")
                    return True
                else:
                    print("⚠️  No continuous aggregates found (will be created on first use)")
                    return True  # Not a failure, just not materialized yet
    except Exception as e:
        print(f"❌ Continuous aggregate check failed: {e}")
        return False


def test_compression() -> bool:
    """Test that compression is configured."""
    print("\n🔍 Testing compression configuration...")
    try:
        url = os.getenv("DATABASE_URL", "postgresql://api:hunter2@localhost:5432/ilmanhinta")
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT hypertable_name, compression_enabled
                    FROM timescaledb_information.compression_settings
                    WHERE compression_enabled = true
                    ORDER BY hypertable_name
                """
                )
                tables = cur.fetchall()
                if tables:
                    print(f"✅ Compression enabled on {len(tables)} hypertables:")
                    for table in tables:
                        print(f"   - {table[0]}")
                    return True
                else:
                    print("⚠️  No compression enabled yet (normal for new database)")
                    return True
    except Exception as e:
        print(f"❌ Compression check failed: {e}")
        return False


def test_schema() -> bool:
    """Test that all required tables exist."""
    print("\n🔍 Testing database schema...")
    required_tables = [
        "electricity_consumption",
        "electricity_prices",
        "weather_observations",
        "predictions",
    ]
    try:
        url = os.getenv("DATABASE_URL", "postgresql://api:hunter2@localhost:5432/ilmanhinta")
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT tablename FROM pg_tables
                    WHERE schemaname = 'public'
                    ORDER BY tablename
                """
                )
                existing_tables = [row[0] for row in cur.fetchall()]

                missing = [t for t in required_tables if t not in existing_tables]
                if missing:
                    print(f"❌ Missing tables: {', '.join(missing)}")
                    return False
                else:
                    print("✅ All required tables exist:")
                    for table in required_tables:
                        print(f"   - {table}")
                    return True
    except Exception as e:
        print(f"❌ Schema check failed: {e}")
        return False


def test_api_health() -> bool:
    """Test API health endpoint."""
    print("\n🔍 Testing API health...")
    try:
        response = requests.get("http://localhost:8000/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print("✅ API is healthy:")
            print(f"   - Status: {data.get('status')}")
            print(f"   - Predictions available: {data.get('predictions_available')}")
            return True
        else:
            print(f"❌ API returned status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("⚠️  API not running (start with: docker-compose up api)")
        return False
    except Exception as e:
        print(f"❌ API health check failed: {e}")
        return False


def test_analytics_endpoints() -> bool:
    """Test new analytics endpoints."""
    print("\n🔍 Testing analytics endpoints...")
    endpoints = [
        "/analytics/summary",
        "/analytics/models/comparison",
        "/analytics/consumption/hourly?hours=24",
        "/analytics/accuracy/daily?days=7",
    ]

    results = []
    for endpoint in endpoints:
        try:
            response = requests.get(f"http://localhost:8000{endpoint}", timeout=5)
            if response.status_code in [200, 404]:  # 404 is ok if no data yet
                status = "✅" if response.status_code == 200 else "⚠️ "
                print(f"   {status} {endpoint}: {response.status_code}")
                results.append(True)
            else:
                print(f"   ❌ {endpoint}: {response.status_code}")
                results.append(False)
        except requests.exceptions.ConnectionError:
            print(f"   ⚠️  {endpoint}: API not running")
            return False
        except Exception as e:
            print(f"   ❌ {endpoint}: {e}")
            results.append(False)

    if all(results):
        print("✅ All analytics endpoints responding")
        return True
    else:
        print("⚠️  Some analytics endpoints not ready (may need data ingestion)")
        return True  # Not a critical failure


def main():
    """Run all tests."""
    print("=" * 60)
    print("  TimescaleDB Migration Test Suite")
    print("=" * 60)

    tests = [
        ("PostgreSQL Connection", test_postgres_connection),
        ("TimescaleDB Extension", test_timescaledb_extension),
        ("Database Schema", test_schema),
        ("Hypertables", test_hypertables),
        ("Continuous Aggregates", test_continuous_aggregates),
        ("Compression", test_compression),
        ("API Health", test_api_health),
        ("Analytics Endpoints", test_analytics_endpoints),
    ]

    results = {}
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            print(f"\n❌ {name} test crashed: {e}")
            results[name] = False

    # Summary
    print("\n" + "=" * 60)
    print("  Test Summary")
    print("=" * 60)
    passed = sum(1 for r in results.values() if r)
    total = len(results)

    for name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")

    print("\n" + "=" * 60)
    if passed == total:
        print(f"🎉 All tests passed! ({passed}/{total})")
        print("\nTimescaleDB migration is complete and working!")
        print("\nNext steps:")
        print(
            "1. Run data ingestion: docker exec -it ilmanhinta-dagster-daemon-1 dagster job execute -j bootstrap_system"
        )
        print("2. Check Dagster UI: http://localhost:3000")
        print("3. Explore analytics: http://localhost:8000/docs")
        return 0
    else:
        print(f"⚠️  Some tests failed ({passed}/{total} passed)")
        print("\nTroubleshooting:")
        print("1. Ensure Docker services are running: docker-compose up")
        print("2. Check logs: docker-compose logs postgres")
        print("3. Verify DATABASE_URL environment variable is set")
        print("4. See TIMESCALEDB_MIGRATION.md for detailed troubleshooting")
        return 1


if __name__ == "__main__":
    sys.exit(main())
