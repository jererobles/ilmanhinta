"""Test script for the comparison API endpoints.

Run this after starting the API server:
    uvicorn ilmanhinta.api.main:app --reload

Or from this script directly (starts embedded server).
"""

import asyncio

import httpx

API_BASE_URL = "http://localhost:8000"


async def test_api_endpoints():
    """Test all comparison API endpoints."""
    print("=" * 60)
    print("Testing Price Comparison API")
    print("=" * 60)

    async with httpx.AsyncClient() as client:
        # Test 1: Health check
        print("\n1. Testing health check...")
        response = await client.get(f"{API_BASE_URL}/health")
        if response.status_code == 200:
            print(f"✓ Health check passed: {response.json()}")
        else:
            print(f"✗ Health check failed: {response.status_code}")

        # Test 2: Latest predictions
        print("\n2. Testing latest predictions...")
        response = await client.get(
            f"{API_BASE_URL}/api/v1/predictions/latest",
            params={"hours_ahead": 24, "model_type": "production"},
        )

        if response.status_code == 200:
            predictions = response.json()
            print(f"✓ Got {len(predictions)} predictions")
            if predictions:
                print(f"  Sample: {predictions[0]}")
        elif response.status_code == 404:
            print("ℹ No predictions found yet (expected if you haven't generated any)")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")

        # Test 3: Comparison data
        print("\n3. Testing comparison data...")
        response = await client.get(
            f"{API_BASE_URL}/api/v1/comparison",
            params={"hours_back": 168, "only_with_actuals": False, "limit": 50},
        )

        if response.status_code == 200:
            comparisons = response.json()
            print(f"✓ Got {len(comparisons)} comparison records")
            if comparisons:
                print(f"  Sample: {comparisons[0]}")

                # Count wins
                ml_wins = sum(1 for c in comparisons if c.get("winner") == "ML")
                fingrid_wins = sum(1 for c in comparisons if c.get("winner") == "Fingrid")
                ties = sum(1 for c in comparisons if c.get("winner") == "Tie")

                print("\n  Win Distribution:")
                print(f"    ML Wins: {ml_wins}")
                print(f"    Fingrid Wins: {fingrid_wins}")
                print(f"    Ties: {ties}")

        elif response.status_code == 404:
            print("ℹ No comparison data yet")
            print("  Make sure you have:")
            print("    1. Generated ML predictions")
            print("    2. Stored Fingrid forecasts")
            print("    3. Refreshed materialized views")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")

        # Test 4: Performance summary
        print("\n4. Testing performance summary...")
        response = await client.get(
            f"{API_BASE_URL}/api/v1/performance/summary",
            params={"hours_back": 168},
        )

        if response.status_code == 200:
            summary = response.json()
            print("✓ Performance Summary:")
            print(f"  Period: {summary['period_hours']} hours")
            print(f"  Total Predictions: {summary['total_predictions']}")
            print(f"  With Actuals: {summary['predictions_with_actuals']}")
            if summary.get("ml_win_rate") is not None:
                print(f"  ML Win Rate: {summary['ml_win_rate']:.1f}%")

            print("\n  Metrics:")
            for metric in summary.get("metrics", []):
                print(f"    {metric['metric_name']}:")
                if metric.get("ml_value") is not None:
                    print(f"      ML: {metric['ml_value']:.2f}")
                if metric.get("fingrid_value") is not None:
                    print(f"      Fingrid: {metric['fingrid_value']:.2f}")
                if metric.get("improvement_pct") is not None:
                    sign = "+" if metric["improvement_pct"] > 0 else ""
                    print(f"      Improvement: {sign}{metric['improvement_pct']:.1f}%")

        elif response.status_code == 404:
            print("ℹ No performance data available yet")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")

        # Test 5: Hourly performance
        print("\n5. Testing hourly performance...")
        response = await client.get(
            f"{API_BASE_URL}/api/v1/performance/hourly",
            params={"hours_back": 168},
        )

        if response.status_code == 200:
            hourly = response.json()
            print(f"✓ Got {len(hourly)} hourly performance records")
            if hourly:
                print(f"  Latest hour: {hourly[0]}")
        elif response.status_code == 404:
            print("ℹ No hourly performance data yet")
        else:
            print(f"✗ Error: {response.status_code} - {response.text}")

        # Test 6: Win conditions
        print("\n6. Testing win conditions analysis...")
        for group_by in ["hour", "day_of_week"]:
            print(f"\n  Grouping by: {group_by}")
            response = await client.get(
                f"{API_BASE_URL}/api/v1/insights/win-conditions",
                params={"hours_back": 720, "group_by": group_by, "min_cases": 5},
            )

            if response.status_code == 200:
                conditions = response.json()
                print(f"  ✓ Found {len(conditions)} win conditions")

                # Show top 3
                for i, cond in enumerate(conditions[:3], 1):
                    print(f"\n    {i}. {cond['condition']}")
                    print(f"       Cases: {cond['total_cases']}, Wins: {cond['ml_wins']}")
                    print(f"       Win Rate: {cond['win_rate']:.1f}%")
                    print(
                        f"       ML Error: {cond['avg_ml_error']:.2f} vs "
                        f"Fingrid: {cond['avg_fingrid_error']:.2f}"
                    )

            elif response.status_code == 404:
                print(f"  ℹ Not enough data for {group_by} grouping")
            else:
                print(f"  ✗ Error: {response.status_code}")

        # Test 7: Refresh views (admin)
        print("\n7. Testing admin view refresh...")
        response = await client.post(f"{API_BASE_URL}/api/v1/admin/refresh-views")

        if response.status_code == 200:
            result = response.json()
            print(f"✓ Views refreshed: {result['message']}")
        else:
            print(f"⚠ Refresh failed (might need to run migrations first): {response.status_code}")

    print("\n" + "=" * 60)
    print("API Testing Complete!")
    print("=" * 60)
    print("\nTo start the API server:")
    print("  uvicorn ilmanhinta.api.main:app --reload")
    print("\nAPI Documentation (once running):")
    print("  http://localhost:8000/docs")
    print("  http://localhost:8000/redoc")


def start_embedded_server():
    """Start the API server directly (for testing)."""
    import uvicorn
    from ilmanhinta.api.main import app

    print("Starting API server on http://localhost:8000")
    print("Press Ctrl+C to stop")
    print("\nAPI Docs: http://localhost:8000/docs")
    print("=" * 60)

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        # Start embedded server
        start_embedded_server()
    else:
        # Test endpoints
        asyncio.run(test_api_endpoints())
