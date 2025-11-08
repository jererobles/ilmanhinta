"""Quick-start example for hybrid forecasting system.

This script demonstrates how to use the new Fingrid price/forecast datasets
and the comparison framework.

Run this after setting up the database with:
    psql -U api -d ilmanhinta -f db/migrations/001_hybrid_forecasting_schema.sql
"""

import asyncio
from datetime import UTC, datetime, timedelta

import polars as pl
from ilmanhinta.clients.fingrid import FingridClient
from ilmanhinta.clients.fmi import FMIClient
from ilmanhinta.db.postgres_client import PostgresClient


async def example_fetch_training_data():
    """Example: Fetch historical data for model training."""
    print("\n=== Fetching Training Data ===")

    async with FingridClient() as client:
        # Fetch last 7 days of data (actuals + prices)
        data = await client.fetch_training_data(hours=168)

        print(f"Consumption data points: {len(data['consumption'])}")
        print(f"Production data points: {len(data['production'])}")
        print(f"Price data points: {len(data['price_imbalance'])}")

        # Preview price data
        if data["price_imbalance"]:
            sample = data["price_imbalance"][:3]
            print("\nSample price data:")
            for point in sample:
                print(f"  {point.start_time}: {point.value:.2f} EUR/MWh")

    return data


async def example_fetch_forecast_data():
    """Example: Fetch forecast data for making predictions."""
    print("\n=== Fetching Forecast Data ===")

    async with FingridClient() as client:
        # Fetch next 48 hours of forecasts
        forecasts = await client.fetch_forecast_data(hours=48)

        print(f"Consumption forecast (RT): {len(forecasts['consumption_forecast_rt'])} points")
        print(f"Production forecast: {len(forecasts['production_forecast'])} points")
        print(f"Wind forecast: {len(forecasts['wind_forecast'])} points")

        # Preview consumption forecast
        if forecasts["consumption_forecast_rt"]:
            sample = forecasts["consumption_forecast_rt"][:3]
            print("\nSample consumption forecast:")
            for point in sample:
                print(f"  {point.start_time}: {point.value:.2f} MW")

    return forecasts


def example_fetch_weather_data():
    """Example: Fetch weather observations and forecasts."""
    print("\n=== Fetching Weather Data ===")

    # Helsinki Kaisaniemi station
    fmi = FMIClient(station_id="101004")

    # Fetch last 24 hours of observations
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(hours=24)

    obs = fmi.fetch_observations(start_time, end_time)
    print(f"Weather observations: {len(obs.observations)} points from {obs.station_name}")

    if obs.observations:
        sample = obs.observations[:3]
        print("\nSample weather observations:")
        for o in sample:
            print(f"  {o.timestamp}: {o.temperature}°C, Wind: {o.wind_speed} m/s")

    # Fetch next 48 hours of forecasts
    fcst = fmi.fetch_forecast(hours=48)
    print(f"Weather forecasts: {len(fcst.observations)} points")

    if fcst.observations:
        sample = fcst.observations[:3]
        print("\nSample weather forecasts:")
        for o in sample:
            print(f"  {o.timestamp}: {o.temperature}°C, Wind: {o.wind_speed} m/s")

    return obs, fcst


def example_store_data_in_database(fingrid_data, weather_obs, weather_fcst):
    """Example: Store data in PostgreSQL/TimescaleDB."""
    print("\n=== Storing Data in Database ===")

    db = PostgresClient()

    # Convert Fingrid price data to DataFrame
    price_records = [
        {
            "time": point.start_time,
            "price_eur_mwh": point.value,
        }
        for point in fingrid_data["price_imbalance"]
    ]

    if price_records:
        price_df = pl.DataFrame(price_records)

        # Insert prices
        rows_inserted = db.insert_prices(
            price_df,
            area="FI",
            price_type="imbalance",
            source="fingrid",
        )
        print(f"Inserted {rows_inserted} price records")

    # Convert weather observations to DataFrame
    obs_records = [
        {
            "time": obs.timestamp,
            "temperature_c": obs.temperature,
            "wind_speed_ms": obs.wind_speed,
            "wind_direction": obs.wind_direction,
            "humidity_percent": obs.humidity,
            "pressure_hpa": obs.pressure,
            "cloud_cover": obs.cloud_cover,
            "precipitation_mm": obs.precipitation,
        }
        for obs in weather_obs.observations
        if obs.temperature is not None
    ]

    if obs_records:
        obs_df = pl.DataFrame(obs_records)
        rows_inserted = db.insert_weather(obs_df, station_id=weather_obs.station_id)
        print(f"Inserted {rows_inserted} weather observation records")

    # Convert weather forecasts to DataFrame
    fcst_records = [
        {
            "forecast_time": obs.timestamp,
            "temperature_c": obs.temperature,
            "wind_speed_ms": obs.wind_speed,
            "wind_direction": obs.wind_direction,
            "humidity_percent": obs.humidity,
            "pressure_hpa": obs.pressure,
            "cloud_cover": obs.cloud_cover,
            "precipitation_mm": obs.precipitation,
        }
        for obs in weather_fcst.observations
        if obs.temperature is not None
    ]

    if fcst_records:
        fcst_df = pl.DataFrame(fcst_records)
        rows_inserted = db.insert_weather_forecast(
            fcst_df,
            station_id=weather_fcst.station_id,
            forecast_model="harmonie",
        )
        print(f"Inserted {rows_inserted} weather forecast records")

    db.close()


def example_query_comparison_data():
    """Example: Query and display prediction comparison data."""
    print("\n=== Querying Comparison Data ===")

    db = PostgresClient()

    # Get latest comparison data
    try:
        comparison = db.get_prediction_comparison(limit=10)

        if len(comparison) > 0:
            print(f"Found {len(comparison)} comparison records")
            print("\nLatest predictions vs actuals:")
            print(
                comparison.select(
                    [
                        "time",
                        "ml_prediction",
                        "fingrid_price_forecast",
                        "actual_price",
                        "winner",
                    ]
                )
            )
        else:
            print("No comparison data yet. You need to:")
            print("  1. Insert historical prices (actuals)")
            print("  2. Insert Fingrid forecasts")
            print("  3. Train and run your ML model to generate predictions")
            print("  4. Refresh the materialized view: db.refresh_materialized_views()")

    except Exception as e:
        print(f"Error querying comparison data: {e}")
        print("Make sure you've run the migration: db/migrations/001_hybrid_forecasting_schema.sql")

    # Get performance summary
    try:
        summary = db.get_performance_summary(hours_back=24)

        if summary:
            print("\nPerformance Summary (last 24 hours):")
            for metric, values in summary.items():
                print(f"\n{metric}:")
                print(f"  ML Model: {values['ml']}")
                print(f"  Fingrid: {values['fingrid']}")
                if values["improvement_pct"]:
                    print(f"  Improvement: {values['improvement_pct']}%")
        else:
            print("\nNo performance data available yet")

    except Exception as e:
        print(f"Note: Performance summary not available yet: {e}")

    db.close()


def example_generate_mock_predictions():
    """Example: Generate and store mock predictions for testing comparison.

    In production, this would be your actual ML model.
    """
    print("\n=== Generating Mock Predictions ===")

    db = PostgresClient()

    # Get some historical prices to base predictions on
    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=1)

    prices = db.get_prices(start_time, end_time)

    if len(prices) > 0:
        # Create mock predictions (actual price + small random noise)
        import random

        predictions = []
        for row in prices.iter_rows(named=True):
            actual = row["price_eur_mwh"]
            # Add ±10% noise to simulate prediction
            noise = random.uniform(-0.1, 0.1) * actual
            predicted = actual + noise

            predictions.append(
                {
                    "prediction_time": row["time"],
                    "predicted_price_eur_mwh": predicted,
                    "confidence_lower": predicted - abs(noise) * 2,
                    "confidence_upper": predicted + abs(noise) * 2,
                }
            )

        pred_df = pl.DataFrame(predictions)

        rows_inserted = db.insert_predictions(
            pred_df,
            model_type="mock_model",
            model_version="v0.1_demo",
        )

        print(f"Inserted {rows_inserted} mock predictions")
        print("Now run: db.refresh_materialized_views() to see comparison data")

    else:
        print("No historical prices found. Fetch some first using fetch_training_data()")

    db.close()


async def main():
    """Run all examples."""
    print("=" * 60)
    print("Hybrid Forecasting Quick-Start Examples")
    print("=" * 60)

    # 1. Fetch training data from Fingrid
    fingrid_data = await example_fetch_training_data()

    # 2. Fetch forecast data from Fingrid
    fingrid_forecasts = await example_fetch_forecast_data()

    # 3. Fetch weather data from FMI
    weather_obs, weather_fcst = example_fetch_weather_data()

    # 4. Store data in database
    example_store_data_in_database(fingrid_data, weather_obs, weather_fcst)

    # 5. Generate mock predictions (for testing)
    example_generate_mock_predictions()

    # 6. Query comparison data
    example_query_comparison_data()

    print("\n" + "=" * 60)
    print("Examples completed!")
    print("\nNext steps:")
    print("  1. Review HYBRID_FORECASTING_GUIDE.md for detailed documentation")
    print("  2. Build feature engineering pipeline (Phase 3)")
    print("  3. Train your ML model on historical data")
    print("  4. Generate real predictions and compare!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
