"""Tests for data processing with Polars."""

from datetime import datetime, timedelta

import polars as pl
import pytest

from ilmanhinta.models.fingrid import FingridDataPoint
from ilmanhinta.models.fmi import FMIObservation
from ilmanhinta.processing.dataset_builder import PredictionDatasetBuilder
from ilmanhinta.processing.features import FeatureEngineer
from ilmanhinta.processing.joins import TemporalJoiner


def test_fingrid_to_polars() -> None:
    """Test conversion of Fingrid data to Polars DataFrame."""
    data = [
        FingridDataPoint(
            start_time=datetime(2024, 1, 1, 12, 0),
            end_time=datetime(2024, 1, 1, 13, 0),
            value=8500.0,
        ),
        FingridDataPoint(
            start_time=datetime(2024, 1, 1, 13, 0),
            end_time=datetime(2024, 1, 1, 14, 0),
            value=8600.0,
        ),
    ]

    df = TemporalJoiner.fingrid_to_polars(data)

    assert len(df) == 2
    assert "timestamp" in df.columns
    assert "consumption_mw" in df.columns


def test_fmi_to_polars() -> None:
    """Test conversion of FMI data to Polars DataFrame."""
    observations = [
        FMIObservation(
            timestamp=datetime(2024, 1, 1, 12, 0),
            temperature=-5.0,
            humidity=75.0,
            wind_speed=None,
            wind_direction=None,
            pressure=None,
            precipitation=None,
            cloud_cover=None,
        ),
        FMIObservation(
            timestamp=datetime(2024, 1, 1, 13, 0),
            temperature=-4.5,
            humidity=76.0,
            wind_speed=None,
            wind_direction=None,
            pressure=None,
            precipitation=None,
            cloud_cover=None,
        ),
    ]

    df = TemporalJoiner.fmi_to_polars(observations)

    assert len(df) == 2
    assert "timestamp" in df.columns
    assert "temperature_c" in df.columns
    assert "humidity_percent" in df.columns


def test_add_time_features() -> None:
    """Test time-based feature engineering."""
    df = pl.DataFrame(
        {
            "timestamp": [datetime(2024, 1, 1, 12, 0), datetime(2024, 1, 6, 18, 0)],
            "consumption_mw": [8500.0, 8600.0],
        }
    )

    result = FeatureEngineer.add_time_features(df)

    assert "hour_of_day" in result.columns
    assert "day_of_week" in result.columns
    assert "is_weekend" in result.columns

    # Check values
    assert result["hour_of_day"][0] == 12
    assert not result["is_weekend"][0]  # Monday
    assert result["is_weekend"][1]  # Saturday


def test_align_to_hourly() -> None:
    """Test alignment to hourly resolution."""
    # Create data with 15-minute intervals
    timestamps = [
        datetime(2024, 1, 1, 12, 0),
        datetime(2024, 1, 1, 12, 15),
        datetime(2024, 1, 1, 12, 30),
        datetime(2024, 1, 1, 12, 45),
    ]

    df = pl.DataFrame(
        {
            "timestamp": timestamps,
            "consumption_mw": [8500.0, 8520.0, 8510.0, 8530.0],
        }
    )

    result = TemporalJoiner.align_to_hourly(df)

    # Should aggregate to single hour
    assert len(result) == 1
    assert result["consumption_mw"][0] == pytest.approx((8500 + 8520 + 8510 + 8530) / 4)


def test_prediction_dataset_builder_uses_forecasts() -> None:
    """PredictionDatasetBuilder should combine weather + Fingrid forecasts."""

    start = datetime(2024, 1, 1, 12, 0, 0)
    end = datetime(2024, 1, 1, 13, 0, 0)

    class DummyDB:
        def get_weather_forecasts(
            self, start_time: datetime, end_time: datetime, **_: object
        ) -> pl.DataFrame:  # noqa: ANN001
            times = [start_time, end_time]
            return pl.DataFrame(
                {
                    "forecast_time": times,
                    "station_id": ["101004", "101004"],
                    "temperature_c": [-5.0, -4.0],
                    "wind_speed_ms": [3.0, 4.5],
                    "humidity_percent": [75.0, 80.0],
                    "pressure_hpa": [1005.0, 1003.0],
                    "cloud_cover": [5.0, 6.0],
                }
            )

        def get_fingrid_forecasts(
            self, start_time: datetime, end_time: datetime, **_: object
        ) -> pl.DataFrame:  # noqa: ANN001
            rows = []
            for ts in [start_time, end_time]:
                rows.extend(
                    [
                        {"forecast_time": ts, "forecast_type": "consumption", "value": 10000.0},
                        {"forecast_time": ts, "forecast_type": "production", "value": 9500.0},
                        {"forecast_time": ts, "forecast_type": "wind", "value": 1200.0},
                    ]
                )
            return pl.DataFrame(rows)

        def get_prices(self, start_time: datetime, end_time: datetime, **_: object) -> pl.DataFrame:  # noqa: ANN001
            del start_time, end_time
            base_time = start - timedelta(hours=1)
            return pl.DataFrame(
                {
                    "time": [
                        base_time,
                        base_time - timedelta(hours=1),
                        base_time - timedelta(hours=24),
                    ],
                    "price_eur_mwh": [50.0, 48.0, 60.0],
                }
            )

    builder = PredictionDatasetBuilder(db=DummyDB(), station_id="101004")

    df = builder.build(start, end, lookback_hours=2)

    assert not df.is_empty()
    assert "temperature" in df.columns
    assert "consumption_forecast_mw" in df.columns
    assert "price_lag_1h" in df.columns
    assert df.select("price_lag_1h").drop_nulls().height > 0
