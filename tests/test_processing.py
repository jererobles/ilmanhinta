"""Tests for data processing with Polars."""

from datetime import datetime

import polars as pl
import pytest
from ilmanhinta.models.fingrid import FingridDataPoint
from ilmanhinta.models.fmi import FMIObservation
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
        ),
        FMIObservation(
            timestamp=datetime(2024, 1, 1, 13, 0),
            temperature=-4.5,
            humidity=76.0,
        ),
    ]

    df = TemporalJoiner.fmi_to_polars(observations)

    assert len(df) == 2
    assert "timestamp" in df.columns
    assert "temperature" in df.columns


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
