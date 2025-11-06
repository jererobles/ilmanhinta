"""Tests for Pydantic models."""

from datetime import datetime

from ilmanhinta.models.fingrid import FingridDataPoint, FingridResponse
from ilmanhinta.models.fmi import FMIObservation, WeatherData


def test_fingrid_data_point() -> None:
    """Test FingridDataPoint model."""
    point = FingridDataPoint(
        start_time=datetime(2024, 1, 1, 12, 0),
        end_time=datetime(2024, 1, 1, 13, 0),
        value=8500.0,
    )

    assert point.value == 8500.0
    assert point.start_time.hour == 12


def test_fingrid_response() -> None:
    """Test FingridResponse model."""
    response = FingridResponse(
        data=[
            FingridDataPoint(
                start_time=datetime(2024, 1, 1, 12, 0),
                end_time=datetime(2024, 1, 1, 13, 0),
                value=8500.0,
            )
        ]
    )

    assert len(response.data) == 1


def test_fmi_observation() -> None:
    """Test FMIObservation model."""
    obs = FMIObservation(
        timestamp=datetime(2024, 1, 1, 12, 0),
        temperature=-5.0,
        humidity=75.0,
        wind_speed=8.5,
    )

    assert obs.temperature == -5.0
    assert obs.humidity == 75.0


def test_weather_data() -> None:
    """Test WeatherData model."""
    data = WeatherData(
        station_id="101004",
        station_name="Helsinki Kaisaniemi",
        observations=[
            FMIObservation(
                timestamp=datetime(2024, 1, 1, 12, 0),
                temperature=-5.0,
            )
        ],
    )

    assert data.station_id == "101004"
    assert len(data.observations) == 1
