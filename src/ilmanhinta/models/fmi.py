"""Pydantic models for FMI weather data."""

from datetime import datetime

from pydantic import BaseModel, Field


class FMIObservation(BaseModel):
    """Single weather observation from FMI."""

    timestamp: datetime = Field(..., description="Observation timestamp in UTC")
    temperature: float | None = Field(None, description="Temperature (°C)")
    humidity: float | None = Field(None, description="Relative humidity (%)")
    wind_speed: float | None = Field(None, description="Wind speed (m/s)")
    wind_direction: float | None = Field(None, description="Wind direction (degrees)")
    pressure: float | None = Field(None, description="Pressure (hPa)")
    precipitation: float | None = Field(None, description="Precipitation (mm)")
    cloud_cover: float | None = Field(None, description="Cloud cover (oktas)")


class WeatherData(BaseModel):
    """Collection of weather observations."""

    station_id: str = Field(..., description="FMI station identifier")
    station_name: str = Field(..., description="Station name")
    observations: list[FMIObservation] = Field(
        default_factory=list, description="Weather observations"
    )


class PredictionInput(BaseModel):
    """Input features for ML model prediction."""

    timestamp: datetime
    temperature: float
    humidity: float | None = None
    wind_speed: float | None = None
    pressure: float | None = None
    hour_of_day: int = Field(..., ge=0, lt=24)
    day_of_week: int = Field(..., ge=0, lt=7)
    is_weekend: bool
    consumption_lag_1h: float | None = None
    consumption_lag_24h: float | None = None
    consumption_rolling_mean_24h: float | None = None
    consumption_rolling_std_24h: float | None = None


class PredictionOutput(BaseModel):
    """Output from ML model."""

    timestamp: datetime
    predicted_consumption_mw: float
    confidence_lower: float
    confidence_upper: float
    model_version: str
