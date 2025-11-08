"""Database and data schemas with column name constants.

This module defines constants for column names used throughout the application
to avoid hardcoding and make refactoring easier.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class ElectricityColumns:
    """Column names for electricity consumption and production data."""

    TIME = "time"
    TIMESTAMP = "timestamp"
    CONSUMPTION_MW = "consumption_mw"
    PRODUCTION_MW = "production_mw"
    WIND_MW = "wind_mw"
    NUCLEAR_MW = "nuclear_mw"
    NET_IMPORT_MW = "net_import_mw"
    SOURCE = "source"
    INGESTED_AT = "ingested_at"


class WeatherColumns:
    """Column names for weather observations and forecasts."""

    TIME = "time"
    TIMESTAMP = "timestamp"
    STATION_ID = "station_id"

    # Database column names (with units for clarity in SQL)
    TEMPERATURE_C = "temperature_c"
    HUMIDITY_PERCENT = "humidity_percent"
    WIND_SPEED_MS = "wind_speed_ms"
    WIND_DIRECTION = "wind_direction"
    PRESSURE_HPA = "pressure_hpa"
    PRECIPITATION_MM = "precipitation_mm"
    CLOUD_COVER = "cloud_cover"

    # Python-friendly aliases (without units, used in feature engineering)
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    WIND_SPEED = "wind_speed"
    PRESSURE = "pressure"

    DATA_TYPE = "data_type"
    INGESTED_AT = "ingested_at"


class PriceColumns:
    """Column names for electricity prices."""

    TIME = "time"
    PRICE_EUR_MWH = "price_eur_mwh"
    AREA = "area"
    SOURCE = "source"
    INGESTED_AT = "ingested_at"


class PredictionColumns:
    """Column names for model predictions."""

    TIMESTAMP = "timestamp"
    PREDICTED_CONSUMPTION_MW = "predicted_consumption_mw"
    CONFIDENCE_LOWER = "confidence_lower"
    CONFIDENCE_UPPER = "confidence_upper"
    MODEL_TYPE = "model_type"
    MODEL_VERSION = "model_version"
    GENERATED_AT = "generated_at"

    # Prophet-specific output columns
    YHAT = "yhat"
    YHAT_LOWER = "yhat_lower"
    YHAT_UPPER = "yhat_upper"
    DS = "ds"
    Y = "y"


class FeatureColumns:
    """Column names for engineered features."""

    # Time features
    HOUR_OF_DAY = "hour_of_day"
    DAY_OF_WEEK = "day_of_week"
    MONTH = "month"
    DAY_OF_MONTH = "day_of_month"
    IS_WEEKEND = "is_weekend"

    # Weather interaction features
    HEATING_DEGREE_DAYS = "heating_degree_days"
    COOLING_DEGREE_DAYS = "cooling_degree_days"
    TEMPERATURE_SQUARED = "temperature_squared"
    WIND_CHILL = "wind_chill"

    @staticmethod
    def lag_feature(column: str, lag_hours: int) -> str:
        """Generate lag feature name."""
        return f"{column}_lag_{lag_hours}h"

    @staticmethod
    def rolling_mean(column: str, window_hours: int) -> str:
        """Generate rolling mean feature name."""
        return f"{column}_rolling_mean_{window_hours}h"

    @staticmethod
    def rolling_std(column: str, window_hours: int) -> str:
        """Generate rolling std feature name."""
        return f"{column}_rolling_std_{window_hours}h"

    @staticmethod
    def rolling_min(column: str, window_hours: int) -> str:
        """Generate rolling min feature name."""
        return f"{column}_rolling_min_{window_hours}h"

    @staticmethod
    def rolling_max(column: str, window_hours: int) -> str:
        """Generate rolling max feature name."""
        return f"{column}_rolling_max_{window_hours}h"


class FingridDatasets:
    """Fingrid API dataset IDs."""

    CONSUMPTION = 124  # Electricity consumption (MW), 3-minute intervals
    PRODUCTION = 192  # Total electricity production (MW), 3-minute intervals
    WIND = 75  # Wind power production (MW), 15-minute intervals
    NUCLEAR = 188  # Nuclear power production (MW), 3-minute intervals
    NET_IMPORT = 194  # Net import/export (MW), 3-minute intervals


# Default weather features used by Prophet model
DEFAULT_WEATHER_FEATURES = [
    WeatherColumns.TEMPERATURE,
    WeatherColumns.HUMIDITY,
    WeatherColumns.WIND_SPEED,
    WeatherColumns.PRESSURE,
]

# Critical columns that should not be null in training data
CRITICAL_TRAINING_COLUMNS = [
    ElectricityColumns.CONSUMPTION_MW,
    WeatherColumns.TEMPERATURE,
    WeatherColumns.HUMIDITY,
    WeatherColumns.WIND_SPEED,
    WeatherColumns.PRESSURE,
]


# ==============================================================================
# Pydantic Models for Data Validation
# ==============================================================================


class ElectricityConsumptionRow(BaseModel):
    """Validation model for electricity consumption data."""

    time: datetime
    consumption_mw: float | None = Field(None, ge=0, description="Consumption in MW (non-negative)")
    production_mw: float | None = Field(None, ge=0, description="Production in MW (non-negative)")
    wind_mw: float | None = Field(None, ge=0, description="Wind production in MW (non-negative)")
    nuclear_mw: float | None = Field(
        None, ge=0, description="Nuclear production in MW (non-negative)"
    )
    net_import_mw: float | None = Field(
        None, description="Net import in MW (positive = import, negative = export)"
    )
    source: str = Field(default="fingrid", description="Data source")

    class Config:
        """Pydantic config."""

        frozen = False
        validate_assignment = True


class WeatherObservationRow(BaseModel):
    """Validation model for weather observations."""

    time: datetime
    station_id: str = Field(..., min_length=1, description="Weather station identifier")
    temperature_c: float | None = Field(
        None, ge=-50, le=50, description="Temperature in Celsius (-50 to 50)"
    )
    humidity_percent: float | None = Field(
        None, ge=0, le=100, description="Humidity percentage (0-100)"
    )
    wind_speed_ms: float | None = Field(None, ge=0, le=100, description="Wind speed in m/s (0-100)")
    wind_direction: float | None = Field(
        None, ge=0, le=360, description="Wind direction in degrees (0-360)"
    )
    pressure_hpa: float | None = Field(
        None, ge=800, le=1100, description="Pressure in hPa (800-1100)"
    )
    precipitation_mm: float | None = Field(
        None, ge=0, description="Precipitation in mm (non-negative)"
    )
    cloud_cover: int | None = Field(None, ge=0, le=8, description="Cloud cover in oktas (0-8)")
    data_type: str = Field(default="observation", description="Data type: observation or forecast")

    class Config:
        """Pydantic config."""

        frozen = False
        validate_assignment = True


class ElectricityPriceRow(BaseModel):
    """Validation model for electricity prices."""

    time: datetime
    price_eur_mwh: float = Field(..., description="Price in EUR/MWh")
    area: str = Field(default="FI", description="Price area")
    source: str = Field(default="nordpool", description="Data source")

    @field_validator("price_eur_mwh")
    @classmethod
    def validate_price(cls, v: float) -> float:
        """Validate price is reasonable (allow negative for rare cases)."""
        if v < -500 or v > 5000:
            raise ValueError(f"Price {v} EUR/MWh is outside reasonable range (-500 to 5000)")
        return v

    class Config:
        """Pydantic config."""

        frozen = False
        validate_assignment = True


class PredictionRow(BaseModel):
    """Validation model for model predictions."""

    timestamp: datetime
    predicted_consumption_mw: float = Field(..., ge=0, description="Predicted consumption in MW")
    confidence_lower: float = Field(..., ge=0, description="Lower confidence bound in MW")
    confidence_upper: float = Field(..., ge=0, description="Upper confidence bound in MW")
    model_type: str = Field(..., description="Model type (e.g., 'prophet', 'lightgbm')")
    model_version: str | None = Field(None, description="Model version identifier")

    @field_validator("confidence_upper")
    @classmethod
    def validate_confidence_bounds(cls, v: float, info) -> float:
        """Ensure confidence_upper >= confidence_lower."""
        if "confidence_lower" in info.data and v < info.data["confidence_lower"]:
            raise ValueError("confidence_upper must be >= confidence_lower")
        return v

    @field_validator("predicted_consumption_mw")
    @classmethod
    def validate_prediction_in_bounds(cls, v: float, info) -> float:
        """Ensure prediction is within confidence bounds if available."""
        if "confidence_lower" in info.data and "confidence_upper" in info.data:
            if not (info.data["confidence_lower"] <= v <= info.data["confidence_upper"]):
                raise ValueError("predicted_consumption_mw should be within confidence bounds")
        return v

    class Config:
        """Pydantic config."""

        frozen = False
        validate_assignment = True
