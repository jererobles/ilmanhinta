"""Pydantic models for Fingrid electricity data."""

from datetime import datetime

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class FingridDataPoint(BaseModel):
    """Single data point from Fingrid API."""

    # Fingrid returns camelCase keys (startTime/endTime). Accept both.
    start_time: datetime = Field(
        ...,
        description="Start timestamp in UTC",
        validation_alias=AliasChoices("start_time", "startTime"),
    )
    end_time: datetime = Field(
        ...,
        description="End timestamp in UTC",
        validation_alias=AliasChoices("end_time", "endTime"),
    )
    value: float = Field(..., description="Value (MW for consumption/production)")

    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class FingridResponse(BaseModel):
    """Response wrapper from Fingrid API."""

    data: list[FingridDataPoint] = Field(default_factory=list, description="Time series data")

    model_config = ConfigDict(extra="ignore")


# Dataset IDs for Fingrid API
class FingridDatasets:
    """Fingrid dataset identifiers."""

    # === Actual Values (Observations) ===
    CONSUMPTION = 124  # Real-time electricity consumption (MW)
    PRODUCTION = 192  # Total electricity production (MW)
    WIND_PRODUCTION = 75  # Wind power production (MW) - Updated to match client
    NUCLEAR_PRODUCTION = 188  # Nuclear power production (MW)
    NET_IMPORT = 194  # Net import/export (MW) - positive = import
    SHORTAGE = 336  # Power shortage status

    # === Prices (Historical - Target Variable) ===
    PRICE_DOWN_REGULATION = 106  # Down-regulation price in balancing market (EUR/MWh)
    PRICE_IMBALANCE_SALE = 93  # Production imbalance electricity sale price (EUR/MWh)
    PRICE_IMBALANCE_BUY = 96  # Production imbalance electricity buy price (EUR/MWh)
    PRICE_IMBALANCE = 92  # Imbalance power pricing (EUR/MWh)

    # === Forecasts (Features for Prediction) ===
    FORECAST_CONSUMPTION_DAILY = 165  # Consumption forecast - 24h ahead (MW)
    FORECAST_CONSUMPTION_RT = 166  # Consumption forecast - updated every 15min (MW)
    FORECAST_PRODUCTION = 241  # Production forecast (MW)
    FORECAST_WIND = 245  # Wind generation forecast (MW)

    # === Transmission & Constraints ===
    TRANSMISSION_FI_EE = 180  # Transmission Finland-Estonia (MW)
    CAPACITY_FI_SE3 = 27  # Day-ahead capacity FI-SE3 (MW)
    CAPACITY_FI_SE1 = 24  # Day-ahead capacity FI-SE1 (MW)
    CAPACITY_FI_EE = 25  # Day-ahead capacity FI-EE (MW)
