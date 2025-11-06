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

    CONSUMPTION = 124  # Real-time electricity consumption (MW)
    PRODUCTION = 192  # Total electricity production (MW)
    SHORTAGE = 336  # Power shortage status
    WIND_PRODUCTION = 181  # Wind power production (MW)
    NUCLEAR_PRODUCTION = 188  # Nuclear power production (MW)
