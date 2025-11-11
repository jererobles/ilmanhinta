"""Application configuration using Pydantic v2 settings."""

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Global application settings with environment variable support."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Fingrid API
    fingrid_api_key: str = Field(..., description="Fingrid API key from data.fingrid.fi")

    # FMI Settings
    fmi_station_id: str = Field(default="101004", description="FMI station ID (Helsinki)")
    fmi_bbox: str = Field(
        default="19.0,59.0,32.0,70.5", description="Finland bounding box (W,S,E,N)"
    )

    # Application
    log_level: str = Field(default="INFO", description="Logging level")
    cache_ttl_seconds: int = Field(default=180, description="Cache TTL for real-time data")

    # Observability
    observability_service_name: str = Field(
        default="ilmanhinta",
        validation_alias=AliasChoices("OTEL_SERVICE_NAME", "LOGFIRE_PROJECT"),
        description="Service name reported in OpenTelemetry exports",
    )
    observability_environment: str = Field(
        default="production",
        validation_alias=AliasChoices("OTEL_ENVIRONMENT", "LOGFIRE_ENVIRONMENT"),
        description="Environment label attached to telemetry exports",
    )

    # ML Model
    model_retrain_hours: int = Field(default=24, description="Hours between model retraining")
    prediction_horizon_hours: int = Field(default=24, description="Prediction horizon")

    # API
    api_host: str = Field(default="0.0.0.0", description="API host")
    api_port: int = Field(default=8000, description="API port")

    # Dagster
    dagster_home: str = Field(default="/app/dagster_home", description="Dagster home directory")


settings = Settings()
