"""FMI (Finnish Meteorological Institute) API client."""

from datetime import datetime, timedelta

from fmiopendata.wfs import download_stored_query
from loguru import logger

from ilmanhinta.config import settings
from ilmanhinta.models.fmi import FMIObservation, WeatherData


class FMIClient:
    """Client for FMI Open Data API (no API key needed)."""

    def __init__(self, station_id: str | None = None) -> None:
        """Initialize FMI client."""
        self.station_id = station_id or settings.fmi_station_id

    def fetch_observations(
        self, start_time: datetime, end_time: datetime, station_id: str | None = None
    ) -> WeatherData:
        """Fetch weather observations from FMI."""
        station = station_id or self.station_id

        logger.info(f"Fetching FMI observations for station {station} from {start_time} to {end_time}")

        try:
            # Use fmiopendata library to fetch data
            obs = download_stored_query(
                "fmi::observations::weather::multipointcoverage",
                args=[
                    f"fmisid={station}",
                    f"starttime={start_time.isoformat()}",
                    f"endtime={end_time.isoformat()}",
                ],
            )

            observations: list[FMIObservation] = []

            # Parse the observations
            if hasattr(obs, "data") and station in obs.data:
                station_data = obs.data[station]

                # FMI returns data with timestamps as keys
                for timestamp_str, values in station_data.items():
                    # Parse timestamp (format: YYYY-MM-DDTHH:MM:SSZ)
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

                    observation = FMIObservation(
                        timestamp=timestamp,
                        temperature=values.get("Air temperature", {}).get("value"),
                        humidity=values.get("Relative humidity", {}).get("value"),
                        wind_speed=values.get("Wind speed", {}).get("value"),
                        wind_direction=values.get("Wind direction", {}).get("value"),
                        pressure=values.get("Pressure (msl)", {}).get("value"),
                        precipitation=values.get("Precipitation amount", {}).get("value"),
                        cloud_cover=values.get("Cloud amount", {}).get("value"),
                    )
                    observations.append(observation)

            # Get station name
            station_name = getattr(obs, "location_metadata", {}).get(station, {}).get("name", "Unknown")

            logger.info(f"Fetched {len(observations)} observations from FMI station {station}")

            return WeatherData(
                station_id=station,
                station_name=station_name,
                observations=observations,
            )

        except Exception as e:
            logger.error(f"Error fetching FMI data: {e}")
            raise

    def fetch_realtime_observations(self, hours: int = 24) -> WeatherData:
        """Fetch real-time weather observations for the last N hours."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return self.fetch_observations(start_time, end_time)

    def fetch_forecast(self, hours: int = 24, station_id: str | None = None) -> WeatherData:
        """Fetch weather forecast from FMI."""
        station = station_id or self.station_id
        start_time = datetime.utcnow()
        end_time = start_time + timedelta(hours=hours)

        logger.info(f"Fetching FMI forecast for station {station} from {start_time} to {end_time}")

        try:
            # Use forecast query
            forecast = download_stored_query(
                "fmi::forecast::hirlam::surface::point::multipointcoverage",
                args=[
                    f"fmisid={station}",
                    f"starttime={start_time.isoformat()}",
                    f"endtime={end_time.isoformat()}",
                ],
            )

            observations: list[FMIObservation] = []

            if hasattr(forecast, "data") and station in forecast.data:
                station_data = forecast.data[station]

                for timestamp_str, values in station_data.items():
                    timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

                    observation = FMIObservation(
                        timestamp=timestamp,
                        temperature=values.get("Temperature", {}).get("value"),
                        humidity=values.get("Relative humidity", {}).get("value"),
                        wind_speed=values.get("Wind speed", {}).get("value"),
                        wind_direction=values.get("Wind direction", {}).get("value"),
                        pressure=values.get("Pressure", {}).get("value"),
                        precipitation=values.get("Precipitation amount", {}).get("value"),
                        cloud_cover=values.get("Total cloud cover", {}).get("value"),
                    )
                    observations.append(observation)

            station_name = getattr(forecast, "location_metadata", {}).get(station, {}).get("name", "Unknown")

            logger.info(f"Fetched {len(observations)} forecast points from FMI station {station}")

            return WeatherData(
                station_id=station,
                station_name=station_name,
                observations=observations,
            )

        except Exception as e:
            logger.error(f"Error fetching FMI forecast: {e}")
            raise
