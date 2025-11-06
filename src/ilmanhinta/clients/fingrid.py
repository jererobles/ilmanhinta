"""Fingrid API client with rate limiting and caching."""

from datetime import datetime, timedelta
from typing import Any

import httpx
import logfire

from ilmanhinta.config import settings
from ilmanhinta.models.fingrid import FingridDataPoint, FingridResponse


class FingridClient:
    """Client for Fingrid Open Data API with aggressive caching."""

    BASE_URL = "https://data.fingrid.fi/api/datasets"

    def __init__(self, api_key: str | None = None) -> None:
        """Initialize Fingrid client."""
        self.api_key = api_key or settings.fingrid_api_key
        self.client = httpx.AsyncClient(
            headers={"x-api-key": self.api_key},
            timeout=30.0,
        )
        self._cache: dict[str, tuple[datetime, list[FingridDataPoint]]] = {}

    async def fetch_data(
        self,
        dataset_id: int,
        start_time: datetime,
        end_time: datetime,
        page_size: int = 1000,
    ) -> list[FingridDataPoint]:
        """Fetch data from Fingrid API with caching."""
        cache_key = f"{dataset_id}_{start_time.isoformat()}_{end_time.isoformat()}"

        # Check cache
        if cache_key in self._cache:
            cached_at, cached_data = self._cache[cache_key]
            if datetime.utcnow() - cached_at < timedelta(seconds=settings.cache_ttl_seconds):
                logfire.debug(f"Cache hit for dataset {dataset_id}")
                return cached_data

        # Format timestamps for API (ISO 8601)
        params: dict[str, Any] = {
            "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "pageSize": page_size,
            "format": "json",
        }

        url = f"{self.BASE_URL}/{dataset_id}/data"
        logfire.info(f"Fetching Fingrid dataset {dataset_id} from {start_time} to {end_time}")

        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()

            data = response.json()
            parsed = FingridResponse(data=data.get("data", []))

            # Cache the result
            self._cache[cache_key] = (datetime.utcnow(), parsed.data)

            logfire.info(
                f"Fetched {len(parsed.data)} data points from Fingrid dataset {dataset_id}"
            )
            return parsed.data

        except httpx.HTTPStatusError as e:
            logfire.error(f"HTTP error fetching Fingrid data: {e}")
            raise
        except Exception as e:
            logfire.error(f"Error fetching Fingrid data: {e}")
            raise

    async def fetch_realtime_consumption(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time electricity consumption for the last N hours."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(124, start_time, end_time)

    async def fetch_realtime_production(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time electricity production for the last N hours."""
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(192, start_time, end_time)

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def __aenter__(self) -> "FingridClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()
