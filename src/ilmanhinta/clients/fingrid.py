"""Fingrid API client with rate limiting and caching."""

import asyncio
from datetime import datetime, timedelta
from typing import Any

import httpx

from ilmanhinta.config import settings
from ilmanhinta.logging import logfire
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
        max_retries: int = 3,
    ) -> list[FingridDataPoint]:
        """Fetch data from Fingrid API with caching and exponential backoff for rate limits."""
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

        # Retry with exponential backoff for rate limit errors
        for attempt in range(max_retries):
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
                # Retry on rate limit errors (429) with exponential backoff
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    delay = 2**attempt  # 1s, 2s, 4s
                    logfire.warning(
                        f"Rate limited (429) on dataset {dataset_id}, retrying in {delay}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logfire.error(f"HTTP error fetching Fingrid data: {e}")
                    raise
            except Exception as e:
                logfire.error(f"Error fetching Fingrid data: {e}")
                raise

        # This should never be reached, but just in case
        raise RuntimeError(f"Failed to fetch dataset {dataset_id} after {max_retries} attempts")

    async def fetch_realtime_consumption(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time electricity consumption for the last N hours.

        Dataset 124: Electricity consumption in Finland (updated every 3 minutes).
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(124, start_time, end_time)

    async def fetch_realtime_production(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time total electricity production for the last N hours.

        Dataset 192: Total electricity production in Finland (updated every 3 minutes).
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(192, start_time, end_time)

    async def fetch_wind_production(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time wind power production for the last N hours.

        Dataset 75: Wind power generation (15 min intervals).
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(75, start_time, end_time)

    async def fetch_nuclear_production(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time nuclear power production for the last N hours.

        Dataset 188: Nuclear power production (updated every 3 minutes).
        """
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(188, start_time, end_time)

    async def fetch_all_electricity_data(
        self, hours: int = 24
    ) -> dict[str, list[FingridDataPoint]]:
        """Fetch all electricity data (consumption, production, wind, nuclear) with rate limiting.

        Fetches datasets sequentially with small delays to avoid hitting API rate limits.

        Returns:
            Dict with keys: consumption, production, wind, nuclear
        """
        logfire.info(f"Fetching all electricity data for last {hours} hours (with rate limiting)")

        # Fetch datasets sequentially with small delays to avoid rate limiting
        # Parallel requests were triggering 429 Too Many Requests
        consumption = await self.fetch_realtime_consumption(hours)
        await asyncio.sleep(0.5)  # Small delay to avoid rate limiting

        production = await self.fetch_realtime_production(hours)
        await asyncio.sleep(0.5)

        wind = await self.fetch_wind_production(hours)
        await asyncio.sleep(0.5)

        nuclear = await self.fetch_nuclear_production(hours)

        return {
            "consumption": consumption,
            "production": production,
            "wind": wind,
            "nuclear": nuclear,
        }

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def __aenter__(self) -> "FingridClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()
