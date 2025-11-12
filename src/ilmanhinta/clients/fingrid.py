"""Fingrid API client with rate limiting and caching."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from ilmanhinta.config import settings
from ilmanhinta.logging import get_logger
from ilmanhinta.models.fingrid import FingridDataPoint, FingridResponse

logger = get_logger(__name__)


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
        max_retries: int = 30,
    ) -> list[FingridDataPoint]:
        """Fetch data from Fingrid API with caching and exponential backoff for rate limits."""
        cache_key = f"{dataset_id}_{start_time.isoformat()}_{end_time.isoformat()}"

        # Check cache
        if cache_key in self._cache:
            cached_at, cached_data = self._cache[cache_key]
            if datetime.now(UTC) - cached_at < timedelta(seconds=settings.cache_ttl_seconds):
                logger.debug(f"Cache hit for dataset {dataset_id}")
                return cached_data

        # Format timestamps for API (ISO 8601)
        params: dict[str, Any] = {
            "startTime": start_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "endTime": end_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "pageSize": page_size,
            "format": "json",
        }

        url = f"{self.BASE_URL}/{dataset_id}/data"
        logger.info(f"Fetching Fingrid dataset {dataset_id} from {start_time} to {end_time}")

        # Retry with exponential backoff for rate limit errors
        for attempt in range(max_retries):
            try:
                response = await self.client.get(url, params=params)
                response.raise_for_status()

                data = response.json()
                parsed = FingridResponse(data=data.get("data", []))

                # Cache the result
                self._cache[cache_key] = (datetime.now(UTC), parsed.data)

                logger.info(
                    f"Fetched {len(parsed.data)} data points from Fingrid dataset {dataset_id}"
                )
                return parsed.data

            except httpx.HTTPStatusError as e:
                # Retry on rate limit errors (429) with exponential backoff
                if e.response.status_code == 429 and attempt < max_retries - 1:
                    delay = 2**attempt  # 1s, 2s, 4s
                    logger.warning(
                        f"Rate limited (429) on dataset {dataset_id}, retrying in {delay}s (attempt {attempt + 1}/{max_retries})"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"HTTP error fetching Fingrid data: {e}")
                    raise
            except Exception as e:
                logger.error(f"Error fetching Fingrid data: {e}")
                raise

        # This should never be reached, but just in case
        raise RuntimeError(f"Failed to fetch dataset {dataset_id} after {max_retries} attempts")

    async def close(self) -> None:
        """Close the HTTP client."""
        await self.client.aclose()

    async def __aenter__(self) -> "FingridClient":
        """Async context manager entry."""
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Async context manager exit."""
        await self.close()
