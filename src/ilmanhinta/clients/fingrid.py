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
        max_retries: int = 3,
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

    async def fetch_realtime_consumption(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time electricity consumption for the last N hours.

        Dataset 124: Electricity consumption in Finland (updated every 3 minutes).
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(124, start_time, end_time)

    async def fetch_realtime_production(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time total electricity production for the last N hours.

        Dataset 192: Total electricity production in Finland (updated every 3 minutes).
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(192, start_time, end_time)

    async def fetch_wind_production(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time wind power production for the last N hours.

        Dataset 75: Wind power generation (15 min intervals).
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(75, start_time, end_time)

    async def fetch_nuclear_production(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time nuclear power production for the last N hours.

        Dataset 188: Nuclear power production (updated every 3 minutes).
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(188, start_time, end_time)

    async def fetch_net_import(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch real-time net import/export for the last N hours.

        Dataset 194: Net import/export (positive = import, negative = export).
        Updated every 3 minutes.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(194, start_time, end_time)

    # === Price Data Methods ===

    async def fetch_down_regulation_price(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch down-regulation price in balancing market.

        Dataset 106: Down-regulation price (EUR/MWh).
        Updated every 15 minutes.
        This is a key price signal for the balancing market.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(106, start_time, end_time)

    async def fetch_imbalance_sale_price(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch production imbalance electricity sale price.

        Dataset 93: Imbalance sale price (EUR/MWh).
        Updated hourly.
        Price at which excess production is sold.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(93, start_time, end_time)

    async def fetch_imbalance_buy_price(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch production imbalance electricity buy price.

        Dataset 96: Imbalance buy price (EUR/MWh).
        Updated hourly.
        Price at which deficit production is purchased.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(96, start_time, end_time)

    async def fetch_imbalance_price(self, hours: int = 24) -> list[FingridDataPoint]:
        """Fetch imbalance power pricing.

        Dataset 92: Imbalance price (EUR/MWh).
        Updated hourly.
        Primary price signal for imbalance settlement.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(92, start_time, end_time)

    # === Forecast Data Methods ===

    async def fetch_consumption_forecast_daily(self, hours: int = 48) -> list[FingridDataPoint]:
        """Fetch daily consumption forecast (24h ahead).

        Dataset 165: Consumption forecast - daily (MW).
        Updated daily.
        Official Fingrid consumption prediction.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(165, start_time, end_time)

    async def fetch_consumption_forecast_realtime(self, hours: int = 48) -> list[FingridDataPoint]:
        """Fetch real-time consumption forecast (updated every 15 min).

        Dataset 166: Consumption forecast - real-time (MW).
        Updated every 15 minutes.
        Most current Fingrid consumption prediction.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(166, start_time, end_time)

    async def fetch_production_forecast(self, hours: int = 48) -> list[FingridDataPoint]:
        """Fetch production forecast.

        Dataset 241: Production forecast (MW).
        Updated every 15 minutes.
        Official Fingrid total production prediction.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(241, start_time, end_time)

    async def fetch_wind_forecast(self, hours: int = 48) -> list[FingridDataPoint]:
        """Fetch wind generation forecast.

        Dataset 245: Wind generation forecast (MW).
        Updated every 15 minutes.
        Official Fingrid wind power prediction.
        """
        end_time = datetime.now(UTC)
        start_time = end_time - timedelta(hours=hours)
        return await self.fetch_data(245, start_time, end_time)

    async def fetch_all_electricity_data(
        self, hours: int = 24
    ) -> dict[str, list[FingridDataPoint]]:
        """Fetch all electricity data (consumption, production, wind, nuclear, net_import) with rate limiting.

        Fetches datasets sequentially with small delays to avoid hitting API rate limits.

        Returns:
            Dict with keys: consumption, production, wind, nuclear, net_import
        """
        logger.info(f"Fetching all electricity data for last {hours} hours (with rate limiting)")

        # Fetch datasets sequentially with small delays to avoid rate limiting
        # Parallel requests were triggering 429 Too Many Requests
        consumption = await self.fetch_realtime_consumption(hours)
        await asyncio.sleep(0.5)  # Small delay to avoid rate limiting

        production = await self.fetch_realtime_production(hours)
        await asyncio.sleep(0.5)

        wind = await self.fetch_wind_production(hours)
        await asyncio.sleep(0.5)

        nuclear = await self.fetch_nuclear_production(hours)
        await asyncio.sleep(0.5)

        net_import = await self.fetch_net_import(hours)

        return {
            "consumption": consumption,
            "production": production,
            "wind": wind,
            "nuclear": nuclear,
            "net_import": net_import,
        }

    async def fetch_training_data(self, hours: int = 24) -> dict[str, list[FingridDataPoint]]:
        """Fetch all data needed for model training (actuals + prices).

        This includes:
        - Actual consumption, production, wind, nuclear, net import
        - Price data (imbalance prices for target variable)

        Fetches sequentially with rate limiting.

        Returns:
            Dict with keys: consumption, production, wind, nuclear, net_import,
                           price_imbalance, price_imbalance_sale, price_imbalance_buy,
                           price_down_regulation
        """
        logger.info(f"Fetching training data for last {hours} hours")

        # Fetch actuals
        consumption = await self.fetch_realtime_consumption(hours)
        await asyncio.sleep(0.5)

        production = await self.fetch_realtime_production(hours)
        await asyncio.sleep(0.5)

        wind = await self.fetch_wind_production(hours)
        await asyncio.sleep(0.5)

        nuclear = await self.fetch_nuclear_production(hours)
        await asyncio.sleep(0.5)

        net_import = await self.fetch_net_import(hours)
        await asyncio.sleep(0.5)

        # Fetch prices (target variable)
        price_imbalance = await self.fetch_imbalance_price(hours)
        await asyncio.sleep(0.5)

        price_imbalance_sale = await self.fetch_imbalance_sale_price(hours)
        await asyncio.sleep(0.5)

        price_imbalance_buy = await self.fetch_imbalance_buy_price(hours)
        await asyncio.sleep(0.5)

        price_down_regulation = await self.fetch_down_regulation_price(hours)

        logger.info("Completed fetching training data")

        return {
            "consumption": consumption,
            "production": production,
            "wind": wind,
            "nuclear": nuclear,
            "net_import": net_import,
            "price_imbalance": price_imbalance,
            "price_imbalance_sale": price_imbalance_sale,
            "price_imbalance_buy": price_imbalance_buy,
            "price_down_regulation": price_down_regulation,
        }

    async def fetch_forecast_data(self, hours: int = 48) -> dict[str, list[FingridDataPoint]]:
        """Fetch all forecast data for prediction (features only, no actuals).

        This includes Fingrid's official forecasts:
        - Consumption forecasts (daily and real-time)
        - Production forecast
        - Wind generation forecast

        These are used as features for your ML model.

        Fetches sequentially with rate limiting.

        Returns:
            Dict with keys: consumption_forecast_rt, consumption_forecast_daily,
                           production_forecast, wind_forecast
        """
        logger.info(f"Fetching forecast data for next {hours} hours")

        # Fetch Fingrid's official forecasts
        consumption_forecast_rt = await self.fetch_consumption_forecast_realtime(hours)
        await asyncio.sleep(0.5)

        consumption_forecast_daily = await self.fetch_consumption_forecast_daily(hours)
        await asyncio.sleep(0.5)

        production_forecast = await self.fetch_production_forecast(hours)
        await asyncio.sleep(0.5)

        wind_forecast = await self.fetch_wind_forecast(hours)

        logger.info("Completed fetching forecast data")

        return {
            "consumption_forecast_rt": consumption_forecast_rt,
            "consumption_forecast_daily": consumption_forecast_daily,
            "production_forecast": production_forecast,
            "wind_forecast": wind_forecast,
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
