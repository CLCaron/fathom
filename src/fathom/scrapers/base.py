import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import httpx

logger = logging.getLogger(__name__)


@dataclass
class ScrapedItem:
    """Base class for items returned by scrapers."""
    source: str


class BaseScraper(ABC):
    """Abstract base scraper with retry logic and rate limiting."""

    def __init__(self, rate_limit_delay: float = 0.5, max_retries: int = 3):
        self.rate_limit_delay = rate_limit_delay
        self.max_retries = max_retries
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        return self._client

    async def _fetch(self, url: str, params: dict | None = None, headers: dict | None = None) -> httpx.Response:
        client = await self._get_client()

        for attempt in range(self.max_retries):
            try:
                await asyncio.sleep(self.rate_limit_delay)
                response = await client.get(url, params=params, headers=headers)

                if response.status_code == 429:
                    wait = min(2 ** (attempt + 2), 60)
                    logger.warning(f"Rate limited on {url}, waiting {wait}s")
                    await asyncio.sleep(wait)
                    continue

                response.raise_for_status()
                return response

            except httpx.HTTPStatusError as e:
                if e.response.status_code >= 500 and attempt < self.max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Server error {e.response.status_code} on {url}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise
            except httpx.RequestError as e:
                if attempt < self.max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Request error on {url}: {e}, retry in {wait}s")
                    await asyncio.sleep(wait)
                    continue
                raise

        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} retries")

    @abstractmethod
    async def scrape(self) -> list[Any]:
        """Execute the scrape and return a list of items."""
        ...

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()
