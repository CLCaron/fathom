"""Capitol Trades scraper for congressional stock trades."""

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from fathom.scrapers.base import BaseScraper, ScrapedItem

logger = logging.getLogger(__name__)

BASE_URL = "https://www.capitoltrades.com/trades"

# RSC header triggers the React Server Component payload (JSON-like data)
RSC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "RSC": "1",
    "Next-Router-State-Tree": (
        "%5B%22%22%2C%7B%22children%22%3A%5B%22(public)%22%2C%7B%22children%22"
        "%3A%5B%22trades%22%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%5D"
        "%7D%5D%7D%5D%7D%2Cnull%2Cnull%2Ctrue%5D"
    ),
}

TRADES_PER_PAGE = 12

# Capitol Trades reports amounts as midpoint values of STOCK Act ranges.
# Map midpoints back to the actual statutory reporting ranges.
AMOUNT_RANGE_MAP: dict[int, tuple[float, float]] = {
    8000: (1_001, 15_000),
    32500: (15_001, 50_000),
    75000: (50_001, 100_000),
    175000: (100_001, 250_000),
    375000: (250_001, 500_000),
    750000: (500_001, 1_000_000),
    5000000: (1_000_001, 50_000_000),
}

# Capitol Trades sector slugs -> our sector names
SECTOR_NORMALIZE: dict[str, str] = {
    "consumer-discretionary": "Consumer",
    "consumer-staples": "Consumer",
    "energy": "Energy",
    "financials": "Finance",
    "health-care": "Healthcare",
    "industrials": "Industrial",
    "information-technology": "Technology",
    "materials": "Materials",
    "real-estate": "Real Estate",
    "communication-services": "Telecom",
    "utilities": "Utilities",
    "defense": "Defense",
    "technology": "Technology",
}


@dataclass
class CongressionalTradeItem(ScrapedItem):
    """A single congressional trade from Capitol Trades."""

    source: str = "capitol_trades"
    member_name: str = ""
    chamber: str = ""  # HOUSE or SENATE
    state: str | None = None
    party: str | None = None
    ticker: str | None = None
    asset_name: str | None = None
    trade_type: str = ""  # PURCHASE or SALE
    amount_min: float | None = None
    amount_max: float | None = None
    trade_date: date | None = None
    disclosure_date: date | None = None
    source_url: str | None = None
    sector: str | None = None


class CapitolTradesScraper(BaseScraper):
    """Scrapes congressional stock trades from capitoltrades.com."""

    def __init__(self, lookback_days: int = 90):
        super().__init__(rate_limit_delay=1.0, max_retries=3)
        self.lookback_days = lookback_days
        self._cutoff = date.today() - timedelta(days=lookback_days)

    async def scrape(self) -> list[CongressionalTradeItem]:
        """Fetch congressional trades from Capitol Trades.

        Pages are sorted by publication (disclosure) date, newest first.
        Trades disclosed recently may have old trade dates, so we paginate
        until disclosure dates fall before our cutoff window.
        """
        all_items: list[CongressionalTradeItem] = []
        page = 1

        while True:
            try:
                trades, total_pages = await self._fetch_page(page)
            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

            if not trades:
                break

            # Keep all trades — even old trade_dates matter if recently disclosed
            all_items.extend(t for t in trades if t.trade_date)

            # Stop when the oldest disclosure date on this page is before cutoff.
            # Pages are sorted by pubDate desc, so once disclosures are old we're done.
            disclosure_dates = [
                t.disclosure_date for t in trades if t.disclosure_date
            ]
            if disclosure_dates and min(disclosure_dates) < self._cutoff:
                logger.info(
                    f"Disclosure dates before cutoff on page {page}, stopping"
                )
                break

            if page >= total_pages:
                break

            page += 1

        logger.info(f"Scraped {len(all_items)} congressional trades from {page} pages")
        return all_items

    async def _fetch_page(
        self, page: int
    ) -> tuple[list[CongressionalTradeItem], int]:
        """Fetch a single page of trades. Returns (trades, total_pages)."""
        url = f"{BASE_URL}?page={page}"
        response = await self._fetch(url, headers=RSC_HEADERS)
        text = response.text

        trades = self._extract_trades(text)
        total_pages = self._extract_total_pages(text)

        return trades, total_pages

    def _extract_trades(self, rsc_text: str) -> list[CongressionalTradeItem]:
        """Extract trade records from RSC payload."""
        items: list[CongressionalTradeItem] = []

        # The RSC payload contains a JSON array in a "data" field
        # Each trade object has _txId, _politicianId, issuer, politician, etc.
        data_match = re.search(r'"data":\[(\{.*?\})\]', rsc_text, re.DOTALL)
        if not data_match:
            return items

        # Extract the full data array by finding its bounds
        start = rsc_text.find('"data":[') + len('"data":')
        if start < len('"data":'):
            return items

        # Parse the JSON array by tracking bracket depth
        array_str = self._extract_json_array(rsc_text, start)
        if not array_str:
            return items

        try:
            trades_data = json.loads(array_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse trades JSON: {e}")
            return items

        for raw in trades_data:
            try:
                item = self._parse_trade(raw)
                if item:
                    items.append(item)
            except Exception as e:
                logger.warning(f"Failed to parse trade {raw.get('_txId', '?')}: {e}")

        return items

    def _extract_json_array(self, text: str, start: int) -> str | None:
        """Extract a JSON array starting at the given position."""
        if start >= len(text) or text[start] != "[":
            return None

        depth = 0
        for i in range(start, len(text)):
            if text[i] == "[":
                depth += 1
            elif text[i] == "]":
                depth -= 1
                if depth == 0:
                    return text[start : i + 1]

        return None

    def _parse_trade(self, raw: dict) -> CongressionalTradeItem | None:
        """Parse a single trade dict into a CongressionalTradeItem."""
        politician = raw.get("politician", {})
        issuer = raw.get("issuer", {})

        if not politician or not raw.get("txDate"):
            return None

        # Build member name
        first = politician.get("firstName", "")
        last = politician.get("lastName", "")
        member_name = f"{first} {last}".strip()
        if not member_name:
            return None

        # Chamber
        chamber_raw = raw.get("chamber", "").upper()
        chamber = chamber_raw if chamber_raw in ("HOUSE", "SENATE") else ""

        # Ticker: format is "IBP:US" -> "IBP"
        ticker_raw = issuer.get("issuerTicker", "")
        ticker = ticker_raw.split(":")[0] if ticker_raw else None

        # Trade type
        tx_type = raw.get("txType", "").lower()
        type_map = {"buy": "PURCHASE", "sell": "SALE", "exchange": "EXCHANGE"}
        trade_type = type_map.get(tx_type, tx_type.upper())

        # Amount range from midpoint value
        value = raw.get("value")
        amount_min, amount_max = self._parse_amount(value)

        # Dates
        trade_date = self._parse_date(raw.get("txDate"))
        pub_date_str = raw.get("pubDate", "")
        disclosure_date = self._parse_date(pub_date_str[:10]) if pub_date_str else None

        # Sector from Capitol Trades
        ct_sector = issuer.get("sector", "")
        sector = SECTOR_NORMALIZE.get(ct_sector)

        # State and party
        state = politician.get("_stateId", "").upper() or None
        party_raw = politician.get("party", "")
        party = party_raw.capitalize() if party_raw else None

        return CongressionalTradeItem(
            member_name=member_name,
            chamber=chamber,
            state=state,
            party=party,
            ticker=ticker,
            asset_name=issuer.get("issuerName"),
            trade_type=trade_type,
            amount_min=amount_min,
            amount_max=amount_max,
            trade_date=trade_date,
            disclosure_date=disclosure_date,
            source_url=f"https://www.capitoltrades.com/trades?txId={raw.get('_txId', '')}",
            sector=sector,
        )

    @staticmethod
    def _parse_amount(value: int | None) -> tuple[float | None, float | None]:
        """Convert Capitol Trades midpoint value to STOCK Act range."""
        if value is None:
            return None, None
        range_tuple = AMOUNT_RANGE_MAP.get(value)
        if range_tuple:
            return range_tuple
        # Unknown midpoint — use the value as both min and max
        return float(value), float(value)

    @staticmethod
    def _parse_date(date_str: str | None) -> date | None:
        """Parse a date string (YYYY-MM-DD)."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return None

    def _extract_total_pages(self, rsc_text: str) -> int:
        """Extract total page count from RSC payload."""
        match = re.search(r'"totalPages":(\d+)', rsc_text)
        if match:
            return int(match.group(1))
        return 1
