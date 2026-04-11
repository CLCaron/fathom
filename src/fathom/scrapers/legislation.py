"""Legislation and votes scraper using congress.gov API."""

import logging
from dataclasses import dataclass, field
from datetime import date

from fathom.config import settings
from fathom.scrapers.base import BaseScraper, ScrapedItem

logger = logging.getLogger(__name__)

API_BASE = "https://api.congress.gov/v3"

# Only fetch substantive bill types, skip procedural resolutions
SUBSTANTIVE_BILL_TYPES = ["hr", "s", "hjres", "sjres"]

# Keyword-to-sector mapping for tagging bills by their text.
BILL_SECTOR_KEYWORDS: dict[str, list[str]] = {
    "Defense": [
        "defense", "military", "armed forces", "weapons", "pentagon",
        "veteran", "national security", "army", "navy", "air force",
        "intelligence", "surveillance", "missile", "nuclear weapon",
    ],
    "Energy": [
        "oil", "gas", "energy", "pipeline", "drilling", "renewable",
        "solar", "wind", "nuclear energy", "fossil fuel", "petroleum",
        "electric grid", "power plant", "lng", "natural gas",
    ],
    "Healthcare": [
        "pharmaceutical", "drug", "health", "medicare", "medicaid",
        "fda", "hospital", "medical", "vaccine", "biotech",
        "opioid", "mental health", "prescription", "insurance",
    ],
    "Technology": [
        "cyber", "data privacy", "artificial intelligence", "semiconductor",
        "broadband", "internet", "software", "telecom", "digital",
        "technology", "computing", "spectrum", "5g",
    ],
    "Finance": [
        "banking", "financial", "securities", "credit", "insurance",
        "wall street", "federal reserve", "monetary", "treasury",
        "tax", "tariff", "trade agreement", "irs",
    ],
    "Consumer": [
        "consumer protection", "retail", "food safety", "product safety",
        "agriculture", "farming", "food", "nutrition",
    ],
    "Industrial": [
        "infrastructure", "transportation", "manufacturing", "construction",
        "highway", "railroad", "aviation", "shipping", "port",
    ],
}


@dataclass
class BillItem(ScrapedItem):
    """A bill from congress.gov."""

    source: str = "congress_gov"
    bill_id: str = ""
    title: str = ""
    summary: str | None = None
    congress_number: int = 119
    introduced_date: date | None = None
    last_action_date: date | None = None
    status: str | None = None
    sectors_affected: list[str] = field(default_factory=list)
    sponsor_name: str | None = None
    bill_url: str | None = None


@dataclass
class VoteItem(ScrapedItem):
    """A roll call vote record."""

    source: str = "congress_gov"
    bill_id: str = ""
    member_name: str = ""
    chamber: str = ""
    vote: str = ""  # YEA, NAY, PRESENT, NOT_VOTING
    vote_date: date | None = None


class LegislationScraper(BaseScraper):
    """Scrapes bills and votes from congress.gov API."""

    def __init__(self, bills_per_type: int = 100):
        super().__init__(rate_limit_delay=0.5, max_retries=3)
        self._api_key = settings.congress_api_key
        self._bills_per_type = bills_per_type

    async def scrape(self) -> tuple[list[BillItem], list[VoteItem]]:
        """Fetch recent bills and votes."""
        if not self._api_key:
            logger.warning(
                "CONGRESS_API_KEY not set, skipping legislation scrape. "
                "Register at https://api.congress.gov/sign-up/"
            )
            return [], []

        bills = await self._fetch_bills()
        votes = await self._fetch_votes()

        logger.info(f"Scraped {len(bills)} bills and {len(votes)} votes")
        return bills, votes

    async def _fetch_bills(self) -> list[BillItem]:
        """Fetch recent substantive bills by type."""
        items: list[BillItem] = []

        for bill_type in SUBSTANTIVE_BILL_TYPES:
            url = f"{API_BASE}/bill/119/{bill_type}"
            params = {
                "api_key": self._api_key,
                "sort": "updateDate+desc",
                "limit": self._bills_per_type,
                "format": "json",
            }

            try:
                response = await self._fetch(url, params=params)
                data = response.json()
            except Exception as e:
                logger.error(f"Failed to fetch {bill_type} bills: {e}")
                continue

            for bill_data in data.get("bills", []):
                try:
                    item = self._parse_bill(bill_data)
                    if item:
                        items.append(item)
                except Exception as e:
                    logger.warning(f"Failed to parse bill: {e}")

        return items

    def _parse_bill(self, raw: dict) -> BillItem | None:
        """Parse a bill record from the API response."""
        bill_type = raw.get("type", "").upper()
        bill_number = raw.get("number", "")
        if not bill_number:
            return None

        bill_id = f"{bill_type}-{bill_number}"
        title = raw.get("title", "")

        # Skip placeholder/reserved bills
        title_lower = title.lower().strip()
        if title_lower.startswith("reserved"):
            return None

        # Parse dates
        introduced = self._parse_date(raw.get("introducedDate"))
        last_action_date = self._parse_date(
            raw.get("latestAction", {}).get("actionDate")
        )

        # Status from latest action
        status = (
            raw.get("latestAction", {}).get("text", "")[:50]
            if raw.get("latestAction")
            else None
        )

        # Tag sectors from title
        sectors = self._tag_sectors(title)

        # Sponsor
        sponsors = raw.get("sponsors", [])
        sponsor_name = sponsors[0].get("fullName") if sponsors else None

        bill_url = raw.get("url")

        return BillItem(
            bill_id=bill_id,
            title=title,
            congress_number=raw.get("congress", 119),
            introduced_date=introduced,
            last_action_date=last_action_date,
            status=status,
            sectors_affected=sectors,
            sponsor_name=sponsor_name,
            bill_url=bill_url,
        )

    async def _fetch_votes(self) -> list[VoteItem]:
        """Fetch recent roll call votes."""
        items: list[VoteItem] = []

        for chamber in ["house", "senate"]:
            url = f"{API_BASE}/vote/{chamber}"
            params = {
                "api_key": self._api_key,
                "congress": 119,
                "limit": 20,
                "format": "json",
            }

            try:
                response = await self._fetch(url, params=params)
                data = response.json()
            except Exception as e:
                logger.warning(f"Failed to fetch {chamber} votes: {e}")
                continue

            for vote_data in data.get("votes", []):
                try:
                    vote_items = await self._parse_vote(vote_data, chamber)
                    items.extend(vote_items)
                except Exception as e:
                    logger.warning(f"Failed to parse vote: {e}")

        return items

    async def _parse_vote(self, raw: dict, chamber: str) -> list[VoteItem]:
        """Parse a vote record -- fetches individual member votes if a bill is linked."""
        items: list[VoteItem] = []

        # Only process votes linked to bills
        bill_ref = raw.get("bill")
        if not bill_ref:
            return items

        bill_type = bill_ref.get("type", "").upper()
        bill_number = bill_ref.get("number", "")
        bill_id = f"{bill_type}-{bill_number}" if bill_number else None
        if not bill_id:
            return items

        vote_date = self._parse_date(raw.get("date", "")[:10])
        chamber_upper = "HOUSE" if chamber == "house" else "SENATE"

        # Fetch detailed vote record with member votes
        vote_url = raw.get("url")
        if not vote_url:
            return items

        try:
            response = await self._fetch(
                vote_url, params={"api_key": self._api_key, "format": "json"}
            )
            detail = response.json()
        except Exception as e:
            logger.warning(f"Failed to fetch vote detail: {e}")
            return items

        vote_detail = detail.get("vote", {})
        for position_group in vote_detail.get("positions", []):
            member_name = position_group.get("memberFullName", "")
            vote_position = (position_group.get("votePosition") or "").upper()

            # Normalize vote values
            vote_map = {
                "YEA": "YEA",
                "YES": "YEA",
                "NAY": "NAY",
                "NO": "NAY",
                "PRESENT": "PRESENT",
                "NOT VOTING": "NOT_VOTING",
            }
            vote = vote_map.get(vote_position, vote_position)

            if member_name and vote:
                items.append(
                    VoteItem(
                        bill_id=bill_id,
                        member_name=member_name,
                        chamber=chamber_upper,
                        vote=vote,
                        vote_date=vote_date,
                    )
                )

        return items

    @staticmethod
    def _tag_sectors(text: str) -> list[str]:
        """Tag a bill with sectors based on keyword matching."""
        text_lower = text.lower()
        matched = []
        for sector, keywords in BILL_SECTOR_KEYWORDS.items():
            if any(kw in text_lower for kw in keywords):
                matched.append(sector)
        return matched

    @staticmethod
    def _parse_date(date_str: str | None) -> date | None:
        if not date_str:
            return None
        try:
            from datetime import datetime

            return datetime.strptime(date_str[:10], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
