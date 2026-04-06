"""Committee membership scraper using congress.gov API."""

import logging
from dataclasses import dataclass, field

from fathom.config import settings
from fathom.scrapers.base import BaseScraper, ScrapedItem

logger = logging.getLogger(__name__)

API_BASE = "https://api.congress.gov/v3"

# Map committee system codes to sector coverage.
# Keys use the Thomas committee code format from congress.gov.
COMMITTEE_SECTOR_MAP: dict[str, list[str]] = {
    # House committees
    "hsas": ["Defense"],
    "hsba": ["Finance"],
    "hsbu": [],  # Budget
    "hsed": [],  # Education & Workforce
    "hsif": ["Energy", "Healthcare", "Technology"],  # Energy & Commerce
    "hsfa": ["Defense"],  # Foreign Affairs
    "hsha": [],  # Administration
    "hshm": ["Defense"],  # Homeland Security
    "hsii": [],  # Natural Resources
    "hsju": [],  # Judiciary
    "hsag": [],  # Agriculture
    "hsap": [],  # Appropriations
    "hspw": ["Industrial"],  # Transportation & Infrastructure
    "hsgo": [],  # Oversight
    "hsru": [],  # Rules
    "hssm": [],  # Small Business
    "hssy": ["Technology"],  # Science, Space, & Technology
    "hsvr": ["Healthcare"],  # Veterans' Affairs
    "hswm": ["Finance"],  # Ways & Means
    "hlig": [],  # Intelligence
    # Senate committees
    "ssas": ["Defense"],  # Armed Services
    "ssbk": ["Finance"],  # Banking, Housing, & Urban Affairs
    "ssbu": [],  # Budget
    "sscm": ["Technology", "Consumer"],  # Commerce, Science, & Transportation
    "sseg": ["Energy"],  # Energy & Natural Resources
    "ssev": [],  # Environment & Public Works
    "ssfi": ["Finance"],  # Finance
    "ssfr": ["Defense"],  # Foreign Relations
    "sshr": [],  # Health, Education, Labor & Pensions — broad
    "ssga": [],  # Homeland Security
    "ssju": [],  # Judiciary
    "ssra": [],  # Rules & Administration
    "sssb": [],  # Small Business
    "slin": [],  # Intelligence
    "ssva": ["Healthcare"],  # Veterans' Affairs
    "ssag": [],  # Agriculture
    "ssap": [],  # Appropriations
    "slia": [],  # Indian Affairs
}


@dataclass
class CommitteeMembershipItem(ScrapedItem):
    """A committee membership record."""

    source: str = "congress_gov"
    member_name: str = ""
    chamber: str = ""
    committee_code: str = ""
    committee_name: str = ""
    role: str = "MEMBER"  # CHAIR, RANKING_MEMBER, MEMBER
    congress_number: int = 119
    sectors_covered: list[str] = field(default_factory=list)


class CommitteeScraper(BaseScraper):
    """Scrapes committee memberships from congress.gov API."""

    def __init__(self):
        super().__init__(rate_limit_delay=0.5, max_retries=3)
        self._api_key = settings.congress_api_key

    async def scrape(self) -> list[CommitteeMembershipItem]:
        """Fetch all committee memberships for the current Congress."""
        if not self._api_key:
            logger.warning(
                "CONGRESS_API_KEY not set, skipping committee scrape. "
                "Register at https://api.congress.gov/sign-up/"
            )
            return []

        items: list[CommitteeMembershipItem] = []

        for chamber_path in ["house", "senate"]:
            try:
                committees = await self._fetch_committees(chamber_path)
            except Exception as e:
                logger.error(f"Failed to fetch {chamber_path} committees: {e}")
                continue

            for committee in committees:
                code = committee.get("systemCode", "").lower()
                name = committee.get("name", "")
                chamber = "HOUSE" if chamber_path == "house" else "SENATE"

                try:
                    members = await self._fetch_members(
                        chamber_path, committee.get("systemCode", "")
                    )
                except Exception as e:
                    logger.warning(f"Failed to fetch members for {code}: {e}")
                    continue

                sectors = COMMITTEE_SECTOR_MAP.get(code, [])

                for member in members:
                    role = self._determine_role(member)
                    member_name = member.get("name", "")
                    if not member_name:
                        continue

                    items.append(
                        CommitteeMembershipItem(
                            member_name=member_name,
                            chamber=chamber,
                            committee_code=code,
                            committee_name=name,
                            role=role,
                            congress_number=119,
                            sectors_covered=sectors,
                        )
                    )

        logger.info(f"Scraped {len(items)} committee memberships")
        return items

    async def _fetch_committees(self, chamber: str) -> list[dict]:
        """Fetch list of committees for a chamber."""
        url = f"{API_BASE}/committee/{chamber}"
        params = {
            "api_key": self._api_key,
            "congress": 119,
            "limit": 250,
            "format": "json",
        }
        response = await self._fetch(url, params=params)
        data = response.json()
        return data.get("committees", [])

    async def _fetch_members(self, chamber: str, system_code: str) -> list[dict]:
        """Fetch members of a specific committee."""
        url = f"{API_BASE}/committee/{chamber}/{system_code}"
        params = {
            "api_key": self._api_key,
            "format": "json",
        }
        response = await self._fetch(url, params=params)
        data = response.json()

        # The API nests members under committee -> currentMembers
        committee_data = data.get("committee", {})
        return committee_data.get("currentMembers", [])

    @staticmethod
    def _determine_role(member: dict) -> str:
        """Determine the member's role on the committee."""
        title = (member.get("title") or "").lower()
        if "chair" in title and "ranking" not in title:
            return "CHAIR"
        if "ranking" in title:
            return "RANKING_MEMBER"
        return "MEMBER"
