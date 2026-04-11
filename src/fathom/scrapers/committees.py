"""Committee membership scraper using unitedstates/congress-legislators GitHub data.

The congress.gov API does not expose committee membership rosters.
Instead we fetch from the unitedstates/congress-legislators project,
which maintains current committee assignments in YAML format.
"""

import logging
from dataclasses import dataclass, field

import yaml

from fathom.scrapers.base import BaseScraper, ScrapedItem

logger = logging.getLogger(__name__)

GITHUB_RAW = "https://raw.githubusercontent.com/unitedstates/congress-legislators/main"
MEMBERSHIP_URL = f"{GITHUB_RAW}/committee-membership-current.yaml"
COMMITTEES_URL = f"{GITHUB_RAW}/committees-current.yaml"

# Map Thomas committee codes (uppercase) to sector coverage.
# Only map committees with clear sector alignment; leave broad
# committees (Appropriations, Judiciary, Budget, etc.) unmapped.
COMMITTEE_SECTOR_MAP: dict[str, list[str]] = {
    # House committees
    "HSAS": ["Defense"],                            # Armed Services
    "HSBA": ["Finance"],                            # Financial Services
    "HSED": ["Healthcare"],                         # Education & Workforce
    "HSIF": ["Energy", "Healthcare", "Technology"], # Energy & Commerce
    "HSFA": ["Defense"],                            # Foreign Affairs
    "HSHM": ["Defense"],                            # Homeland Security
    "HSII": ["Energy"],                             # Natural Resources
    "HLIG": ["Defense", "Technology"],              # Intelligence
    "HSPW": ["Industrial"],                         # Transportation & Infrastructure
    "HSSY": ["Technology"],                         # Science, Space, & Technology
    "HSVR": ["Healthcare"],                         # Veterans' Affairs
    "HSWM": ["Finance"],                            # Ways & Means
    "HSAG": ["Consumer"],                           # Agriculture
    # Senate committees
    "SSAS": ["Defense"],                            # Armed Services
    "SSBK": ["Finance"],                            # Banking, Housing, & Urban Affairs
    "SSCM": ["Technology", "Consumer"],             # Commerce, Science, & Transportation
    "SSEG": ["Energy"],                             # Energy & Natural Resources
    "SSEV": ["Energy", "Industrial"],               # Environment & Public Works
    "SSFI": ["Finance"],                            # Finance
    "SSFR": ["Defense"],                            # Foreign Relations
    "SSHR": ["Healthcare"],                         # Health, Education, Labor & Pensions
    "SSVA": ["Healthcare"],                         # Veterans' Affairs
    "SLIN": ["Defense", "Technology"],              # Intelligence
    "SSAF": ["Consumer"],                           # Agriculture, Nutrition, & Forestry
    "SSGA": ["Defense"],                            # Homeland Security & Governmental Affairs
}


@dataclass
class CommitteeMembershipItem(ScrapedItem):
    """A committee membership record."""

    source: str = "unitedstates_github"
    member_name: str = ""
    chamber: str = ""
    committee_code: str = ""
    committee_name: str = ""
    role: str = "MEMBER"  # CHAIR, RANKING_MEMBER, MEMBER
    congress_number: int = 119
    sectors_covered: list[str] = field(default_factory=list)


class CommitteeScraper(BaseScraper):
    """Scrapes committee memberships from unitedstates/congress-legislators GitHub data."""

    def __init__(self):
        super().__init__(rate_limit_delay=0.2, max_retries=3)

    async def scrape(self) -> list[CommitteeMembershipItem]:
        """Fetch current committee memberships from GitHub YAML files."""
        try:
            committees_meta = await self._fetch_committees_meta()
            memberships_raw = await self._fetch_memberships()
        except Exception as e:
            logger.error(f"Failed to fetch committee data from GitHub: {e}")
            return []

        items: list[CommitteeMembershipItem] = []

        for code, members in memberships_raw.items():
            # Skip subcommittees (codes with trailing digits like HSAS04)
            parent_code = self._parent_code(code)
            if parent_code != code:
                continue

            meta = committees_meta.get(code, {})
            committee_name = meta.get("name", code)
            chamber = self._chamber_from_code(code)
            sectors = COMMITTEE_SECTOR_MAP.get(code, [])

            for member in members:
                name = member.get("name", "")
                if not name:
                    continue

                role = self._determine_role(member)

                items.append(
                    CommitteeMembershipItem(
                        member_name=name,
                        chamber=chamber,
                        committee_code=code,
                        committee_name=committee_name,
                        role=role,
                        congress_number=119,
                        sectors_covered=sectors,
                    )
                )

        logger.info(f"Scraped {len(items)} committee memberships")
        return items

    async def _fetch_committees_meta(self) -> dict[str, dict]:
        """Fetch committee metadata (names, types) from GitHub."""
        response = await self._fetch(COMMITTEES_URL)
        data = yaml.safe_load(response.text)

        result: dict[str, dict] = {}
        for committee in data:
            thomas_id = committee.get("thomas_id", "")
            if thomas_id:
                result[thomas_id] = {
                    "name": committee.get("name", ""),
                    "type": committee.get("type", ""),
                }
        return result

    async def _fetch_memberships(self) -> dict[str, list[dict]]:
        """Fetch committee membership assignments from GitHub."""
        response = await self._fetch(MEMBERSHIP_URL)
        return yaml.safe_load(response.text)

    @staticmethod
    def _parent_code(code: str) -> str:
        """Extract the parent committee code, stripping subcommittee digits.

        E.g. 'HSAS04' -> 'HSAS', 'SSAF13' -> 'SSAF', 'HSAS' -> 'HSAS'
        """
        # Parent codes are 4 letters (house/joint) or 4 letters (senate)
        # Subcommittees append 2 digits
        for i, ch in enumerate(code):
            if ch.isdigit():
                return code[:i]
        return code

    @staticmethod
    def _chamber_from_code(code: str) -> str:
        """Determine chamber from Thomas committee code prefix."""
        if code.startswith("HS") or code.startswith("HL"):
            return "HOUSE"
        if code.startswith("SS") or code.startswith("SL") or code.startswith("SP") or code.startswith("SC"):
            return "SENATE"
        if code.startswith("JS") or code.startswith("JC") or code.startswith("JE"):
            return "JOINT"
        return "UNKNOWN"

    @staticmethod
    def _determine_role(member: dict) -> str:
        """Determine the member's role from the title field."""
        title = (member.get("title") or "").lower()
        if "chairman" in title or "chairwoman" in title or "chair" in title:
            if "vice" in title:
                return "MEMBER"
            if "ranking" in title:
                return "RANKING_MEMBER"
            return "CHAIR"
        if "ranking" in title:
            return "RANKING_MEMBER"
        return "MEMBER"
