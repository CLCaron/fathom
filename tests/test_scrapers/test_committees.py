"""Tests for the committee membership scraper (GitHub YAML-based)."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import yaml

from fathom.scrapers.committees import (
    COMMITTEE_SECTOR_MAP,
    CommitteeMembershipItem,
    CommitteeScraper,
)


SAMPLE_COMMITTEES_YAML = [
    {"thomas_id": "HSAS", "name": "House Committee on Armed Services", "type": "house"},
    {"thomas_id": "SSHR", "name": "Senate HELP Committee", "type": "senate"},
]

SAMPLE_MEMBERSHIP_YAML = {
    "HSAS": [
        {"name": "Jane Smith", "party": "majority", "rank": 1, "title": "Chairman", "bioguide": "S000001"},
        {"name": "John Doe", "party": "minority", "rank": 1, "title": "Ranking Member", "bioguide": "D000001"},
        {"name": "Bob Jones", "party": "majority", "rank": 2, "bioguide": "J000001"},
    ],
    "HSAS04": [
        {"name": "Sub Member", "party": "majority", "rank": 1, "bioguide": "M000001"},
    ],
    "SSHR": [
        {"name": "Alice Brown", "party": "majority", "rank": 1, "title": "Chairman", "bioguide": "B000001"},
    ],
}


def _make_yaml_response(data):
    """Create a mock httpx response that returns YAML text."""
    resp = MagicMock()
    resp.text = yaml.dump(data)
    resp.status_code = 200
    return resp


class TestParentCode:
    def test_parent_committee(self):
        assert CommitteeScraper._parent_code("HSAS") == "HSAS"

    def test_subcommittee(self):
        assert CommitteeScraper._parent_code("HSAS04") == "HSAS"
        assert CommitteeScraper._parent_code("SSAF13") == "SSAF"

    def test_four_letter_code(self):
        assert CommitteeScraper._parent_code("SLIN") == "SLIN"


class TestChamberFromCode:
    def test_house(self):
        assert CommitteeScraper._chamber_from_code("HSAS") == "HOUSE"
        assert CommitteeScraper._chamber_from_code("HLIG") == "HOUSE"

    def test_senate(self):
        assert CommitteeScraper._chamber_from_code("SSAF") == "SENATE"
        assert CommitteeScraper._chamber_from_code("SLIN") == "SENATE"

    def test_joint(self):
        assert CommitteeScraper._chamber_from_code("JSLC") == "JOINT"
        assert CommitteeScraper._chamber_from_code("JSEC") == "JOINT"


class TestDetermineRole:
    def test_chair(self):
        assert CommitteeScraper._determine_role({"title": "Chairman"}) == "CHAIR"
        assert CommitteeScraper._determine_role({"title": "Chairwoman"}) == "CHAIR"

    def test_ranking_member(self):
        assert CommitteeScraper._determine_role({"title": "Ranking Member"}) == "RANKING_MEMBER"

    def test_regular_member(self):
        assert CommitteeScraper._determine_role({}) == "MEMBER"
        assert CommitteeScraper._determine_role({"title": None}) == "MEMBER"
        assert CommitteeScraper._determine_role({"title": ""}) == "MEMBER"

    def test_vice_chair_is_member(self):
        assert CommitteeScraper._determine_role({"title": "Vice Chairman"}) == "MEMBER"


class TestCommitteeSectorMap:
    def test_armed_services_covers_defense(self):
        assert "Defense" in COMMITTEE_SECTOR_MAP["HSAS"]
        assert "Defense" in COMMITTEE_SECTOR_MAP["SSAS"]

    def test_finance_committees(self):
        assert "Finance" in COMMITTEE_SECTOR_MAP["HSBA"]
        assert "Finance" in COMMITTEE_SECTOR_MAP["SSBK"]

    def test_energy_commerce(self):
        sectors = COMMITTEE_SECTOR_MAP["HSIF"]
        assert "Energy" in sectors
        assert "Healthcare" in sectors
        assert "Technology" in sectors

    def test_help_has_healthcare(self):
        assert "Healthcare" in COMMITTEE_SECTOR_MAP["SSHR"]

    def test_natural_resources_has_energy(self):
        assert "Energy" in COMMITTEE_SECTOR_MAP["HSII"]

    def test_intelligence_has_defense_and_tech(self):
        assert "Defense" in COMMITTEE_SECTOR_MAP["SLIN"]
        assert "Technology" in COMMITTEE_SECTOR_MAP["SLIN"]


class TestScrape:
    async def test_parses_committees_and_members(self):
        scraper = CommitteeScraper()

        committees_resp = _make_yaml_response(SAMPLE_COMMITTEES_YAML)
        membership_resp = _make_yaml_response(SAMPLE_MEMBERSHIP_YAML)

        call_count = 0

        async def mock_fetch(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "committee-membership" in url:
                return membership_resp
            return committees_resp

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            items = await scraper.scrape()

        # HSAS: 3 members (subcommittee HSAS04 is skipped), SSHR: 1 member = 4 total
        assert len(items) == 4
        assert any(i.role == "CHAIR" for i in items)
        assert any(i.role == "RANKING_MEMBER" for i in items)
        assert any(i.role == "MEMBER" for i in items)
        await scraper.close()

    async def test_subcommittees_are_skipped(self):
        scraper = CommitteeScraper()

        committees_resp = _make_yaml_response(SAMPLE_COMMITTEES_YAML)
        membership_resp = _make_yaml_response(SAMPLE_MEMBERSHIP_YAML)

        async def mock_fetch(url, **kwargs):
            if "committee-membership" in url:
                return membership_resp
            return committees_resp

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            items = await scraper.scrape()

        # No items should have HSAS04 code
        assert not any(i.committee_code == "HSAS04" for i in items)
        await scraper.close()

    async def test_armed_services_gets_defense_sector(self):
        scraper = CommitteeScraper()

        committees_resp = _make_yaml_response(SAMPLE_COMMITTEES_YAML)
        membership_resp = _make_yaml_response(SAMPLE_MEMBERSHIP_YAML)

        async def mock_fetch(url, **kwargs):
            if "committee-membership" in url:
                return membership_resp
            return committees_resp

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            items = await scraper.scrape()

        armed = [i for i in items if i.committee_code == "HSAS"]
        assert len(armed) == 3
        for item in armed:
            assert item.sectors_covered == ["Defense"]
        await scraper.close()

    async def test_help_gets_healthcare_sector(self):
        scraper = CommitteeScraper()

        committees_resp = _make_yaml_response(SAMPLE_COMMITTEES_YAML)
        membership_resp = _make_yaml_response(SAMPLE_MEMBERSHIP_YAML)

        async def mock_fetch(url, **kwargs):
            if "committee-membership" in url:
                return membership_resp
            return committees_resp

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            items = await scraper.scrape()

        help_items = [i for i in items if i.committee_code == "SSHR"]
        assert len(help_items) == 1
        assert help_items[0].sectors_covered == ["Healthcare"]
        await scraper.close()

    async def test_github_fetch_failure_returns_empty(self):
        scraper = CommitteeScraper()

        async def mock_fetch(url, **kwargs):
            raise RuntimeError("GitHub is down")

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            items = await scraper.scrape()

        assert items == []
        await scraper.close()
