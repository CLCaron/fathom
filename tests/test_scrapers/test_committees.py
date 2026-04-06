"""Tests for the committee membership scraper."""

from unittest.mock import AsyncMock, patch

import pytest

from fathom.scrapers.committees import (
    COMMITTEE_SECTOR_MAP,
    CommitteeMembershipItem,
    CommitteeScraper,
)
from tests.test_scrapers.conftest import make_httpx_response


SAMPLE_COMMITTEES_RESPONSE = {
    "committees": [
        {"systemCode": "hsas", "name": "Committee on Armed Services"},
        {"systemCode": "hsba", "name": "Committee on Financial Services"},
    ]
}

SAMPLE_MEMBERS_RESPONSE = {
    "committee": {
        "currentMembers": [
            {"name": "Jane Smith", "title": "Chair"},
            {"name": "John Doe", "title": "Ranking Member"},
            {"name": "Bob Jones", "title": None},
        ]
    }
}


class TestDetermineRole:
    def test_chair(self):
        assert CommitteeScraper._determine_role({"title": "Chair"}) == "CHAIR"
        assert CommitteeScraper._determine_role({"title": "Chairman"}) == "CHAIR"

    def test_ranking_member(self):
        assert CommitteeScraper._determine_role({"title": "Ranking Member"}) == "RANKING_MEMBER"

    def test_regular_member(self):
        assert CommitteeScraper._determine_role({"title": None}) == "MEMBER"
        assert CommitteeScraper._determine_role({"title": ""}) == "MEMBER"
        assert CommitteeScraper._determine_role({}) == "MEMBER"


class TestCommitteeSectorMap:
    def test_armed_services_covers_defense(self):
        assert "Defense" in COMMITTEE_SECTOR_MAP["hsas"]
        assert "Defense" in COMMITTEE_SECTOR_MAP["ssas"]

    def test_finance_committees(self):
        assert "Finance" in COMMITTEE_SECTOR_MAP["hsba"]
        assert "Finance" in COMMITTEE_SECTOR_MAP["ssbk"]

    def test_energy_commerce(self):
        sectors = COMMITTEE_SECTOR_MAP["hsif"]
        assert "Energy" in sectors
        assert "Healthcare" in sectors
        assert "Technology" in sectors


class TestScrapeSkipsWithoutKey:
    async def test_no_api_key_returns_empty(self):
        with patch("fathom.scrapers.committees.settings") as mock_settings:
            mock_settings.congress_api_key = ""
            scraper = CommitteeScraper()
            scraper._api_key = ""
            result = await scraper.scrape()
            assert result == []
            await scraper.close()


class TestScrape:
    async def test_parses_committees_and_members(self):
        scraper = CommitteeScraper()
        scraper._api_key = "test-key"

        committees_resp = make_httpx_response(json_data=SAMPLE_COMMITTEES_RESPONSE)
        members_resp = make_httpx_response(json_data=SAMPLE_MEMBERS_RESPONSE)

        call_count = 0

        async def mock_fetch(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "committee/house/" in url or "committee/senate/" in url:
                return members_resp
            return committees_resp

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            items = await scraper.scrape()

        # 2 chambers x 2 committees x 3 members = 12
        assert len(items) == 12
        assert any(i.role == "CHAIR" for i in items)
        assert any(i.role == "RANKING_MEMBER" for i in items)
        assert any(i.role == "MEMBER" for i in items)
        await scraper.close()

    async def test_armed_services_gets_defense_sector(self):
        scraper = CommitteeScraper()
        scraper._api_key = "test-key"

        committees_resp = make_httpx_response(json_data={
            "committees": [{"systemCode": "hsas", "name": "Armed Services"}]
        })
        members_resp = make_httpx_response(json_data={
            "committee": {"currentMembers": [{"name": "Test Person", "title": None}]}
        })

        async def mock_fetch(url, **kwargs):
            if "committee/house/" in url or "committee/senate/" in url:
                return members_resp
            return committees_resp

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            items = await scraper.scrape()

        armed_services = [i for i in items if i.committee_code == "hsas"]
        assert len(armed_services) >= 1
        assert armed_services[0].sectors_covered == ["Defense"]
        await scraper.close()
