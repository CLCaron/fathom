"""Tests for the legislation and votes scraper."""

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from fathom.scrapers.legislation import (
    BILL_SECTOR_KEYWORDS,
    BillItem,
    LegislationScraper,
    VoteItem,
)
from tests.test_scrapers.conftest import make_httpx_response


SAMPLE_BILLS_RESPONSE = {
    "bills": [
        {
            "type": "HR",
            "number": "1234",
            "title": "National Defense Authorization Act for Fiscal Year 2026",
            "congress": 119,
            "introducedDate": "2026-01-15",
            "latestAction": {
                "actionDate": "2026-03-01",
                "text": "Passed House by voice vote",
            },
            "sponsors": [{"fullName": "Rep. Smith, John"}],
            "url": "https://api.congress.gov/v3/bill/119/hr/1234",
        },
        {
            "type": "S",
            "number": "567",
            "title": "Clean Energy Innovation Act",
            "congress": 119,
            "introducedDate": "2026-02-10",
            "latestAction": {
                "actionDate": "2026-03-15",
                "text": "Read twice and referred to committee",
            },
            "sponsors": [],
            "url": None,
        },
    ]
}


class TestTagSectors:
    def test_defense_keywords(self):
        sectors = LegislationScraper._tag_sectors("National Defense Authorization Act")
        assert "Defense" in sectors

    def test_energy_keywords(self):
        sectors = LegislationScraper._tag_sectors("Clean Energy Innovation Act")
        assert "Energy" in sectors

    def test_healthcare_keywords(self):
        sectors = LegislationScraper._tag_sectors("Medicare Prescription Drug Price Act")
        assert "Healthcare" in sectors

    def test_technology_keywords(self):
        sectors = LegislationScraper._tag_sectors("Semiconductor Manufacturing Incentives")
        assert "Technology" in sectors

    def test_multiple_sectors(self):
        sectors = LegislationScraper._tag_sectors(
            "Defense and Cybersecurity Modernization Act"
        )
        assert "Defense" in sectors
        assert "Technology" in sectors

    def test_no_match(self):
        sectors = LegislationScraper._tag_sectors("Naming a post office")
        assert sectors == []

    def test_case_insensitive(self):
        sectors = LegislationScraper._tag_sectors("DEFENSE SPENDING BILL")
        assert "Defense" in sectors


class TestParseBill:
    def test_basic_fields(self):
        scraper = LegislationScraper()
        bill = scraper._parse_bill(SAMPLE_BILLS_RESPONSE["bills"][0])

        assert bill is not None
        assert bill.bill_id == "HR-1234"
        assert "Defense" in bill.title
        assert bill.congress_number == 119
        assert bill.introduced_date == date(2026, 1, 15)
        assert bill.last_action_date == date(2026, 3, 1)
        assert bill.sponsor_name == "Rep. Smith, John"
        assert "Defense" in bill.sectors_affected

    def test_no_sponsor(self):
        scraper = LegislationScraper()
        bill = scraper._parse_bill(SAMPLE_BILLS_RESPONSE["bills"][1])
        assert bill.sponsor_name is None

    def test_missing_number_returns_none(self):
        scraper = LegislationScraper()
        bill = scraper._parse_bill({"type": "HR", "number": "", "title": "Test"})
        assert bill is None


class TestScrapeSkipsWithoutKey:
    async def test_no_api_key_returns_empty(self):
        with patch("fathom.scrapers.legislation.settings") as mock_settings:
            mock_settings.congress_api_key = ""
            scraper = LegislationScraper()
            scraper._api_key = ""
            bills, votes = await scraper.scrape()
            assert bills == []
            assert votes == []
            await scraper.close()


class TestFetchBills:
    async def test_fetches_and_parses_bills(self):
        scraper = LegislationScraper()
        scraper._api_key = "test-key"

        resp = make_httpx_response(json_data=SAMPLE_BILLS_RESPONSE)

        with patch.object(scraper, "_fetch", new_callable=AsyncMock, return_value=resp):
            bills = await scraper._fetch_bills()

        assert len(bills) == 2
        assert bills[0].bill_id == "HR-1234"
        assert bills[1].bill_id == "S-567"
        await scraper.close()

    async def test_handles_fetch_error(self):
        scraper = LegislationScraper()
        scraper._api_key = "test-key"

        with patch.object(
            scraper, "_fetch",
            new_callable=AsyncMock,
            side_effect=RuntimeError("network error"),
        ):
            bills = await scraper._fetch_bills()

        assert bills == []
        await scraper.close()


class TestParseDate:
    def test_valid_date(self):
        assert LegislationScraper._parse_date("2026-03-15") == date(2026, 3, 15)

    def test_none(self):
        assert LegislationScraper._parse_date(None) is None

    def test_invalid(self):
        assert LegislationScraper._parse_date("not-a-date") is None

    def test_truncates_to_10_chars(self):
        assert LegislationScraper._parse_date("2026-03-15T10:00:00Z") == date(2026, 3, 15)


class TestBillSectorKeywords:
    def test_all_sectors_have_keywords(self):
        for sector, keywords in BILL_SECTOR_KEYWORDS.items():
            assert len(keywords) > 0, f"{sector} has no keywords"

    def test_keywords_are_lowercase(self):
        for sector, keywords in BILL_SECTOR_KEYWORDS.items():
            for kw in keywords:
                assert kw == kw.lower(), f"Keyword '{kw}' in {sector} should be lowercase"
