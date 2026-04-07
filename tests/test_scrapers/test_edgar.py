"""Tests for the SEC EDGAR Form 4 scraper."""

import pytest
from datetime import date
from unittest.mock import AsyncMock, patch, MagicMock

import httpx

from fathom.scrapers.edgar import EdgarScraper, InsiderTradeItem, TRACKED_CIKS
from tests.test_scrapers.conftest import (
    make_httpx_response,
    SAMPLE_CIK_TICKER_JSON,
    SAMPLE_SUBMISSIONS_JSON,
    SAMPLE_FORM4_XML_BUY,
    SAMPLE_FORM4_XML_SELL,
    SAMPLE_FORM4_XML_EXERCISE,
    SAMPLE_FORM4_XML_UNKNOWN_CODE,
    SAMPLE_FORM4_XML_NO_DATE,
    SAMPLE_FORM4_XML_MISSING_ISSUER,
    SAMPLE_FORM4_XML_MALFORMED,
)


@pytest.fixture
def scraper():
    return EdgarScraper()


class TestCikTickerMap:
    async def test_load_cik_ticker_map(self, scraper):
        """CIK-to-ticker map should load and zero-pad CIKs to 10 digits."""
        response = make_httpx_response(json_data=SAMPLE_CIK_TICKER_JSON)
        scraper._fetch = AsyncMock(return_value=response)

        await scraper._load_cik_ticker_map()

        assert len(scraper._cik_ticker_map) == 3
        assert scraper._cik_ticker_map["0000320193"] == "AAPL"
        assert scraper._cik_ticker_map["0000789019"] == "MSFT"
        assert scraper._cik_ticker_map["0000002488"] == "AMD"

    async def test_load_cik_ticker_map_cached(self, scraper):
        """Should not re-fetch if map is already loaded."""
        scraper._cik_ticker_map = {"0000320193": "AAPL"}
        scraper._fetch = AsyncMock()

        await scraper._load_cik_ticker_map()

        scraper._fetch.assert_not_called()

    async def test_cik_to_ticker_padding(self, scraper):
        """Should zero-pad the CIK to 10 digits before lookup."""
        scraper._cik_ticker_map = {"0000320193": "AAPL"}
        assert scraper._cik_to_ticker("320193") == "AAPL"
        assert scraper._cik_to_ticker("0000320193") == "AAPL"
        assert scraper._cik_to_ticker("999999999") is None


class TestGetRecentForm4Filings:
    async def test_success(self, scraper):
        """Should extract Form 4 filings from submissions response."""
        response = make_httpx_response(json_data=SAMPLE_SUBMISSIONS_JSON)
        scraper._fetch = AsyncMock(return_value=response)

        filings = await scraper._get_recent_form4_filings("320193", days_back=90)

        # Should get 3 Form 4 filings (the 10-Q is excluded)
        assert len(filings) == 3
        assert filings[0]["cik"] == "320193"
        assert "doc.xml" in filings[0]["xml_url"]

    async def test_404_returns_empty(self, scraper):
        """404 should return empty list, not raise."""
        scraper._fetch = AsyncMock(
            side_effect=httpx.HTTPStatusError("404", request=MagicMock(), response=MagicMock())
        )

        filings = await scraper._get_recent_form4_filings("9999999", days_back=90)
        assert filings == []

    async def test_server_error_returns_empty(self, scraper):
        """500 errors should be caught and return empty list."""
        scraper._fetch = AsyncMock(side_effect=Exception("Connection failed"))

        filings = await scraper._get_recent_form4_filings("320193", days_back=90)
        assert filings == []


class TestGetForm4FilingsSince:
    async def test_recent_only(self, scraper):
        """Should return Form 4 filings from the recent block only."""
        response = make_httpx_response(json_data=SAMPLE_SUBMISSIONS_JSON)
        scraper._fetch = AsyncMock(return_value=response)

        filings = await scraper.get_form4_filings_since(
            "320193", since=date(2026, 1, 1), include_archive=False
        )

        # 3 Form 4 filings in SAMPLE_SUBMISSIONS_JSON, all >= 2026-01-01
        assert len(filings) == 3
        scraper._fetch.assert_called_once()

    async def test_since_filters_out_older(self, scraper):
        """Filings older than `since` should be excluded."""
        response = make_httpx_response(json_data=SAMPLE_SUBMISSIONS_JSON)
        scraper._fetch = AsyncMock(return_value=response)

        filings = await scraper.get_form4_filings_since(
            "320193", since=date(2026, 3, 1), include_archive=False
        )

        # Only 2 filings are on/after 2026-03-01 (2026-03-28 and 2026-03-15)
        assert len(filings) == 2

    async def test_archive_descent(self, scraper):
        """Should descend into archive files when more data may exist."""
        # Recent block with oldest entry still >= cutoff -> signal to check archive
        submissions_with_archive = {
            "filings": {
                "recent": {
                    "form": ["4"],
                    "filingDate": ["2022-06-15"],
                    "accessionNumber": ["0000320193-22-000001"],
                    "primaryDocument": ["recent.xml"],
                },
                "files": [
                    {
                        "name": "CIK0000320193-submissions-001.json",
                        "filingCount": 500,
                        "filingFrom": "2019-01-01",
                        "filingTo": "2022-06-14",
                    },
                ],
            }
        }
        archive_data = {
            "form": ["4", "4"],
            "filingDate": ["2021-03-15", "2020-11-20"],
            "accessionNumber": ["0000320193-21-000001", "0000320193-20-000001"],
            "primaryDocument": ["arch1.xml", "arch2.xml"],
        }

        call_log = []

        async def mock_fetch(url, **kwargs):
            call_log.append(url)
            if "submissions-001" in url:
                return make_httpx_response(json_data=archive_data)
            return make_httpx_response(json_data=submissions_with_archive)

        scraper._fetch = AsyncMock(side_effect=mock_fetch)

        filings = await scraper.get_form4_filings_since(
            "320193", since=date(2020, 1, 1), include_archive=True
        )

        # 1 from recent + 2 from archive = 3
        assert len(filings) == 3
        assert scraper._fetch.call_count == 2

    async def test_archive_skipped_when_filing_to_before_cutoff(self, scraper):
        """Archive files entirely before cutoff should not be fetched."""
        submissions = {
            "filings": {
                "recent": {
                    "form": ["4"],
                    "filingDate": ["2022-06-15"],
                    "accessionNumber": ["0000320193-22-000001"],
                    "primaryDocument": ["recent.xml"],
                },
                "files": [
                    {
                        "name": "CIK0000320193-submissions-001.json",
                        "filingCount": 500,
                        "filingFrom": "2015-01-01",
                        "filingTo": "2019-12-31",  # entirely before 2020-01-01 cutoff
                    },
                ],
            }
        }
        response = make_httpx_response(json_data=submissions)
        scraper._fetch = AsyncMock(return_value=response)

        filings = await scraper.get_form4_filings_since(
            "320193", since=date(2020, 1, 1), include_archive=True
        )

        # Only the recent block, archive is skipped without fetching
        assert len(filings) == 1
        # Only the main submissions URL was fetched
        assert scraper._fetch.call_count == 1

    async def test_include_archive_false_skips_files(self, scraper):
        """include_archive=False should never fetch archive files."""
        submissions = {
            "filings": {
                "recent": {
                    "form": ["4"],
                    "filingDate": ["2022-06-15"],
                    "accessionNumber": ["0000320193-22-000001"],
                    "primaryDocument": ["recent.xml"],
                },
                "files": [
                    {
                        "name": "CIK0000320193-submissions-001.json",
                        "filingCount": 500,
                        "filingFrom": "2019-01-01",
                        "filingTo": "2022-06-14",
                    },
                ],
            }
        }
        response = make_httpx_response(json_data=submissions)
        scraper._fetch = AsyncMock(return_value=response)

        filings = await scraper.get_form4_filings_since(
            "320193", since=date(2019, 1, 1), include_archive=False
        )

        assert len(filings) == 1
        assert scraper._fetch.call_count == 1

    async def test_fetch_error_returns_empty(self, scraper):
        """Network errors should return empty list, not raise."""
        scraper._fetch = AsyncMock(side_effect=Exception("connection failed"))

        filings = await scraper.get_form4_filings_since(
            "999999", since=date(2020, 1, 1)
        )
        assert filings == []


class TestParseForm4Xml:
    def test_buy(self, scraper):
        """Should parse a purchase transaction correctly."""
        scraper._cik_ticker_map = {}  # ticker comes from XML
        items = scraper._parse_form4_xml(SAMPLE_FORM4_XML_BUY, "http://test.xml", "2026-03-28")

        assert len(items) == 1
        item = items[0]
        assert item.trade_type == "BUY"
        assert item.ticker == "AAPL"
        assert item.filer_name == "John Doe"
        assert item.filer_title == "SVP Engineering"
        assert item.shares == 1000
        assert item.price_per_share == 185.50
        assert item.total_value == 185500.0
        assert item.trade_date == date(2026, 3, 25)

    def test_sell(self, scraper):
        """Should parse a sale transaction."""
        scraper._cik_ticker_map = {}
        items = scraper._parse_form4_xml(SAMPLE_FORM4_XML_SELL, "http://test.xml", "2026-03-28")

        assert len(items) == 1
        assert items[0].trade_type == "SELL"

    def test_exercise(self, scraper):
        """Should parse an option exercise transaction."""
        scraper._cik_ticker_map = {}
        items = scraper._parse_form4_xml(SAMPLE_FORM4_XML_EXERCISE, "http://test.xml", "2026-03-28")

        assert len(items) == 1
        assert items[0].trade_type == "EXERCISE"

    def test_unknown_code_filtered(self, scraper):
        """Unknown transaction codes should be filtered out."""
        scraper._cik_ticker_map = {}
        items = scraper._parse_form4_xml(SAMPLE_FORM4_XML_UNKNOWN_CODE, "http://test.xml", "2026-03-28")

        assert len(items) == 0

    def test_missing_date_filtered(self, scraper):
        """Transactions without a date should be filtered out."""
        scraper._cik_ticker_map = {}
        items = scraper._parse_form4_xml(SAMPLE_FORM4_XML_NO_DATE, "http://test.xml", "2026-03-28")

        assert len(items) == 0

    def test_missing_issuer(self, scraper):
        """Missing issuer element should return empty list."""
        scraper._cik_ticker_map = {}
        items = scraper._parse_form4_xml(SAMPLE_FORM4_XML_MISSING_ISSUER, "http://test.xml", "2026-03-28")

        assert items == []

    def test_malformed_xml(self, scraper):
        """Malformed XML should return empty list."""
        scraper._cik_ticker_map = {}
        items = scraper._parse_form4_xml(SAMPLE_FORM4_XML_MALFORMED, "http://test.xml", "2026-03-28")

        assert items == []

    def test_ticker_fallback_to_cik_map(self, scraper):
        """When XML has no trading symbol, should fall back to CIK map."""
        xml_no_ticker = SAMPLE_FORM4_XML_BUY.replace(
            "<issuerTradingSymbol>AAPL</issuerTradingSymbol>",
            "<issuerTradingSymbol></issuerTradingSymbol>",
        )
        scraper._cik_ticker_map = {"0000320193": "AAPL"}
        items = scraper._parse_form4_xml(xml_no_ticker, "http://test.xml", "2026-03-28")

        assert len(items) == 1
        assert items[0].ticker == "AAPL"


class TestScrapeEndToEnd:
    async def test_full_scrape_cycle(self, scraper):
        """Full scrape should load CIK map, fetch submissions, parse XMLs."""
        cik_response = make_httpx_response(json_data=SAMPLE_CIK_TICKER_JSON)
        submissions_response = make_httpx_response(json_data=SAMPLE_SUBMISSIONS_JSON)
        xml_response = make_httpx_response(text=SAMPLE_FORM4_XML_BUY)

        call_count = 0

        async def mock_fetch(url, **kwargs):
            nonlocal call_count
            call_count += 1
            if "company_tickers" in url:
                return cik_response
            elif "submissions" in url:
                return submissions_response
            else:
                return xml_response

        scraper._fetch = mock_fetch

        items = await scraper.scrape()

        assert len(items) > 0
        assert all(isinstance(item, InsiderTradeItem) for item in items)
        assert call_count > 1  # CIK map + submissions + XML fetches


class TestTrackedCiks:
    def test_amd_cik_is_correct(self):
        """AMD CIK should be 2488, not 2488552."""
        assert TRACKED_CIKS.get("2488") == "AMD"
        assert "2488552" not in TRACKED_CIKS

    def test_no_duplicate_tickers(self):
        """Each ticker should appear only once."""
        tickers = list(TRACKED_CIKS.values())
        assert len(tickers) == len(set(tickers))

    def test_no_duplicate_ciks(self):
        """Each CIK should appear only once."""
        ciks = list(TRACKED_CIKS.keys())
        assert len(ciks) == len(set(ciks))
