"""Tests for the Capitol Trades scraper."""

import json
from datetime import date
from unittest.mock import AsyncMock, patch

import pytest

from fathom.scrapers.capitol_trades import (
    AMOUNT_RANGE_MAP,
    CapitolTradesScraper,
    CongressionalTradeItem,
    SECTOR_NORMALIZE,
)
from tests.test_scrapers.conftest import make_httpx_response


def _build_rsc_payload(trades: list[dict], total_pages: int = 1) -> str:
    """Build a minimal RSC payload containing trade data."""
    data_json = json.dumps(trades)
    return (
        f'0:["buildId"]\n'
        f'2:["$","main",null,{{"children":[]}}]\n'
        f'7:["$","div",null,{{"children":[["$","$L9",null,'
        f'{{"columns":"$a","data":{data_json}}}]]}}]\n'
        f'8:["$","div",null,{{"totalCount":{len(trades) * total_pages},'
        f'"totalPages":{total_pages}}}]\n'
    )


SAMPLE_TRADE = {
    "_issuerId": 12345,
    "_politicianId": "S000123",
    "_txId": 99999,
    "chamber": "senate",
    "comment": "",
    "issuer": {
        "_stateId": "wa",
        "c2iq": "ABC123",
        "country": "us",
        "issuerName": "Microsoft Corp",
        "issuerTicker": "MSFT:US",
        "sector": "information-technology",
    },
    "owner": "self",
    "politician": {
        "_stateId": "wa",
        "chamber": "senate",
        "dob": "1960-01-01",
        "firstName": "Jane",
        "gender": "female",
        "lastName": "Smith",
        "nickname": None,
        "party": "democrat",
    },
    "price": 420.50,
    "pubDate": "2026-03-20T10:00:00Z",
    "reportingGap": 5,
    "txDate": "2026-03-15",
    "txType": "buy",
    "txTypeExtended": None,
    "value": 75000,
}


class TestParseTradeItem:
    def test_basic_fields(self):
        scraper = CapitolTradesScraper()
        item = scraper._parse_trade(SAMPLE_TRADE)

        assert item is not None
        assert item.member_name == "Jane Smith"
        assert item.chamber == "SENATE"
        assert item.state == "WA"
        assert item.party == "Democrat"
        assert item.ticker == "MSFT"
        assert item.asset_name == "Microsoft Corp"
        assert item.trade_type == "PURCHASE"
        assert item.trade_date == date(2026, 3, 15)
        assert item.disclosure_date == date(2026, 3, 20)
        assert item.sector == "Technology"
        assert item.source == "capitol_trades"

    def test_amount_range(self):
        scraper = CapitolTradesScraper()
        item = scraper._parse_trade(SAMPLE_TRADE)

        assert item.amount_min == 50_001
        assert item.amount_max == 100_000

    def test_sell_type(self):
        scraper = CapitolTradesScraper()
        trade = {**SAMPLE_TRADE, "txType": "sell"}
        item = scraper._parse_trade(trade)
        assert item.trade_type == "SALE"

    def test_exchange_type(self):
        scraper = CapitolTradesScraper()
        trade = {**SAMPLE_TRADE, "txType": "exchange"}
        item = scraper._parse_trade(trade)
        assert item.trade_type == "EXCHANGE"

    def test_ticker_strips_suffix(self):
        scraper = CapitolTradesScraper()
        trade = {**SAMPLE_TRADE}
        trade["issuer"] = {**SAMPLE_TRADE["issuer"], "issuerTicker": "AAPL:US"}
        item = scraper._parse_trade(trade)
        assert item.ticker == "AAPL"

    def test_missing_politician_returns_none(self):
        scraper = CapitolTradesScraper()
        trade = {**SAMPLE_TRADE, "politician": {}}
        item = scraper._parse_trade(trade)
        assert item is None

    def test_missing_tx_date_returns_none(self):
        scraper = CapitolTradesScraper()
        trade = {**SAMPLE_TRADE, "txDate": None}
        item = scraper._parse_trade(trade)
        assert item is None

    def test_house_chamber(self):
        scraper = CapitolTradesScraper()
        trade = {**SAMPLE_TRADE, "chamber": "house"}
        item = scraper._parse_trade(trade)
        assert item.chamber == "HOUSE"

    def test_source_url(self):
        scraper = CapitolTradesScraper()
        item = scraper._parse_trade(SAMPLE_TRADE)
        assert "99999" in item.source_url


class TestAmountRangeMapping:
    def test_known_ranges(self):
        assert CapitolTradesScraper._parse_amount(8000) == (1_001, 15_000)
        assert CapitolTradesScraper._parse_amount(32500) == (15_001, 50_000)
        assert CapitolTradesScraper._parse_amount(75000) == (50_001, 100_000)
        assert CapitolTradesScraper._parse_amount(175000) == (100_001, 250_000)
        assert CapitolTradesScraper._parse_amount(375000) == (250_001, 500_000)
        assert CapitolTradesScraper._parse_amount(750000) == (500_001, 1_000_000)
        assert CapitolTradesScraper._parse_amount(5000000) == (1_000_001, 50_000_000)

    def test_none_value(self):
        assert CapitolTradesScraper._parse_amount(None) == (None, None)

    def test_unknown_value_uses_as_is(self):
        assert CapitolTradesScraper._parse_amount(999) == (999.0, 999.0)


class TestExtractTrades:
    def test_extracts_from_rsc_payload(self):
        scraper = CapitolTradesScraper()
        rsc = _build_rsc_payload([SAMPLE_TRADE])
        trades = scraper._extract_trades(rsc)

        assert len(trades) == 1
        assert trades[0].member_name == "Jane Smith"

    def test_empty_data_array(self):
        scraper = CapitolTradesScraper()
        rsc = _build_rsc_payload([])
        trades = scraper._extract_trades(rsc)
        assert trades == []

    def test_no_data_field(self):
        scraper = CapitolTradesScraper()
        trades = scraper._extract_trades("random text with no data")
        assert trades == []

    def test_malformed_json(self):
        scraper = CapitolTradesScraper()
        rsc = '"data":[{broken json'
        trades = scraper._extract_trades(rsc)
        assert trades == []

    def test_bad_trade_skipped(self):
        scraper = CapitolTradesScraper()
        bad_trade = {**SAMPLE_TRADE, "politician": None}
        rsc = _build_rsc_payload([SAMPLE_TRADE, bad_trade])
        trades = scraper._extract_trades(rsc)
        assert len(trades) == 1


class TestExtractTotalPages:
    def test_finds_total_pages(self):
        scraper = CapitolTradesScraper()
        rsc = '{"totalCount":120,"totalPages":10}'
        assert scraper._extract_total_pages(rsc) == 10

    def test_missing_returns_1(self):
        scraper = CapitolTradesScraper()
        assert scraper._extract_total_pages("no pages here") == 1


class TestSectorNormalize:
    def test_all_sectors_map_to_known_values(self):
        known_sectors = {
            "Consumer", "Energy", "Finance", "Healthcare", "Industrial",
            "Materials", "Real Estate", "Technology", "Telecom", "Utilities",
            "Defense",
        }
        for slug, sector in SECTOR_NORMALIZE.items():
            assert sector in known_sectors, f"{slug} -> {sector} not in known set"


class TestFetchPage:
    @pytest.mark.asyncio
    async def test_fetch_page(self):
        scraper = CapitolTradesScraper()
        rsc = _build_rsc_payload([SAMPLE_TRADE], total_pages=5)
        mock_response = make_httpx_response(text=rsc)

        with patch.object(scraper, "_fetch", new_callable=AsyncMock, return_value=mock_response):
            trades, total_pages = await scraper._fetch_page(1)

        assert len(trades) == 1
        assert total_pages == 5
        await scraper.close()


class TestScrape:
    @pytest.mark.asyncio
    async def test_stops_at_cutoff(self):
        scraper = CapitolTradesScraper(lookback_days=30)

        old_trade = {**SAMPLE_TRADE, "pubDate": "2020-01-01T00:00:00Z"}
        page1_rsc = _build_rsc_payload([SAMPLE_TRADE], total_pages=100)
        page2_rsc = _build_rsc_payload([old_trade], total_pages=100)

        responses = [
            make_httpx_response(text=page1_rsc),
            make_httpx_response(text=page2_rsc),
        ]
        call_count = 0

        async def mock_fetch(url, **kwargs):
            nonlocal call_count
            resp = responses[min(call_count, len(responses) - 1)]
            call_count += 1
            return resp

        with patch.object(scraper, "_fetch", side_effect=mock_fetch):
            trades = await scraper.scrape()

        assert call_count == 2  # stopped after page 2
        await scraper.close()

    @pytest.mark.asyncio
    async def test_stops_on_empty_page(self):
        scraper = CapitolTradesScraper(lookback_days=30)
        empty_rsc = _build_rsc_payload([], total_pages=1)

        with patch.object(
            scraper, "_fetch",
            new_callable=AsyncMock,
            return_value=make_httpx_response(text=empty_rsc),
        ):
            trades = await scraper.scrape()

        assert trades == []
        await scraper.close()

    @pytest.mark.asyncio
    async def test_handles_fetch_error(self):
        scraper = CapitolTradesScraper(lookback_days=30)

        with patch.object(
            scraper, "_fetch",
            new_callable=AsyncMock,
            side_effect=RuntimeError("connection failed"),
        ):
            trades = await scraper.scrape()

        assert trades == []
        await scraper.close()
