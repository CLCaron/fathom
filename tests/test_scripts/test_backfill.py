"""Tests for the historical backfill scripts.

The scripts live outside the src/ package, so we load them via importlib.
We mock the scrapers and database so tests stay fast and hermetic.
"""

import importlib.util
import sys
from datetime import date, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fathom.models import Base
from fathom.scrapers.capitol_trades import CongressionalTradeItem
from fathom.scrapers.edgar import InsiderTradeItem
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


SCRIPTS_DIR = Path(__file__).parent.parent.parent / "scripts"


def _load_script(name: str):
    """Load a script module from scripts/ by filename."""
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
async def temp_engine():
    """Create an in-memory DB and patch the backfill modules' engine/session."""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    yield engine, factory
    await engine.dispose()


class TestCapitolTradesBackfill:
    async def test_paginates_and_stores(self, temp_engine):
        """backfill() should paginate through all pages and store trades."""
        engine, factory = temp_engine
        module = _load_script("backfill_capitol_trades")

        # Build 2 pages of 2 trades each
        def make_trade(txid: str, tdate: date, ddate: date) -> CongressionalTradeItem:
            return CongressionalTradeItem(
                member_name=f"Member {txid}",
                chamber="HOUSE",
                state="CA",
                party="Democrat",
                ticker="AAPL",
                asset_name="Apple Inc.",
                trade_type="PURCHASE",
                amount_min=1001,
                amount_max=15000,
                trade_date=tdate,
                disclosure_date=ddate,
                source_url=f"https://capitoltrades.com/trades?txId={txid}",
                sector="Technology",
            )

        pages = {
            1: (
                [
                    make_trade("1", date(2025, 1, 15), date(2025, 2, 1)),
                    make_trade("2", date(2025, 1, 10), date(2025, 1, 28)),
                ],
                2,  # total_pages
            ),
            2: (
                [
                    make_trade("3", date(2024, 12, 20), date(2025, 1, 5)),
                    make_trade("4", date(2024, 12, 15), date(2025, 1, 2)),
                ],
                2,
            ),
        }

        async def fake_fetch_page(self, page):
            return pages[page]

        with patch.object(module, "engine", engine), \
             patch.object(module, "async_session", factory), \
             patch(
                 "fathom.scrapers.capitol_trades.CapitolTradesScraper._fetch_page",
                 fake_fetch_page,
             ):
            await module.backfill(start_page=1, max_pages=None)

        # Verify all 4 trades were stored
        from fathom.models.congressional_trade import CongressionalTrade
        from sqlalchemy import select, func

        async with factory() as session:
            count = await session.execute(select(func.count(CongressionalTrade.id)))
            assert count.scalar() == 4

    async def test_idempotent_on_rerun(self, temp_engine):
        """Running the backfill twice should not create duplicates."""
        engine, factory = temp_engine
        module = _load_script("backfill_capitol_trades")

        trade = CongressionalTradeItem(
            member_name="Jane Doe",
            chamber="SENATE",
            state="NY",
            party="Republican",
            ticker="MSFT",
            asset_name="Microsoft",
            trade_type="SALE",
            amount_min=15001,
            amount_max=50000,
            trade_date=date(2025, 3, 1),
            disclosure_date=date(2025, 3, 15),
            source_url="https://capitoltrades.com/trades?txId=abc",
            sector="Technology",
        )

        async def fake_fetch_page(self, page):
            return ([trade], 1)

        with patch.object(module, "engine", engine), \
             patch.object(module, "async_session", factory), \
             patch(
                 "fathom.scrapers.capitol_trades.CapitolTradesScraper._fetch_page",
                 fake_fetch_page,
             ):
            await module.backfill(start_page=1, max_pages=1)
            await module.backfill(start_page=1, max_pages=1)

        from fathom.models.congressional_trade import CongressionalTrade
        from sqlalchemy import select, func

        async with factory() as session:
            count = await session.execute(select(func.count(CongressionalTrade.id)))
            assert count.scalar() == 1

    async def test_max_pages_limits_scraping(self, temp_engine):
        """--max-pages should cap how many pages are scraped."""
        engine, factory = temp_engine
        module = _load_script("backfill_capitol_trades")

        call_log = []

        async def fake_fetch_page(self, page):
            call_log.append(page)
            trade = CongressionalTradeItem(
                member_name=f"Member {page}",
                chamber="HOUSE",
                trade_type="PURCHASE",
                trade_date=date(2025, 1, 1),
                disclosure_date=date(2025, 1, 10),
            )
            return ([trade], 100)  # pretend 100 total pages exist

        with patch.object(module, "engine", engine), \
             patch.object(module, "async_session", factory), \
             patch(
                 "fathom.scrapers.capitol_trades.CapitolTradesScraper._fetch_page",
                 fake_fetch_page,
             ):
            await module.backfill(start_page=1, max_pages=3)

        assert call_log == [1, 2, 3]


class TestEdgarBackfill:
    async def test_backfills_single_cik(self, temp_engine):
        """backfill() should fetch filings and store parsed insider trades."""
        engine, factory = temp_engine
        module = _load_script("backfill_edgar")

        filings = [
            {
                "xml_url": "https://www.sec.gov/test1.xml",
                "filing_date": "2024-06-15",
                "accession": "0000320193-24-000001",
                "cik": "320193",
            },
        ]

        trade_item = InsiderTradeItem(
            source="edgar",
            cik="0001234567",
            filer_name="John Doe",
            filer_title="CEO",
            company_name="Apple Inc.",
            ticker="AAPL",
            trade_type="BUY",
            shares=1000,
            price_per_share=180.0,
            total_value=180000.0,
            trade_date=date(2024, 6, 10),
            filing_date=datetime(2024, 6, 15),
            filing_url="https://www.sec.gov/test1.xml",
        )

        with patch.object(module, "engine", engine), \
             patch.object(module, "async_session", factory), \
             patch("fathom.scrapers.edgar.EdgarScraper._load_cik_ticker_map", new_callable=AsyncMock), \
             patch(
                 "fathom.scrapers.edgar.EdgarScraper.get_form4_filings_since",
                 new_callable=AsyncMock,
                 return_value=filings,
             ), \
             patch(
                 "fathom.scrapers.edgar.EdgarScraper._fetch_and_parse_form4",
                 new_callable=AsyncMock,
                 return_value=[trade_item],
             ):
            await module.backfill(
                ciks={"320193": "AAPL"},
                since=date(2020, 1, 1),
                include_archive=True,
            )

        from fathom.models.insider_trade import InsiderTrade
        from sqlalchemy import select, func

        async with factory() as session:
            count = await session.execute(select(func.count(InsiderTrade.id)))
            assert count.scalar() == 1

    async def test_idempotent_on_rerun(self, temp_engine):
        """Running the EDGAR backfill twice should not create duplicates."""
        engine, factory = temp_engine
        module = _load_script("backfill_edgar")

        filings = [{"xml_url": "x", "filing_date": "2024-01-01", "accession": "a", "cik": "320193"}]
        trade_item = InsiderTradeItem(
            source="edgar",
            cik="0001234567",
            filer_name="Jane Doe",
            filer_title=None,
            company_name="Apple",
            ticker="AAPL",
            trade_type="SELL",
            shares=500,
            price_per_share=200.0,
            total_value=100000.0,
            trade_date=date(2024, 1, 1),
            filing_date=datetime(2024, 1, 2),
            filing_url="x",
        )

        with patch.object(module, "engine", engine), \
             patch.object(module, "async_session", factory), \
             patch("fathom.scrapers.edgar.EdgarScraper._load_cik_ticker_map", new_callable=AsyncMock), \
             patch(
                 "fathom.scrapers.edgar.EdgarScraper.get_form4_filings_since",
                 new_callable=AsyncMock,
                 return_value=filings,
             ), \
             patch(
                 "fathom.scrapers.edgar.EdgarScraper._fetch_and_parse_form4",
                 new_callable=AsyncMock,
                 return_value=[trade_item],
             ):
            await module.backfill(ciks={"320193": "AAPL"}, since=date(2020, 1, 1))
            await module.backfill(ciks={"320193": "AAPL"}, since=date(2020, 1, 1))

        from fathom.models.insider_trade import InsiderTrade
        from sqlalchemy import select, func

        async with factory() as session:
            count = await session.execute(select(func.count(InsiderTrade.id)))
            assert count.scalar() == 1

    async def test_fetch_failure_continues_to_next_cik(self, temp_engine):
        """A failing CIK should not halt backfill of the rest."""
        engine, factory = temp_engine
        module = _load_script("backfill_edgar")

        async def fake_get_filings(self, cik, since, include_archive=True):
            if cik == "bad":
                raise Exception("simulated failure")
            return [
                {"xml_url": f"https://test/{cik}.xml", "filing_date": "2024-01-01",
                 "accession": "a", "cik": cik}
            ]

        trade_item = InsiderTradeItem(
            source="edgar",
            cik="0001234567",
            filer_name="Alice",
            filer_title=None,
            company_name="Good Co",
            ticker="GOOD",
            trade_type="BUY",
            shares=100,
            price_per_share=50.0,
            total_value=5000.0,
            trade_date=date(2024, 1, 1),
            filing_date=datetime(2024, 1, 2),
            filing_url="https://test/good.xml",
        )

        with patch.object(module, "engine", engine), \
             patch.object(module, "async_session", factory), \
             patch("fathom.scrapers.edgar.EdgarScraper._load_cik_ticker_map", new_callable=AsyncMock), \
             patch(
                 "fathom.scrapers.edgar.EdgarScraper.get_form4_filings_since",
                 fake_get_filings,
             ), \
             patch(
                 "fathom.scrapers.edgar.EdgarScraper._fetch_and_parse_form4",
                 new_callable=AsyncMock,
                 return_value=[trade_item],
             ):
            await module.backfill(
                ciks={"bad": "BAD", "good": "GOOD"},
                since=date(2020, 1, 1),
            )

        from fathom.models.insider_trade import InsiderTrade
        from sqlalchemy import select, func

        async with factory() as session:
            count = await session.execute(select(func.count(InsiderTrade.id)))
            # Only the good CIK's trade was stored
            assert count.scalar() == 1
