"""Tests for the data pipeline."""

import pytest
from datetime import date, datetime
from unittest.mock import AsyncMock, patch, MagicMock

from sqlalchemy import select

from fathom.engine.pipeline import (
    resolve_sector,
    SECTOR_MAP,
    _store_insider_trade,
    run_edgar_pipeline,
    _fetch_and_store_prices,
)
from fathom.models.insider_trade import InsiderTrade
from fathom.models.stock_price import StockPrice
from fathom.scrapers.edgar import InsiderTradeItem
from fathom.scrapers.stock_prices import StockPriceItem


class TestResolveSector:
    async def test_known_ticker(self, db_session):
        assert await resolve_sector(db_session, "AAPL") == "Technology"
        assert await resolve_sector(db_session, "JPM") == "Finance"
        assert await resolve_sector(db_session, "XOM") == "Energy"

    async def test_unknown_ticker_returns_none(self, db_session):
        with patch("fathom.engine.pipeline._lookup_sector_yfinance", new_callable=AsyncMock, return_value=None):
            result = await resolve_sector(db_session, "ZZZZ")
        assert result is None

    async def test_none_ticker(self, db_session):
        assert await resolve_sector(db_session, None) is None

    async def test_case_insensitive(self, db_session):
        assert await resolve_sector(db_session, "aapl") == "Technology"
        assert await resolve_sector(db_session, "Aapl") == "Technology"

    async def test_new_sectors_exist(self, db_session):
        """Verify newly added sectors are present in the hardcoded map."""
        assert await resolve_sector(db_session, "CSCO") == "Technology"
        assert await resolve_sector(db_session, "MRVL") == "Semiconductors"
        assert await resolve_sector(db_session, "T") == "Telecom"
        assert await resolve_sector(db_session, "BMY") == "Healthcare"
        assert await resolve_sector(db_session, "SBUX") == "Consumer"
        assert await resolve_sector(db_session, "AXP") == "Finance"


class TestSectorMapConsistency:
    def test_all_tracked_ciks_have_sectors(self):
        """Every ticker in TRACKED_CIKS should have a sector mapping."""
        from fathom.scrapers.edgar import TRACKED_CIKS

        unmapped = []
        for ticker in TRACKED_CIKS.values():
            if ticker not in SECTOR_MAP:
                unmapped.append(ticker)
        assert unmapped == [], f"Tickers missing from SECTOR_MAP: {unmapped}"


class TestStoreInsiderTrade:
    async def test_new_trade(self, db_session):
        """New trade should be stored and return True."""
        item = InsiderTradeItem(
            source="edgar",
            cik="320193",
            filer_name="Tim Cook",
            filer_title="CEO",
            company_name="Apple Inc.",
            ticker="AAPL",
            trade_type="SELL",
            shares=50000,
            price_per_share=185.50,
            total_value=9275000.0,
            trade_date=date(2026, 3, 25),
            filing_date=datetime(2026, 3, 28),
            filing_url="https://sec.gov/test.xml",
        )

        result = await _store_insider_trade(db_session, item)
        assert result is True

        # Verify it was actually stored
        rows = await db_session.execute(select(InsiderTrade))
        trades = rows.scalars().all()
        assert len(trades) == 1
        assert trades[0].ticker == "AAPL"
        assert trades[0].sector == "Technology"

    async def test_duplicate_trade(self, db_session):
        """Duplicate trade should be skipped and return False."""
        item = InsiderTradeItem(
            source="edgar",
            cik="320193",
            filer_name="Tim Cook",
            filer_title="CEO",
            company_name="Apple Inc.",
            ticker="AAPL",
            trade_type="SELL",
            shares=50000,
            price_per_share=185.50,
            total_value=9275000.0,
            trade_date=date(2026, 3, 25),
            filing_date=datetime(2026, 3, 28),
            filing_url="https://sec.gov/test.xml",
        )

        first = await _store_insider_trade(db_session, item)
        await db_session.flush()
        second = await _store_insider_trade(db_session, item)

        assert first is True
        assert second is False

    async def test_unknown_sector_stored_as_none(self, db_session):
        """Trade with unknown ticker should have sector=None when yfinance returns None."""
        item = InsiderTradeItem(
            source="edgar",
            cik="999999",
            filer_name="Unknown Person",
            filer_title=None,
            company_name="Unknown Corp",
            ticker="ZZZZ",
            trade_type="BUY",
            shares=100,
            price_per_share=10.0,
            total_value=1000.0,
            trade_date=date(2026, 3, 25),
            filing_date=datetime(2026, 3, 28),
            filing_url=None,
        )

        with patch("fathom.engine.pipeline._lookup_sector_yfinance", new_callable=AsyncMock, return_value=None):
            await _store_insider_trade(db_session, item)
        await db_session.flush()

        rows = await db_session.execute(select(InsiderTrade))
        trade = rows.scalars().first()
        assert trade.sector is None


class TestRunEdgarPipeline:
    @patch("fathom.engine.pipeline._fetch_and_store_prices", new_callable=AsyncMock)
    @patch("fathom.engine.pipeline.EdgarScraper")
    async def test_stores_trades(self, MockScraper, mock_prices, db_engine):
        """Pipeline should store scraped trades in the DB."""
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        mock_instance = MagicMock()
        mock_instance.scrape = AsyncMock(return_value=[
            InsiderTradeItem(
                source="edgar",
                cik="320193",
                filer_name="Tim Cook",
                filer_title="CEO",
                company_name="Apple Inc.",
                ticker="AAPL",
                trade_type="SELL",
                shares=50000,
                price_per_share=185.50,
                total_value=9275000.0,
                trade_date=date(2026, 3, 25),
                filing_date=datetime(2026, 3, 28),
                filing_url="https://sec.gov/test.xml",
            )
        ])
        mock_instance.close = AsyncMock()
        MockScraper.return_value = mock_instance

        test_session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
        with patch("fathom.engine.pipeline.async_session", test_session_factory):
            result = await run_edgar_pipeline()

        assert result == 1
        mock_prices.assert_called_once()

    @patch("fathom.engine.pipeline.EdgarScraper")
    async def test_no_items_returns_zero(self, MockScraper):
        """Empty scrape should return 0."""
        mock_instance = MagicMock()
        mock_instance.scrape = AsyncMock(return_value=[])
        mock_instance.close = AsyncMock()
        MockScraper.return_value = mock_instance

        result = await run_edgar_pipeline()
        assert result == 0


class TestFetchAndStorePrices:
    async def test_dedup(self, db_engine):
        """Should not store duplicate prices."""
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

        session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

        # Pre-insert a price
        async with session_factory() as session:
            session.add(StockPrice(
                ticker="AAPL",
                date=date(2026, 3, 28),
                open=150.0,
                high=155.0,
                low=149.0,
                close=154.0,
                adj_close=154.0,
                volume=1000000,
            ))
            await session.commit()

        items = [
            StockPriceItem(
                source="yfinance",
                ticker="AAPL",
                date=date(2026, 3, 28),  # duplicate
                open=150.0, high=155.0, low=149.0,
                close=154.0, adj_close=154.0, volume=1000000,
            ),
            StockPriceItem(
                source="yfinance",
                ticker="AAPL",
                date=date(2026, 3, 31),  # new
                open=155.0, high=160.0, low=154.0,
                close=158.0, adj_close=158.0, volume=1100000,
            ),
        ]

        with patch("fathom.engine.pipeline.fetch_stock_prices", return_value=items):
            with patch("fathom.engine.pipeline.async_session", session_factory):
                await _fetch_and_store_prices(["AAPL"])

        async with session_factory() as session:
            rows = await session.execute(select(StockPrice))
            prices = rows.scalars().all()
            assert len(prices) == 2  # original + 1 new, not 3
