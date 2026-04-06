"""Tests for the congressional trade pipeline."""

from datetime import date
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy import select

from fathom.engine.pipeline import _store_congressional_trade, run_congressional_pipeline
from fathom.models.congressional_trade import CongressionalTrade
from fathom.scrapers.capitol_trades import CongressionalTradeItem


def _make_item(**overrides) -> CongressionalTradeItem:
    defaults = dict(
        source="capitol_trades",
        member_name="Jane Smith",
        chamber="SENATE",
        state="WA",
        party="Democrat",
        ticker="MSFT",
        asset_name="Microsoft Corp",
        trade_type="PURCHASE",
        amount_min=50_001,
        amount_max=100_000,
        trade_date=date(2026, 3, 15),
        disclosure_date=date(2026, 3, 20),
        source_url="https://www.capitoltrades.com/trades?txId=99999",
        sector="Technology",
    )
    defaults.update(overrides)
    return CongressionalTradeItem(**defaults)


class TestStoreCongressionalTrade:
    async def test_stores_new_trade(self, db_session):
        item = _make_item()
        result = await _store_congressional_trade(db_session, item)
        await db_session.commit()

        assert result is True
        trades = (await db_session.execute(select(CongressionalTrade))).scalars().all()
        assert len(trades) == 1
        assert trades[0].member_name == "Jane Smith"
        assert trades[0].ticker == "MSFT"
        assert trades[0].trade_type == "PURCHASE"
        assert trades[0].sector == "Technology"

    async def test_dedup_existing_trade(self, db_session):
        item = _make_item()
        await _store_congressional_trade(db_session, item)
        await db_session.commit()

        # Same trade again
        result = await _store_congressional_trade(db_session, item)
        assert result is False

    async def test_different_trade_stored(self, db_session):
        item1 = _make_item(ticker="MSFT")
        item2 = _make_item(ticker="AAPL")
        await _store_congressional_trade(db_session, item1)
        await _store_congressional_trade(db_session, item2)
        await db_session.commit()

        trades = (await db_session.execute(select(CongressionalTrade))).scalars().all()
        assert len(trades) == 2

    async def test_falls_back_to_resolve_sector(self, db_session):
        item = _make_item(sector=None, ticker="AAPL")
        with patch(
            "fathom.engine.pipeline.resolve_sector",
            new_callable=AsyncMock,
            return_value="Technology",
        ):
            await _store_congressional_trade(db_session, item)
        await db_session.commit()

        trade = (await db_session.execute(select(CongressionalTrade))).scalar_one()
        assert trade.sector == "Technology"

    async def test_uses_item_sector_over_resolve(self, db_session):
        item = _make_item(sector="Finance", ticker="MSFT")
        # resolve_sector should NOT be called when item already has sector
        with patch(
            "fathom.engine.pipeline.resolve_sector",
            new_callable=AsyncMock,
        ) as mock_resolve:
            await _store_congressional_trade(db_session, item)
            mock_resolve.assert_not_called()
        await db_session.commit()

        trade = (await db_session.execute(select(CongressionalTrade))).scalar_one()
        assert trade.sector == "Finance"


class TestRunCongressionalPipeline:
    async def test_full_pipeline(self, db_session):
        items = [_make_item(), _make_item(ticker="AAPL", member_name="John Doe")]

        with patch(
            "fathom.engine.pipeline.CapitolTradesScraper"
        ) as MockScraper:
            mock_instance = AsyncMock()
            mock_instance.scrape.return_value = items
            MockScraper.return_value = mock_instance

            with patch("fathom.engine.pipeline.async_session") as mock_session_factory:
                mock_session_factory.return_value.__aenter__ = AsyncMock(
                    return_value=db_session
                )
                mock_session_factory.return_value.__aexit__ = AsyncMock(
                    return_value=False
                )
                result = await run_congressional_pipeline()

        assert result == 2

    async def test_returns_zero_on_empty(self, db_session):
        with patch(
            "fathom.engine.pipeline.CapitolTradesScraper"
        ) as MockScraper:
            mock_instance = AsyncMock()
            mock_instance.scrape.return_value = []
            MockScraper.return_value = mock_instance

            result = await run_congressional_pipeline()

        assert result == 0
