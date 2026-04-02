"""Data pipeline: scrape → normalize → store.

Phase 1 focuses on EDGAR insider trades and stock prices.
Correlation engine will be added in Phase 2.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from fathom.database import async_session
from fathom.models.insider_trade import InsiderTrade
from fathom.models.stock_price import StockPrice
from fathom.scrapers.edgar import EdgarScraper, InsiderTradeItem
from fathom.scrapers.stock_prices import StockPriceItem, fetch_stock_prices

logger = logging.getLogger(__name__)

SECTOR_MAP = {
    # Energy
    "XOM": "Energy", "CVX": "Energy", "OXY": "Energy", "COP": "Energy",
    "SLB": "Energy", "EOG": "Energy", "MPC": "Energy", "PSX": "Energy",
    "VLO": "Energy", "HAL": "Energy",
    # Defense
    "LMT": "Defense", "RTX": "Defense", "NOC": "Defense", "GD": "Defense",
    "BA": "Defense", "LHX": "Defense", "HII": "Defense",
    # Technology
    "AAPL": "Technology", "MSFT": "Technology", "GOOGL": "Technology",
    "AMZN": "Technology", "META": "Technology", "NVDA": "Technology",
    "AMD": "Technology", "INTC": "Technology", "CRM": "Technology",
    "ORCL": "Technology", "ADBE": "Technology", "AVGO": "Technology",
    "CSCO": "Technology", "QCOM": "Technology", "TXN": "Technology",
    "IBM": "Technology", "AMAT": "Technology", "MU": "Technology",
    "NOW": "Technology", "PANW": "Technology", "SNPS": "Technology",
    "CDNS": "Technology",
    # Semiconductors
    "MRVL": "Semiconductors", "KLAC": "Semiconductors", "LRCX": "Semiconductors",
    # Finance
    "JPM": "Finance", "BAC": "Finance", "WFC": "Finance", "GS": "Finance",
    "MS": "Finance", "C": "Finance", "BLK": "Finance", "SCHW": "Finance",
    "AXP": "Finance", "USB": "Finance", "PNC": "Finance", "TFC": "Finance",
    "CME": "Finance",
    # Healthcare
    "JNJ": "Healthcare", "UNH": "Healthcare", "PFE": "Healthcare",
    "ABBV": "Healthcare", "MRK": "Healthcare", "LLY": "Healthcare",
    "TMO": "Healthcare", "ABT": "Healthcare", "BMY": "Healthcare",
    "AMGN": "Healthcare", "GILD": "Healthcare", "MDT": "Healthcare",
    "ISRG": "Healthcare", "SYK": "Healthcare",
    # Consumer
    "WMT": "Consumer", "PG": "Consumer", "KO": "Consumer", "PEP": "Consumer",
    "COST": "Consumer", "HD": "Consumer", "MCD": "Consumer", "NKE": "Consumer",
    "SBUX": "Consumer", "TGT": "Consumer", "LOW": "Consumer", "TJX": "Consumer",
    "BKNG": "Consumer",
    # Telecom
    "T": "Telecom", "VZ": "Telecom", "TMUS": "Telecom",
    # Industrial
    "CAT": "Industrial", "DE": "Industrial", "UPS": "Industrial",
    "HON": "Industrial", "GE": "Industrial", "MMM": "Industrial",
    # Real Estate
    "AMT": "Real Estate", "PLD": "Real Estate", "CCI": "Real Estate",
    "SPG": "Real Estate", "O": "Real Estate",
    # Utilities
    "NEE": "Utilities", "DUK": "Utilities", "SO": "Utilities",
    "D": "Utilities", "AEP": "Utilities",
}


def resolve_sector(ticker: str | None) -> str | None:
    if not ticker:
        return None
    return SECTOR_MAP.get(ticker.upper())


async def run_edgar_pipeline():
    """Scrape EDGAR Form 4 filings and store insider trades."""
    scraper = EdgarScraper()

    try:
        items = await scraper.scrape()
        if not items:
            logger.info("No new insider trades found")
            return 0

        new_count = 0
        async with async_session() as session:
            for item in items:
                new = await _store_insider_trade(session, item)
                if new:
                    new_count += 1
            await session.commit()

        logger.info(f"Stored {new_count} new insider trades (of {len(items)} scraped)")

        # Fetch stock prices for any new tickers
        tickers = list({item.ticker for item in items if item.ticker})
        if tickers:
            await _fetch_and_store_prices(tickers)

        return new_count

    finally:
        await scraper.close()


async def _store_insider_trade(session: AsyncSession, item: InsiderTradeItem) -> bool:
    """Store an insider trade, returning True if it was new."""
    sector = resolve_sector(item.ticker)

    # Check for existing record
    existing = await session.execute(
        select(InsiderTrade).where(
            InsiderTrade.cik == item.cik,
            InsiderTrade.ticker == item.ticker,
            InsiderTrade.trade_date == item.trade_date,
            InsiderTrade.trade_type == item.trade_type,
            InsiderTrade.shares == item.shares,
        )
    )
    if existing.scalar_one_or_none():
        return False

    trade = InsiderTrade(
        cik=item.cik,
        filer_name=item.filer_name,
        filer_title=item.filer_title,
        company_name=item.company_name,
        ticker=item.ticker,
        trade_type=item.trade_type,
        shares=item.shares,
        price_per_share=item.price_per_share,
        total_value=item.total_value,
        trade_date=item.trade_date,
        filing_date=item.filing_date,
        filing_url=item.filing_url,
        sector=sector,
    )
    session.add(trade)
    return True


async def _fetch_and_store_prices(tickers: list[str]):
    """Fetch and store stock prices for given tickers."""
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        items = await loop.run_in_executor(pool, fetch_stock_prices, tickers)

    async with async_session() as session:
        for item in items:
            existing = await session.execute(
                select(StockPrice).where(
                    StockPrice.ticker == item.ticker,
                    StockPrice.date == item.date,
                )
            )
            if existing.scalar_one_or_none():
                continue

            price = StockPrice(
                ticker=item.ticker,
                date=item.date,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                adj_close=item.adj_close,
                volume=item.volume,
            )
            session.add(price)
        await session.commit()

    logger.info(f"Stored prices for {len(tickers)} tickers")
