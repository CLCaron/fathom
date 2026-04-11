"""Data pipeline: scrape -> normalize -> store -> correlate."""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta

import yfinance as yf
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from fathom.config import settings
from fathom.database import async_session
from fathom.engine.correlator import (
    SignalCandidate,
    find_committee_overlap_signals,
    find_legislation_timing_signals,
    merge_candidates,
)
from fathom.models.committee_membership import CommitteeMembership
from fathom.models.congressional_trade import CongressionalTrade
from fathom.models.insider_trade import InsiderTrade
from fathom.models.legislation import Legislation, LegislationVote
from fathom.models.sector_cache import SectorCache
from fathom.models.signal import Signal
from fathom.models.stock_price import StockPrice
from fathom.scrapers.capitol_trades import CapitolTradesScraper, CongressionalTradeItem
from fathom.scrapers.committees import CommitteeScraper, CommitteeMembershipItem
from fathom.scrapers.edgar import EdgarScraper, InsiderTradeItem
from fathom.scrapers.legislation import BillItem, LegislationScraper, VoteItem
from fathom.scrapers.stock_prices import StockPriceItem, fetch_stock_prices

logger = logging.getLogger(__name__)

# Canonicalize yfinance sector names to our internal vocabulary.
_SECTOR_ALIASES: dict[str, str] = {
    "Financial Services": "Finance",
    "Basic Materials": "Materials",
    "Communication Services": "Telecom",
    "Consumer Cyclical": "Consumer",
    "Consumer Defensive": "Consumer",
    "Industrials": "Industrial",
}


def canonicalize_sector(value: str | None) -> str | None:
    """Map yfinance sector names to our canonical vocabulary."""
    if not value:
        return None
    return _SECTOR_ALIASES.get(value, value)


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


async def resolve_sector(session: AsyncSession, ticker: str | None) -> str | None:
    """Resolve a ticker's sector using hardcoded map, cache, or yfinance fallback."""
    if not ticker:
        return None

    upper = ticker.upper()

    # Fast path: hardcoded map
    if upper in SECTOR_MAP:
        return SECTOR_MAP[upper]

    # Check cache
    result = await session.execute(
        select(SectorCache).where(SectorCache.ticker == upper)
    )
    cached = result.scalar_one_or_none()

    if cached:
        ttl = timedelta(days=settings.sector_cache_ttl_days)
        if datetime.utcnow() - cached.fetched_at < ttl:
            return cached.sector

    # yfinance fallback
    sector = await _lookup_sector_yfinance(upper)

    # Upsert into cache
    if cached:
        cached.sector = sector
        cached.source = "yfinance"
        cached.fetched_at = datetime.utcnow()
    else:
        session.add(SectorCache(
            ticker=upper,
            sector=sector,
            source="yfinance",
            fetched_at=datetime.utcnow(),
        ))

    return sector


async def _lookup_sector_yfinance(ticker: str) -> str | None:
    """Look up a ticker's sector via yfinance (runs in thread pool)."""
    loop = asyncio.get_event_loop()

    def _fetch():
        try:
            info = yf.Ticker(ticker).info
            return info.get("sector")
        except Exception:
            logger.debug(f"yfinance sector lookup failed for {ticker}")
            return None

    with ThreadPoolExecutor() as pool:
        raw = await loop.run_in_executor(pool, _fetch)
    return canonicalize_sector(raw)


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
    sector = await resolve_sector(session, item.ticker)

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
        items = await loop.run_in_executor(
            pool, fetch_stock_prices, tickers, settings.price_lookback_days
        )

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


async def run_congressional_pipeline():
    """Scrape Capitol Trades, store trades, then run correlation engine."""
    scraper = CapitolTradesScraper(lookback_days=settings.dashboard_max_days)

    try:
        items = await scraper.scrape()
        if not items:
            logger.info("No new congressional trades found")
            # Still run correlation -- new committee/legislation data may have arrived
            await run_correlation_pipeline()
            return 0

        new_count = 0
        async with async_session() as session:
            for item in items:
                new = await _store_congressional_trade(session, item)
                if new:
                    new_count += 1
            await session.commit()

        logger.info(
            f"Stored {new_count} new congressional trades (of {len(items)} scraped)"
        )

        # Run correlation engine after storing trades
        await run_correlation_pipeline()

        return new_count

    finally:
        await scraper.close()


async def _store_congressional_trade(
    session: AsyncSession, item: CongressionalTradeItem
) -> bool:
    """Store a congressional trade, returning True if it was new."""
    # Use Capitol Trades sector if available, fall back to resolve_sector
    sector = item.sector
    if not sector and item.ticker:
        sector = await resolve_sector(session, item.ticker)

    # Check for existing record (matches unique constraint)
    existing = await session.execute(
        select(CongressionalTrade).where(
            CongressionalTrade.member_name == item.member_name,
            CongressionalTrade.ticker == item.ticker,
            CongressionalTrade.trade_date == item.trade_date,
            CongressionalTrade.trade_type == item.trade_type,
            CongressionalTrade.amount_min == item.amount_min,
        )
    )
    if existing.scalar_one_or_none():
        return False

    trade = CongressionalTrade(
        member_name=item.member_name,
        chamber=item.chamber,
        state=item.state,
        party=item.party,
        ticker=item.ticker,
        asset_name=item.asset_name,
        trade_type=item.trade_type,
        amount_min=item.amount_min,
        amount_max=item.amount_max,
        trade_date=item.trade_date,
        disclosure_date=item.disclosure_date,
        source_url=item.source_url,
        sector=sector,
    )
    session.add(trade)
    return True


async def run_committees_pipeline():
    """Scrape congress.gov and store/update committee memberships."""
    scraper = CommitteeScraper()

    try:
        items = await scraper.scrape()
        if not items:
            logger.info("No committee memberships found (API key missing?)")
            return 0

        new_count = 0
        updated_count = 0
        async with async_session() as session:
            for item in items:
                is_new, is_updated = await _upsert_committee_membership(session, item)
                if is_new:
                    new_count += 1
                if is_updated:
                    updated_count += 1
            await session.commit()

        logger.info(
            f"Committees: {new_count} new, {updated_count} updated "
            f"(of {len(items)} scraped)"
        )
        return new_count + updated_count

    finally:
        await scraper.close()


async def _upsert_committee_membership(
    session: AsyncSession, item: CommitteeMembershipItem
) -> tuple[bool, bool]:
    """Upsert a committee membership. Returns (is_new, is_updated)."""
    existing = await session.execute(
        select(CommitteeMembership).where(
            CommitteeMembership.member_name == item.member_name,
            CommitteeMembership.committee_code == item.committee_code,
            CommitteeMembership.congress_number == item.congress_number,
        )
    )
    record = existing.scalar_one_or_none()

    if record:
        changed = False
        if record.role != item.role:
            record.role = item.role
            changed = True
        if record.sectors_covered != item.sectors_covered:
            record.sectors_covered = item.sectors_covered
            changed = True
        return False, changed

    membership = CommitteeMembership(
        member_name=item.member_name,
        chamber=item.chamber,
        committee_code=item.committee_code,
        committee_name=item.committee_name,
        role=item.role,
        congress_number=item.congress_number,
        sectors_covered=item.sectors_covered,
    )
    session.add(membership)
    return True, False


async def run_legislation_pipeline():
    """Scrape congress.gov and store bills and votes."""
    scraper = LegislationScraper()

    try:
        bills, votes = await scraper.scrape()
        if not bills and not votes:
            logger.info("No legislation data found (API key missing?)")
            return 0

        bill_count = 0
        vote_count = 0
        async with async_session() as session:
            for bill in bills:
                is_new = await _upsert_bill(session, bill)
                if is_new:
                    bill_count += 1

            for vote in votes:
                is_new = await _store_vote(session, vote)
                if is_new:
                    vote_count += 1

            await session.commit()

        logger.info(f"Legislation: {bill_count} bills, {vote_count} votes stored")
        return bill_count + vote_count

    finally:
        await scraper.close()


async def _upsert_bill(session: AsyncSession, item: BillItem) -> bool:
    """Upsert a bill. Returns True if new or updated."""
    existing = await session.execute(
        select(Legislation).where(Legislation.bill_id == item.bill_id)
    )
    record = existing.scalar_one_or_none()

    if record:
        record.status = item.status
        record.last_action_date = item.last_action_date
        if item.sectors_affected:
            record.sectors_affected = item.sectors_affected
        return False

    bill = Legislation(
        bill_id=item.bill_id,
        title=item.title,
        summary=item.summary,
        congress_number=item.congress_number,
        introduced_date=item.introduced_date,
        last_action_date=item.last_action_date,
        status=item.status,
        sectors_affected=item.sectors_affected,
        sponsor_name=item.sponsor_name,
        bill_url=item.bill_url,
    )
    session.add(bill)
    return True


async def _store_vote(session: AsyncSession, item: VoteItem) -> bool:
    """Store a vote record, returning True if new."""
    existing = await session.execute(
        select(LegislationVote).where(
            LegislationVote.bill_id == item.bill_id,
            LegislationVote.member_name == item.member_name,
            LegislationVote.vote_date == item.vote_date,
        )
    )
    if existing.scalar_one_or_none():
        return False

    # Only store if the bill exists (FK constraint)
    bill_exists = await session.execute(
        select(Legislation.bill_id).where(Legislation.bill_id == item.bill_id)
    )
    if not bill_exists.scalar_one_or_none():
        return False

    vote = LegislationVote(
        bill_id=item.bill_id,
        member_name=item.member_name,
        chamber=item.chamber,
        vote=item.vote,
        vote_date=item.vote_date,
    )
    session.add(vote)
    return True


async def run_correlation_pipeline(lookback_days: int = 90) -> int:
    """Run the correlation engine over recent congressional trades.

    Finds committee-overlap and legislation-timing signals, scores them,
    and stores all candidates (regardless of confidence threshold).
    """
    since = date.today() - timedelta(days=lookback_days)

    async with async_session() as session:
        # Fetch recent congressional trades
        result = await session.execute(
            select(CongressionalTrade)
            .where(CongressionalTrade.trade_date >= since)
            .order_by(desc(CongressionalTrade.trade_date))
        )
        trades = list(result.scalars().all())

        if not trades:
            logger.info("No congressional trades in lookback window")
            return 0

        logger.info(
            f"Running correlation on {len(trades)} trades "
            f"(since {since})"
        )

        # Run both matchers
        committee_signals = await find_committee_overlap_signals(session, trades)
        legislation_signals = await find_legislation_timing_signals(session, trades)

        all_candidates = merge_candidates(committee_signals + legislation_signals)

        # Store signals with dedup
        new_count = 0
        for candidate in all_candidates:
            stored = await _store_signal(session, candidate)
            if stored:
                new_count += 1

        await session.commit()

    logger.info(
        f"Correlation complete: {new_count} new signals stored "
        f"(of {len(all_candidates)} candidates)"
    )
    return new_count


async def _store_signal(session: AsyncSession, candidate: SignalCandidate) -> bool:
    """Store a signal candidate, deduplicating by type+sector+trade+date.

    Returns True if the signal was new and stored.
    """
    # Dedupe: same signal_type + sector + source trade within 24h
    trade_id = candidate.source_trade_ids[0] if candidate.source_trade_ids else None
    today = date.today()

    existing = await session.execute(
        select(Signal).where(
            Signal.signal_type == candidate.signal_type,
            Signal.sector == candidate.sector,
            func.date(Signal.detected_at) == today,
        )
    )
    # Check if any existing signal covers the same source trade
    for sig in existing.scalars().all():
        existing_ids = sig.source_trade_ids or []
        if trade_id and trade_id in existing_ids:
            return False

    signal = Signal(
        signal_type=candidate.signal_type,
        ticker=candidate.ticker,
        sector=candidate.sector,
        headline=candidate.headline,
        confidence=candidate.confidence,
        details={
            **candidate.details,
            "explanation": candidate.explanation,
        },
        source_trade_ids=candidate.source_trade_ids,
        detected_at=datetime.utcnow(),
    )
    session.add(signal)
    return True
