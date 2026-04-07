"""One-time backfill script for Capitol Trades congressional trade data.

Walks every page on capitoltrades.com (ignoring the lookback cutoff) and
stores all historical congressional trades. Safe to re-run -- the pipeline's
dedup logic skips existing records.

Usage:
    python scripts/backfill_capitol_trades.py
    python scripts/backfill_capitol_trades.py --start-page 500
    python scripts/backfill_capitol_trades.py --max-pages 100

The scraper uses a 1s rate limit delay, so a full run of ~3,000 pages takes
roughly 50 minutes.
"""

import argparse
import asyncio
import logging
import sys

from fathom.database import async_session, engine
from fathom.models import Base
from fathom.engine.pipeline import _store_congressional_trade
from fathom.scrapers.capitol_trades import CapitolTradesScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")


async def backfill(start_page: int = 1, max_pages: int | None = None) -> None:
    """Paginate through all Capitol Trades pages and store every trade."""
    # Ensure schema exists
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    scraper = CapitolTradesScraper(lookback_days=0)
    page = start_page
    total_scraped = 0
    total_new = 0
    total_skipped = 0

    try:
        # First fetch to discover total_pages
        trades, total_pages = await scraper._fetch_page(page)
        end_page = total_pages
        if max_pages is not None:
            end_page = min(end_page, start_page + max_pages - 1)

        logger.info(
            f"Starting backfill: pages {start_page}-{end_page} "
            f"(total available: {total_pages})"
        )

        while page <= end_page:
            if page != start_page:
                try:
                    trades, _ = await scraper._fetch_page(page)
                except Exception as e:
                    logger.error(f"Page {page} failed: {e}. Skipping.")
                    page += 1
                    continue

            if not trades:
                logger.warning(f"Page {page} returned no trades")
                page += 1
                continue

            page_new = 0
            page_skipped = 0
            async with async_session() as session:
                for item in trades:
                    if not item.trade_date:
                        continue
                    is_new = await _store_congressional_trade(session, item)
                    if is_new:
                        page_new += 1
                    else:
                        page_skipped += 1
                await session.commit()

            total_scraped += len(trades)
            total_new += page_new
            total_skipped += page_skipped

            if page % 10 == 0 or page == end_page:
                pct = (page - start_page + 1) / (end_page - start_page + 1) * 100
                logger.info(
                    f"Page {page}/{end_page} ({pct:.1f}%) | "
                    f"this page: +{page_new} new, {page_skipped} dup | "
                    f"total: +{total_new} new, {total_skipped} dup"
                )

            page += 1

    finally:
        await scraper.close()

    logger.info(
        f"Backfill complete: {total_scraped} trades scraped, "
        f"{total_new} new, {total_skipped} duplicates skipped"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--start-page", type=int, default=1, help="Page to start from (default: 1)"
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximum pages to scrape (default: all)",
    )
    args = parser.parse_args()

    asyncio.run(backfill(start_page=args.start_page, max_pages=args.max_pages))
    return 0


if __name__ == "__main__":
    sys.exit(main())
