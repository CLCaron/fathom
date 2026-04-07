"""Historical backfill script for SEC EDGAR insider trades.

For each tracked CIK, walks the submissions API (recent + archive files)
back to `--since` and stores every Form 4 transaction. Safe to re-run --
the pipeline's dedup logic skips existing records.

Usage:
    # Backfill all tracked CIKs from 5 years ago (default)
    python scripts/backfill_edgar.py

    # Backfill a single CIK for testing
    python scripts/backfill_edgar.py --cik 320193

    # Custom date range
    python scripts/backfill_edgar.py --since 2020-01-01

    # Only walk "recent" block, skip archive descent
    python scripts/backfill_edgar.py --no-archive

SEC rate limit is 10 req/s. The scraper uses 0.15s delay (~6 req/s).
A full 92-CIK backfill can take 1-3 hours depending on filing volume.
"""

import argparse
import asyncio
import logging
import sys
from datetime import date, timedelta

from fathom.database import async_session, engine
from fathom.engine.pipeline import _store_insider_trade
from fathom.models import Base
from fathom.scrapers.edgar import EdgarScraper, TRACKED_CIKS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("backfill")


async def backfill(
    ciks: dict[str, str],
    since: date,
    include_archive: bool = True,
) -> None:
    """Backfill insider trades for the given CIKs since the given date."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    scraper = EdgarScraper()
    total_filings = 0
    total_items = 0
    total_new = 0
    total_skipped = 0
    total_ciks = len(ciks)

    try:
        await scraper._load_cik_ticker_map()

        for idx, (cik, ticker) in enumerate(ciks.items(), start=1):
            logger.info(f"[{idx}/{total_ciks}] {ticker} (CIK {cik}): fetching filings...")

            try:
                filings = await scraper.get_form4_filings_since(
                    cik, since=since, include_archive=include_archive
                )
            except Exception as e:
                logger.error(f"  Failed to fetch filings for {ticker}: {e}")
                continue

            if not filings:
                logger.info(f"  No Form 4 filings found for {ticker}")
                continue

            logger.info(f"  Found {len(filings)} Form 4 filings, parsing...")
            total_filings += len(filings)

            cik_items = []
            for filing in filings:
                try:
                    items = await scraper._fetch_and_parse_form4(filing)
                    cik_items.extend(items)
                except Exception as e:
                    logger.warning(f"  Failed to parse {filing['xml_url']}: {e}")

            total_items += len(cik_items)
            logger.info(f"  Parsed {len(cik_items)} transactions, storing...")

            cik_new = 0
            cik_skipped = 0
            async with async_session() as session:
                for item in cik_items:
                    is_new = await _store_insider_trade(session, item)
                    if is_new:
                        cik_new += 1
                    else:
                        cik_skipped += 1
                await session.commit()

            total_new += cik_new
            total_skipped += cik_skipped
            logger.info(
                f"  {ticker}: +{cik_new} new, {cik_skipped} dup | "
                f"running total: +{total_new} new, {total_skipped} dup"
            )

    finally:
        await scraper.close()

    logger.info("=" * 60)
    logger.info(f"Backfill complete:")
    logger.info(f"  CIKs processed:  {total_ciks}")
    logger.info(f"  Form 4 filings:  {total_filings}")
    logger.info(f"  Transactions:    {total_items}")
    logger.info(f"  New records:     {total_new}")
    logger.info(f"  Duplicates:      {total_skipped}")


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> int:
    default_since = (date.today() - timedelta(days=365 * 5)).isoformat()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cik",
        action="append",
        default=None,
        help="Specific CIK(s) to backfill. Repeat for multiple. Default: all tracked CIKs.",
    )
    parser.add_argument(
        "--since",
        type=_parse_date,
        default=_parse_date(default_since),
        help=f"Earliest filing date (YYYY-MM-DD). Default: {default_since} (5 years ago).",
    )
    parser.add_argument(
        "--no-archive",
        action="store_true",
        help="Only walk the 'recent' submissions block, skip archive files.",
    )
    args = parser.parse_args()

    if args.cik:
        ciks = {c: TRACKED_CIKS.get(c, "?") for c in args.cik}
    else:
        ciks = dict(TRACKED_CIKS)

    logger.info(f"Backfilling {len(ciks)} CIK(s) since {args.since}")
    asyncio.run(
        backfill(ciks=ciks, since=args.since, include_archive=not args.no_archive)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
