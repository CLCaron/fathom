"""APScheduler job definitions."""

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from fathom.config import settings
from fathom.engine.pipeline import (
    run_committees_pipeline,
    run_congressional_pipeline,
    run_correlation_pipeline,
    run_edgar_pipeline,
    run_legislation_pipeline,
)

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def setup_scheduler():
    """Configure and start the job scheduler."""

    # SEC EDGAR Form 4 scraper
    scheduler.add_job(
        run_edgar_pipeline,
        trigger=IntervalTrigger(minutes=settings.edgar_scrape_interval_minutes),
        id="edgar_scraper",
        name="SEC EDGAR Form 4 Scraper",
        replace_existing=True,
    )

    # Capitol Trades congressional scraper
    scheduler.add_job(
        run_congressional_pipeline,
        trigger=IntervalTrigger(hours=settings.congressional_scrape_interval_hours),
        id="congressional_scraper",
        name="Capitol Trades Congressional Scraper",
        replace_existing=True,
    )

    # Congress.gov committee membership scraper
    scheduler.add_job(
        run_committees_pipeline,
        trigger=IntervalTrigger(hours=settings.committee_scrape_interval_hours),
        id="committee_scraper",
        name="Congress.gov Committee Scraper",
        replace_existing=True,
    )

    # Congress.gov legislation and votes scraper
    scheduler.add_job(
        run_legislation_pipeline,
        trigger=IntervalTrigger(hours=settings.legislation_scrape_interval_hours),
        id="legislation_scraper",
        name="Congress.gov Legislation Scraper",
        replace_existing=True,
    )

    logger.info("Scheduler configured with jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name} ({job.trigger})")


async def run_job_now(job_id: str) -> str:
    """Manually trigger a scheduled job."""
    job_map = {
        "edgar_scraper": run_edgar_pipeline,
        "congressional_scraper": run_congressional_pipeline,
        "committee_scraper": run_committees_pipeline,
        "legislation_scraper": run_legislation_pipeline,
        "correlation_engine": run_correlation_pipeline,
    }

    func = job_map.get(job_id)
    if not func:
        return f"Unknown job: {job_id}"

    try:
        result = await func()
        return f"Job {job_id} completed. Result: {result}"
    except Exception as e:
        logger.error(f"Job {job_id} failed: {e}")
        return f"Job {job_id} failed: {e}"
