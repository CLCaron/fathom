"""APScheduler job definitions."""

import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from fathom.engine.pipeline import run_edgar_pipeline

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


def setup_scheduler():
    """Configure and start the job scheduler."""

    # SEC EDGAR Form 4 scraper — every 15 minutes during market hours
    scheduler.add_job(
        run_edgar_pipeline,
        trigger=IntervalTrigger(minutes=15),
        id="edgar_scraper",
        name="SEC EDGAR Form 4 Scraper",
        replace_existing=True,
    )

    logger.info("Scheduler configured with jobs:")
    for job in scheduler.get_jobs():
        logger.info(f"  - {job.name} ({job.trigger})")


async def run_job_now(job_id: str) -> str:
    """Manually trigger a scheduled job."""
    job_map = {
        "edgar_scraper": run_edgar_pipeline,
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
