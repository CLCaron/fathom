"""Tests for scheduler job configuration and manual execution."""

import pytest
from unittest.mock import AsyncMock, patch

from fathom.scheduler.jobs import scheduler, setup_scheduler, run_job_now


class TestSetupScheduler:
    def test_edgar_job_registered(self):
        """setup_scheduler should register the edgar_scraper job."""
        scheduler.remove_all_jobs()
        setup_scheduler()

        jobs = {job.id: job for job in scheduler.get_jobs()}
        assert "edgar_scraper" in jobs
        assert "SEC EDGAR Form 4 Scraper" == jobs["edgar_scraper"].name

    def test_congressional_job_registered(self):
        """setup_scheduler should register the congressional_scraper job."""
        scheduler.remove_all_jobs()
        setup_scheduler()

        jobs = {job.id: job for job in scheduler.get_jobs()}
        assert "congressional_scraper" in jobs
        assert "Capitol Trades" in jobs["congressional_scraper"].name

    def test_job_interval_uses_settings(self):
        """Job interval should match settings.edgar_scrape_interval_minutes."""
        from fathom.config import settings

        scheduler.remove_all_jobs()
        setup_scheduler()

        job = scheduler.get_job("edgar_scraper")
        # APScheduler stores the interval; check the trigger's interval attribute
        assert job.trigger.interval.total_seconds() == settings.edgar_scrape_interval_minutes * 60


class TestRunJobNow:
    async def test_known_job_runs(self):
        """run_job_now with a valid job ID should call the pipeline and return success."""
        with patch("fathom.scheduler.jobs.run_edgar_pipeline", new_callable=AsyncMock, return_value=5) as mock_pipeline:
            result = await run_job_now("edgar_scraper")

        mock_pipeline.assert_called_once()
        assert "completed" in result
        assert "5" in result

    async def test_congressional_job_runs(self):
        """run_job_now with congressional_scraper should call the pipeline."""
        with patch("fathom.scheduler.jobs.run_congressional_pipeline", new_callable=AsyncMock, return_value=10) as mock_pipeline:
            result = await run_job_now("congressional_scraper")

        mock_pipeline.assert_called_once()
        assert "completed" in result
        assert "10" in result

    async def test_unknown_job_returns_error(self):
        """run_job_now with an unknown job ID should return an error string."""
        result = await run_job_now("nonexistent_job")
        assert "Unknown job" in result
        assert "nonexistent_job" in result

    async def test_job_exception_returns_error(self):
        """If the pipeline raises, run_job_now should return an error string."""
        with patch("fathom.scheduler.jobs.run_edgar_pipeline", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
            result = await run_job_now("edgar_scraper")

        assert "failed" in result
        assert "boom" in result
