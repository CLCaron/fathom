"""Tests for admin API routes."""

import pytest
from unittest.mock import AsyncMock, patch


class TestAdminPage:
    async def test_renders(self, test_client, db_session):
        """GET /admin should return 200 with HTML."""
        response = await test_client.get("/admin")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    async def test_shows_stats(self, test_client, db_session):
        """Admin page should show database stats."""
        response = await test_client.get("/admin")
        assert response.status_code == 200
        # Should contain stat numbers (even if 0)
        assert "0" in response.text or "insider" in response.text.lower()


class TestTriggerJob:
    @patch("fathom.api.admin.run_job_now", new_callable=AsyncMock)
    async def test_known_job(self, mock_run, test_client, db_session):
        """POST /admin/run/edgar_scraper should return HTML success fragment."""
        mock_run.return_value = "Job edgar_scraper completed. Result: 5"

        response = await test_client.post("/admin/run/edgar_scraper")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "scraper-success" in response.text
        assert "Result: 5" in response.text
        mock_run.assert_called_once_with("edgar_scraper")

    @patch("fathom.api.admin.run_job_now", new_callable=AsyncMock)
    async def test_unknown_job(self, mock_run, test_client, db_session):
        """POST /admin/run/nonexistent should return HTML with result."""
        mock_run.return_value = "Unknown job: nonexistent"

        response = await test_client.post("/admin/run/nonexistent")
        assert response.status_code == 200
        assert "Unknown job" in response.text or "nonexistent" in response.text

    @patch("fathom.api.admin.run_job_now", new_callable=AsyncMock)
    async def test_error_returns_error_fragment(self, mock_run, test_client, db_session):
        """POST /admin/run/... should return error HTML on exception."""
        mock_run.side_effect = RuntimeError("connection failed")

        response = await test_client.post("/admin/run/edgar_scraper")
        assert response.status_code == 200
        assert "scraper-error" in response.text
        assert "connection failed" in response.text
