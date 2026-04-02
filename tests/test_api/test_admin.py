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
        """POST /admin/run/edgar_scraper should trigger the job."""
        mock_run.return_value = "Job edgar_scraper completed. Result: 5"

        response = await test_client.post("/admin/run/edgar_scraper")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        mock_run.assert_called_once_with("edgar_scraper")

    @patch("fathom.api.admin.run_job_now", new_callable=AsyncMock)
    async def test_unknown_job(self, mock_run, test_client, db_session):
        """POST /admin/run/nonexistent should return error message."""
        mock_run.return_value = "Unknown job: nonexistent"

        response = await test_client.post("/admin/run/nonexistent")
        assert response.status_code == 200
        data = response.json()
        assert "Unknown job" in data["message"] or "nonexistent" in data["message"]
