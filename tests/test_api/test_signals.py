"""Tests for signal/dashboard API routes."""

import pytest
from datetime import date, datetime

from fathom.models.insider_trade import InsiderTrade


class TestDashboard:
    async def test_renders(self, test_client, db_session):
        """GET / should return 200 with HTML."""
        response = await test_client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]
        assert "Financial Signals" in response.text or "dashboard" in response.text.lower()

    async def test_with_trades(self, test_client, db_session):
        """Dashboard should display trades that exist in DB."""
        db_session.add(InsiderTrade(
            cik="320193",
            filer_name="Tim Cook",
            filer_title="CEO",
            company_name="Apple Inc.",
            ticker="AAPL",
            trade_type="SELL",
            shares=50000,
            price_per_share=185.50,
            total_value=9275000.0,
            trade_date=date.today(),
            filing_date=datetime.utcnow(),
            filing_url="https://sec.gov/test.xml",
            sector="Technology",
        ))
        await db_session.commit()

        response = await test_client.get("/")
        assert response.status_code == 200
        assert "AAPL" in response.text

    async def test_sector_filter(self, test_client, db_session):
        """GET /?sector=Technology should filter trades."""
        for ticker, sector in [("AAPL", "Technology"), ("JPM", "Finance")]:
            db_session.add(InsiderTrade(
                cik="123",
                filer_name="Test Person",
                filer_title=None,
                company_name=f"{ticker} Corp",
                ticker=ticker,
                trade_type="BUY",
                shares=100,
                price_per_share=100.0,
                total_value=10000.0,
                trade_date=date.today(),
                filing_date=datetime.utcnow(),
                filing_url=None,
                sector=sector,
            ))
        await db_session.commit()

        response = await test_client.get("/?sector=Technology")
        assert response.status_code == 200
        assert "AAPL" in response.text

    async def test_trade_type_filter(self, test_client, db_session):
        """GET /?trade_type=BUY should filter by trade type."""
        response = await test_client.get("/?trade_type=BUY")
        assert response.status_code == 200

    async def test_days_filter(self, test_client, db_session):
        """GET /?days=30 should accept days parameter."""
        response = await test_client.get("/?days=30")
        assert response.status_code == 200

    async def test_days_validation(self, test_client, db_session):
        """days must be between 1 and 90."""
        response = await test_client.get("/?days=0")
        assert response.status_code == 422

        response = await test_client.get("/?days=91")
        assert response.status_code == 422


class TestTradesPartial:
    async def test_returns_html(self, test_client, db_session):
        """GET /api/trades should return HTML partial."""
        response = await test_client.get("/api/trades")
        assert response.status_code == 200
        assert "text/html" in response.headers["content-type"]

    async def test_with_filters(self, test_client, db_session):
        """Partial should accept the same filters."""
        response = await test_client.get("/api/trades?sector=Technology&days=14")
        assert response.status_code == 200
