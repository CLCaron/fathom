"""Tests for congressional trade API routes."""

from datetime import date

import pytest
from fathom.models.congressional_trade import CongressionalTrade


@pytest.fixture
def sample_congressional_trade():
    return CongressionalTrade(
        member_name="Jane Smith",
        chamber="SENATE",
        state="WA",
        party="Democrat",
        ticker="MSFT",
        asset_name="Microsoft Corp",
        trade_type="PURCHASE",
        amount_min=50_001,
        amount_max=100_000,
        trade_date=date.today(),
        disclosure_date=date.today(),
        source_url="https://www.capitoltrades.com/trades?txId=99999",
        sector="Technology",
    )


class TestCongressionalDashboard:
    async def test_renders(self, test_client):
        response = await test_client.get("/congressional")
        assert response.status_code == 200
        assert "Congressional Trades" in response.text

    async def test_with_trades(self, test_client, db_session, sample_congressional_trade):
        db_session.add(sample_congressional_trade)
        await db_session.commit()

        response = await test_client.get("/congressional?days=90")
        assert response.status_code == 200
        assert "Jane Smith" in response.text
        assert "MSFT" in response.text

    async def test_chamber_filter(self, test_client, db_session, sample_congressional_trade):
        db_session.add(sample_congressional_trade)
        await db_session.commit()

        response = await test_client.get("/congressional?chamber=SENATE&days=90")
        assert response.status_code == 200
        assert "Jane Smith" in response.text

        response = await test_client.get("/congressional?chamber=HOUSE&days=90")
        assert response.status_code == 200
        assert "Jane Smith" not in response.text

    async def test_party_filter(self, test_client, db_session, sample_congressional_trade):
        db_session.add(sample_congressional_trade)
        await db_session.commit()

        response = await test_client.get("/congressional?party=Democrat&days=90")
        assert response.status_code == 200
        assert "Jane Smith" in response.text

        response = await test_client.get("/congressional?party=Republican&days=90")
        assert response.status_code == 200
        assert "Jane Smith" not in response.text


class TestCongressionalTradesPartial:
    async def test_returns_html(self, test_client):
        response = await test_client.get("/api/congressional-trades")
        assert response.status_code == 200

    async def test_with_filters(self, test_client, db_session, sample_congressional_trade):
        db_session.add(sample_congressional_trade)
        await db_session.commit()

        response = await test_client.get(
            "/api/congressional-trades?chamber=SENATE&trade_type=PURCHASE&days=90"
        )
        assert response.status_code == 200
        assert "Jane Smith" in response.text
