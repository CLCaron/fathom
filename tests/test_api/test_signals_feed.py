"""Tests for signal feed routes in fathom.api.signals."""

import pytest
from datetime import datetime

from fathom.models.signal import Signal


@pytest.fixture
async def seed_signals(db_session):
    """Insert two signals with different confidence levels."""
    high = Signal(
        signal_type="COMMITTEE_TRADE",
        ticker="LMT",
        sector="Defense",
        headline="Rep. Smith (Armed Services, CHAIR) purchase LMT",
        confidence=30.0,
        details={"explanation": "committee overlap"},
        source_trade_ids=[1],
        detected_at=datetime.utcnow(),
    )
    low = Signal(
        signal_type="COMMITTEE_TRADE",
        ticker="PFE",
        sector="Healthcare",
        headline="Sen. Jones traded PFE with committee overlap",
        confidence=10.0,
        details={"explanation": "committee overlap low confidence"},
        source_trade_ids=[2],
        detected_at=datetime.utcnow(),
    )
    db_session.add_all([high, low])
    await db_session.commit()
    return high, low


async def test_signals_empty_db(test_client):
    """GET /signals with no signals returns 200 and shows empty state."""
    resp = await test_client.get("/signals")
    assert resp.status_code == 200
    assert "No signals found" in resp.text


async def test_signals_default_filters_hide_low_confidence(db_session, seed_signals, test_client):
    """Default view (show_candidates=false) hides signals below min_confidence (25)."""
    high, low = seed_signals

    resp = await test_client.get("/signals")
    assert resp.status_code == 200
    assert high.headline in resp.text
    assert low.headline not in resp.text


async def test_signals_show_candidates(db_session, seed_signals, test_client):
    """show_candidates=true shows all signals regardless of confidence."""
    high, low = seed_signals

    resp = await test_client.get("/signals", params={"show_candidates": "true"})
    assert resp.status_code == 200
    assert high.headline in resp.text
    assert low.headline in resp.text


async def test_signals_sector_filter(db_session, seed_signals, test_client):
    """Filtering by sector returns only matching signals."""
    high, low = seed_signals

    resp = await test_client.get(
        "/signals",
        params={"sector": "Defense", "show_candidates": "true"},
    )
    assert resp.status_code == 200
    assert high.headline in resp.text
    assert low.headline not in resp.text


async def test_signals_feed_partial(test_client):
    """GET /api/signals/feed returns 200 (HTMX partial)."""
    resp = await test_client.get("/api/signals/feed")
    assert resp.status_code == 200
