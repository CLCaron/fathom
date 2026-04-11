"""Tests for the correlation pipeline integration (run_correlation_pipeline + _store_signal)."""

import pytest
from datetime import date, datetime
from unittest.mock import patch

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from fathom.engine.correlator import SignalCandidate
from fathom.engine.pipeline import run_correlation_pipeline, _store_signal
from fathom.models.committee_membership import CommitteeMembership
from fathom.models.congressional_trade import CongressionalTrade
from fathom.models.signal import Signal


def _make_trade(**overrides) -> CongressionalTrade:
    defaults = dict(
        member_name="Adam Schiff",
        chamber="HOUSE",
        state="CA",
        party="D",
        ticker="LMT",
        asset_name="Lockheed Martin Corp",
        trade_type="PURCHASE",
        amount_min=15001.0,
        amount_max=50000.0,
        trade_date=date.today(),
        disclosure_date=date.today(),
        source_url="https://capitoltrades.com/trades/1",
        sector="Defense",
    )
    defaults.update(overrides)
    return CongressionalTrade(**defaults)


def _make_membership(**overrides) -> CommitteeMembership:
    defaults = dict(
        member_name="Adam Schiff",
        chamber="HOUSE",
        committee_code="HSAS",
        committee_name="Armed Services",
        role="CHAIR",
        congress_number=119,
        sectors_covered=["Defense"],
    )
    defaults.update(overrides)
    return CommitteeMembership(**defaults)


class TestRunCorrelationPipeline:
    async def test_creates_signal_rows(self, db_engine):
        """Insert matching trade + membership, run pipeline, verify Signal rows."""
        session_factory = async_sessionmaker(
            db_engine, class_=AsyncSession, expire_on_commit=False
        )

        # Seed data
        async with session_factory() as session:
            session.add(_make_trade())
            session.add(_make_membership())
            await session.commit()

        with patch("fathom.engine.pipeline.async_session", session_factory):
            new_count = await run_correlation_pipeline(lookback_days=90)

        assert new_count >= 1

        # Verify signals were stored
        async with session_factory() as session:
            result = await session.execute(select(Signal))
            signals = result.scalars().all()

        assert len(signals) >= 1
        assert any(s.signal_type == "COMMITTEE_TRADE" for s in signals)

    async def test_dedup_second_run_produces_zero(self, db_engine):
        """Running correlation twice with same data -> second run stores 0 new."""
        session_factory = async_sessionmaker(
            db_engine, class_=AsyncSession, expire_on_commit=False
        )

        async with session_factory() as session:
            session.add(_make_trade())
            session.add(_make_membership())
            await session.commit()

        with patch("fathom.engine.pipeline.async_session", session_factory):
            first = await run_correlation_pipeline(lookback_days=90)
            second = await run_correlation_pipeline(lookback_days=90)

        assert first >= 1
        assert second == 0

    async def test_no_trades_returns_zero(self, db_engine):
        """No congressional trades in lookback window -> returns 0."""
        session_factory = async_sessionmaker(
            db_engine, class_=AsyncSession, expire_on_commit=False
        )

        with patch("fathom.engine.pipeline.async_session", session_factory):
            result = await run_correlation_pipeline(lookback_days=90)

        assert result == 0


class TestStoreSignal:
    async def test_stores_new_signal(self, db_session):
        """A fresh candidate should be stored and return True."""
        candidate = SignalCandidate(
            signal_type="COMMITTEE_TRADE",
            ticker="LMT",
            sector="Defense",
            confidence=25.0,
            headline="Test signal",
            explanation="Test explanation",
            details={"member": "Adam Schiff"},
            source_trade_ids=[1],
        )

        stored = await _store_signal(db_session, candidate)
        await db_session.flush()

        assert stored is True

        result = await db_session.execute(select(Signal))
        signals = result.scalars().all()
        assert len(signals) == 1
        assert signals[0].signal_type == "COMMITTEE_TRADE"
        assert signals[0].sector == "Defense"
        assert signals[0].source_trade_ids == [1]

    async def test_dedup_same_trade_same_day(self, db_session):
        """Duplicate candidate (same type, sector, trade, day) -> returns False."""
        candidate = SignalCandidate(
            signal_type="COMMITTEE_TRADE",
            ticker="LMT",
            sector="Defense",
            confidence=25.0,
            headline="Test signal",
            explanation="Test explanation",
            details={"member": "Adam Schiff"},
            source_trade_ids=[42],
        )

        first = await _store_signal(db_session, candidate)
        await db_session.flush()
        second = await _store_signal(db_session, candidate)

        assert first is True
        assert second is False

        result = await db_session.execute(select(Signal))
        signals = result.scalars().all()
        assert len(signals) == 1
