"""Tests for the correlation engine matchers."""

import pytest
from datetime import date

from sqlalchemy import select

from fathom.engine.correlator import (
    SignalCandidate,
    find_committee_overlap_signals,
    find_legislation_timing_signals,
)
from fathom.models.committee_membership import CommitteeMembership
from fathom.models.congressional_trade import CongressionalTrade
from fathom.models.legislation import Legislation, LegislationVote


def _make_trade(**overrides) -> CongressionalTrade:
    """Build a CongressionalTrade with sensible defaults, applying overrides."""
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
        trade_date=date(2026, 3, 15),
        disclosure_date=date(2026, 4, 1),
        source_url="https://capitoltrades.com/trades/1",
        sector="Defense",
    )
    defaults.update(overrides)
    return CongressionalTrade(**defaults)


def _make_membership(**overrides) -> CommitteeMembership:
    """Build a CommitteeMembership with sensible defaults, applying overrides."""
    defaults = dict(
        member_name="Adam Schiff",
        chamber="HOUSE",
        committee_code="HSAS",
        committee_name="Armed Services",
        role="MEMBER",
        congress_number=119,
        sectors_covered=["Defense"],
    )
    defaults.update(overrides)
    return CommitteeMembership(**defaults)


def _make_bill(**overrides) -> Legislation:
    """Build a Legislation row with sensible defaults."""
    defaults = dict(
        bill_id="HR-1234",
        title="National Defense Authorization Act",
        summary="Annual defense spending bill",
        congress_number=119,
        introduced_date=date(2026, 1, 10),
        last_action_date=date(2026, 3, 20),
        status="PASSED_HOUSE",
        sectors_affected=["Defense"],
        sponsor_name="Jack Reed",
        bill_url="https://congress.gov/bill/119th-congress/hr1234",
    )
    defaults.update(overrides)
    return Legislation(**defaults)


# ---------------------------------------------------------------------------
# Matcher 1: Committee overlap
# ---------------------------------------------------------------------------
class TestCommitteeOverlapSignals:
    async def test_chair_produces_signal_confidence_25(self, db_session):
        """Chair of a committee overseeing the traded sector -> confidence 25."""
        trade = _make_trade()
        membership = _make_membership(role="CHAIR")

        db_session.add(membership)
        db_session.add(trade)
        await db_session.flush()

        candidates = await find_committee_overlap_signals(db_session, [trade])

        assert len(candidates) == 1
        c = candidates[0]
        assert c.signal_type == "COMMITTEE_TRADE"
        assert c.confidence == 25
        assert c.details["role"] == "CHAIR"
        assert c.details["committee_name"] == "Armed Services"
        assert c.details["sector"] == "Defense"
        assert c.details["ticker"] == "LMT"

    async def test_regular_member_confidence_15(self, db_session):
        """Regular committee member -> confidence 15."""
        trade = _make_trade()
        membership = _make_membership(role="MEMBER")

        db_session.add(membership)
        db_session.add(trade)
        await db_session.flush()

        candidates = await find_committee_overlap_signals(db_session, [trade])

        assert len(candidates) == 1
        assert candidates[0].confidence == 15
        assert candidates[0].details["role"] == "MEMBER"

    async def test_no_signal_when_sector_not_covered(self, db_session):
        """Trade in a sector the committee doesn't cover -> no signal."""
        trade = _make_trade(sector="Healthcare", ticker="JNJ")
        membership = _make_membership(sectors_covered=["Defense"])

        db_session.add(membership)
        db_session.add(trade)
        await db_session.flush()

        candidates = await find_committee_overlap_signals(db_session, [trade])
        assert candidates == []

    async def test_no_signal_when_trade_has_no_sector(self, db_session):
        """Trade with sector=None -> skipped, no signal."""
        trade = _make_trade(sector=None)
        membership = _make_membership()

        db_session.add(membership)
        db_session.add(trade)
        await db_session.flush()

        candidates = await find_committee_overlap_signals(db_session, [trade])
        assert candidates == []

    async def test_name_normalization_matches(self, db_session):
        """Committee has 'Adam B. Schiff', trade has 'Adam Schiff' -> should match."""
        trade = _make_trade(member_name="Adam Schiff")
        membership = _make_membership(member_name="Adam B. Schiff")

        db_session.add(membership)
        db_session.add(trade)
        await db_session.flush()

        candidates = await find_committee_overlap_signals(db_session, [trade])

        assert len(candidates) == 1
        assert candidates[0].signal_type == "COMMITTEE_TRADE"
        assert candidates[0].details["member"] == "Adam Schiff"


# ---------------------------------------------------------------------------
# Matcher 2: Legislation timing
# ---------------------------------------------------------------------------
class TestLegislationTimingSignals:
    async def test_trade_within_7_days_of_bill_action(self, db_session):
        """Trade 5 days before bill action in same sector -> within_7d (20 pts)."""
        trade = _make_trade(trade_date=date(2026, 3, 15))
        bill = _make_bill(last_action_date=date(2026, 3, 20))

        db_session.add(trade)
        db_session.add(bill)
        await db_session.flush()

        candidates = await find_legislation_timing_signals(db_session, [trade])

        assert len(candidates) == 1
        c = candidates[0]
        assert c.signal_type == "LEGISLATION_TIMING"
        assert c.confidence == 20
        assert c.details["bill_id"] == "HR-1234"
        assert c.details["proximity_days"] == -5
        assert "legislation_within_7d" in c.details["evidence_keys"]

    async def test_trade_within_30_days_of_bill_action(self, db_session):
        """Trade 20 days before bill action -> within_30d (10 pts)."""
        trade = _make_trade(trade_date=date(2026, 3, 1))
        bill = _make_bill(last_action_date=date(2026, 3, 21))

        db_session.add(trade)
        db_session.add(bill)
        await db_session.flush()

        candidates = await find_legislation_timing_signals(db_session, [trade])

        assert len(candidates) == 1
        c = candidates[0]
        assert c.confidence == 10
        assert "legislation_within_30d" in c.details["evidence_keys"]

    async def test_no_signal_outside_30_day_window(self, db_session):
        """Trade 35 days from bill action -> no signal."""
        trade = _make_trade(trade_date=date(2026, 2, 10))
        bill = _make_bill(last_action_date=date(2026, 3, 20))

        db_session.add(trade)
        db_session.add(bill)
        await db_session.flush()

        candidates = await find_legislation_timing_signals(db_session, [trade])
        assert candidates == []

    async def test_no_signal_different_sector(self, db_session):
        """Bill in different sector than trade -> no signal."""
        trade = _make_trade(
            trade_date=date(2026, 3, 15),
            sector="Healthcare",
            ticker="JNJ",
        )
        bill = _make_bill(
            last_action_date=date(2026, 3, 20),
            sectors_affected=["Defense"],
        )

        db_session.add(trade)
        db_session.add(bill)
        await db_session.flush()

        candidates = await find_legislation_timing_signals(db_session, [trade])
        assert candidates == []

    async def test_sponsor_bonus_adds_10_points(self, db_session):
        """Bill sponsored by the trading member -> extra 10 points."""
        trade = _make_trade(
            member_name="Jack Reed",
            trade_date=date(2026, 3, 15),
        )
        bill = _make_bill(
            last_action_date=date(2026, 3, 20),
            sponsor_name="Jack Reed",
        )

        db_session.add(trade)
        db_session.add(bill)
        await db_session.flush()

        candidates = await find_legislation_timing_signals(db_session, [trade])

        assert len(candidates) == 1
        c = candidates[0]
        # within_7d (20) + sponsor_bonus (10) = 30
        assert c.confidence == 30
        assert "legislation_within_7d" in c.details["evidence_keys"]
        assert "legislation_sponsor_bonus" in c.details["evidence_keys"]
        assert c.details["is_sponsor"] is True
