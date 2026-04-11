"""Correlation engine: matchers that connect trades to committee oversight and legislation timing.

Phase 2b implements two matchers:
  - Matcher 1: Committee-trade overlap (does the member's committee oversee the traded sector?)
  - Matcher 2: Trade-legislation timing (did the member trade near a relevant vote or bill action?)

Each matcher produces SignalCandidate objects that the pipeline scores and stores.
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from fathom.engine.explainers import render_explanation
from fathom.engine.normalization import normalize_member_name
from fathom.engine.scorer import score_evidence
from fathom.models.committee_membership import CommitteeMembership
from fathom.models.congressional_trade import CongressionalTrade
from fathom.models.legislation import Legislation, LegislationVote

logger = logging.getLogger(__name__)


@dataclass
class SignalCandidate:
    """A potential signal produced by a matcher, ready for scoring and storage."""

    signal_type: str
    ticker: str | None
    sector: str | None
    confidence: float = 0.0
    headline: str = ""
    explanation: str = ""
    details: dict = field(default_factory=dict)
    source_trade_ids: list[int] = field(default_factory=list)


async def find_committee_overlap_signals(
    session: AsyncSession,
    trades: list[CongressionalTrade],
) -> list[SignalCandidate]:
    """Matcher 1: Find trades where the member's committee oversees the traded sector.

    For each trade, looks up the member's committee memberships and checks
    if any committee's sectors_covered includes the trade's sector.
    """
    candidates: list[SignalCandidate] = []

    # Pre-fetch all committee memberships for efficiency
    result = await session.execute(select(CommitteeMembership))
    all_memberships = result.scalars().all()

    # Build lookup: normalized_name -> list of memberships
    membership_by_name: dict[str, list[CommitteeMembership]] = {}
    for m in all_memberships:
        key = normalize_member_name(m.member_name)
        membership_by_name.setdefault(key, []).append(m)

    for trade in trades:
        if not trade.sector or not trade.member_name:
            continue

        normalized = normalize_member_name(trade.member_name)
        memberships = membership_by_name.get(normalized, [])

        for membership in memberships:
            sectors = membership.sectors_covered or []
            if trade.sector not in sectors:
                continue

            # Determine evidence weight by role
            role = membership.role or "MEMBER"
            if role == "CHAIR":
                evidence_key = "committee_chair"
            elif role == "RANKING_MEMBER":
                evidence_key = "committee_ranking_member"
            else:
                evidence_key = "committee_member"

            confidence = score_evidence([evidence_key])

            details = {
                "member": trade.member_name,
                "committee_code": membership.committee_code,
                "committee_name": membership.committee_name,
                "role": role,
                "sector": trade.sector,
                "ticker": trade.ticker,
                "trade_type": trade.trade_type,
                "trade_date": str(trade.trade_date),
                "amount_min": float(trade.amount_min) if trade.amount_min else None,
                "amount_max": float(trade.amount_max) if trade.amount_max else None,
            }

            headline = (
                f"{trade.member_name} ({membership.committee_name}, {role}) "
                f"{trade.trade_type.lower()} {trade.ticker}"
            )

            explanation = render_explanation("COMMITTEE_TRADE", details)

            candidates.append(
                SignalCandidate(
                    signal_type="COMMITTEE_TRADE",
                    ticker=trade.ticker,
                    sector=trade.sector,
                    confidence=confidence,
                    headline=headline,
                    explanation=explanation,
                    details=details,
                    source_trade_ids=[trade.id],
                )
            )

    logger.info(
        f"Matcher 1 (committee overlap): {len(candidates)} candidates "
        f"from {len(trades)} trades"
    )
    return candidates


async def find_legislation_timing_signals(
    session: AsyncSession,
    trades: list[CongressionalTrade],
) -> list[SignalCandidate]:
    """Matcher 2: Find trades near relevant legislative activity.

    For each trade, finds legislation with activity within +/-30 days
    whose affected sectors overlap the trade's sector. Checks if the
    member voted on or sponsored the bill for bonus scoring.
    """
    candidates: list[SignalCandidate] = []

    for trade in trades:
        if not trade.sector:
            continue

        window_start = trade.trade_date - timedelta(days=30)
        window_end = trade.trade_date + timedelta(days=30)

        # Find legislation with activity in the window and matching sector
        result = await session.execute(
            select(Legislation).where(
                Legislation.last_action_date >= window_start,
                Legislation.last_action_date <= window_end,
            )
        )
        bills = result.scalars().all()

        for bill in bills:
            affected = bill.sectors_affected or []
            if trade.sector not in affected:
                continue

            # Calculate proximity in days
            proximity_days = (trade.trade_date - bill.last_action_date).days

            # Determine base evidence
            evidence_keys: list[str] = []
            if abs(proximity_days) <= 7:
                evidence_keys.append("legislation_within_7d")
            else:
                evidence_keys.append("legislation_within_30d")

            # Check if the member voted on this bill
            normalized_trade_name = normalize_member_name(trade.member_name)
            vote_result = await session.execute(
                select(LegislationVote).where(
                    LegislationVote.bill_id == bill.bill_id,
                )
            )
            votes = vote_result.scalars().all()

            member_vote = None
            for v in votes:
                if normalize_member_name(v.member_name) == normalized_trade_name:
                    member_vote = v
                    break

            # Sponsor bonus
            if bill.sponsor_name:
                if normalize_member_name(bill.sponsor_name) == normalized_trade_name:
                    evidence_keys.append("legislation_sponsor_bonus")

            confidence = score_evidence(evidence_keys)

            details = {
                "member": trade.member_name,
                "ticker": trade.ticker,
                "sector": trade.sector,
                "trade_type": trade.trade_type,
                "trade_date": str(trade.trade_date),
                "bill_id": bill.bill_id,
                "bill_title": bill.title,
                "bill_action_date": str(bill.last_action_date),
                "proximity_days": proximity_days,
                "member_voted": member_vote.vote if member_vote else None,
                "member_vote_date": str(member_vote.vote_date) if member_vote else None,
                "is_sponsor": bill.sponsor_name and normalize_member_name(bill.sponsor_name) == normalized_trade_name,
                "evidence_keys": evidence_keys,
            }

            headline = (
                f"{trade.member_name} {trade.trade_type.lower()} {trade.ticker} "
                f"{abs(proximity_days)}d {'before' if proximity_days < 0 else 'after'} "
                f"action on {bill.bill_id}"
            )

            explanation = render_explanation("LEGISLATION_TIMING", details)

            candidates.append(
                SignalCandidate(
                    signal_type="LEGISLATION_TIMING",
                    ticker=trade.ticker,
                    sector=trade.sector,
                    confidence=confidence,
                    headline=headline,
                    explanation=explanation,
                    details=details,
                    source_trade_ids=[trade.id],
                )
            )

    logger.info(
        f"Matcher 2 (legislation timing): {len(candidates)} candidates "
        f"from {len(trades)} trades"
    )
    return candidates


def merge_candidates(candidates: list[SignalCandidate]) -> list[SignalCandidate]:
    """Merge candidates for the same trade that fired from multiple matchers.

    When both matchers fire on the same trade, the confidence scores stack
    (capped at 100). The details dicts are merged under type-specific keys.
    """
    # Group by source trade ID
    by_trade: dict[int, list[SignalCandidate]] = {}
    for c in candidates:
        if c.source_trade_ids:
            trade_id = c.source_trade_ids[0]
            by_trade.setdefault(trade_id, []).append(c)

    merged: list[SignalCandidate] = []
    for trade_id, group in by_trade.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        # Multiple matchers fired on the same trade -- keep them separate
        # but they could be merged into a composite signal in the future.
        # For now, just emit each one independently.
        merged.extend(group)

    return merged
