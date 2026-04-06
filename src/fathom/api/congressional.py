"""Congressional trade routes."""

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from fathom.config import settings
from fathom.database import get_session
from fathom.models.committee_membership import CommitteeMembership
from fathom.models.congressional_trade import CongressionalTrade
from fathom.models.legislation import Legislation

router = APIRouter()
templates = Jinja2Templates(directory="src/fathom/templates")


@router.get("/congressional", response_class=HTMLResponse)
async def congressional_dashboard(
    request: Request,
    session: AsyncSession = Depends(get_session),
    chamber: str | None = None,
    party: str | None = None,
    trade_type: str | None = None,
    sector: str | None = None,
    days: int = Query(default=30, ge=1, le=settings.dashboard_max_days),
):
    """Congressional trades page."""
    since = date.today() - timedelta(days=days)

    query = (
        select(CongressionalTrade)
        .where(CongressionalTrade.disclosure_date >= since)
        .order_by(desc(CongressionalTrade.disclosure_date))
    )

    if chamber:
        query = query.where(CongressionalTrade.chamber == chamber)
    if party:
        query = query.where(CongressionalTrade.party == party)
    if trade_type:
        query = query.where(CongressionalTrade.trade_type == trade_type)
    if sector:
        query = query.where(CongressionalTrade.sector == sector)

    query = query.limit(settings.dashboard_trade_limit)
    result = await session.execute(query)
    trades = result.scalars().all()

    # Sector counts
    sector_query = (
        select(CongressionalTrade.sector, func.count(CongressionalTrade.id))
        .where(CongressionalTrade.disclosure_date >= since)
        .where(CongressionalTrade.sector.isnot(None))
        .group_by(CongressionalTrade.sector)
        .order_by(desc(func.count(CongressionalTrade.id)))
    )
    sector_counts = (await session.execute(sector_query)).all()

    # Stats
    total = (await session.execute(
        select(func.count(CongressionalTrade.id))
        .where(CongressionalTrade.disclosure_date >= since)
    )).scalar() or 0
    purchases = (await session.execute(
        select(func.count(CongressionalTrade.id))
        .where(CongressionalTrade.disclosure_date >= since)
        .where(CongressionalTrade.trade_type == "PURCHASE")
    )).scalar() or 0
    sales = (await session.execute(
        select(func.count(CongressionalTrade.id))
        .where(CongressionalTrade.disclosure_date >= since)
        .where(CongressionalTrade.trade_type == "SALE")
    )).scalar() or 0

    return templates.TemplateResponse(
        request,
        "congressional.html",
        {
            "trades": trades,
            "sector_counts": sector_counts,
            "stats": {"total": total, "purchases": purchases, "sales": sales},
            "filters": {
                "chamber": chamber,
                "party": party,
                "trade_type": trade_type,
                "sector": sector,
                "days": days,
            },
        },
    )


@router.get("/api/congressional-trades", response_class=HTMLResponse)
async def congressional_trades_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
    chamber: str | None = None,
    party: str | None = None,
    trade_type: str | None = None,
    sector: str | None = None,
    days: int = Query(default=30, ge=1, le=settings.dashboard_max_days),
):
    """HTMX partial: returns just the congressional trades table rows."""
    since = date.today() - timedelta(days=days)

    query = (
        select(CongressionalTrade)
        .where(CongressionalTrade.disclosure_date >= since)
        .order_by(desc(CongressionalTrade.disclosure_date))
    )

    if chamber:
        query = query.where(CongressionalTrade.chamber == chamber)
    if party:
        query = query.where(CongressionalTrade.party == party)
    if trade_type:
        query = query.where(CongressionalTrade.trade_type == trade_type)
    if sector:
        query = query.where(CongressionalTrade.sector == sector)

    query = query.limit(settings.dashboard_trade_limit)
    result = await session.execute(query)
    trades = result.scalars().all()

    return templates.TemplateResponse(
        request,
        "components/congressional_table.html",
        {"trades": trades},
    )


@router.get("/congressional/committees", response_class=HTMLResponse)
async def committees_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    chamber: str | None = None,
):
    """Committee membership view."""
    query = select(CommitteeMembership).order_by(
        CommitteeMembership.committee_name, CommitteeMembership.member_name
    )

    if chamber:
        query = query.where(CommitteeMembership.chamber == chamber)

    result = await session.execute(query)
    memberships = result.scalars().all()

    # Count by committee
    committee_query = (
        select(
            CommitteeMembership.committee_name,
            func.count(CommitteeMembership.id),
        )
        .group_by(CommitteeMembership.committee_name)
        .order_by(desc(func.count(CommitteeMembership.id)))
    )
    committee_counts = (await session.execute(committee_query)).all()

    return templates.TemplateResponse(
        request,
        "committees.html",
        {
            "memberships": memberships,
            "committee_counts": committee_counts,
            "filters": {"chamber": chamber},
        },
    )


@router.get("/congressional/legislation", response_class=HTMLResponse)
async def legislation_page(
    request: Request,
    session: AsyncSession = Depends(get_session),
    sector: str | None = None,
):
    """Legislation browser."""
    query = select(Legislation).order_by(desc(Legislation.last_action_date))

    if sector:
        # JSON array contains check — SQLite uses json_each
        query = query.where(Legislation.sectors_affected.contains(sector))

    query = query.limit(100)
    result = await session.execute(query)
    bills = result.scalars().all()

    return templates.TemplateResponse(
        request,
        "legislation.html",
        {
            "bills": bills,
            "filters": {"sector": sector},
        },
    )
