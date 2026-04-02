"""Signal feed and insider trade API routes."""

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from fathom.database import get_session
from fathom.models.insider_trade import InsiderTrade
from fathom.models.signal import Signal

router = APIRouter()
templates = Jinja2Templates(directory="src/fathom/templates")


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    session: AsyncSession = Depends(get_session),
    sector: str | None = None,
    trade_type: str | None = None,
    days: int = Query(default=7, ge=1, le=90),
):
    """Main dashboard page showing recent insider trades."""
    since = date.today() - timedelta(days=days)

    query = (
        select(InsiderTrade)
        .where(InsiderTrade.trade_date >= since)
        .order_by(desc(InsiderTrade.filing_date))
    )

    if sector:
        query = query.where(InsiderTrade.sector == sector)
    if trade_type:
        query = query.where(InsiderTrade.trade_type == trade_type)

    query = query.limit(100)
    result = await session.execute(query)
    trades = result.scalars().all()

    # Get sector counts for the filter sidebar
    sector_query = (
        select(InsiderTrade.sector, func.count(InsiderTrade.id))
        .where(InsiderTrade.trade_date >= since)
        .where(InsiderTrade.sector.isnot(None))
        .group_by(InsiderTrade.sector)
        .order_by(desc(func.count(InsiderTrade.id)))
    )
    sector_result = await session.execute(sector_query)
    sector_counts = sector_result.all()

    # Summary stats
    total_trades = await session.execute(
        select(func.count(InsiderTrade.id)).where(InsiderTrade.trade_date >= since)
    )
    total_buys = await session.execute(
        select(func.count(InsiderTrade.id))
        .where(InsiderTrade.trade_date >= since)
        .where(InsiderTrade.trade_type == "BUY")
    )
    total_sells = await session.execute(
        select(func.count(InsiderTrade.id))
        .where(InsiderTrade.trade_date >= since)
        .where(InsiderTrade.trade_type == "SELL")
    )

    stats = {
        "total": total_trades.scalar() or 0,
        "buys": total_buys.scalar() or 0,
        "sells": total_sells.scalar() or 0,
    }

    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "trades": trades,
            "sector_counts": sector_counts,
            "stats": stats,
            "filters": {
                "sector": sector,
                "trade_type": trade_type,
                "days": days,
            },
        },
    )


@router.get("/api/trades", response_class=HTMLResponse)
async def trades_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
    sector: str | None = None,
    trade_type: str | None = None,
    days: int = Query(default=7, ge=1, le=90),
):
    """HTMX partial: returns just the trades table rows."""
    since = date.today() - timedelta(days=days)

    query = (
        select(InsiderTrade)
        .where(InsiderTrade.trade_date >= since)
        .order_by(desc(InsiderTrade.filing_date))
    )

    if sector:
        query = query.where(InsiderTrade.sector == sector)
    if trade_type:
        query = query.where(InsiderTrade.trade_type == trade_type)

    query = query.limit(100)
    result = await session.execute(query)
    trades = result.scalars().all()

    return templates.TemplateResponse(
        request,
        "components/trades_table.html",
        {"trades": trades},
    )
