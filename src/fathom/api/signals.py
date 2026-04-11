"""Signal feed and insider trade API routes."""

from datetime import date, timedelta

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, func, desc, and_
from sqlalchemy.ext.asyncio import AsyncSession

from fathom.config import settings
from fathom.database import get_session
from fathom.models.insider_trade import InsiderTrade
from fathom.models.signal import Signal

router = APIRouter()
templates = Jinja2Templates(directory="src/fathom/templates")

SIGNAL_TYPE_LABELS = {
    "COMMITTEE_TRADE": "Committee Overlap",
    "LEGISLATION_TIMING": "Legislation Timing",
}


@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    session: AsyncSession = Depends(get_session),
    sector: str | None = None,
    trade_type: str | None = None,
    days: int = Query(default=7, ge=1, le=settings.dashboard_max_days),
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

    query = query.limit(settings.dashboard_trade_limit)
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
    days: int = Query(default=7, ge=1, le=settings.dashboard_max_days),
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

    query = query.limit(settings.dashboard_trade_limit)
    result = await session.execute(query)
    trades = result.scalars().all()

    return templates.TemplateResponse(
        request,
        "components/trades_table.html",
        {"trades": trades},
    )


@router.get("/signals", response_class=HTMLResponse)
async def signals_feed(
    request: Request,
    session: AsyncSession = Depends(get_session),
    signal_type: str | None = None,
    sector: str | None = None,
    min_confidence: int = Query(default=0, ge=0, le=100),
    days: int = Query(default=30, ge=1, le=settings.dashboard_max_days),
    show_candidates: bool = False,
):
    """Signal feed page showing correlated signals."""
    since = date.today() - timedelta(days=days)

    query = (
        select(Signal)
        .where(Signal.detected_at >= since)
        .order_by(desc(Signal.confidence), desc(Signal.detected_at))
    )

    if signal_type:
        query = query.where(Signal.signal_type == signal_type)
    if sector:
        query = query.where(Signal.sector == sector)
    if not show_candidates:
        query = query.where(Signal.confidence >= settings.min_confidence)
    if min_confidence > 0:
        query = query.where(Signal.confidence >= min_confidence)

    query = query.limit(200)
    result = await session.execute(query)
    signals = result.scalars().all()

    # Stats
    total_q = select(func.count(Signal.id)).where(Signal.detected_at >= since)
    total = (await session.execute(total_q)).scalar() or 0

    above_threshold_q = total_q.where(Signal.confidence >= settings.min_confidence)
    above_threshold = (await session.execute(above_threshold_q)).scalar() or 0

    # Sector counts for filter
    sector_q = (
        select(Signal.sector, func.count(Signal.id))
        .where(Signal.detected_at >= since)
        .where(Signal.sector.isnot(None))
        .group_by(Signal.sector)
        .order_by(desc(func.count(Signal.id)))
    )
    sector_counts = (await session.execute(sector_q)).all()

    # Signal type counts for filter
    type_q = (
        select(Signal.signal_type, func.count(Signal.id))
        .where(Signal.detected_at >= since)
        .group_by(Signal.signal_type)
        .order_by(desc(func.count(Signal.id)))
    )
    type_counts = (await session.execute(type_q)).all()

    return templates.TemplateResponse(
        request,
        "signals.html",
        {
            "signals": signals,
            "stats": {"total": total, "above_threshold": above_threshold},
            "sector_counts": sector_counts,
            "type_counts": type_counts,
            "type_labels": SIGNAL_TYPE_LABELS,
            "min_confidence_setting": settings.min_confidence,
            "filters": {
                "signal_type": signal_type,
                "sector": sector,
                "min_confidence": min_confidence,
                "days": days,
                "show_candidates": show_candidates,
            },
        },
    )


@router.get("/api/signals/feed", response_class=HTMLResponse)
async def signals_feed_partial(
    request: Request,
    session: AsyncSession = Depends(get_session),
    signal_type: str | None = None,
    sector: str | None = None,
    min_confidence: int = Query(default=0, ge=0, le=100),
    days: int = Query(default=30, ge=1, le=settings.dashboard_max_days),
    show_candidates: bool = False,
):
    """HTMX partial: returns just the signal cards."""
    since = date.today() - timedelta(days=days)

    query = (
        select(Signal)
        .where(Signal.detected_at >= since)
        .order_by(desc(Signal.confidence), desc(Signal.detected_at))
    )

    if signal_type:
        query = query.where(Signal.signal_type == signal_type)
    if sector:
        query = query.where(Signal.sector == sector)
    if not show_candidates:
        query = query.where(Signal.confidence >= settings.min_confidence)
    if min_confidence > 0:
        query = query.where(Signal.confidence >= min_confidence)

    query = query.limit(200)
    result = await session.execute(query)
    signals = result.scalars().all()

    return templates.TemplateResponse(
        request,
        "components/signal_cards.html",
        {
            "signals": signals,
            "type_labels": SIGNAL_TYPE_LABELS,
            "min_confidence_setting": settings.min_confidence,
        },
    )
