"""Admin routes for manual scraper control and status."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from fathom.database import async_session
from fathom.models.committee_membership import CommitteeMembership
from fathom.models.congressional_trade import CongressionalTrade
from fathom.models.insider_trade import InsiderTrade
from fathom.models.legislation import Legislation
from fathom.models.signal import Signal
from fathom.models.stock_price import StockPrice
from fathom.scheduler.jobs import run_job_now

from sqlalchemy import select, func

router = APIRouter(prefix="/admin")
templates = Jinja2Templates(directory="src/fathom/templates")


@router.get("", response_class=HTMLResponse)
async def admin_page(request: Request):
    """Admin dashboard with scraper status and controls."""
    async with async_session() as session:
        trade_count = await session.execute(select(func.count(InsiderTrade.id)))
        price_count = await session.execute(select(func.count(StockPrice.id)))
        congress_count = await session.execute(select(func.count(CongressionalTrade.id)))
        committee_count = await session.execute(select(func.count(CommitteeMembership.id)))
        legislation_count = await session.execute(select(func.count(Legislation.id)))
        signal_count = await session.execute(select(func.count(Signal.id)))

    db_stats = {
        "insider_trades": trade_count.scalar() or 0,
        "stock_prices": price_count.scalar() or 0,
        "congressional_trades": congress_count.scalar() or 0,
        "committee_memberships": committee_count.scalar() or 0,
        "legislation": legislation_count.scalar() or 0,
        "signals": signal_count.scalar() or 0,
    }

    return templates.TemplateResponse(
        request,
        "admin.html",
        {"db_stats": db_stats},
    )


@router.post("/run/{job_id}", response_class=HTMLResponse)
async def trigger_job(job_id: str):
    """Manually trigger a scraper job. Returns an HTML fragment for HTMX."""
    try:
        result = await run_job_now(job_id)
        return f'<span class="scraper-success">&#10003; {result}</span>'
    except Exception as e:
        return f'<span class="scraper-error">&#10007; Error: {e}</span>'
