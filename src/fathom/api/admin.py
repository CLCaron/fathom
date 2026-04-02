"""Admin routes for manual scraper control and status."""

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from fathom.database import async_session
from fathom.models.insider_trade import InsiderTrade
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

    db_stats = {
        "insider_trades": trade_count.scalar() or 0,
        "stock_prices": price_count.scalar() or 0,
    }

    return templates.TemplateResponse(
        request,
        "admin.html",
        {"db_stats": db_stats},
    )


@router.post("/run/{job_id}")
async def trigger_job(job_id: str):
    """Manually trigger a scraper job."""
    result = await run_job_now(job_id)
    return {"status": "ok", "message": result}
