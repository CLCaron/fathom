"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from fathom.api.signals import router as signals_router
from fathom.api.admin import router as admin_router
from fathom.config import settings
from fathom.database import engine
from fathom.models import Base
from fathom.scheduler.jobs import scheduler, setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Create tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables ready")

    # Start the scheduler
    setup_scheduler()
    scheduler.start()
    logger.info("Scheduler started")

    yield

    # Shutdown
    scheduler.shutdown()
    await engine.dispose()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Fathom",
    description="Financial research tool connecting insider trades, congressional trades, and market events to historical patterns",
    lifespan=lifespan,
)

# Mount static files
app.mount("/static", StaticFiles(directory="src/fathom/static"), name="static")

# Include routers
app.include_router(signals_router)
app.include_router(admin_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "fathom.main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
    )
