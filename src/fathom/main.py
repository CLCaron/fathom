"""FastAPI application entry point."""

import logging
from contextlib import asynccontextmanager

from alembic import command
from alembic.config import Config as AlembicConfig
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from fathom.api.signals import router as signals_router
from fathom.api.admin import router as admin_router
from fathom.api.congressional import router as congressional_router
from fathom.config import settings
from fathom.database import engine
from fathom.scheduler.jobs import scheduler, setup_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def run_migrations():
    """Run Alembic migrations to bring the database schema up to date.

    Must be called from a thread (not the async event loop) because Alembic's
    async env.py uses asyncio.run() internally, which cannot nest inside an
    already-running loop.
    """
    alembic_cfg = AlembicConfig("alembic.ini")
    command.upgrade(alembic_cfg, "head")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Run migrations in a thread to avoid asyncio.run() nesting inside the event loop
    import asyncio
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_migrations)
    logger.info("Database migrations complete")

    if "user@example.com" in settings.sec_edgar_user_agent:
        logger.warning(
            "SEC EDGAR user agent is still the default placeholder. "
            "Set SEC_EDGAR_USER_AGENT in .env to your real contact info."
        )

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
app.include_router(congressional_router)
app.include_router(admin_router)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "fathom.main:app",
        host=settings.host,
        port=settings.port,
        reload=True,
    )
