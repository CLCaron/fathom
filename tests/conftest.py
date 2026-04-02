"""Shared test fixtures: in-memory async DB, FastAPI test client."""

import pytest
from datetime import date, datetime
from unittest.mock import AsyncMock

from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from fathom.models import Base
from fathom.models.insider_trade import InsiderTrade


@pytest.fixture
async def db_engine():
    """Create an in-memory SQLite async engine for tests."""
    engine = create_async_engine("sqlite+aiosqlite://", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def db_session(db_engine):
    """Yield an async session that rolls back after each test."""
    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()


@pytest.fixture
async def test_client(db_engine):
    """FastAPI async test client with DB dependency override."""
    from fathom.main import app
    from fathom.database import get_session

    session_factory = async_sessionmaker(db_engine, class_=AsyncSession, expire_on_commit=False)

    async def override_get_session():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def sample_insider_trade():
    """A sample InsiderTrade ORM instance for testing."""
    return InsiderTrade(
        cik="320193",
        filer_name="Tim Cook",
        filer_title="CEO",
        company_name="Apple Inc.",
        ticker="AAPL",
        trade_type="SELL",
        shares=50000,
        price_per_share=185.50,
        total_value=9275000.00,
        trade_date=date.today(),
        filing_date=datetime.utcnow(),
        filing_url="https://www.sec.gov/Archives/edgar/data/320193/test.xml",
        sector="Technology",
    )
