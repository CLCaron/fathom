"""Tests for async resolve_sector with SectorCache and yfinance fallback."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

from sqlalchemy import select

from fathom.engine.pipeline import resolve_sector
from fathom.models.sector_cache import SectorCache


class TestResolveSectorHardcodedMap:
    async def test_known_ticker_no_db_hit(self, db_session):
        """Tickers in SECTOR_MAP should return immediately without DB access."""
        with patch("fathom.engine.pipeline._lookup_sector_yfinance") as mock_yf:
            result = await resolve_sector(db_session, "AAPL")

        assert result == "Technology"
        mock_yf.assert_not_called()

    async def test_known_ticker_case_insensitive(self, db_session):
        with patch("fathom.engine.pipeline._lookup_sector_yfinance") as mock_yf:
            result = await resolve_sector(db_session, "nvda")

        assert result == "Technology"
        mock_yf.assert_not_called()

    async def test_none_ticker(self, db_session):
        result = await resolve_sector(db_session, None)
        assert result is None


class TestResolveSectorCache:
    async def test_cache_hit_fresh(self, db_session):
        """Ticker in SectorCache within TTL should be returned without yfinance."""
        db_session.add(SectorCache(
            ticker="NEWCO",
            sector="Robotics",
            source="yfinance",
            fetched_at=datetime.utcnow(),
        ))
        await db_session.flush()

        with patch("fathom.engine.pipeline._lookup_sector_yfinance") as mock_yf:
            result = await resolve_sector(db_session, "NEWCO")

        assert result == "Robotics"
        mock_yf.assert_not_called()

    async def test_cache_hit_stale_triggers_refresh(self, db_session):
        """Stale cache entry should trigger a new yfinance lookup."""
        db_session.add(SectorCache(
            ticker="OLDCO",
            sector="OldSector",
            source="yfinance",
            fetched_at=datetime.utcnow() - timedelta(days=60),
        ))
        await db_session.flush()

        with patch("fathom.engine.pipeline._lookup_sector_yfinance", new_callable=AsyncMock, return_value="NewSector") as mock_yf:
            result = await resolve_sector(db_session, "OLDCO")

        assert result == "NewSector"
        mock_yf.assert_called_once_with("OLDCO")


class TestResolveSectorYfinanceFallback:
    async def test_yfinance_called_for_unknown_ticker(self, db_session):
        """Unknown ticker not in map or cache should call yfinance."""
        with patch("fathom.engine.pipeline._lookup_sector_yfinance", new_callable=AsyncMock, return_value="Technology") as mock_yf:
            result = await resolve_sector(db_session, "UNKN")

        assert result == "Technology"
        mock_yf.assert_called_once_with("UNKN")

    async def test_yfinance_result_cached(self, db_session):
        """yfinance result should be stored in SectorCache."""
        with patch("fathom.engine.pipeline._lookup_sector_yfinance", new_callable=AsyncMock, return_value="Finance"):
            await resolve_sector(db_session, "CACHR")
            await db_session.flush()

        cached = await db_session.execute(
            select(SectorCache).where(SectorCache.ticker == "CACHR")
        )
        entry = cached.scalar_one_or_none()
        assert entry is not None
        assert entry.sector == "Finance"
        assert entry.source == "yfinance"

    async def test_none_sector_is_cached(self, db_session):
        """None sector from yfinance should be cached to avoid repeated lookups."""
        with patch("fathom.engine.pipeline._lookup_sector_yfinance", new_callable=AsyncMock, return_value=None):
            result = await resolve_sector(db_session, "NOINFO")
            await db_session.flush()

        assert result is None

        cached = await db_session.execute(
            select(SectorCache).where(SectorCache.ticker == "NOINFO")
        )
        entry = cached.scalar_one_or_none()
        assert entry is not None
        assert entry.sector is None

    async def test_cached_none_not_refreshed_within_ttl(self, db_session):
        """Cached None sector within TTL should not trigger another yfinance call."""
        db_session.add(SectorCache(
            ticker="STILLNONE",
            sector=None,
            source="yfinance",
            fetched_at=datetime.utcnow(),
        ))
        await db_session.flush()

        with patch("fathom.engine.pipeline._lookup_sector_yfinance") as mock_yf:
            result = await resolve_sector(db_session, "STILLNONE")

        assert result is None
        mock_yf.assert_not_called()
