"""Tests for database engine and session configuration."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from fathom.database import engine, async_session, get_session


class TestEngine:
    def test_engine_is_async(self):
        """Database engine should be an AsyncEngine."""
        assert isinstance(engine, AsyncEngine)

    def test_engine_uses_sqlite(self):
        """Engine URL should use SQLite."""
        url = str(engine.url)
        assert "sqlite" in url


class TestSession:
    async def test_session_yields_async_session(self):
        """async_session() should yield an AsyncSession."""
        async with async_session() as session:
            assert isinstance(session, AsyncSession)

    async def test_get_session_yields_async_session(self):
        """get_session() dependency should yield an AsyncSession."""
        gen = get_session()
        session = await gen.__anext__()
        assert isinstance(session, AsyncSession)
        # Clean up
        try:
            await gen.aclose()
        except Exception:
            pass


class TestSQLitePragmas:
    async def test_wal_mode_enabled(self):
        """SQLite should use WAL journal mode."""
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql("PRAGMA journal_mode")
            row = result.fetchone()
        assert row[0] == "wal"

    async def test_foreign_keys_enabled(self):
        """SQLite foreign key enforcement should be ON."""
        async with engine.connect() as conn:
            result = await conn.exec_driver_sql("PRAGMA foreign_keys")
            row = result.fetchone()
        assert row[0] == 1
