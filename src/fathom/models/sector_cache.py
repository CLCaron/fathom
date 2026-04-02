from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class SectorCache(Base):
    __tablename__ = "sector_cache"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), unique=True, index=True)
    sector: Mapped[str | None] = mapped_column(String(100))
    source: Mapped[str] = mapped_column(String(20))  # "hardcoded" or "yfinance"
    fetched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
