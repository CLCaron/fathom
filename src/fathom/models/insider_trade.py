from datetime import date, datetime

from sqlalchemy import Date, DateTime, Index, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class InsiderTrade(Base):
    __tablename__ = "insider_trades"

    id: Mapped[int] = mapped_column(primary_key=True)
    cik: Mapped[str] = mapped_column(String(20))
    filer_name: Mapped[str] = mapped_column(String(255))
    filer_title: Mapped[str | None] = mapped_column(String(100))
    company_name: Mapped[str] = mapped_column(String(255))
    ticker: Mapped[str | None] = mapped_column(String(10), index=True)
    trade_type: Mapped[str] = mapped_column(String(10))  # BUY, SELL, EXERCISE
    shares: Mapped[int | None] = mapped_column()
    price_per_share: Mapped[float | None] = mapped_column(Numeric(12, 4))
    total_value: Mapped[float | None] = mapped_column(Numeric(16, 2))
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    filing_date: Mapped[datetime] = mapped_column(DateTime)
    filing_url: Mapped[str | None] = mapped_column(String(512))
    sector: Mapped[str | None] = mapped_column(String(100), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("cik", "ticker", "trade_date", "trade_type", "shares", name="uq_insider_trade"),
    )
