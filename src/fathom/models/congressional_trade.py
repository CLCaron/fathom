from datetime import date, datetime

from sqlalchemy import Date, DateTime, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class CongressionalTrade(Base):
    __tablename__ = "congressional_trades"

    id: Mapped[int] = mapped_column(primary_key=True)
    member_name: Mapped[str] = mapped_column(String(255), index=True)
    chamber: Mapped[str] = mapped_column(String(10))  # HOUSE, SENATE
    state: Mapped[str | None] = mapped_column(String(2))
    party: Mapped[str | None] = mapped_column(String(20))
    ticker: Mapped[str | None] = mapped_column(String(10), index=True)
    asset_name: Mapped[str | None] = mapped_column(String(255))
    trade_type: Mapped[str] = mapped_column(String(10))  # PURCHASE, SALE
    amount_min: Mapped[float | None] = mapped_column(Numeric(16, 2))
    amount_max: Mapped[float | None] = mapped_column(Numeric(16, 2))
    trade_date: Mapped[date] = mapped_column(Date, index=True)
    disclosure_date: Mapped[date | None] = mapped_column(Date)
    source_url: Mapped[str | None] = mapped_column(String(512))
    sector: Mapped[str | None] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("member_name", "ticker", "trade_date", "trade_type", "amount_min", name="uq_congress_trade"),
    )
