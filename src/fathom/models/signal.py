from datetime import datetime

from sqlalchemy import JSON, DateTime, Numeric, String
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class Signal(Base):
    __tablename__ = "signals"

    id: Mapped[int] = mapped_column(primary_key=True)
    signal_type: Mapped[str] = mapped_column(String(30), index=True)
    ticker: Mapped[str | None] = mapped_column(String(10))
    sector: Mapped[str | None] = mapped_column(String(100))
    headline: Mapped[str] = mapped_column(String(512))
    confidence: Mapped[float] = mapped_column(Numeric(5, 2), index=True)
    details: Mapped[dict | None] = mapped_column(JSON, default=dict)
    source_trade_ids: Mapped[list | None] = mapped_column(JSON, default=list)
    detected_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    price_at_detection: Mapped[float | None] = mapped_column(Numeric(12, 4))
    sector_etf_price: Mapped[float | None] = mapped_column(Numeric(12, 4))
