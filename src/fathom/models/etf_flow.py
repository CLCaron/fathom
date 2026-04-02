from datetime import date

from sqlalchemy import Date, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class EtfFlow(Base):
    __tablename__ = "etf_flows"

    id: Mapped[int] = mapped_column(primary_key=True)
    etf_ticker: Mapped[str] = mapped_column(String(10), index=True)
    etf_name: Mapped[str | None] = mapped_column(String(255))
    sector: Mapped[str | None] = mapped_column(String(100), index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    flow_amount: Mapped[float | None] = mapped_column(Numeric(16, 2))
    aum: Mapped[float | None] = mapped_column(Numeric(20, 2))
    flow_pct: Mapped[float | None] = mapped_column(Numeric(8, 4))

    __table_args__ = (
        UniqueConstraint("etf_ticker", "date", name="uq_etf_flow"),
    )
