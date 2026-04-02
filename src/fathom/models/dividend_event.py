from datetime import date

from sqlalchemy import Date, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class DividendEvent(Base):
    __tablename__ = "dividend_events"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    ex_date: Mapped[date] = mapped_column(Date)
    pay_date: Mapped[date | None] = mapped_column(Date)
    amount: Mapped[float | None] = mapped_column(Numeric(10, 4))
    frequency: Mapped[str | None] = mapped_column(String(20))
    yield_at_announce: Mapped[float | None] = mapped_column(Numeric(8, 4))

    __table_args__ = (
        UniqueConstraint("ticker", "ex_date", "amount", name="uq_dividend_event"),
    )
