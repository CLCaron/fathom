from datetime import date

from sqlalchemy import BigInteger, Date, Numeric, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class StockPrice(Base):
    __tablename__ = "stock_prices"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), index=True)
    date: Mapped[date] = mapped_column(Date, index=True)
    open: Mapped[float | None] = mapped_column(Numeric(12, 4))
    high: Mapped[float | None] = mapped_column(Numeric(12, 4))
    low: Mapped[float | None] = mapped_column(Numeric(12, 4))
    close: Mapped[float | None] = mapped_column(Numeric(12, 4))
    adj_close: Mapped[float | None] = mapped_column(Numeric(12, 4))
    volume: Mapped[int | None] = mapped_column(BigInteger)

    __table_args__ = (
        UniqueConstraint("ticker", "date", name="uq_stock_price"),
    )
