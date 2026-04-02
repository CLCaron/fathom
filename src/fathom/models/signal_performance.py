from datetime import date

from sqlalchemy import Boolean, Date, ForeignKey, Numeric, Text
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class SignalPerformance(Base):
    __tablename__ = "signal_performance"

    id: Mapped[int] = mapped_column(primary_key=True)
    signal_id: Mapped[int] = mapped_column(ForeignKey("signals.id"), index=True)
    check_date: Mapped[date] = mapped_column(Date, index=True)
    days_elapsed: Mapped[int] = mapped_column()
    price_current: Mapped[float | None] = mapped_column(Numeric(12, 4))
    price_change_pct: Mapped[float | None] = mapped_column(Numeric(8, 4))
    sector_change_pct: Mapped[float | None] = mapped_column(Numeric(8, 4))
    outperformed: Mapped[bool | None] = mapped_column(Boolean)
    signal_played_out: Mapped[bool | None] = mapped_column(Boolean)
    notes: Mapped[str | None] = mapped_column(Text)
