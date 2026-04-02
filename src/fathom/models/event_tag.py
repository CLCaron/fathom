from datetime import date

from sqlalchemy import JSON, Date, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class EventTag(Base):
    __tablename__ = "event_tags"

    id: Mapped[int] = mapped_column(primary_key=True)
    event_name: Mapped[str] = mapped_column(String(255))
    event_type: Mapped[str] = mapped_column(String(50))  # WAR, LEGISLATION, FED_ACTION, CRISIS, ELECTION
    start_date: Mapped[date] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    sectors_affected: Mapped[list | None] = mapped_column(JSON, default=list)
    description: Mapped[str | None] = mapped_column(Text)
