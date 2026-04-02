from datetime import date

from sqlalchemy import JSON, Date, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class Legislation(Base):
    __tablename__ = "legislation"

    id: Mapped[int] = mapped_column(primary_key=True)
    bill_id: Mapped[str] = mapped_column(String(20), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(512))
    summary: Mapped[str | None] = mapped_column(Text)
    congress_number: Mapped[int | None] = mapped_column()
    introduced_date: Mapped[date | None] = mapped_column(Date)
    last_action_date: Mapped[date | None] = mapped_column(Date)
    status: Mapped[str | None] = mapped_column(String(50))
    sectors_affected: Mapped[list | None] = mapped_column(JSON, default=list)
    sponsor_name: Mapped[str | None] = mapped_column(String(255))
    bill_url: Mapped[str | None] = mapped_column(String(512))


class LegislationVote(Base):
    __tablename__ = "legislation_votes"

    id: Mapped[int] = mapped_column(primary_key=True)
    bill_id: Mapped[str] = mapped_column(String(20), index=True)
    member_name: Mapped[str] = mapped_column(String(255), index=True)
    chamber: Mapped[str] = mapped_column(String(10))
    vote: Mapped[str] = mapped_column(String(10))  # YEA, NAY, PRESENT, NOT_VOTING
    vote_date: Mapped[date] = mapped_column(Date)
