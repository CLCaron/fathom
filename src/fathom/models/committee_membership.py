from datetime import date

from sqlalchemy import JSON, Date, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from fathom.models import Base


class CommitteeMembership(Base):
    __tablename__ = "committee_memberships"

    id: Mapped[int] = mapped_column(primary_key=True)
    member_name: Mapped[str] = mapped_column(String(255), index=True)
    chamber: Mapped[str] = mapped_column(String(10))
    committee_code: Mapped[str | None] = mapped_column(String(20), index=True)
    committee_name: Mapped[str] = mapped_column(String(255))
    role: Mapped[str | None] = mapped_column(String(50))  # CHAIR, RANKING_MEMBER, MEMBER
    congress_number: Mapped[int | None] = mapped_column()
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    sectors_covered: Mapped[list | None] = mapped_column(JSON, default=list)

    __table_args__ = (
        UniqueConstraint(
            "member_name", "committee_code", "congress_number",
            name="uq_committee_membership",
        ),
    )
