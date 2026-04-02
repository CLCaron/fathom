"""add sector cache and model fixes

Revision ID: d7e6201ea915
Revises: c0962a77e45b
Create Date: 2026-04-02 13:30:57.530893

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'd7e6201ea915'
down_revision: Union[str, None] = 'c0962a77e45b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # sector_cache may already exist on databases created before this migration
    bind = op.get_bind()
    existing_tables = bind.dialect.get_table_names(bind)
    if 'sector_cache' not in existing_tables:
        op.create_table('sector_cache',
            sa.Column('id', sa.Integer(), nullable=False),
            sa.Column('ticker', sa.String(length=10), nullable=False),
            sa.Column('sector', sa.String(length=100), nullable=True),
            sa.Column('source', sa.String(length=20), nullable=False),
            sa.Column('fetched_at', sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint('id')
        )
        op.create_index(op.f('ix_sector_cache_ticker'), 'sector_cache', ['ticker'], unique=True)

    # New composite indexes
    op.create_index('ix_congress_member_ticker', 'congressional_trades', ['member_name', 'ticker'], unique=False)
    op.create_index('ix_legvote_bill_member', 'legislation_votes', ['bill_id', 'member_name'], unique=False)
    op.create_index('ix_sigperf_signal_date', 'signal_performance', ['signal_id', 'check_date'], unique=False)

    # Batch mode for SQLite constraint changes
    with op.batch_alter_table('committee_memberships') as batch_op:
        batch_op.create_unique_constraint(
            'uq_committee_membership', ['member_name', 'committee_code', 'congress_number']
        )

    with op.batch_alter_table('legislation_votes') as batch_op:
        batch_op.create_foreign_key(
            'fk_legvote_bill_id', 'legislation', ['bill_id'], ['bill_id']
        )


def downgrade() -> None:
    with op.batch_alter_table('legislation_votes') as batch_op:
        batch_op.drop_constraint('fk_legvote_bill_id', type_='foreignkey')

    with op.batch_alter_table('committee_memberships') as batch_op:
        batch_op.drop_constraint('uq_committee_membership', type_='unique')

    op.drop_index('ix_sigperf_signal_date', table_name='signal_performance')
    op.drop_index('ix_legvote_bill_member', table_name='legislation_votes')
    op.drop_index('ix_congress_member_ticker', table_name='congressional_trades')

    bind = op.get_bind()
    existing_tables = bind.dialect.get_table_names(bind)
    if 'sector_cache' in existing_tables:
        op.drop_index(op.f('ix_sector_cache_ticker'), table_name='sector_cache')
        op.drop_table('sector_cache')
