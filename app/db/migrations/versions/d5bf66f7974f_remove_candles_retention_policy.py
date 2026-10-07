"""remove candles retention policy

Revision ID: d5bf66f7974f
Revises: c7106c5afbcd
Create Date: 2026-07-21 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd5bf66f7974f'
down_revision: Union[str, None] = 'c7106c5afbcd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("SELECT remove_retention_policy('candles', if_exists => TRUE)")


def downgrade() -> None:
    op.execute("""
        SELECT add_retention_policy('candles', INTERVAL '90 days', if_not_exists => TRUE)
    """)
