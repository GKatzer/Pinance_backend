"""timescaledb hypertable

Revision ID: c7106c5afbcd
Revises: c1ca7834ee2b
Create Date: 2026-05-14 10:46:29.665889

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c7106c5afbcd'
down_revision: Union[str, None] = 'c1ca7834ee2b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
    op.execute("""
        SELECT create_hypertable(
            'candles', 'ts',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        )
    """)
    op.execute("""
        SELECT add_retention_policy('candles', INTERVAL '90 days', if_not_exists => TRUE)
    """)


def downgrade() -> None:
    op.execute("DROP EXTENSION IF EXISTS timescaledb CASCADE")
