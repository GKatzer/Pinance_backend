"""predictions table (per-horizon, VDS2 pull)

Revision ID: d4e8f1a72b9c
Revises: c7106c5afbcd
Create Date: 2026-07-23 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd4e8f1a72b9c'
down_revision: Union[str, None] = 'c7106c5afbcd'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'predictions',
        sa.Column('symbol', sa.String(length=16), nullable=False),
        sa.Column('ts_candle', sa.DateTime(), nullable=False),
        sa.Column('horizon', sa.SmallInteger(), nullable=False),
        sa.Column('target_ts', sa.DateTime(), nullable=False),
        sa.Column('close_at_predict', sa.Float(), nullable=False),
        sa.Column('r_pred', sa.Float(), nullable=False),
        sa.Column('price_pred', sa.Float(), nullable=False),
        sa.Column('fetched_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('actual_price', sa.Float(), nullable=True),
        sa.Column('actual_r', sa.Float(), nullable=True),
        sa.Column('hit', sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint('symbol', 'ts_candle', 'horizon'),
    )
    op.create_index(
        'ix_predictions_symbol_ts_desc', 'predictions',
        ['symbol', 'ts_candle'], unique=False, postgresql_using='btree',
    )
    op.create_index(
        'ix_predictions_target_ts', 'predictions',
        ['symbol', 'target_ts'], unique=False,
    )

    op.execute("""
        SELECT create_hypertable(
            'predictions', 'ts_candle',
            chunk_time_interval => INTERVAL '7 days',
            if_not_exists => TRUE
        )
    """)
    op.execute("""
        SELECT add_retention_policy('predictions', INTERVAL '90 days', if_not_exists => TRUE)
    """)


def downgrade() -> None:
    op.drop_index('ix_predictions_target_ts', table_name='predictions')
    op.drop_index('ix_predictions_symbol_ts_desc', table_name='predictions', postgresql_using='btree')
    op.drop_table('predictions')
