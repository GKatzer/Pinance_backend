"""predictions: join-on-read (drop denormalized actual_*/close_at_predict)

Таблица уже создана и применена (d4e8f1a72b9c) — это не альтер, а пересоздание
с более простой схемой: убираем actual_price/actual_r/hit/close_at_predict/
fetched_at, переименовываем ts_candle -> as_of_ts, fetched_at -> created_at,
добавляем model_version (nullable, VDS2 пока его не шлёт).

Данные в таблице на момент этой миграции — тестовые (пайплайн только что
заработал), пересоздание безопасно.

Revision ID: f3c7a9e2b1d4
Revises: e9a1f4c7d2b6
Create Date: 2026-07-25 19:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f3c7a9e2b1d4'
down_revision: Union[str, None] = 'e9a1f4c7d2b6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("DROP TABLE IF EXISTS predictions CASCADE")

    op.create_table(
        'predictions',
        sa.Column('symbol', sa.String(length=16), nullable=False),
        sa.Column('as_of_ts', sa.DateTime(), nullable=False),
        sa.Column('horizon', sa.SmallInteger(), nullable=False),
        sa.Column('target_ts', sa.DateTime(), nullable=False),
        sa.Column('r_pred', sa.Float(), nullable=False),
        sa.Column('price_pred', sa.Float(), nullable=False),
        sa.Column('model_version', sa.String(length=32), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('symbol', 'as_of_ts', 'horizon'),
    )
    op.create_index(
        'ix_predictions_symbol_as_of_desc', 'predictions',
        ['symbol', 'as_of_ts'], unique=False, postgresql_using='btree',
    )
    op.create_index(
        'ix_predictions_target_ts', 'predictions',
        ['symbol', 'target_ts'], unique=False,
    )

    op.execute("""
        SELECT create_hypertable(
            'predictions', 'as_of_ts',
            chunk_time_interval => INTERVAL '7 days',
            if_not_exists => TRUE
        )
    """)
    op.execute("""
        SELECT add_retention_policy('predictions', INTERVAL '90 days', if_not_exists => TRUE)
    """)


def downgrade() -> None:
    op.drop_index('ix_predictions_target_ts', table_name='predictions')
    op.drop_index('ix_predictions_symbol_as_of_desc', table_name='predictions', postgresql_using='btree')
    op.drop_table('predictions')
