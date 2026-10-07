"""retrain events and inference ms

retrain_events — лог решений ретрейна (см. app/db/models.py RetrainEvent),
пишется через POST /admin/retrain-events из Pinance_ML. inference_ms —
аддитивная nullable-колонка в predictions, по тому же образцу, что
quantile_model_version (см. миграцию a1b3e7f596a7).

autogenerate заодно поймал 'candles_ts_idx'/'predictions_as_of_ts_idx' как
"лишние" индексы на drop — это НЕ так, это внутренние индексы TimescaleDB
(создаются `create_hypertable()` в c7106c5afbcd, не декларированы в
SQLAlchemy-моделях, поэтому autogenerate их не видит и считает чужими).
Дропать их — реальная регрессия производительности hypertable-сканов,
вырезано из upgrade()/downgrade() руками, оставлен только retrain_events
и inference_ms.

Revision ID: 73f92880f1bb
Revises: a1b3e7f596a7
Create Date: 2026-08-16 14:01:59.228933

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = '73f92880f1bb'
down_revision: Union[str, None] = 'a1b3e7f596a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table('retrain_events',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('kind', sa.String(length=16), nullable=False),
    sa.Column('symbol', sa.String(length=16), nullable=False),
    sa.Column('candidate_version', sa.String(length=32), nullable=True),
    sa.Column('production_version', sa.String(length=32), nullable=True),
    sa.Column('decision', sa.String(length=16), nullable=False),
    sa.Column('metric_name', sa.String(length=32), nullable=True),
    sa.Column('candidate_value', sa.Float(), nullable=True),
    sa.Column('production_value', sa.Float(), nullable=True),
    sa.Column('threshold', sa.Float(), nullable=True),
    sa.Column('n_samples', sa.Integer(), nullable=True),
    sa.Column('train_wall_seconds', sa.Float(), nullable=True),
    sa.Column('decided_at', sa.DateTime(), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_retrain_events_symbol_kind_decided', 'retrain_events', ['symbol', 'kind', 'decided_at'], unique=False)
    op.add_column('predictions', sa.Column('inference_ms', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('predictions', 'inference_ms')
    op.drop_index('ix_retrain_events_symbol_kind_decided', table_name='retrain_events')
    op.drop_table('retrain_events')
