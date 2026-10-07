"""predictions: model_version into PK (shadow deployment support)

Позволяет production и shadow-кандидату писать в одну таблицу под одним и
тем же (symbol, as_of_ts, horizon), не затирая друг друга — различаются
по model_version. Существующие строки (все писал только production-пайплайн)
бэкофилятся литеральным 'production' перед тем, как колонка становится
NOT NULL и частью PK.

Revision ID: a7b2e5f9c3d1
Revises: f3c7a9e2b1d4
Create Date: 2026-07-26 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a7b2e5f9c3d1'
down_revision: Union[str, None] = 'f3c7a9e2b1d4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("UPDATE predictions SET model_version = 'production' WHERE model_version IS NULL")
    op.alter_column('predictions', 'model_version', existing_type=sa.String(length=32), nullable=False)

    op.drop_constraint('predictions_pkey', 'predictions', type_='primary')
    op.create_primary_key(
        'predictions_pkey', 'predictions',
        ['symbol', 'as_of_ts', 'horizon', 'model_version'],
    )


def downgrade() -> None:
    op.drop_constraint('predictions_pkey', 'predictions', type_='primary')
    op.create_primary_key(
        'predictions_pkey', 'predictions',
        ['symbol', 'as_of_ts', 'horizon'],
    )
    op.alter_column('predictions', 'model_version', existing_type=sa.String(length=32), nullable=True)
