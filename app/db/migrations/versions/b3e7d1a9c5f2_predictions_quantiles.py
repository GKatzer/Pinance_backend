"""predictions: quantile columns (r_q10, r_q90, price_q10, price_q90)

VDS2 (с 2026-08-02) добавляет 10%/90% квантили к каждому горизонту, в
дополнение к существующим r_pred/price_pred (аддитивный контракт, ничего
старого не меняется). Nullable — строки, записанные до этой миграции,
честно не имеют этих данных, бэкофилл фиктивным значением был бы
нечестным (см. app/db/models.py). Для всех новых строк VDS2 гарантированно
их присылает.

Revision ID: b3e7d1a9c5f2
Revises: a7b2e5f9c3d1
Create Date: 2026-08-02 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b3e7d1a9c5f2'
down_revision: Union[str, None] = 'a7b2e5f9c3d1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('predictions', sa.Column('r_q10', sa.Float(), nullable=True))
    op.add_column('predictions', sa.Column('r_q90', sa.Float(), nullable=True))
    op.add_column('predictions', sa.Column('price_q10', sa.Float(), nullable=True))
    op.add_column('predictions', sa.Column('price_q90', sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column('predictions', 'price_q90')
    op.drop_column('predictions', 'price_q10')
    op.drop_column('predictions', 'r_q90')
    op.drop_column('predictions', 'r_q10')
