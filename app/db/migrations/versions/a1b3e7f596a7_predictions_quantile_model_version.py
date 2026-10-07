"""predictions: quantile_model_version column

VDS2 версионирует point-модель и квантильную корзину независимо друг от
друга (раздельные MinIO-артефакты, раздельный promote_candidate_point /
promote_candidate_quantiles на стороне Pinance_ML — см. model_storage.py
там). До сих пор `/predict/{symbol}` отдавал наружу только одну версию
(`model_version`) на весь ответ, так что квантильная корзина не имела
собственного трейсабилити в БД —q10_coverage/q90_coverage в admin.compare
были вынужденно (неверно) сгруппированы по point-версии, хотя корзина
могла обновиться independently (см. Pinance_ML/scripts/promote_if_better.py
docstring, "a genuinely independent quantile-only sample count needs
schema changes on predictor-backend's side").

Аддитивная колонка, по образцу model_version (см. b3e7d1a9c5f2): не в PK,
nullable — старые строки честно не имеют этих данных (записаны до того,
как VDS2 начал присылать quantile_model_version отдельным полем), и
опциональна per-модель так же, как сами квантили (point-only слот шлёт
NULL).

Revision ID: a1b3e7f596a7
Revises: d2a6f9c1b4e8
Create Date: 2026-08-14 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'a1b3e7f596a7'
down_revision: Union[str, None] = 'd2a6f9c1b4e8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('predictions', sa.Column('quantile_model_version', sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column('predictions', 'quantile_model_version')
