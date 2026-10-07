"""predictions: split slot (production/candidate) out of model_version

model_version был перегружен двумя разными смыслами: (1) дискриминатор
слота production/candidate в PK, по которому фильтрует весь read-путь
(pred_history, metrics.*, actualizer — WHERE model_version = 'production'),
и (2) реальная версия обученной модели для трейсабилити. Пока VDS2 не
присылал версию, оба смысла случайно совпадали (production-опрос всегда
писал литерал 'production'). Как только VDS2 начал реально присылать
model_version в ответе, продовые строки стали писаться под настоящей
версией — и переставали совпадать с литералом 'production' в фильтрах,
из-за чего прогнозы пропадали из pred_history/metrics для уже закрытых
свечей.

Здесь заводим отдельную колонку slot ('production' | 'candidate') — она
и так известна на стороне predictor.py в момент опроса (какой URL дёрнули
на VDS2), делаем её частью PK вместо model_version. model_version остаётся
обычной (не PK, nullable) колонкой — реальная версия модели, только для
admin.compare.

Backfill: UPDATE ... SET slot = 'production' для всех существующих строк.
Это безопасно именно в этом деплое — ML_SHADOW_ENABLED всегда был false
здесь, то есть poll_symbol_shadow ни разу не писал в БД, все имеющиеся
строки гарантированно из production-опроса, независимо от того, что у них
записано в старом model_version (литерал 'production' или уже настоящая
версия — на слот это не влияет).

Revision ID: d2a6f9c1b4e8
Revises: b3e7d1a9c5f2
Create Date: 2026-08-03 18:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd2a6f9c1b4e8'
down_revision: Union[str, None] = 'b3e7d1a9c5f2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('predictions', sa.Column('slot', sa.String(length=16), nullable=True))
    op.execute("UPDATE predictions SET slot = 'production' WHERE slot IS NULL")
    op.alter_column('predictions', 'slot', existing_type=sa.String(length=16), nullable=False)

    op.drop_constraint('predictions_pkey', 'predictions', type_='primary')
    op.create_primary_key(
        'predictions_pkey', 'predictions',
        ['symbol', 'as_of_ts', 'horizon', 'slot'],
    )

    # model_version больше не в PK — чисто информационная, разрешаем NULL
    # (на случай если VDS2 когда-то её не пришлёт).
    op.alter_column('predictions', 'model_version', existing_type=sa.String(length=32), nullable=True)


def downgrade() -> None:
    op.alter_column('predictions', 'model_version', existing_type=sa.String(length=32), nullable=False)

    op.drop_constraint('predictions_pkey', 'predictions', type_='primary')
    op.create_primary_key(
        'predictions_pkey', 'predictions',
        ['symbol', 'as_of_ts', 'horizon', 'model_version'],
    )

    op.drop_column('predictions', 'slot')
