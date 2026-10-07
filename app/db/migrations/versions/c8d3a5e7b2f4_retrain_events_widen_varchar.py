"""retrain_events: widen candidate_version/production_version/metric_name to 64

POST /admin/retrain-events started returning 500 with
StringDataRightTruncationError (value too long for type character varying(32))
— Pinance_ML now sends a string longer than 32 chars in one of these three
columns. The failed events were lost (append-only log, nothing stored on
error); the caller has to resend them.

Widening varchar in Postgres is a metadata-only change (no table rewrite),
safe on a live table. Downgrade narrows back to 32 and will fail if rows
longer than that were written in the meantime — intended, not silently
truncating.

Revision ID: c8d3a5e7b2f4
Revises: b6e2c4a8f1d3
Create Date: 2026-10-02T13:20:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'c8d3a5e7b2f4'
down_revision: Union[str, None] = 'b6e2c4a8f1d3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_COLUMNS = ('candidate_version', 'production_version', 'metric_name')


def upgrade() -> None:
    for col in _COLUMNS:
        op.alter_column('retrain_events', col,
                        existing_type=sa.String(length=32),
                        type_=sa.String(length=64),
                        existing_nullable=True)


def downgrade() -> None:
    for col in _COLUMNS:
        op.alter_column('retrain_events', col,
                        existing_type=sa.String(length=64),
                        type_=sa.String(length=32),
                        existing_nullable=True)
