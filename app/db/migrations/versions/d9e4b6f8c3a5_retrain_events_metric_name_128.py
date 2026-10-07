"""retrain_events: widen metric_name to 128

Follow-up to c8d3a5e7b2f4 (which widened it to 64). Schema-change events
sent by Pinance_ML promote_new_scheme.py carry a longer metric_name, so it
gets its own, larger limit. candidate_version/production_version stay at 64.
Metadata-only change in Postgres, safe on a live table.

Revision ID: d9e4b6f8c3a5
Revises: c8d3a5e7b2f4
Create Date: 2026-10-02T14:00:00
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'd9e4b6f8c3a5'
down_revision: Union[str, None] = 'c8d3a5e7b2f4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column('retrain_events', 'metric_name',
                    existing_type=sa.String(length=64),
                    type_=sa.String(length=128),
                    existing_nullable=True)


def downgrade() -> None:
    op.alter_column('retrain_events', 'metric_name',
                    existing_type=sa.String(length=128),
                    type_=sa.String(length=64),
                    existing_nullable=True)
