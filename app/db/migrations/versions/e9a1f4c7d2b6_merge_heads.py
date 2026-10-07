"""merge heads

Revision ID: e9a1f4c7d2b6
Revises: d4e8f1a72b9c, d5bf66f7974f
Create Date: 2026-07-25 17:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'e9a1f4c7d2b6'
down_revision: Union[str, None] = ('d4e8f1a72b9c', 'd5bf66f7974f')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    pass
