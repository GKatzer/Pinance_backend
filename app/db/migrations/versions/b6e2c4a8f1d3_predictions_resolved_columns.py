"""predictions: materialize resolved outcome (actual_price/actual_r/hit/sim_return)

f3c7a9e2b1d4 dropped these same columns in favor of join-on-read, when the
table held test data. At production scale (~1M+ rows, 10 read-sites doing
`predictions JOIN candles c1 JOIN candles c0` each) that join-on-read became
the single biggest cost in the API — a live /summary "all" window was
observed running 20+ minutes and pinning the DB connection pool, starving
the very scheduler job meant to keep it cached (see app/scheduler/jobs.py).

Old data doesn't change once a prediction matures (target_ts candle closed,
outcome fixed forever) — new data trickles in a few dozen rows/minute. That's
exactly the case for write-once-at-maturity instead of join-every-read:
app.ingest.actualizer already computes actual_price/actual_r/hit for every
row that matures (to publish the live result feed) — this just keeps that
computation instead of throwing it away. sim_return is new (didn't exist
before f3c7a9e2b1d4), needed for the sharpe calc in metrics._window_metrics.

Nullable, no default — unresolved (not yet matured) rows simply have
actual_price IS NULL, same meaning as "no join match" before. Existing rows
are backfilled once by app.backfill.resolve_predictions (batched, run
separately — not part of this migration, would hold the whole table locked
for the exact duration we're trying to eliminate).

ix_predictions_resolved is a partial index (WHERE actual_price IS NOT NULL)
matching the (symbol, slot, target_ts range, actual_price IS NOT NULL)
pattern every rewritten read-site now uses — empty at creation time (no rows
resolved yet), so creating it here is effectively instant regardless of
table size.

Revision ID: b6e2c4a8f1d3
Revises: 73f92880f1bb
Create Date: 2026-09-11T07:40:24
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'b6e2c4a8f1d3'
down_revision: Union[str, None] = '73f92880f1bb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('predictions', sa.Column('actual_price', sa.Float(), nullable=True))
    op.add_column('predictions', sa.Column('actual_r', sa.Float(), nullable=True))
    op.add_column('predictions', sa.Column('hit', sa.Boolean(), nullable=True))
    op.add_column('predictions', sa.Column('sim_return', sa.Float(), nullable=True))

    op.create_index(
        'ix_predictions_resolved', 'predictions',
        ['symbol', 'slot', 'target_ts'],
        postgresql_where=sa.text('actual_price IS NOT NULL'),
    )


def downgrade() -> None:
    op.drop_index('ix_predictions_resolved', table_name='predictions')
    op.drop_column('predictions', 'sim_return')
    op.drop_column('predictions', 'hit')
    op.drop_column('predictions', 'actual_r')
    op.drop_column('predictions', 'actual_price')
