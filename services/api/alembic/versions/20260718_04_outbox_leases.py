"""Add durable leasing and retry state to Temporal approval delivery.

Revision ID: 20260718_04
Revises: 20260718_03
Create Date: 2026-07-18
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260718_04"
down_revision = "20260718_03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Make approval delivery recoverable across API crashes and replica races."""

    op.add_column(
        "temporal_outbox",
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "temporal_outbox",
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.add_column("temporal_outbox", sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True))
    op.add_column("temporal_outbox", sa.Column("last_error", sa.Text(), nullable=True))
    op.create_index(
        "temporal_outbox_pending_delivery",
        "temporal_outbox",
        ["next_attempt_at", "lease_until"],
        postgresql_where=sa.text("delivered_at IS NULL"),
    )


def downgrade() -> None:
    """Refuse destructive schema rollback; restore through a compatible application release."""

    raise RuntimeError("Cogito supervisor migrations are forward-only and cannot be downgraded")
